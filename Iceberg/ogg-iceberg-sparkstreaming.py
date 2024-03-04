import sys
import time

from pyspark.sql import SparkSession
import getopt
from pyspark.sql.functions import col, from_json, schema_of_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from urllib.parse import urlparse
import boto3
import json


def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

class CDCProcessUtil:
    def __init__(self,
                 spark,
                 region,
                 tableconffile,
                 logger,
                 jobname,
                 databasename):
        self.region = region
        self.spark = spark
        self.tableconffile = tableconffile
        self.logger = logger
        self.jobname = jobname
        #table.json中的配置
        self.tables_ds = self._load_tables_config(region, tableconffile)
        #icebergdb
        self.config = {
            "database_name": databasename,
        }

    def _writeJobLogger(self, logs):
        self.logger.info(self.jobname + " [CUSTOM-LOG]:{0}".format(logs))

    def _load_tables_config(self, aws_region, config_s3_path):
        o = urlparse(config_s3_path, allow_fragments=False)
        client = boto3.client('s3', region_name=aws_region)
        data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
        file_content = data['Body'].read().decode("utf-8")
        json_content = json.loads(file_content)
        return json_content

    def processBatch(self, data_frame_batch, batchId):
        if data_frame_batch.count() > 0:

            data_frame = data_frame_batch.cache()

            schema = StructType([
                StructField("table", StringType(), True),
                StructField("op_type", StringType(), True),
                StructField("op_ts", StringType(), True),
                StructField("current_ts", StringType(), True),
                StructField("pos", StringType(), True),
                StructField("before", StringType(), True),
                StructField("after", StringType(), True)
            ])

            self._writeJobLogger("## Source Data from Kafka Batch\r\n + " + getShowString(data_frame, truncate=False))

            dataJsonDF = data_frame.select(from_json(col("value").cast("string"), schema).alias("data")).select(col("data.*"))
            self._writeJobLogger("## Create DataFrame \r\n" + getShowString(dataJsonDF, truncate=False))

            dataInsert = dataJsonDF.filter("op_type in ('r','c','I') and after is not null")
            # 过滤 区分 insert upsert delete
            dataUpsert = dataJsonDF.filter("op_type in ('u','U') and after is not null")

            dataDelete = dataJsonDF.filter("op_type in ('d','D') and before is not null")

            if dataInsert.count() > 0:
                tableName = dataInsert.select("table").first()[0].lower().split('.')[-1]
                self._writeJobLogger("Insert Table [%],Counts[%]".format(tableName, str(dataInsert.count())))
                #获取after
                dataDF = dataInsert.select(col("after"))
                datajson = dataDF.select('after').first()
                schemadata = schema_of_json(datajson[0])
                self._writeJobLogger("############  Insert Into-GetSchema-FirstRow:" + datajson[0])

                '''识别字段'''

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"))

                # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                self._InsertDataLake(tableName, dataDFOutput)

            if dataUpsert.count() > 0:

                tableName = dataUpsert.select("table").first()[0].lower().split('.')[-1]

                self._writeJobLogger("Upsert Table [%],Counts[%]".format(tableName, str(dataUpsert.count())))

                #获取after
                dataDF = dataUpsert.select(col("after"), col("op_ts"))
                datajson = dataDF.select('after').first()
                schemadata = schema_of_json(datajson[0])
                self._writeJobLogger("MERGE INTO Table [" + tableName + "]\r\n" + getShowString(dataDF, truncate=False))
                database_name = self.config["database_name"]

                ##由于merge into schema顺序的问题，这里schema从表中获取
                refreshtable = True
                if refreshtable:
                    self.spark.sql(f"REFRESH TABLE hive_prod.{database_name}.{tableName}")
                    self._writeJobLogger("Refresh table - True")

                schemadata = self.spark.table(f"hive_prod.{database_name}.{tableName}").schema
                print(schemadata)
                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemadata).alias("DFADD"), col("op_ts")).select(col("DFADD.*"), to_timestamp(col("op_ts")).alias('op_ts'))

                self._writeJobLogger("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput, truncate=False))
                self._MergeIntoDataLake(tableName, dataDFOutput, batchId)


            if dataDelete.count() > 0:
                tableName = dataDelete.select("table").first()[0].lower().split('.')[-1]
                self._writeJobLogger("Delete Table [%],Counts[%]".format(tableName, str(dataDelete.count())))
                dataDF = dataDelete.select(col("before"))
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"), schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                self._DeleteDataFromDataLake(tableName, dataDFOutput, batchId)
    def _InsertDataLake(self, tableName, dataFrame):

        database_name = self.config["database_name"]
        # partition as id
        ###如果表不存在，创建一个空表
        '''
        如果表不存在，新建。解决在 writeto 的时候，空表没有字段的问题。
        write.spark.accept-any-schema 用于在写入 DataFrame 时，Spark可以自适应字段。
        format-version 使用iceberg v2版本
        '''
        format_version = "2"
        write_merge_mode = "copy-on-write"
        write_update_mode = "copy-on-write"
        write_delete_mode = "copy-on-write"
        timestamp_fields = ""

        for item in self.tables_ds:
            if item['db'] == database_name and item['table'] == tableName:
                format_version = item['format-version']
                write_merge_mode = item['write.merge.mode']
                write_update_mode = item['write.update.mode']
                write_delete_mode = item['write.delete.mode']
                if 'timestamp.fields' in item:
                    timestamp_fields = item['timestamp.fields']
        #根据配置文件将Timestamp字段类型转换
        if timestamp_fields != "":
            for cols in dataFrame.schema:
                if cols.name in timestamp_fields:
                    dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                    self._writeJobLogger("Covert time type-Column:" + cols.name)

        #dyDataFrame = dataFrame.repartition(4, col("id"))

        creattbsql = f"""CREATE TABLE IF NOT EXISTS hive_prod.{database_name}.{tableName}
              USING iceberg
              TBLPROPERTIES ('write.distribution-mode'='hash',
              'format-version'='{format_version}',
              'write.merge.mode'='{write_merge_mode}',
              'write.update.mode'='{write_update_mode}',
              'write.delete.mode'='{write_delete_mode}',
              'write.metadata.delete-after-commit.enabled'='true',
              'write.metadata.previous-versions-max'='10',
              'write.spark.accept-any-schema'='true')"""

        self._writeJobLogger( "####### IF table not exists, create it:" + creattbsql)
        self.spark.sql(creattbsql)

        dataFrame.writeTo(f"hive_prod.{database_name}.{tableName}") \
            .option("merge-schema", "true") \
            .option("check-ordering", "false").append()

    def _MergeIntoDataLake(self, tableName, dataFrame, batchId):

        database_name = self.config["database_name"]
        primary_key = 'ID'
        timestamp_fields = ''
        precombine_key = ''
        for item in self.tables_ds:
            if item['db'] == database_name and item['table'] == tableName:
                if 'primary_key' in item:
                    primary_key = item['primary_key']
                if 'precombine_key' in item:# 控制一批数据中对数据做了多次修改的情况，取最新的一条记录
                    precombine_key = item['precombine_key']
                if 'timestamp.fields' in item:
                    timestamp_fields = item['timestamp.fields']


        # dataMergeFrame = spark.range(1)
        if timestamp_fields != '':
            ##Timestamp字段转换
            for cols in dataFrame.schema:
                if cols.name in timestamp_fields:
                    dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                    self._writeJobLogger("Covert time type-Column:" + cols.name)

        # columns = ''
        # # 组合 cols
        # for cols in dataFrame.schema:
        #     columns = "{}{},".format(columns, cols)



        self._writeJobLogger("############  TEMP TABLE batch {}  ############### {}\r\n".format(str(batchId),
                                                                                                getShowString(dataFrame, truncate=False)))
        t = time.time()  # 当前时间
        ts = (int(round(t * 1000000)))  # 微秒级时间戳
        #将更新数据转为临时视图
        TempTable = "tmp_" + tableName + "_u_" + str(batchId) + "_" + str(ts)
        dataFrame.createOrReplaceGlobalTempView(TempTable)

        MergeTempTable = ''

        ##dataFrame.sparkSession.sql(f"REFRESH TABLE {TempTable}")
        # 修改为全局试图OK，为什么？[待解决]
        #这段代码的目的是将临时表中的数据合并到目标表中，如果数据在目标表中已存在（通过主键匹配），则更新对应字段；如果数据在目标表中不存在，则插入整行数据
        if precombine_key == '':
            query = f"""MERGE INTO hive_prod.{database_name}.{tableName} t USING (SELECT * FROM global_temp.{TempTable}) u
                ON t.{primary_key} = u.{primary_key}
                    WHEN MATCHED THEN UPDATE
                        SET *
                    WHEN NOT MATCHED THEN INSERT * """
        else:
            ## 排除重复数据生成新的 DF


            queryTemp = f"""
                SELECT a.* FROM global_temp.{TempTable} a join 
                (SELECT {primary_key},{precombine_key},
                    row_number() over(PARTITION BY {primary_key} ORDER BY op_ts DESC) AS rank 
                    FROM global_temp.{TempTable}) b 
                        ON a.{primary_key} = b.{primary_key} and 
                           a.{precombine_key} = b.{precombine_key} and 
                           b.rank = 1
            """
            self.logger.info("####### Execute SQL({}):{}".format(TempTable, queryTemp))
            tmpDF = self.spark.sql(queryTemp)

            # 移除字段 op_ts
            mergeDF = tmpDF.drop("op_ts")
            MergeTempTable = "tmp_merge_" + tableName + "_u_" + str(batchId) + "_" + str(ts)

            self._writeJobLogger(f"############ MERGE TEMP TABLE {MergeTempTable} ############### \r\n" + getShowString(mergeDF, truncate=False))

            mergeDF.createOrReplaceGlobalTempView(MergeTempTable)
            query = f"""MERGE INTO hive_prod.{database_name}.{tableName} t USING 
                (SELECT * FROM global_temp.{MergeTempTable}) u 
                  ON t.{primary_key} = u.{primary_key}
                     WHEN MATCHED THEN UPDATE
                         SET *
                     WHEN NOT MATCHED THEN INSERT * """
            # query = f"""MERGE INTO hive_prod.{database_name}.{tableName} t USING
            #     (SELECT a.* FROM global_temp.{TempTable} a join (SELECT {primary_key},{precombine_key},
            #         row_number() over(PARTITION BY {primary_key},{precombine_key} ORDER BY {precombine_key} DESC) AS rank
            #         from global_temp.{TempTable}) b on
            #         a.{primary_key} = b.{primary_key} and a.{precombine_key} = b.{precombine_key} and rank = 1) u
            #     ON t.{primary_key} = u.{primary_key}
            #         WHEN MATCHED THEN UPDATE
            #             SET *
            #         WHEN NOT MATCHED THEN INSERT * """

        self.logger.info("####### Execute SQL:" + query)
        try:
            self.spark.sql(query)
        except Exception as err:
            self.logger.error("Error of MERGE INTO")
            self.logger.error(err)
            pass
        self.spark.catalog.dropGlobalTempView(TempTable)
        if MergeTempTable != '':
            self.spark.catalog.dropGlobalTempView(MergeTempTable)

    def _DeleteDataFromDataLake(self, tableName, dataFrame, batchId):

        database_name = self.config["database_name"]
        primary_key = 'ID'
        for item in self.tables_ds:
            if item['db'] == database_name and item['table'] == tableName:
                primary_key = item['primary_key']

        database_name = self.config["database_name"]
        t = time.time()  # 当前时间
        ts = (int(round(t * 1000000)))  # 微秒级时间戳
        TempTable = "tmp_" + tableName + "_d_" + str(batchId) + "_" + str(ts)
        dataFrame.createOrReplaceGlobalTempView(TempTable)
        query = f"""DELETE FROM hive_prod.{database_name}.{tableName} AS t1
             where EXISTS (SELECT {primary_key} FROM global_temp.{TempTable} WHERE t1.{primary_key} = {primary_key})"""
        try:
            self.spark.sql(query)
        except Exception as err:
            self.logger.error("Error of DELETE")
            self.logger.error(err)
            pass
        self.spark.catalog.dropGlobalTempView(TempTable)

'''
Kafka（MSK Serverless） -EMR Serverless -> Iceberg -> S3
通过消费 MSK/MSK Serverless 的数据，写S3（Iceberg）。多表，支持I U D

1. 支持多表，通过MSK Connect 将数据库的数据CDC到MSK后，使用 [topics] 配置参数，可以接入多个topic的数据。
2. 支持MSK Serverless IAM认证
3. 提交参数说明
    (1). starting_offsets_of_kafka_topic: 'latest', 'earliest'
    (2). topics: 消费的Topic名称，如果消费多个topic，之间使用逗号分割（,）,例如 kafka1.db1.topica,kafka1.db2.topicb
    (3). icebergdb: 数据写入的iceberg database名称
    (4). warehouse: iceberg warehouse path
    (5). tablejsonfile: 记录对表需要做特殊处理的配置，例如设置表的primary key，时间字段，iceberg的针对性属性配置
    (6). mskconnect: MSK Connect 名称，用以获取MSK Serverless的数据
    (7). checkpointpath: 记录Spark streaming的Checkpoint的地址
    (8). region: 例如 us-east-1
    (9). kafkaserver: MSK 的 boostrap server
4. 只有在spark3.3版本中，才能支持iceberg的schame自适应。
5. MSK Serverless 认证只支持IAM，因此在Kafka连接的时候需要包含IAM认证相关的代码。
'''



JOB_NAME = "cdc-kafka-iceberg"
## Init
if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                               "j:o:t:d:w:f:r:k:c:",
                               ["jobname=",
                                "starting_offsets_of_kafka_topic=",
                                "topics=",
                                "icebergdb=",
                                "warehouse=",
                                "tablejsonfile=",
                                "region=",
                                "kafkaserver=",
                                "checkpointpath="])
    for opt_name, opt_value in opts:
        if opt_name in ('-o', '--starting_offsets_of_kafka_topic'):
            STARTING_OFFSETS_OF_KAFKA_TOPIC = opt_value
            print("STARTING_OFFSETS_OF_KAFKA_TOPIC:" + STARTING_OFFSETS_OF_KAFKA_TOPIC)
        elif opt_name in ('-j', '--jobname'):
            JOB_NAME = opt_value
            print("JOB_NAME:" + JOB_NAME)
        elif opt_name in ('-t', '--topics'):
            TOPICS = opt_value.replace('"', '')
            print("TOPICS:" + TOPICS)
        elif opt_name in ('-d', '--icebergdb'):
            DATABASE_NAME = opt_value
            print("DATABASE_NAME:" + DATABASE_NAME)
        elif opt_name in ('-w', '--warehouse'):
            WAREHOUSE = opt_value
            print("WAREHOUSE:" + WAREHOUSE)
        elif opt_name in ('-f', '--tablejsonfile'):
            TABLECONFFILE = opt_value
            print("TABLECONFFILE:" + TABLECONFFILE)
        elif opt_name in ('-r', '--region'):
            REGION = opt_value
            print("REGION:" + REGION)
        elif opt_name in ('-k', '--kafkaserver'):
            KAFKA_BOOSTRAPSERVER = opt_value
            print("KAFKA_BOOSTRAPSERVER:" + KAFKA_BOOSTRAPSERVER)
        elif opt_name in ('-c', '--checkpointpath'):
            CHECKPOINT_LOCATION = opt_value
            print("CHECKPOINT_LOCATION:" + CHECKPOINT_LOCATION)
        else:
            print("need parameters [starting_offsets_of_kafka_topic,topics,icebergdb etc.]")
            exit()
else:
    print("Job failed. Please provided params STARTING_OFFSETS_OF_KAFKA_TOPIC,TOPICS .etc ")
    sys.exit(1)

config = {
    "database_name": DATABASE_NAME,
}

checkpointpath = CHECKPOINT_LOCATION + "/" + JOB_NAME + "/checkpoint/" + "20240229" + "/"

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hive_prod.warehouse", WAREHOUSE) \
    .config("spark.sql.catalog.hive_prod.type", "hive") \
    .config("spark.sql.catalog.hive_prod.uri", "thrift://ip-10-120-141-27.ap-southeast-1.compute.internal:9083") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.sql.catalog.hive_prod.iceberg.handle-timestamp-without-timezone", True) \
    .config("spark.sql.session.timeZone", "UTC+8") \
    .getOrCreate()

sc = spark.sparkContext
log4j = sc._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOSTRAPSERVER,
    "subscribe": TOPICS,
    "kafka.consumer.commit.groupid": "group-" + JOB_NAME,
    "inferSchema": "true",
    "classification": "json",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": 10000,
    "max.partition.fetch.bytes": 10485760,
    "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
    # "kafka.security.protocol": "SASL_SSL",
    # "kafka.sasl.mechanism": "AWS_MSK_IAM",
    # "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    # "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}


def writeJobLogger(logs):
    logger.info(JOB_NAME + " [CUSTOM-LOG]:{0}".format(logs))

def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def load_tables_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    file_content = data['Body'].read().decode("utf-8")
    json_content = json.loads(file_content)
    return json_content


tables_ds = load_tables_config(REGION, TABLECONFFILE)


#从kafka获取数据
reader = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_options)

if STARTING_OFFSETS_OF_KAFKA_TOPIC == "earliest" or STARTING_OFFSETS_OF_KAFKA_TOPIC == "latest":
    reader.option("startingOffsets", STARTING_OFFSETS_OF_KAFKA_TOPIC)
else:
    reader.option("startingTimestamp", STARTING_OFFSETS_OF_KAFKA_TOPIC)

kafka_data = reader.load()

source_data = kafka_data.selectExpr("CAST(value AS STRING)")

process = CDCProcessUtil(spark,
                         REGION,
                         TABLECONFFILE,
                         logger,
                         JOB_NAME,
                         DATABASE_NAME)

source_data \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(process.processBatch) \
    .option("checkpointLocation", checkpointpath) \
    .start() \
    .awaitTermination()

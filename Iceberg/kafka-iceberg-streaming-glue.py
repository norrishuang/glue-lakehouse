import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
import json
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from urllib.parse import urlparse
import boto3

'''
Glue -> Kafka -> Iceberg -> S3
通过 Glue 消费 MSK/MSK Serverless 的数据，写S3（Iceberg）。多表，支持I U D

1. 支持多表，通过MSK Connect 将数据库的数据CDC到MSK后，使用 [topics] 配置参数，可以接入多个topic的数据。
2. 支持MSK Serverless IAM认证，需要提前在Glue Connection配置MSK的connect。MSK Connect 配置在私有子网中，私有子网配置NAT访问公网
3. Job 参数说明
    (1). starting_offsets_of_kafka_topic: 'latest', 'earliest'
    (2). topics: 消费的Topic名称，如果消费多个topic，之间使用逗号分割（,）,例如 kafka1.db1.topica,kafka1.db2.topicb
    (3). icebergdb: 数据写入的iceberg database名称
    (4). warehouse: iceberg warehouse path
    (5). datalake-formats: iceberg 指定使用哪一种datalake技术，包括iceberg hudi deltalake
    (6). mskconnect: MSK Connect 名称，用以获取MSK Serverless的数据
    (7). user-jars-first: True 目前Glue 集成 iceberg 必须设定的参数。
4. Glue 需要使用 4.0引擎，4.0 支持 spark 3.3，只有在spark3.3版本中，才能支持iceberg的schame自适应。
5. MSK Serverless 认证只支持IAM，因此在Kafka连接的时候需要包含IAM认证相关的代码。
'''

'''
读去表配置文件
'''
def load_tables_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    file_content = data['Body'].read().decode("utf-8")
    json_content = json.loads(file_content)
    return json_content


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'starting_offsets_of_kafka_topic',
                                     'topics',
                                     'icebergdb',
                                     'warehouse',
                                     'mskconnect',
                                     'tablejsonfile',
                                     'region'])

'''
获取Glue Job参数
'''
STARTING_OFFSETS_OF_KAFKA_TOPIC = args.get('starting_offsets_of_kafka_topic', 'latest')
TOPICS = args.get('topics')
ICEBERG_DB = args.get('icebergdb')
WAREHOUSE = args.get('warehouse')
KAFKA_CONNECT = args.get('mskconnect')
TABLECONFFILE = args.get('tablejsonfile')
REGION = args.get('region')

config = {
    "database_name": ICEBERG_DB,
    "warehouse": WAREHOUSE
}

tables_ds = load_tables_config(REGION, TABLECONFFILE)


spark = SparkSession.builder \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", config['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")\
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", True)\
    .getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

logger.info("Init...")

logger.info("starting_offsets_of_kafka_topic:" + STARTING_OFFSETS_OF_KAFKA_TOPIC)
logger.info("topics:" + TOPICS)
logger.info("icebergdb:" + ICEBERG_DB)
logger.info("warehouse:" + WAREHOUSE)
logger.info("mskconnect:" + KAFKA_CONNECT)
logger.info("table-config-file:" + TABLECONFFILE)



def writeJobLogger(logs):
    logger.info(args['JOB_NAME'] + " [CUSTOM-LOG]:{0}".format(logs))

### Check Parameter
if TABLECONFFILE == '':
    logger.info("Need Parameter [table-config-file]")
    sys.exit(1)
elif KAFKA_CONNECT == '':
    logger.info("Need Parameter [mskconnect]")
    sys.exit(1)
elif WAREHOUSE == '':
    logger.info("Need Parameter [warehouse]")
    sys.exit(1)

checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + "20230409-02" + "/"

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        schema = StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])

        dataJsonDF = data_frame.select(from_json(col("$json$data_infer_schema$_temporary$").cast("string"), schema).alias("data")).select(col("data.*"))
        writeJobLogger("############  Create DataFrame  ############### \r\n" + getShowString(dataJsonDF,truncate = False))

        '''
        由于Iceberg没有主键，需要通过SQL来处理upsert的场景，需要识别CDC log中的 I/U/D 分别逻辑处理
        '''
        dataInsert = dataJsonDF.filter("op in ('r','c') and after is not null")
        # 过滤 区分 insert upsert delete
        dataUpsert = dataJsonDF.filter("op in ('u') and after is not null")

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataInsert.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataInsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            datatables = dataInsert.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowtables = datatables.collect()

            for cols in rowtables:
                tableName = cols[1]
                dataDF = dataInsert.select(col("after"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                datajson = dataDF.select('after').first()
                schemadata = schema_of_json(datajson[0])
                writeJobLogger("############  Insert Into-GetSchema-FirstRow:" + datajson[0])

                '''识别时间字段'''

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"), current_timestamp().alias("ts"))
                # writeJobLogger("############ dataDFOutput Test Timestamp convert:" + getShowString(dataDFOutput, truncate=False))
                # dataDFOutput.printSchema()
                ### 对时间字段UTC 强制转换
                #df2.withColumn("eventTime1", unix_timestamp($"eventTime", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
                for cols in dataDFOutput.schema:
                    if cols.name in ['created_at', 'updated_at']:
                        dataDFOutput = dataDFOutput.withColumn(cols.name, to_timestamp(col(cols.name)))
                        writeJobLogger("Covert time type-Column:" + cols.name)
                # dataDFOutput.printSchema()
                # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InsertDataLake(tableName, dataDFOutput)

        if(dataUpsert.count() > 0):
            #### 分离一个topics多表的问题。
            sourcejson = dataUpsert.select('source').first()
            schemasource = schema_of_json(sourcejson[0])

            # 获取多表
            datatables = dataUpsert.select(from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowtables = datatables.collect()

            for cols in rowtables:
                tableName = cols[1]
                dataDF = dataUpsert.select(col("after"),
                                           from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")

                ##由于merge into schema顺序的问题，这里schema从表中获取（顺序问题待解决）
                database_name = config["database_name"]
                schemaData = spark.table(f"glue_catalog.{database_name}.{tableName}").schema

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemaData).alias("DFADD")).select(col("DFADD.*"), current_timestamp().alias("ts"))

                ## 将时间字同步到UTC
                for cols in dataDFOutput.schema:
                    if cols.name in ['created_at', 'updated_at']:
                        dataDFOutput = dataDFOutput.withColumn(cols.name, to_timestamp(col(cols.name)))
                        writeJobLogger("Covert time type-Column:" + cols.name)

                writeJobLogger("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput, truncate = False))
                MergeIntoDataLake(tableName, dataDFOutput)


        if(dataDelete.count() > 0):
            sourceJson = dataDelete.select('source').first()

            schemaSource = schema_of_json(sourceJson[0])
            dataTables = dataDelete.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()

            rowTables = dataTables.collect()
            for cols in rowTables:
                tableName = cols[1]
                dataDF = dataDelete.select(col("before"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"), schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                DeleteDataFromDataLake(tableName, dataDFOutput)

def InsertDataLake(tableName,dataFrame):

    database_name = config["database_name"]
    # partition as id
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF().repartition(4, col("id"))

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

    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            format_version = item['format-version']
            write_merge_mode = item['write.merge.mode']
            write_update_mode = item['write.update.mode']
            write_delete_mode = item['write.delete.mode']


    creattbsql = f"""CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{tableName} 
          USING iceberg 
          TBLPROPERTIES ('write.distribution-mode'='hash',
          'format-version'='{format_version}',
          'write.merge.mode'='{write_merge_mode}',
          'write.update.mode'='{write_update_mode}',
          'write.delete.mode'='{write_delete_mode}',
          'write.metadata.delete-after-commit.enabled'='true',
          'write.metadata.previous-versions-max'='10',
          'write.spark.accept-any-schema'='true')"""

    writeJobLogger("####### IF table not exists, create it:" + creattbsql)
    spark.sql(creattbsql)

    dyDataFrame.writeTo(f"glue_catalog.{database_name}.{tableName}") \
        .option("merge-schema", "true") \
        .option("check-ordering", "false").append()

def MergeIntoDataLake(tableName, dataFrame):

    # logger.info("##############  Func:MergeIntoDataLake [ "+ tableName +  "] ############# \r\n"
    #             + getShowString(dataFrame,truncate = False))
    database_name = config["database_name"]
    # table_name = tableIndexs[tableName]
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF()

    primary_key = 'ID'
    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']

    TempTable = "tmp_" + tableName + "_upsert"
    dyDataFrame.createOrReplaceTempView(TempTable)

    query = f"""MERGE INTO glue_catalog.{database_name}.{tableName} t USING (SELECT * FROM {TempTable}) u ON t.{primary_key} = u.{primary_key}
            WHEN MATCHED THEN UPDATE
                SET *
            WHEN NOT MATCHED THEN INSERT * """
    logger.info("####### Execute SQL:" + query)
    spark.sql(query)

def DeleteDataFromDataLake(tableName, dataFrame):
    database_name = config["database_name"]
    primary_key = 'ID'
    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']

    database_name = config["database_name"]
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF()
    dyDataFrame.createOrReplaceTempView("tmp_" + tableName + "_delete")
    query = f"""DELETE FROM glue_catalog.{database_name}.{tableName} AS t1 
         where EXISTS (SELECT {primary_key} FROM tmp_{tableName}_delete WHERE t1.{primary_key} = {primary_key})"""
    spark.sql(query)
# Script generated for node Apache Kafka
kafka_options = {
    "connectionName": KAFKA_CONNECT,
    "topicName": TOPICS,
    "inferSchema": "true",
    "classification": "json",
    "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}

# Script generated for node Apache Kafka
dataframe_ApacheKafka_source = glueContext.create_data_frame.from_options(
    connection_type="kafka",
    connection_options=kafka_options
)

glueContext.forEachBatch(frame=dataframe_ApacheKafka_source,
                         batch_function=processBatch,
                         options={
                             "windowSize": "30 seconds",
                             "recordPollingLimit": "50000",
                             "checkpointLocation": checkpoint_location,
                             "batchMaxRetries": 1
                         })

job.commit()

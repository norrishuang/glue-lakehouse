import sys
import time

from pyspark.sql import SparkSession
import getopt
from pyspark.sql.functions import col, from_json, schema_of_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from urllib.parse import urlparse
import boto3
import json

'''
MSK->EMR Serverless(Spark)->DeltaLake
'''
## @params: [JOB_NAME]

JOB_NAME = "cdc-kafka-daltalake"
## Init
if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                               "j:o:t:d:w:f:r:k:c:",
                               ["jobname=",
                                "starting_offsets_of_kafka_topic=",
                                "topics=",
                                "datbasename=",
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
        elif opt_name in ('-d', '--datbasename'):
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
            print("need parameters [starting_offsets_of_kafka_topic,topics,database name etc.]")
            exit()
else:
    print("Job failed. Please provided params STARTING_OFFSETS_OF_KAFKA_TOPIC,TOPICS .etc ")
    sys.exit(1)


checkpointpath = CHECKPOINT_LOCATION + "/" + JOB_NAME + "/checkpoint/" + "20230526" + "/"

def load_tables_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    file_content = data['Body'].read().decode("utf-8")
    json_content = json.loads(file_content)
    return json_content




tables_ds = load_tables_config(REGION, TABLECONFFILE)

##必须把 glue catalog 的设置加在代码中，不能放在外部的conf中。
spark = SparkSession.builder \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("show databases").show()


sc = spark.sparkContext
log4j = sc._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)

def writeJobLogger(logs):
    logger.info(JOB_NAME + " [CUSTOM-LOG]:{0}".format(logs))

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOSTRAPSERVER,
    "subscribe": TOPICS,
    "kafka.consumer.commit.groupid": "group-" + JOB_NAME,
    "inferSchema": "true",
    "classification": "json",
    "failOnDataLoss": "true",
    "max.partition.fetch.bytes": 10485760,
    "maxOffsetsPerTrigger": 10000,
    "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
    # "kafka.security.protocol": "SASL_SSL",
    # "kafka.sasl.mechanism": "AWS_MSK_IAM",
    # "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    # "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

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

        dataJsonDF = data_frame.select(from_json(col("value").cast("string"), schema).alias("data")).select(col("data.*"))
        writeJobLogger("############  Create DataFrame  ############### \r\n" + getShowString(dataJsonDF, truncate=False))

        dataIpsert = dataJsonDF.filter("op in ('c') and after is not null")

        dataUpdate = dataJsonDF.filter("op in ('u','r') and after is not null")

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataIpsert.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataIpsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataIpsert.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowTables = dataTables.collect()

            for cols in rowTables:
                tableName = cols[1]
                dataDF = dataIpsert.select(col("after"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').first()
                schemaData = schema_of_json(dataJson[0])
                logger.info("############  Insert Into-GetSchema-FirstRow:" + dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemaData).alias("DFADD")).select(col("DFADD.*"))
                # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InsertDataLake(tableName, dataDFOutput)

        if(dataUpdate.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataUpdate.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataUpdate.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowTables = dataTables.collect()

            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataUpdate.select(col("after"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').first()
                schemaData = schema_of_json(dataJson[0])
                logger.info("############  Insert Into-GetSchema-FirstRow:" + dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemaData).alias("DFADD")).select(col("DFADD.*"))
                # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                MergeIntoDataLake(tableName, dataDFOutput, batchId)

        if(dataDelete.count() > 0):
            # dataDelete = dataDeleteDYF.toDF()
            sourceJson = dataDelete.select('source').first()
            # schemaData = schema_of_json([rowjson[0]])

            schemaSource = schema_of_json(sourceJson[0])
            dataTables = dataDelete.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()

            # logger.info("############  Auto Schema Recognize  ############### \r\n" + getShowString(dataDelete,truncate = False))

            rowTables = dataTables.collect()
            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataDelete.select(col("before"),
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE"))\
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"), schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                # logger.info("############  DELETE FROM  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                MergeIntoDataLake(tableName, dataDFOutput, batchId)
                writeJobLogger("Add {0} records in Table:{1}".format(dataDFOutput.count(), tableName))

def InsertDataLake(tableName, dataFrame):

    database_name = DATABASE_NAME
    logger.info("##############  Func:InputDataLake [ "+ tableName +  "] ############# \r\n"
                + getShowString(dataFrame, truncate = False))
    target = "{0}/{1}/{2}".format(WAREHOUSE, database_name, tableName)

    ##如果表不存在，创建一张表
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {database_name}.{tableName} 
        USING delta location
        '{target}'""")

    dataFrame.writeTo(f"""{database_name}.{tableName}""")\
        .option("mergeSchema", "true")\
        .append()

def MergeIntoDataLake(tableName, dataFrame, batchId):

    database_name = DATABASE_NAME
    primary_key = 'ID'
    timestamp_fields = ''
    precombine_key = ''
    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            if 'primary_key' in item:
                primary_key = item['primary_key']
            if 'precombine_key' in item:# 控制一批数据中对数据做了多次修改的情况，取最新的一条记录
                precombine_key = item['precombine_key']
            if 'timestamp.fields' in item:
                timestamp_fields = item['timestamp.fields']

    '''针对 op = r 的情况'''
    target = "{0}/{1}/{2}".format(WAREHOUSE, database_name, tableName)
    ##如果表不存在，创建一张表
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {database_name}.{tableName} 
        USING delta location
        '{target}'""")

    # dataMergeFrame = spark.range(1)
    if timestamp_fields != '':
        ##Timestamp字段转换
        for cols in dataFrame.schema:
            if cols.name in timestamp_fields:
                dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                writeJobLogger("Covert time type-Column:" + cols.name)

    writeJobLogger("############  TEMP TABLE batch {}  ############### \r\n".format(str(batchId)) + getShowString(dataFrame, truncate=False))
    t = time.time()  # 当前时间
    ts = (int(round(t * 1000000)))  # 微秒级时间戳
    TempTable = "tmp_" + tableName + "_u_" + str(batchId) + "_" + str(ts)
    dataFrame.createOrReplaceGlobalTempView(TempTable)

    ##dataFrame.sparkSession.sql(f"REFRESH TABLE {TempTable}")
    # 修改为全局试图OK，为什么？[待解决]
    if precombine_key == '':
        query = f"""MERGE INTO {database_name}.{tableName} t USING (SELECT * FROM global_temp.{TempTable}) u
            ON t.{primary_key} = u.{primary_key}
                WHEN MATCHED THEN UPDATE
                    SET *
                WHEN NOT MATCHED THEN INSERT * """
    else:
        query = f"""MERGE INTO {database_name}.{tableName} t USING 
        (SELECT a.* FROM global_temp.{TempTable} a join (SELECT {primary_key},max({precombine_key}) as {precombine_key} from global_temp.{TempTable} group by {primary_key}) b on
            a.{primary_key} = b.{primary_key} and a.{precombine_key} = b.{precombine_key}) u
            ON t.{primary_key} = u.{primary_key}
                WHEN MATCHED THEN UPDATE
                    SET *
                WHEN NOT MATCHED THEN INSERT * """

    logger.info("####### Execute SQL:" + query)
    try:
        spark.sql(query)
    except Exception as err:
        logger.error("Error of MERGE INTO")
        logger.error(err)
        pass
    spark.catalog.dropGlobalTempView(TempTable)

def DeleteDataFromDataLake(tableName, dataFrame, batchId):

    database_name = DATABASE_NAME
    primary_key = 'ID'
    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']

    database_name = DATABASE_NAME
    t = time.time()  # 当前时间
    ts = (int(round(t * 1000000)))  # 微秒级时间戳
    TempTable = "tmp_" + tableName + "_d_" + str(batchId) + "_" + str(ts)
    dataFrame.createOrReplaceGlobalTempView(TempTable)
    query = f"""DELETE FROM {database_name}.{tableName} AS t1 
         where EXISTS (SELECT {primary_key} FROM global_temp.{TempTable} WHERE t1.{primary_key} = {primary_key})"""
    try:
        spark.sql(query)
    except Exception as err:
        logger.error("Error of DELETE")
        logger.error(err)
        pass
    spark.catalog.dropGlobalTempView(TempTable)


# Script generated for node Apache Kafka
source_data \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(processBatch) \
    .option("checkpointLocation", checkpointpath) \
    .start() \
    .awaitTermination()


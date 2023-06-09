import sys
from awsglue.transforms import *
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext

from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

from urllib.parse import urlparse
import boto3
import json

'''
Glue DelteLake Test
通过 Glue 消费Kafka的数据，写S3（DeltaLake）。多表，支持I U D
'''
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

def load_tables_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    file_content = data['Body'].read().decode("utf-8")
    json_content = json.loads(file_content)
    return json_content


args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'starting_offsets_of_kafka_topic',
                                     'topics',
                                     'datbasename',
                                     'warehouse',
                                     'mskconnect',
                                     'tablejsonfile',
                                     'region'])

'''
获取Glue Job参数
'''
STARTING_OFFSETS_OF_KAFKA_TOPIC = args.get('starting_offsets_of_kafka_topic')
TOPICS = args.get('topics')
KAFKA_CONNECT = args.get('mskconnect')
TABLECONFFILE = args.get('tablejsonfile')
REGION = args.get('region')
DATABASE_NAME = args.get('datbasename')
WAREHOUSE = args.get('warehouse')

config = {
    "database_name": DATABASE_NAME,
}

tables_ds = load_tables_config(REGION, TABLECONFFILE)

spark = SparkSession.builder.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.databricks.delta.schema.autoMerge.enabled', 'true') \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
glueClient = boto3.client('glue')
logger.info('Initialization.')

def writeJobLogger(logs):
    logger.info(args['JOB_NAME'] + " [CUSTOM-LOG]:{0}".format(logs))

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d%")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + "checkpoint-01" + "/"

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def processBatch(data_frame,batchId):
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
        logger.info("############  Create DataFrame  ############### \r\n" + getShowString(dataJsonDF, truncate=False))

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
                MergeIntoDataLake(tableName, dataDFOutput)

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
                MergeIntoDataLake(tableName, dataDFOutput)
                writeJobLogger("Add {0} records in Table:{1}".format(dataDFOutput.count(), tableName))

def InsertDataLake(tableName, dataFrame):

    database_name = config["database_name"]
    logger.info("##############  Func:InputDataLake [ "+tableName+ "] ############# \r\n"
                + getShowString(dataFrame, truncate=False))
    target = "{0}/{1}/{2}".format(WAREHOUSE, database_name, tableName)

    ##如果表不存在，创建一张表
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {database_name}.{tableName} 
        USING delta location
        '{target}'""")

    dataFrame.writeTo(f"""{database_name}.{tableName}""").append()

def MergeIntoDataLake(tableName, dataFrame):
    # dataUpsertDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
    # outputUpsert = dataUpsertDF.toDF()
    logger.info("##############  Func:InputDataLake [ " + tableName + "] ############# \r\n"
                + getShowString(dataFrame, truncate=False))
    database_name = config["database_name"]

    #default
    primary_key = 'id'

    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']

    #需要做一次转换，不然spark session获取不到
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF()
    TempTable = "tmp_" + tableName + "_upsert"
    dyDataFrame.createOrReplaceTempView(TempTable)
    ###如果表不存在，创建一个空表
    # creattbsql = f"""CREATE TABLE IF NOT EXISTS {database_name}.{tableName}
    #     USING DELTA
    #     LOCATION 's3://myemr-bucket-01/data/{database_name}/{tableName}'
    # """
    # logger.info("####### IF table not exists, create it:" + creattbsql)
    # spark.sql(creattbsql)

    query = f"""MERGE INTO {database_name}.{tableName} t USING (select * from {TempTable}) u ON t.{primary_key} = u.{primary_key}
            WHEN MATCHED THEN UPDATE
                SET *
            WHEN NOT MATCHED THEN INSERT * """
    logger.info("####### Execute SQL:" + query)
    spark.sql(query)

def DeleteDataFromDataLake(tableName, dataFrame):
    database_name = config["database_name"]
    #default
    primary_key = 'id'

    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']

    dataFrame.createOrReplaceTempView("tmp_" + tableName + "_delete")
    query = f"""DELETE FROM {database_name}.{tableName} AS t1 where EXISTS (SELECT ID FROM tmp_{tableName}_delete WHERE t1.{primary_key} = {primary_key})"""
    # {"data":{"id":1,"reward":10,"channels":"['email', 'mobile', 'social']","difficulty":"10","duration":"7","offer_type":"bogo","offer_id":"ae264e3637204a6fb9bb56bc8210ddfd"},"op":"+I"}
    spark.sql(query)


kafka_options = {
    "connectionName": KAFKA_CONNECT,
    "topicName": TOPICS,
    "inferSchema": "true",
    "classification": "json",
    "failOnDataLoss": "false",
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
                             "checkpointLocation": checkpoint_location,
                             "batchMaxRetries": 1
                         })

job.commit()

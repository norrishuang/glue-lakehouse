import sys
from awsglue.transforms import *

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
Glue Hudi Test
通过 Glue 消费Kafka的数据，写S3（Hudi）。多表，支持I U D
'''


HUDI_FORMAT = "org.apache.hudi"
config = {
    "database_name": "hudi",
    "primary_key": "id",
    "sort_key": "id",
    "commits_to_retain": "4"
}

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

tables_ds = load_tables_config(REGION, TABLECONFFILE)

spark = SparkSession.builder\
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
glueClient = boto3.client('glue')
logger.info('Initialization.')


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
        logger.info("############  Create DataFrame  ############### \r\n" + getShowString(dataJsonDF,truncate = False))

        dataUpsert = dataJsonDF.filter("op in ('c','r','u') and after is not null")
        # 过滤 区分 insert upsert delete
        # dataUpsert = dataJsonDF.filter("op in ('u') and after is not null")

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataUpsert.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataUpsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            datatables = dataUpsert.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowTables = datatables.collect()

            for cols in rowTables:
                tablename = cols[1]
                dataDF = dataUpsert.select(col("after"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tablename + "'")
                datajson = dataDF.select('after').first()
                schemaData = schema_of_json(datajson[0])
                logger.info("############  Insert Into-GetSchema-FirstRow:" + datajson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"), current_timestamp().alias("ts"))
                # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InsertDataLake(tablename, dataDFOutput)


        if(dataDelete.count() > 0):
            sourcejson = dataDelete.select('source').first()

            schemasource = schema_of_json(sourcejson[0])
            datatables = dataDelete.select(from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()

            rowtables = datatables.collect()
            for cols in rowtables:
                tablename = cols[1]
                dataDF = dataDelete.select(col("before"),
                                           from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tablename + "'")
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"),schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                InsertDataLake(tablename, dataDFOutput)

def InsertDataLake(tableName, dataFrame):

    database_name = config["database_name"]
    primary_key = 'ID'
    storage_type = 'COPY_ON_WRITE'
    sort_key = 'ID'
    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']
            sort_key = item['sort_key']
            storage_type = item['storage_type']


    # logger.info("##############  Func:InputDataLake [ " + tableName + "] ############# \r\n"
    #              + getShowString(dataFrame, truncate=False))

    target = "s3://myemr-bucket-01/data/hudi/" + tableName

    write_options={
        "hoodie.table.name": tableName,
        "className" : "org.apache.hudi",
        "hoodie.datasource.write.storage.type": storage_type,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": primary_key,
        "hoodie.datasource.write.precombine.field": sort_key,
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": database_name,
        "hoodie.datasource.hive_sync.table": tableName,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "path": target
    }

    glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(dataFrame, glueContext, "outputDF"),
                                                 connection_type="custom.spark",
                                                 connection_options=write_options)


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

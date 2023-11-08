import sys

import time
from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from awsglue.context import GlueContext
import json
from awsglue.job import Job
from urllib.parse import urlparse
import boto3
from pyspark.sql.functions import col, from_json, schema_of_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

'''
Kafka -> Redshift [Glue Streaming]
通过 Glue 消费 MSK/MSK Serverless 的数据，写 Redshift 。多表，支持I U D

1. 支持多表，通过MSK Connect 将数据库的数据CDC到MSK后，使用 [topics] 配置参数，可以接入多个topic的数据。
2. 支持MSK Serverless IAM认证，需要提前在Glue Connection配置MSK的connect。MSK Connect 配置在私有子网中，私有子网配置NAT访问公网
3. Job 参数说明
    (1). starting_offsets_of_kafka_topic: 'latest', 'earliest'
4. --additional-python-modules 引用 jproperties。
5. MSK Serverless 认证只支持IAM，因此在Kafka连接的时候需要包含IAM认证相关的代码。
6. Job parameters 
    --kafka_db->
    --kafka_tablename->
    --redshift_connect->
    --redshift_iam_role->
    --tableconffile->
    --region->
    --starting_offsets_of_kafka_topic->
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

'''
获取Glue Job参数
'''

params = [
    'JOB_NAME',
    # 'msk_connect',
    'kafka_db',
    'kafka_tablename',
    'redshift_connect',
    'redshift_iam_role',
    'tableconffile',
    'region',
    'starting_offsets_of_kafka_topic'
]

args = getResolvedOptions(sys.argv, params)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

REGION = args.get('region')
STARTING_OFFSETS_OF_KAFKA_TOPIC = args.get('starting_offsets_of_kafka_topic', 'latest')
# KAFKA_CONNECT = args.get('msk_connect')
REDSHIFT_CONNECT = args.get('redshift_connect')
# REDSHIFT_TABLE = args.get('redshift_table')
REDSHIFT_TMPDIR = args["TempDir"] + "redshift_tmpdir"
REDSHIFT_IAM_ROLE = args.get('redshift_iam_role')
KAFKA_DB = args.get('kafka_db')
KAFKA_TABLENAME = args.get('kafka_tablename')
TABLECONFFILE = args.get('tableconffile')
tables_ds = load_tables_config(REGION, TABLECONFFILE)


logger = glueContext.get_logger()

logger.info("Init...")

logger.info("starting_offsets_of_kafka_topic:" + STARTING_OFFSETS_OF_KAFKA_TOPIC)
logger.info("REDSHIFT_CONNECT:" + REDSHIFT_CONNECT)
# logger.info("MSK_CONNECT:" + KAFKA_CONNECT)
logger.info("table-config-file:" + TABLECONFFILE)


def writeJobLogger(logs):
    logger.info(args['JOB_NAME'] + "-CUSTOM-LOG:{0}".format(logs))


### Check Parameter
if TABLECONFFILE == '':
    logger.info("Need Parameter [table-config-file]")
    sys.exit(1)
elif REDSHIFT_CONNECT == '':
    logger.info("Need Parameter [redshift_connect]")
    sys.exit(1)

checkpoint_location = args["TempDir"] + args['JOB_NAME'] + "/checkpoint/"


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
        writeJobLogger("Source Data \r\n" + getShowString(data_frame, truncate=False))
        writeJobLogger('Batch {}  Record Count:{}'.format(str(batchId), str(data_frame.count())))
        dataJsonDF = data_frame.select(from_json(col("$json$data_infer_schema$_temporary$").cast("string"), schema).alias("data")).select(col("data.*"))
        logger.info("############  Create DataFrame  ############### \r\n" + getShowString(dataJsonDF, truncate=False))

        dataIpsert = dataJsonDF.filter("op in ('c','r') and after is not null")

        dataUpdate = dataJsonDF.filter("op in ('u') and after is not null")

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataIpsert.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataIpsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataIpsert.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            rowTables = dataTables.collect()

            for cols in rowTables:
                tableName = cols[1]
                writeJobLogger('TableName:' + tableName)
                dataDF = dataIpsert.select(col("after"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').first()
                schemaData = schema_of_json(dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemaData).alias("DFADD")).select(col("DFADD.*"))
                InsertDataLake(tableName, dataDFOutput)

        if(dataUpdate.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataUpdate.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataUpdate.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            rowTables = dataTables.collect()

            for cols in rowTables:
                tableName = cols[1]
                writeJobLogger('TableName:' + tableName)
                dataDF = dataUpdate.select(col("after"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').first()
                schemaData = schema_of_json(dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemaData).alias("DFADD")).select(col("DFADD.*"))
                MergeIntoDataLake(tableName, dataDFOutput)

        if(dataDelete.count() > 0):
            # dataDelete = dataDeleteDYF.toDF()
            sourceJson = dataDelete.select('source').first()
            # schemaData = schema_of_json([rowjson[0]])

            schemaSource = schema_of_json(sourceJson[0])
            dataTables = dataDelete.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()

            rowTables = dataTables.collect()
            for cols in rowTables :
                tableName = cols[1]
                writeJobLogger('TableName:' + tableName)
                dataDF = dataDelete.select(col("before"),
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"), schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                MergeIntoDataLake(tableName, dataDFOutput)
                writeJobLogger("Add {0} records in Table:{1}".format(dataDFOutput.count(), tableName))

def InsertDataLake(tableName, dataFrame):

    writeJobLogger("Func:InputDataLake [" + tableName + "] \r\n"
                + getShowString(dataFrame, truncate=False))

    redshiftWriteDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")

    AmazonRedshift_Insert = glueContext.write_dynamic_frame.from_options(
        frame=redshiftWriteDF,
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": REDSHIFT_TMPDIR,
            "useConnectionProperties": "true",
            "dbtable": "{0}.{1}".format("public", tableName),
            "connectionName": REDSHIFT_CONNECT,
            "tempformat": "CSV",
            "aws_iam_role":  REDSHIFT_IAM_ROLE
        },
        transformation_ctx="AmazonRedshift_Insert"
    )

    writeJobLogger("Insert Success. Insert Record Count [{}]".format(dataFrame.count()))

def MergeIntoDataLake(tableName, dataFrame):
    # dataUpsertDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
    # outputUpsert = dataUpsertDF.toDF()
    writeJobLogger("Func:MergeIntoDataLake [" + tableName + "]  \r\n"
                + getShowString(dataFrame, truncate=False))

    #default
    primary_key = 'ID'
    timestamp_fields = ''
    precombine_key = ''
    for item in tables_ds:
        if item['table'] == tableName:
            if 'primary_key' in item:
                primary_key = item['primary_key']
            if 'precombine_key' in item:# 控制一批数据中对数据做了多次修改的情况，取最新的一条记录
                precombine_key = item['precombine_key']
            if 'timestamp.fields' in item:
                timestamp_fields = item['timestamp.fields']


    if timestamp_fields != '':
        ##Timestamp字段转换
        for cols in dataFrame.schema:
            if cols.name in timestamp_fields:
                dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                writeJobLogger("CovertTimeColumn:" + cols.name)

    redshiftWriteDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
    schemaName = "public"
    tempTableName = "vdp_test_temp_b92724"

    if precombine_key == '':
        postactionsSql = f"""BEGIN;
                MERGE INTO {schemaName}.{tableName} t USING (SELECT * FROM {schemaName}.{tempTableName}) u
                    ON t.{primary_key} = u.{primary_key}
                        WHEN MATCHED THEN UPDATE
                            SET *
                        WHEN NOT MATCHED THEN INSERT * ;
                        DROP TABLE {schemaName}.{tempTableName}; END;"""
    else:
        postactionsSql = f"""BEGIN;
                MERGE INTO {schemaName}.{tableName} t USING 
                    (SELECT a.* FROM {schemaName}.{tempTableName} a join (SELECT {primary_key},max({precombine_key}) as {precombine_key} from {schemaName}.{tempTableName} group by {primary_key}) b on
                        a.{primary_key} = b.{primary_key} and a.{precombine_key} = b.{precombine_key}) u
                        ON t.{primary_key} = u.{primary_key}
                            WHEN MATCHED THEN UPDATE
                                SET *
                            WHEN NOT MATCHED THEN INSERT * ;
                        DROP TABLE {schemaName}.{tempTableName}; END;"""
    AmazonRedshift_MergeInto = glueContext.write_dynamic_frame.from_options(
        frame=redshiftWriteDF,
        connection_type="redshift",
        connection_options={
            "postactions": postactionsSql,
            "redshiftTmpDir": REDSHIFT_TMPDIR,
            "useConnectionProperties": "true",
            "dbtable": f"{schemaName}.{tempTableName}",
            "connectionName": REDSHIFT_CONNECT,
            "aws_iam_role":  REDSHIFT_IAM_ROLE
        },
        transformation_ctx="AmazonRedshift_MergeInto",
    )

    writeJobLogger("Upsert Success. Upsert Record Count [{}]".format(dataFrame.count()))


def DeleteDataFromDataLake(tableName, dataFrame):
    #default
    primary_key = 'id'

    for item in tables_ds:
        if item['table'] == tableName:
            primary_key = item['primary_key']
    schemaName = "public"
    tempTableName = "tmp_" + tableName + "_delete"
    postactionsSql = f"""BEGIN;
                        DELETE FROM {schemaName}.{tableName} AS t1 where EXISTS (SELECT ID FROM {tempTableName} WHERE t1.{primary_key} = {primary_key});
                        DROP TABLE {schemaName}.{tempTableName}; 
                        END; """

    redshiftWriteDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
    AmazonRedshift_MergeInto = glueContext.write_dynamic_frame.from_options(
        frame=redshiftWriteDF,
        connection_type="redshift",
        connection_options={
            "postactions": postactionsSql,
            "redshiftTmpDir": REDSHIFT_TMPDIR,
            "useConnectionProperties": "true",
            "dbtable": f"{schemaName}.{tempTableName}",
            "connectionName": REDSHIFT_CONNECT,
            "aws_iam_role":  REDSHIFT_IAM_ROLE
        },
        transformation_ctx="AmazonRedshift_MergeInto",
    )

# Script generated for node Apache Kafka
# kafka_options = {
#     "connectionName": KAFKA_CONNECT,
#     "topicName": TOPICS,
#     "inferSchema": "true",
#     "classification": "json",
#     "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
#     "kafka.security.protocol": "SASL_SSL",
#     "kafka.sasl.mechanism": "AWS_MSK_IAM",
#     "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
#     "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
# }

# Script generated for node Apache Kafka
# dataframe_ApacheKafka_source = glueContext.create_data_frame.from_options(
#     connection_type="kafka",
#     connection_options=kafka_options
# )

dataframe_ApacheKafka_source = glueContext.create_data_frame.from_catalog(
    database=KAFKA_DB,
    table_name=KAFKA_TABLENAME,
    additional_options={"startingOffsets": "earliest", "inferSchema": "true", "emitConsumerLagMetrics": "true"},
    transformation_ctx="dataframe_KafkaStream_node1"
)

glueContext.forEachBatch(frame=dataframe_ApacheKafka_source,
                         batch_function= processBatch,
                         options={
                             "windowSize": "30 seconds",
                             "checkpointLocation": checkpoint_location,
                             "batchMaxRetries": 1
                         })

job.commit()

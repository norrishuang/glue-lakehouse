import sys
import boto3
import json

from pyspark.sql.functions import col

from awsglue import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

'''
该方法实现在Glue中将MSK中的一条消息，写入到Redshift的一个字段中。
由于Glue集成的Kafka Connection会通过配置的数据格式 json，在Dynamic Frame中自动识别Json的数据结构，因此需要通过以下配置来避免这种情况的发生。
注意配置
    1. 如果是代码方式，在 Kafka option 设置以下参数：
        "inferSchema": "false"
        "classification": "none"
        "schema": "`value` STRING"
    2. 如果通过界面配置方式，或者kafka的数据源来自 Glue Data Catalog 的一张表，将以上的参数配置在Glue Data Catalog 表的 Table properties 中。
'''

params = [
    'JOB_NAME',
    'starting_offsets_of_kafka_topic',
    'topics',
    'msk_connect',
    'redshift_connect',
    'redshift_table',
    'redshift_tmpdir',
    'redshift_iam_role'
]

args = getResolvedOptions(sys.argv, params)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

STARTING_OFFSETS_OF_KAFKA_TOPIC = args.get('starting_offsets_of_kafka_topic', 'latest')
TOPICS = args.get('topics')
KAFKA_CONNECT = args.get('msk_connect')
REDSHIFT_CONNECT = args.get('redshift_connect')
REDSHIFT_TABLE = args.get('redshift_table')
REDSHIFT_TMPDIR = args.get('redshift_tmpdir')
REDSHIFT_IAM_ROLE = args.get('redshift_iam_role')

logger = glueContext.get_logger()
job_name = args['JOB_NAME']

logger.info("JOB_NAME:" + job_name)
logger.info("topics:" + TOPICS)
logger.info("KAFKA_CONNECT:" + KAFKA_CONNECT)
logger.info("REDSHIFT_TMPDIR:" + REDSHIFT_TMPDIR)
logger.info("REDSHIFT_TABLE:" + REDSHIFT_TABLE)
logger.info("REDSHIFT_IAM_ROLE:" + REDSHIFT_IAM_ROLE)



# KAFKA_BOOSTRAPSERVER = "" ##MSK Serveer
# TOPICS = "" ##MSK Topic
checkpoint_location = REDSHIFT_TMPDIR + "/" + job_name + "/checkpoint/"

maxerror = 0
# redshift_host = '' ##
# redshift_port = ''
# redshift_username = ''
# redshift_password = ''
redshift_database = ""
redshift_schema = "public"
redshift_table = REDSHIFT_TABLE
redshift_tmpdir = REDSHIFT_TMPDIR
tempformat = "CSV"
redshift_iam_role = REDSHIFT_IAM_ROLE  ##arn



# Script generated for node Apache Kafka
kafka_options = {
    "connectionName": KAFKA_CONNECT,
    "topicName": TOPICS,
    "kafka.consumer.commit.groupid": "group-" + job_name,
    "inferSchema": "false",
    "classification": "none",
    "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": 10000,
    "max.partition.fetch.bytes": 10485760,
    "schema": "`value` STRING",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "emitConsumerLagMetrics": "true"
}


def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


dataframe_ApacheKafka_source = glueContext.create_data_frame.from_catalog(
    database="kafka_db",
    table_name="benckmark_tb",
    additional_options={"startingOffsets": "earliest", "inferSchema": "false", "emitConsumerLagMetrics": "true"},
    transformation_ctx="dataframe_KafkaStream_node1"
)

def processBatch(data_frame, batchId):
    logger.info("############  DATA CHECK  ############### \r\n" + getShowString(data_frame,truncate = False))

    dfr = data_frame.selectExpr("CAST(value AS STRING)")
    # dfr = data_frame
    logger.info("############  CAST AS STRING ALIAS DATA  ############### \r\n" + getShowString(dfr,truncate = False))

    logger.info(job_name + "process batch id: " + str(batchId) + " record number: " + str(dfr.count()))
    if dfr.count() > 0:
        # "$json$data_infer_schema$_temporary$" 默认
        dfr.withColumnRenamed("value", "data")
        logger.info("############  RENAME COLUMN  ############### \r\n" + getShowString(dfr,truncate = False))

        redshiftWriteDF = DynamicFrame.fromDF(dfr, glueContext, "from_data_frame")
        logger.info("############  IMPORT redshift  ############### \r\n" + getShowString(redshiftWriteDF.toDF(), truncate = False))
        AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(
            frame=redshiftWriteDF,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": redshift_tmpdir,
                "useConnectionProperties": "true",
                "dbtable": "{0}.{1}".format(redshift_schema, redshift_table),
                "connectionName": REDSHIFT_CONNECT,
                "tempformat": tempformat
            },
            transformation_ctx="AmazonRedshift_node3"
        )

        logger.info(job_name + " - finish batch id: " + str(batchId))



glueContext.forEachBatch(frame=dataframe_ApacheKafka_source,
                         batch_function=processBatch,
                         options={
                             "windowSize": "30 seconds",
                             "checkpointLocation": checkpoint_location
                         })

job.commit()


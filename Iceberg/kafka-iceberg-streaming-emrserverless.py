import sys

from pyspark.sql import SparkSession
import getopt
from urllib.parse import urlparse
import boto3
import json
from cdc_praser.cdc_process_util import CDCProcessUtil


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

checkpointpath = CHECKPOINT_LOCATION + "/" + JOB_NAME + "/checkpoint/" + "20230526" + "/"

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
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
    .start()\
    .awaitTermination()

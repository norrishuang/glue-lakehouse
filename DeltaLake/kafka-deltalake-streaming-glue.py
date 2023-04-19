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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

'''
Glue DelteLake Test
通过 Glue 消费Kafka的数据，写S3（DeltaLake）。多表，支持I U D
'''
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


config = {
    "table_name": "user_order_list",
    "database_name": "deltalakedb",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_iceberg_norrisdb_01"
}

#源表对应iceberg目标表（多表处理）
tableIndexs = {
    "portfolio": "iceberg_portfolio_10",
    "portfolio_02": "portfolio_02",
    "table02": "table02",
    "table01": "table01",
    "user_order_list_small_file": "user_order_list_small_file",
    "user_order_list": "user_order_list",
    "user_order_main": "user_order_main",
    "user_order_mor": "user_order_mor",
    "tb_schema_evolution": "tb_schema_evolution"
}


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.databricks.delta.schema.autoMerge.enabled','true') \
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
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + "checkpoint-05" + "/"

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

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataUpsert.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataUpsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataUpsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowTables = dataTables.collect()

            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataUpsert.select(col("after"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').first()
                schemaData = schema_of_json(dataJson[0])
                logger.info("############  Insert Into-GetSchema-FirstRow:" + dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"), current_timestamp().alias("ts"))
                # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InsertDataLake(tableName, dataDFOutput)

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
                dataDF = dataDelete.select(col("before"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"),schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                # logger.info("############  DELETE FROM  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InsertDataLake(tableName,dataDFOutput)

def InsertDataLake(tableName,dataFrame):

    database_name = config["database_name"]
    table_name = tableIndexs[tableName]
    logger.info("##############  Func:InputDataLake [ "+ tableName +  "] ############# \r\n"
                 + getShowString(dataFrame,truncate = False))

    target = "s3://myemr-bucket-01/data/deltalakedb/" + table_name

    additional_options={
        "path": target
    }

    dataFrame.write.format("delta") \
        .options(**additional_options) \
        .mode('append') \
        .save()
def MergeIntoDataLake(tableName,dataFrame):
    # dataUpsertDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
    # outputUpsert = dataUpsertDF.toDF()
    logger.info("##############  Func:InputDataLake [ "+ tableName +  "] ############# \r\n"
                + getShowString(dataFrame,truncate = False))

    database_name = config["database_name"]
    table_name = tableIndexs[tableName]
    #需要做一次转换，不然spark session获取不到
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();
    TempTable = "tmp_" + tableName + "_upsert"
    dyDataFrame.createOrReplaceTempView(TempTable)
    # queryDF = spark.sql(f"""select * from {TempTable}""")
    # logger.info("##############  Func:Temp Table [ temp_table] ############# \r\n"
    #             + getShowString(queryDF,truncate = False))

    ###如果表不存在，创建一个空表
    creattbsql = f"""CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
        USING DELTA
        LOCATION 's3://myemr-bucket-01/data/{database_name}/{table_name}'
    """
    logger.info("####### IF table not exists, create it:" + creattbsql)
    spark.sql(creattbsql)

    query = f"""MERGE INTO {database_name}.{table_name} t USING (select * from {TempTable}) u ON t.ID = u.ID
            WHEN MATCHED THEN UPDATE
                SET *
            WHEN NOT MATCHED THEN INSERT * """
    logger.info("####### Execute SQL:" + query)
    spark.sql(query)

kafka_options = {
      "connectionName": "kafka_conn_cdc",
      "topicName": "norrisdb01.norrisdb.user_order_list",
      "startingOffsets": "earliest",
      "inferSchema": "true",
      "classification": "json"
}

# Script generated for node Apache Kafka
dataframe_ApacheKafka_source = glueContext.create_data_frame.from_options(
    connection_type="kafka",
    connection_options=kafka_options
)

glueContext.forEachBatch(frame = dataframe_ApacheKafka_source,
                         batch_function = processBatch,
                         options = {
                             "windowSize": "30 seconds",
                             "checkpointLocation": checkpoint_location,
                             "batchMaxRetries": 1
                         })

job.commit()

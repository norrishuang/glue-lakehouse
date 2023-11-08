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

'''
Introduce
    CDC 数据处理方法，用于解析 debezium 输出的CDC数据
'''

class CDCPraser:
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

        self.tables_ds = self._load_tables_config(region, tableconffile)

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
                StructField("before", StringType(), True),
                StructField("after", StringType(), True),
                StructField("source", StringType(), True),
                StructField("op", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("transaction", StringType(), True)
            ])

            self._writeJobLogger("## Source Data from Kafka Batch\r\n + " + getShowString(data_frame, truncate=False))

            dataJsonDF = data_frame.select(from_json(col("value").cast("string"), schema).alias("data")).select(col("data.*"))
            self._writeJobLogger("## Create DataFrame \r\n" + getShowString(dataJsonDF, truncate=False))

            '''
            由于Iceberg没有主键，需要通过SQL来处理upsert的场景，需要识别CDC log中的 I/U/D 分别逻辑处理
            '''
            dataInsert = dataJsonDF.filter("op in ('r','c') and after is not null")
            # 过滤 区分 insert upsert delete
            dataUpsert = dataJsonDF.filter("op in ('u') and after is not null")

            dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

            if dataInsert.count() > 0:
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
                    self._writeJobLogger("Insert Table [%],Counts[%]".format(tableName, str(dataInsert.count())))
                    dataDF = dataInsert.select(col("after"),
                                               from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                        .filter("SOURCE.table = '" + tableName + "'")
                    datajson = dataDF.select('after').first()
                    schemadata = schema_of_json(datajson[0])
                    self._writeJobLogger("############  Insert Into-GetSchema-FirstRow:" + datajson[0])

                    '''识别时间字段'''

                    dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"))

                    # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                    # self._InsertDataLake(tableName, dataDFOutput)
                    return dataDFOutput

            if dataUpsert.count() > 0:
                #### 分离一个topics多表的问题。
                sourcejson = dataUpsert.select('source').first()
                schemasource = schema_of_json(sourcejson[0])

                # 获取多表
                datatables = dataUpsert.select(from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                    .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
                self._writeJobLogger("MERGE INTO Table Names \r\n"
                                     + getShowString(self, datatables, truncate=False))
                rowtables = datatables.collect()

                for cols in rowtables:
                    tableName = cols[1]
                    self._writeJobLogger("Upsert Table [%],Counts[%]".format(tableName, str(dataUpsert.count())))
                    dataDF = dataUpsert.select(col("after"),
                                               from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                        .filter("SOURCE.table = '" + tableName + "'")

                    self._writeJobLogger("MERGE INTO Table [" + tableName + "]\r\n" + getShowString(dataDF, truncate=False))
                    ##由于merge into schema顺序的问题，这里schema从表中获取（顺序问题待解决）
                    database_name = self.config["database_name"]

                    refreshtable = True
                    if refreshtable:
                        self.spark.sql(f"REFRESH TABLE glue_catalog.{database_name}.{tableName}")
                        self._writeJobLogger("Refresh table - True")

                    schemadata = self.spark.table(f"glue_catalog.{database_name}.{tableName}").schema
                    print(schemadata)
                    dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"))

                    self._writeJobLogger("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput, truncate=False))
                    # self._MergeIntoDataLake(tableName, dataDFOutput, batchId)
                    return dataDFOutput

            if dataDelete.count() > 0:
                sourceJson = dataDelete.select('source').first()

                schemaSource = schema_of_json(sourceJson[0])
                dataTables = dataDelete.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .select(col("SOURCE.db"), col("SOURCE.table")).distinct()

                rowTables = dataTables.collect()
                for cols in rowTables:
                    tableName = cols[1]
                    self._writeJobLogger("Delete Table [%],Counts[%]".format(tableName, str(dataDelete.count())))
                    dataDF = dataDelete.select(col("before"),
                                               from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                        .filter("SOURCE.table = '" + tableName + "'")
                    dataJson = dataDF.select('before').first()

                    schemaData = schema_of_json(dataJson[0])
                    dataDFOutput = dataDF.select(from_json(col("before").cast("string"), schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                    # self._DeleteDataFromDataLake(tableName, dataDFOutput, batchId)
                    return dataDFOutput

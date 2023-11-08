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
                    self._InsertDataLake(tableName, dataDFOutput)

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
                    self._MergeIntoDataLake(tableName, dataDFOutput, batchId)

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

        if timestamp_fields != "":
            ##Timestamp字段转换
            for cols in dataFrame.schema:
                if cols.name in timestamp_fields:
                    dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                    self._writeJobLogger("Covert time type-Column:" + cols.name)

        #dyDataFrame = dataFrame.repartition(4, col("id"))

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

        self._writeJobLogger( "####### IF table not exists, create it:" + creattbsql)
        self.spark.sql(creattbsql)

        dataFrame.writeTo(f"glue_catalog.{database_name}.{tableName}") \
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

        self._writeJobLogger("############  TEMP TABLE batch {}  ############### {}\r\n".format(str(batchId),
                                                                                                      getShowString(dataFrame, truncate=False)))
        t = time.time()  # 当前时间
        ts = (int(round(t * 1000000)))  # 微秒级时间戳
        TempTable = "tmp_" + tableName + "_u_" + str(batchId) + "_" + str(ts)
        dataFrame.createOrReplaceGlobalTempView(TempTable)

        ##dataFrame.sparkSession.sql(f"REFRESH TABLE {TempTable}")
        # 修改为全局试图OK，为什么？[待解决]
        if precombine_key == '':
            query = f"""MERGE INTO glue_catalog.{database_name}.{tableName} t USING (SELECT * FROM global_temp.{TempTable}) u
                ON t.{primary_key} = u.{primary_key}
                    WHEN MATCHED THEN UPDATE
                        SET *
                    WHEN NOT MATCHED THEN INSERT * """
        else:
            query = f"""MERGE INTO glue_catalog.{database_name}.{tableName} t USING 
            (SELECT a.* FROM global_temp.{TempTable} a join (SELECT {primary_key},max({precombine_key}) as {precombine_key} from global_temp.{TempTable} group by {primary_key}) b on
                a.{primary_key} = b.{primary_key} and a.{precombine_key} = b.{precombine_key}) u
                ON t.{primary_key} = u.{primary_key}
                    WHEN MATCHED THEN UPDATE
                        SET *
                    WHEN NOT MATCHED THEN INSERT * """

        self.logger.info("####### Execute SQL:" + query)
        try:
            self.spark.sql(query)
        except Exception as err:
            self.logger.error("Error of MERGE INTO")
            self.logger.error(err)
            pass
        self.spark.catalog.dropGlobalTempView(TempTable)


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
        query = f"""DELETE FROM glue_catalog.{database_name}.{tableName} AS t1 
             where EXISTS (SELECT {primary_key} FROM global_temp.{TempTable} WHERE t1.{primary_key} = {primary_key})"""
        try:
            self.spark.sql(query)
        except Exception as err:
            self.logger.error("Error of DELETE")
            self.logger.error(err)
            pass
        self.spark.catalog.dropGlobalTempView(TempTable)

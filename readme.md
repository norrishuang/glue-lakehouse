

# Introduce

## 目录

```
.
├── code
│   ├── cdc_praser 		# lambda 部署主文件
│   ├── DeltaLake
│   │       ├── kafka-deltalake-streaming-emrserverless.py    #cdc 摄入 datalake, emr serverless
│   │				├── kafka-deltalake-streaming-glue.py             #cdc 摄入 datalake, glue
│   ├── Hudi
│   ├── Iceberg
│   ├── redshift                    
```



# AWS Glue 



AWS Glue 4.0 版本已经支持Hudi，Iceberg，DetlaLake

## **版本说明**

| Data Lake  | Glue 4.0 | Glue 3.0 |
| ---------- | -------- | -------- |
| Hudi       | 0.12.1   | 0.10.1   |
| Iceberg    | 1.0.0    | 0.13.1   |
| Delta Lake | 2.1.0    | 1.0.0    |



## Glue 4.0 Iceberg

- merge-on-read 支持通过 MERGE and UPDATE SQL语法（Spark 3.2 或更高版本）
  - 通过merge into 支持upserts。使用直接复制-写的方式。需要更新的文件会立即被重写。

- 在 Spark 3.3版本中,支持 通过 `AS OF` 语法查询time travel
  - Spark读取Iceberg表可以指定“***as-of-timestamp***”参数，通过指定一个毫秒时间参数查询Iceberg表中数据，iceberg会根据元数据找出***timestamp-ms <= as-of-timestamp 对应的 snapshot-id***

- 新增支持通过DataFrame写操作 [`mergeSchema`](https://github.com/apache/iceberg/pull/4154)
  - 设置TableProperties:”write.spark.accept-any-schema”为true。Spark默认在plan的时候会检查写入的DataFrame和表的schema是否匹配，不匹配就抛出异常。所以需要增加一个TableCapability（TableCapability.ACCEPT_ANY_SCHEMA），这样Spark就不会做这个检查，交由具体的DataSource来检查。`df.writeTo(tableName).option(“merge-schema”, “true”).XX`
  - 建议使用Glue 4.0，Spark 3.1（Glue 3.0）不支持mergeSchema




### Iceberg Job 配置说明

以下配置请在Glue Job details 的 **Job parameters** 中配置

| 配置参数                          | 配置说明                                                     |      |
| --------------------------------- | ------------------------------------------------------------ | ---- |
| --conf                            | spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions |      |
| --datalake-formats                | Iceberg                                                      |      |
| --icebergdb                       | Icebergdb                                                    |      |
| --starting_offsets_of_kafka_topic | earliest                                                     |      |
| --topics                          |                                                              |      |
| --user-jars-first                 | True                                                         |      |
| --warehouse                       | s3://<s3bucket>/data/iceberg-folder/                         |      |
| --mskconnect                      | msk-serverless-connector                                     |      |
| --tablejsonfile                   | 设置表的属性的配置文件路径，该文件存放在S3的指定目录。例：s3//<s3bucket>/config/table.json<br />配置属性例如 primary id，数据存储的类型（COW or MOR），使用Iceberg表的版本（V1/V2） |      |
| --region                          | 指定使用的Region（例如, us-east-1）                          |      |



## Hudi

- 可以轻松更改 Hudi 表的Schema，以适应不断变化的数据模式。通过ALTER TABLE语法为 Spark 3.1.x 和 Spark 3.2.1 添加了 Spark SQL DDL 支持（实验性）
- Hudi 表可以直接通过 AWS 开发工具包同步到 AWS Glue Data Catalog。用户可以设置 org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool为HoodieDeltaStreamer 的同步工具实现，并使 Hudi 表在 Glue Catalog中可被发现。
- Hudi 表的元数据（特别是模式和上次同步提交时间）可以同步到DataHub。用户可以将目标表设置org.apache.hudi.sync.datahub.DataHubSyncTool为HoodieDeltaStreamer的同步工具实现，并将目标表同步为DataHub中的Dataset。
- 添加了对 Spark 3.2 的支持，并附带了 Parquet 1.12， 支持Hudi（COW表）加密功能。

## DeltaLake

- **2.1.0 版本开始支持 Apache Spark 3.3.**
- **支持SQL中的[TIMESTAMP | VERSION] AS OF。在** Spark 3.3 中，Delta现在支持*SQL中的[时间旅行](https://docs.delta.io/2.1.0/delta-batch.html#query-an-older-snapshot-of-a-table-time-travel)* ，方便查询历史版本的数据。时间旅行在SQL中和通过DataFrame API都可以使用。
- **当通过流式数据写入 Delta table时，支持 Trigger.AvailableNow 。**Spark 3.3 引入了[Trigger.AvailableNow](https://issues.apache.org/jira/browse/SPARK-36533)，在对Delta表流式读诗，用于在多个批次中运行像Trigger.Once这样的流式查询。

- - **支持 SHOW COLUMNS** 可返回表中的列**.**
  - **在Scala 和 Python DeltaTable API 中支持** [**DESCRIBE DETAIL**](https://docs.delta.io/2.1.0/delta-utility.html#retrieve-delta-table-details) **。** 使用DeltaTable API和SQL检索Delta表的详细信息。

- **支持从 SQL [Delete](https://github.com/delta-io/delta/pull/1328)，[Merge](https://github.com/delta-io/delta/pull/1327)，[Update](https://github.com/delta-io/delta/pull/1331) 命令返回操作指标。**以前这些SQL命令返回一个空的数据框架，现在它们返回一个带有关于所执行操作的有用指标的数据框架。
- **优化性能改进**

[添加了一个配置](https://docs.delta.io/2.1.0/optimizations-oss.html#compaction-bin-packing)，在 Optimize 中使用 repartition(1) 而不是 coalesce(1) ，以便在压缩许多小文件时获得更好的性能。
通过使用基于队列的方法来并行化压缩工作，[提高 Optimize 的性能](https://github.com/delta-io/delta/pull/1315)
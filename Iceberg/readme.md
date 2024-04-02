

# Iceberg

## EMR Serverless
通过 EMR Serverless 将数据写入 Iceberg
* 数据源：Kafka（MSK）
* 目标 S3（Apache Iceberg）

代码 `./Iceberg/kafka-iceberg-streaming-emrserverless.py`

1. 由于代码有依赖公共方法，因此需要在这个项目的根目录下打包，首先获取代码。
```shell
git clone https://github.com/norrishuang/glue-lakehouse.git
cd glue-lakehouse
```

2. 打包公共类
```shell
pip install wheel
python setup.py bdist_wheel
```

3. 打包 python 环境，并且上传到 S3 指定目录下。
```shell
export S3_PATH=<s3-path>
# python lib
python3 -m venv cdc_venv

source cdc_venv/bin/activate
pip3 install --upgrade pip
pip3 install boto3

pip3 install ./dist/cdc_praser-0.3-py3-none-any.whl --force-reinstall

pip3 install venv-pack
venv-pack -f -o cdc_venv.tar.gz

# upload s3
aws s3 cp cdc_venv.tar.gz $S3_PATH
```

4. 提交EMR Serverless 作业
   **前置条件**
    * 确认已提前创建好 EMR Serverless 的 Application，并且选择 spark 引擎
    * 配置好表的配置文件
    * 创建完成 Execution Role
   
```shell

SPARK_APPLICATION_ID=<applicationid>
JOB_ROLE_ARN=arn:aws:iam::812046859005:role/<ROLE>
S3_BUCKET=<s3-buclet-name>

STARTING_OFFSETS_OF_KAFKA_TOPIC='earliest'
TOPICS='\"<topic-name>\"'
TABLECONFFILE='s3://'$S3_BUCKET'/pyspark/config/tables.json'
REGION='us-east-1'
DATABASE_NAME='emr_icebergdb'
WAREHOUSE='s3://'$S3_BUCKET'/data/iceberg-folder/'
KAFKA_BOOSTRAPSERVER='<msk-bootstrap-server>'
CHECKPOINT_LOCATION='s3://'$S3_BUCKET'/checkpoint/'
JOBNAME="MSKServerless-TO-Iceberg-20240301"

# 第三步上传的文件
PYTHON_ENV=$S3_PATH/cdc_venv.tar.gz

aws emr-serverless start-job-run \
  --application-id $SPARK_APPLICATION_ID \
  --execution-role-arn $JOB_ROLE_ARN \
  --name $JOBNAME \
  --job-driver '{
      "sparkSubmit": {
          "entryPoint": "s3://'${S3_BUCKET}'/pyspark/kafka-iceberg-streaming-emrserverless.py",
          "entryPointArguments":["--jobname","'${JOBNAME}'","--starting_offsets_of_kafka_topic","'${STARTING_OFFSETS_OF_KAFKA_TOPIC}'","--topics","'${TOPICS}'","--tablejsonfile","'${TABLECONFFILE}'","--region","'${REGION}'","--icebergdb","'${DATABASE_NAME}'","--warehouse","'${WAREHOUSE}'","--kafkaserver","'${KAFKA_BOOSTRAPSERVER}'","--checkpointpath","'${CHECKPOINT_LOCATION}'"],
          "sparkSubmitParameters": "--jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,s3://emr-hive-us-east-1-812046859005/pyspark/*.jar --conf spark.executor.instances=10 --conf spark.driver.cores=2 --conf spark.driver.memory=4G --conf spark.executor.memory=4G --conf spark.executor.cores=2 --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.archives='$PYTHON_ENV'#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
     }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": "s3://'${S3_BUCKET}'/sparklogs/"
        }
    }
}'

```



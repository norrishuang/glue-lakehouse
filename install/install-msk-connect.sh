#!/bin/bash

#parameter
MSK_CONNECT_NAME=test-connect
MYSQL_USERNAME=admin
MYSQL_PASSWORD=<password>
MYSQL_HOSTNAME=mysql-cdc-db.cghfgy0zyjlk.us-east-1.rds.amazonaws.com
MSK_SERVERLESS_BOOTSTRAP=b-1.mskforgluestreaming.9o4iir.c13.kafka.us-east-1.amazonaws.com:9098,b-3.mskforgluestreaming.9o4iir.c13.kafka.us-east-1.amazonaws.com:9098,b-2.mskforgluestreaming.9o4iir.c13.kafka.us-east-1.amazonaws.com:9098
MYSQL_DATABASE=norrisdb
SUBNET_1=subnet-04d1fea1328f27ef3
SUBNET_2=subnet-08832d067565b3426
SUBNET_3=subnet-0c78b8f6eb8d08168
SECURITY_GROUP=sg-0fbcde03882a87fbd
SERVICE_EXECUTION_ROLEARN=arn:aws:iam::812046859005:role/KafkaConnectRole-Demo-20221219
CUSTOM_PLUGIN_ARN=arn:aws:kafkaconnect:us-east-1:812046859005:custom-plugin/mysql-connnector/5dbc0687-0f39-4130-a283-7d2914b826cd-4
WORK_CONFIGURATION_ARN=arn:aws:kafkaconnect:us-east-1:812046859005:worker-configuration/mysql-cdc-norrisdb-config-offset-convert-time/edd8ad0f-7ae9-4022-8d6b-8be7817f6ff9-4

#create msk connect json file
cat > create-msk-connect-$MSK_CONNECT_NAME.json <<EOF
{
  "connectorConfiguration": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "$MYSQL_HOSTNAME",
    "database.port": "3306",
    "database.user": "$MYSQL_USERNAME",
    "database.password": "$MYSQL_PASSWORD",
    "database.server.id": "123456",
    "database.server.name": "norrisdb",
    "database.include.list": "$MYSQL_DATABASE",
    "database.connectionTimeZone": "UTC",
    "database.history.kafka.topic": "dbhistory.norrisdb",
    "database.history.kafka.bootstrap.servers": "$MSK_SERVERLESS_BOOTSTRAP",
    "database.history.consumer.security.protocol": "SASL_SSL",
    "database.history.consumer.sasl.mechanism": "AWS_MSK_IAM",
    "database.history.consumer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "database.history.consumer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "database.history.producer.security.protocol": "SASL_SSL",
    "database.history.producer.sasl.mechanism": "AWS_MSK_IAM",
    "database.history.producer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "database.history.producer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "include.schema.changes": "true",
    "topic.creation.default.partitions": "3",
    "topic.creation.default.replication.factor": "3",
    "topic.creation.enable": "true",
    "output.data.format": "JSON",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
    "insert.mode": "upsert",
    "time.precision.mode": "connect",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "transforms": "Reroute",
    "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
    "transforms.Reroute.topic.regex": "(.*)user(.*)",
    "transforms.Reroute.topic.replacement": "\$1norrsdb_tb"
  },
  "connectorName": "$MSK_CONNECT_NAME",
  "kafkaCluster": {
    "apacheKafkaCluster": {
      "bootstrapServers": "$MSK_SERVERLESS_BOOTSTRAP",
      "vpc": {
        "subnets": [
          "$SUBNET_1",
          "$SUBNET_2",
          "$SUBNET_3"
        ],
        "securityGroups": ["$SECURITY_GROUP"]
      }
    }
  },
  "capacity": {
    "provisionedCapacity": {
      "mcuCount": 2,
      "workerCount": 1
    }
  },
  "kafkaConnectVersion": "2.7.1",
  "serviceExecutionRoleArn": "$SERVICE_EXECUTION_ROLEARN",
  "plugins": [{
    "customPlugin": {
      "customPluginArn": "$CUSTOM_PLUGIN_ARN",
      "revision": 1
    }
  }],
  "kafkaClusterEncryptionInTransit": {
    "encryptionType": "TLS"
  },
  "kafkaClusterClientAuthentication": {
    "authenticationType": "IAM"
  },
  "workerConfiguration": {
    "workerConfigurationArn": "$WORK_CONFIGURATION_ARN",
    "revision": 1
  }
}
EOF

aws kafkaconnect create-connector --cli-input-json file://create-msk-connect-$MSK_CONNECT_NAME.json --log-delivery '
{
  "workerLogDelivery": {
      "cloudWatchLogs": {
        "enabled": true,
        "logGroup": "MSKClusterLogs"
      }
    }
}
'
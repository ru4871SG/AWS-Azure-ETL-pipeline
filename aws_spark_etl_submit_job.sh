#!/bin/bash

# Define the path to the local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the path to the PySpark script
SCRIPT_PATH="$(realpath "$(dirname "$0")/aws_spark_etl.py")"

# Define the path to the JARs
HADOOP_AWS_JAR="$(realpath "$(dirname "$0")/jars/hadoop-aws-3.3.4.jar")"
AWS_JAVA_SDK_JAR="$(realpath "$(dirname "$0")/jars/aws-java-sdk-bundle-1.12.481.jar")"
AWS_JAVA_SDK_DYNAMODB_JAR="$(realpath "$(dirname "$0")/jars/dynamodb-2.25.1.jar")"

# Submit Spark job
$SPARK_SUBMIT_PATH \
--master spark://localhost:7077 \
--conf spark.driver.memory=8g \
--conf spark.executor.memory=8g \
--conf spark.total.executor.cores=4 \
--conf spark.executor.cores=2 \
--conf spark.executor.heartbeatInterval=60s \
--conf spark.network.timeout=300s \
--jars $HADOOP_AWS_JAR,$AWS_JAVA_SDK_JAR,$AWS_JAVA_SDK_DYNAMODB_JAR \
--packages com.audienceproject:spark-dynamodb_2.12:1.1.1 \
--name "Divvy Trip Data ETL from S3 to DynamoDB" \
$SCRIPT_PATH
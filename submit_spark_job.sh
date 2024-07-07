#!/bin/bash

# Define the path to the local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the path to the PySpark script
SCRIPT_PATH="$(dirname "$0")/aws_spark_data_cleaning.py"

# Define the path to the JAR files (I don't upload this to the github repo, you can download them Maven Repository)
HADOOP_AWS_JAR="$(dirname "$0")/jars/hadoop-aws-3.3.4.jar"
AWS_JAVA_SDK_JAR="$(dirname "$0")/jars/aws-java-sdk-bundle-1.12.481.jar"

# Define the path to the AWS credentials file
AWS_CREDENTIALS_FILE="/home/ruddy/.aws/credentials"

# Submit Spark job
$SPARK_SUBMIT_PATH \
--master spark://localhost:7077 \
--conf spark.driver.memory=8g \
--conf spark.executor.memory=8g \
--conf spark.total.executor.cores=4 \
--conf spark.executor.cores=2 \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
--conf spark.hadoop.fs.s3a.access.key=$(grep aws_access_key_id $AWS_CREDENTIALS_FILE | cut -d= -f2 | tr -d '[:space:]') \
--conf spark.hadoop.fs.s3a.secret.key=$(grep aws_secret_access_key $AWS_CREDENTIALS_FILE | cut -d= -f2 | tr -d '[:space:]') \
--jars $HADOOP_AWS_JAR,$AWS_JAVA_SDK_JAR \
--name "Divvy Bikes Data Cleaning" \
$SCRIPT_PATH

echo "Spark Job submitted successfully."
#!/bin/bash

# Define the path to the local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the path to the PySpark script
SCRIPT_PATH="$(dirname "$0")/aws_spark_etl.py"

# Define the path to the JAR files
HADOOP_AWS_JAR="$(dirname "$0")/jars/hadoop-aws-3.3.4.jar"
AWS_JAVA_SDK_JAR="$(dirname "$0")/jars/aws-java-sdk-bundle-1.12.481.jar"
AWS_JAVA_SDK_DYNAMODB_JAR="$(dirname "$0")/jars/dynamodb-2.25.1.jar"

# Define the path to the AWS credentials file
AWS_CREDENTIALS_FILE="/home/ruddy/.aws/credentials"

# Read AWS credentials
AWS_ACCESS_KEY_ID=$(grep -w "aws_access_key_id" $AWS_CREDENTIALS_FILE | cut -d= -f2 | tr -d '[:space:]')
AWS_SECRET_ACCESS_KEY=$(grep -w "aws_secret_access_key" $AWS_CREDENTIALS_FILE | cut -d= -f2 | tr -d '[:space:]')

# Submit Spark job
$SPARK_SUBMIT_PATH \
--master spark://localhost:7077 \
--conf spark.driver.memory=8g \
--conf spark.executor.memory=8g \
--conf spark.total.executor.cores=4 \
--conf spark.executor.cores=2 \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
--conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
--conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
--conf spark.dynamodb.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
--conf spark.dynamodb.aws.region=us-east-1 \
--conf spark.dynamodb.access.key=$AWS_ACCESS_KEY_ID \
--conf spark.dynamodb.secret.key=$AWS_SECRET_ACCESS_KEY \
--conf spark.dynamodb.endpoint=dynamodb.us-east-1.amazonaws.com \
--conf spark.executor.extraJavaOptions="-Dcom.amazonaws.services.dynamodbv2.enableV2=true -Daws.accessKeyId=$AWS_ACCESS_KEY_ID -Daws.secretKey=$AWS_SECRET_ACCESS_KEY" \
--conf spark.driver.extraJavaOptions="-Dcom.amazonaws.services.dynamodbv2.enableV2=true -Daws.accessKeyId=$AWS_ACCESS_KEY_ID -Daws.secretKey=$AWS_SECRET_ACCESS_KEY" \
--conf spark.dynamodb.write.throttle.rate=5 \
--conf spark.dynamodb.read.throttle.rate=5 \
--jars $HADOOP_AWS_JAR,$AWS_JAVA_SDK_JAR,$AWS_JAVA_SDK_DYNAMODB_JAR \
--packages com.audienceproject:spark-dynamodb_2.12:1.1.1 \
--name "Divvy Trip Data ETL from S3 to DynamoDB" \
$SCRIPT_PATH

echo "Spark Job submitted successfully."
#!/bin/bash

# Define the path to your local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the path to the PySpark script
SCRIPT_PATH="$(dirname "$0")/aws_spark_data_cleaning.py"

# Set the AWS profile to use
export AWS_PROFILE=default

# Loop through all 12 months
for month in {01..12}
do
    echo "Processing month $month"
    
    # Submit the PySpark script to your Spark Standalone cluster
    $SPARK_SUBMIT_PATH \
    --master spark://localhost:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.481 \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
    --name "Data Cleaning - Month $month" \
    $SCRIPT_PATH $month

    echo "Spark job for month $month completed."
done

echo "All months processed successfully for the data cleaning processes."
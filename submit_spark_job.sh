#!/bin/bash

# Define the path to your local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the path to the PySpark script, use the same path as the script
SCRIPT_PATH="$(dirname "$0")/aws_spark_data_cleaning.py"

# Set the SPARK_LOCAL_IP environment variable if needed
# export SPARK_LOCAL_IP='ip_address'

# Loop through all 12 months
for month in {01..12}
do
    echo "Processing month $month"
    
    # Submit the PySpark script (application) to your master node
    # in your Spark Standalone cluster
    $SPARK_SUBMIT_PATH \
    --master spark://localhost:7077 \
    --conf spark.driver.memory=16g \
    --conf spark.executor.memory=16g \
    --conf spark.total.executor.cores=4 \
    --conf spark.executor.cores=2 \
    --jars $JDBC_DRIVER_PATH \
    --name "Data Cleaning - Month $month" \
    $SCRIPT_PATH $month

    echo "Spark job for month $month completed."
done

echo "All months processed successfully for the data cleaning processes."
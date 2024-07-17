#!/bin/bash

# Define the path to the local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the path to the PySpark script
SCRIPT_PATH="$(realpath "$(dirname "$0")/azure_spark_etl.py")"

# Define the path to the JARs
AZURE_STORAGE_JAR="$(realpath "$(dirname "$0")/jars/azure-storage-8.6.6.jar")"
HADOOP_AZURE_JAR="$(realpath "$(dirname "$0")/jars/hadoop-azure-3.3.5.jar")"

# Submit Spark job
$SPARK_SUBMIT_PATH \
--master spark://localhost:7077 \
--conf spark.driver.memory=8g \
--conf spark.executor.memory=8g \
--conf spark.total.executor.cores=4 \
--conf spark.executor.cores=2 \
--conf spark.executor.heartbeatInterval=60s \
--conf spark.network.timeout=300s \
--packages org.eclipse.jetty:jetty-util:9.4.52.v20230823,org.eclipse.jetty:jetty-util-ajax:9.4.52.v20230823,org.eclipse.jetty:jetty-http:9.4.52.v20230823,org.eclipse.jetty:jetty-io:9.4.52.v20230823,org.eclipse.jetty:jetty-server:9.4.52.v20230823,com.fasterxml.jackson.core:jackson-core:2.13.3,com.fasterxml.jackson.core:jackson-databind:2.13.3,com.fasterxml.jackson.core:jackson-annotations:2.13.3,com.azure.cosmos.spark:azure-cosmos-spark_3-3_2-12:4.17.2 \
--jars $AZURE_STORAGE_JAR,$HADOOP_AZURE_JAR \
--name "Divvy Trip Data ETL from Azure Blob Storage" \
$SCRIPT_PATH
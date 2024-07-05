#!/bin/bash

# Use the full path to spark-submit
SPARK_SUBMIT=$(which spark-submit)

# Check if spark-submit is found
if [ -z "$SPARK_SUBMIT" ]; then
    echo "Error: spark-submit not found in PATH"
    exit 1
fi

# Set SPARK_LOCAL_IP
export SPARK_LOCAL_IP=10.5.50.251

# Run the Python script
$SPARK_SUBMIT check_version.py
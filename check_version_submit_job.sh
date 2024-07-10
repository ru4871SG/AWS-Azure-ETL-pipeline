#!/bin/bash

# Use the full path to spark-submit
SPARK_SUBMIT=$(which spark-submit)

# Check if spark-submit is found
if [ -z "$SPARK_SUBMIT" ]; then
    echo "Error: spark-submit not found in PATH"
    exit 1
fi

# Run the Python script to check the Spark version
$SPARK_SUBMIT check_version.py
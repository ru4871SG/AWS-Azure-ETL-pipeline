# cloud-ETL-ML-pipeline
This readme will be updated later. For now, I've only included PySpark scripts and Spark job submission Bash scripts that work with both AWS and Azure. 

`aws_spark_etl.py` is the PySpark script that can extract CSV data from AWS S3 Bucket, transform the data with Spark, and then load the transformed data to AWS DynamoDB. You can submit the PySpark script to your Spark Standalone cluster with the included Spark job submission script `aws_spark_etl_submit_job.sh`

Meanwhile, `azure_spark_etl.py` is the PySpark script that can extract CSV data from Azure Blob Storage, transform the data with Spark, and then load the transformed data to Azure CosmosDB. To submit the Pyspark script to your Spark Standalone cluster, you can use the included Spark job submission script `azure_spark_etl_submit_job.sh`.

Note: At the moment, all Spark job submission scripts are only optimized for local Spark standalone cluster using Docker containers. That's why I've also included `docker-compose.yaml`. Feel free to edit the settings in the yaml file for your own local usage.

Note 2: For the jars that I use within the Bash scripts, you can easily find them on https://mvnrepository.com/. I do not include them in the repository.
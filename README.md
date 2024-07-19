# ETL pipeline with AWS and Azure
This repository contains PySpark scripts as well as Spark job submission scripts that work with ETL processes in AWS and Azure. The scripts are currently optimized for usage in local Spark standalone cluster using Docker containers. That's why I've also included `docker-compose.yaml`. Feel free to edit the settings in the yaml file for your own cluster.

`aws_spark_etl.py` is the PySpark script that can extract data from AWS S3 Bucket, clean and transform the data with Spark, and then load the transformed data to AWS DynamoDB. You can submit the PySpark script to your Spark Standalone cluster with the included Spark job submission script `aws_spark_etl_submit_job.sh`.

Meanwhile, `azure_spark_etl.py` is the PySpark script that can extract data from Azure Blob Storage, clean and transform the data with Spark, and then load the transformed data to Azure CosmosDB. To submit this Pyspark script to your Spark Standalone cluster, you can use the included Spark job submission script `azure_spark_etl_submit_job.sh`.

For both Azure and AWS configurations, you can store them in `.env` file. Please check `.env.example` for the structure.

Note: For the jars that I use within the Bash scripts, you can easily find them on https://mvnrepository.com/. I do not include them in the repository. Store them in the 'jars' folder or simply edit the path if you like.

I've also included `check_version.py` and `check-version_submit_job.sh` in case you want to check the version of Spark and Hadoop installed on your machine (make sure the JARs and package versions in the scripts are compatible with your Spark version).

## Jupyter Notebook

There's also a Jupyter notebook file `aws_spark_etl_notebook_final.ipynb` that explains my data cleaning steps in both `aws_spark_etl.py` and `azure_spark_etl.py`. All the investigations and reasoning behind them are explained here.

## Data Source

You can download the raw data from my Kaggle: [kaggle.com/datasets/ruddygunawan/divvy-bike-trip-data-and-station-geo-locations](https://www.kaggle.com/datasets/ruddygunawan/divvy-bike-trip-data-and-station-geo-locations) and store the CSV files in either AWS S3 Bucket or Azure Blob Storage.
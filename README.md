# ETL pipeline with AWS and Azure
This repository contains PySpark scripts as well as Spark job submission scripts that work with ETL processes in AWS and Azure. Most of the scripts are optimized for usage in local Spark standalone cluster using Docker containers, but I've also included a script that works for Databricks job runs. 

`aws_spark_etl.py` is the PySpark script that can extract data from Amazon S3 Bucket, clean and transform the data with Spark, and then load the transformed data to Amazon DynamoDB. You can submit the PySpark script to your Spark Standalone cluster with the included Spark job submission script `aws_spark_etl_submit_job.sh`.

**Update (8 August 2024):** I have updated the repo with a new PySpark script named `aws_databricks_etl.py`, which is similar to `aws_spark_etl.py` but optimized for Databricks job runs. It works quite similarly, but I use secret scopes to handle sensitive details (instead of `.env` file). Another difference is that I use `boto3` library to handle the connection to DynamoDB.

As for Databricks cluster configurations, you can easily run `aws_databricks_etl.py` with m5d.large node type (8 GB Memory, 2 Cores) with unrestricted single node policy.

Meanwhile, `azure_spark_etl.py` is the PySpark script that can extract data from Azure Blob Storage, clean and transform the data with Spark, and then load the transformed data to Azure CosmosDB. To submit this Pyspark script to your Spark Standalone cluster, you can use the included Spark job submission script `azure_spark_etl_submit_job.sh`.

For `.env` file configurations, please check `.env.example` to understand the format.

Note: For the jars that I use within the Bash scripts, you can easily find them on https://mvnrepository.com/. I do not include them in the repository. Store them in the 'jars' folder or simply edit the path if you like.

I've also included `check_version.py` and `check-version_submit_job.sh` in case you want to check the version of Spark and Hadoop installed on your machine (make sure the JARs and package versions in the scripts are compatible with your Spark version).

## Jupyter Notebooks

There's also a Jupyter notebook file `aws_spark_notebook.ipynb` that explains my data cleaning and transformation steps in both `aws_spark_etl.py` and `azure_spark_etl.py`. All the investigations and reasoning behind them are explained in the notebook.

I've also added the Databricks version of the same notebook, you can check it out at `aws_databricks_notebook.ipynb`.

## Data Source

You can download the raw data from my Kaggle: [kaggle.com/datasets/ruddygunawan/divvy-bike-trip-data-and-station-geo-locations](https://www.kaggle.com/datasets/ruddygunawan/divvy-bike-trip-data-and-station-geo-locations) and store the CSV files in either AWS S3 Bucket or Azure Blob Storage.
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat, date_format, lit, regexp_replace, unix_timestamp
from pyspark.sql.window import Window

from azure.cosmos import exceptions
from dotenv import load_dotenv
from retry import retry
import os

# Load environment variables
load_dotenv()

# Azure configuration
storage_account_name = os.getenv("AZURE_STORAGE_NAME")
container_name = os.getenv("AZURE_STORAGE_CONTAINER")
storage_account_key = os.getenv("AZURE_STORAGE_KEY")
cosmos_endpoint = os.getenv("AZURE_COSMOS_ENDPOINT")
cosmos_key = os.getenv("AZURE_COSMOS_KEY")
cosmos_database = os.getenv("AZURE_COSMOS_DATABASE")
cosmos_container = os.getenv("AZURE_COSMOS_CONTAINER")

# Create a Spark session
spark = SparkSession.builder \
    .appName("Divvy Trip Data ETL from Azure Blob Storage to CosmosDB") \
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config(f"spark.hadoop.fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key) \
    .config("spark.cosmos.accountEndpoint", cosmos_endpoint) \
    .config("spark.cosmos.accountKey", cosmos_key) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Function to read CSV from Azure Blob Storage
def read_csv_from_azure(file_name):
    wasbs_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{file_name}"
    return spark.read.csv(wasbs_path, header=True, inferSchema=True)

# Read data from Azure Blob Storage for each month and union the dataframes
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dataframes = [read_csv_from_azure(f"divvy_tripdata_2022{month}.csv") for month in months]
sdf = dataframes[0]
for df in dataframes[1:]:
    sdf = sdf.union(df)

print("Data extracted from Azure Blob Storage")

# Cache the dataframe after initial load
sdf = sdf.cache()

# Remove leading/trailing whitespace from string columns
sdf = sdf.select([F.trim(F.col(c)).alias(c) if sdf.schema[c].dataType == StringType() else F.col(c) for c in sdf.columns])

# Drop rows with any null values
sdf = sdf.dropna()

# Calculate ride_length and filter out rides less than 60 seconds
sdf = sdf.withColumn("ride_length", unix_timestamp("ended_at") - unix_timestamp("started_at"))
sdf = sdf.filter(sdf.ride_length >= 60)

# Cache the dataframe again after major transformations
sdf = sdf.cache()

# Clean up station names
sdf = sdf.filter(
    (F.upper(F.col("start_station_name")) != F.col("start_station_name")) &
    (F.upper(F.col("end_station_name")) != F.col("end_station_name")) &
    (~F.lower(F.col("start_station_name")).like("%test%")) &
    (~F.lower(F.col("end_station_name")).like("%test%")) &
    (~F.lower(F.col("start_station_id")).like("%test%")) &
    (~F.lower(F.col("end_station_id")).like("%test%")) &
    (~F.lower(F.col("start_station_name")).like("divvy cassette repair mobile station")) &
    (~F.lower(F.col("end_station_name")).like("divvy cassette repair mobile station")) &
    (~F.lower(F.col("start_station_id")).like("divvy cassette repair mobile station")) &
    (~F.lower(F.col("end_station_id")).like("divvy cassette repair mobile station"))
)

# Remove asterisks and "(Temp)" from station names
sdf = sdf.withColumn("start_station_name", regexp_replace("start_station_name", "\\s?\\*", "")) \
        .withColumn("start_station_name", regexp_replace("start_station_name", "\\s?\\(Temp\\)", "")) \
        .withColumn("end_station_name", regexp_replace("end_station_name", "\\s?\\*", "")) \
        .withColumn("end_station_name", regexp_replace("end_station_name", "\\s?\\(Temp\\)", ""))

# Add id, row_number ,and time_details columns
window_spec = Window.orderBy("started_at")
sdf = sdf.withColumn("row_number", F.row_number().over(window_spec)) \
        .withColumn("id", concat(lit("TRIP_"), F.format_string("%07d", F.col("row_number")))) \
        .withColumn("time_details", date_format("started_at", "yyyyMMddHHmmss"))

print("Data transformation done.")

# Function to write batches to CosmosDB
@retry(exceptions=exceptions.CosmosHttpResponseError, tries=5, delay=2, backoff=2)
def cosmosdb_batch(batch_df):
    batch_df.write \
        .format("cosmos.oltp") \
        .option("spark.synapse.linkedService", "CosmosDBConnection") \
        .option("spark.cosmos.container", cosmos_container) \
        .option("spark.cosmos.database", cosmos_database) \
        .option("upsert", "true") \
        .mode("append") \
        .save()

# Batch write operation
batch_size = 300
total_rows = sdf.count()

for i in range(1, total_rows + 1, batch_size):
    end = min(i + batch_size - 1, total_rows)
    batch_df = sdf.filter((F.col("row_number") >= i) & (F.col("row_number") <= end))
    cosmosdb_batch(batch_df)
    print(f"Batch {(i - 1) // batch_size + 1} (rows {i} to {end}) written to CosmosDB")

print("Data successfully written to CosmosDB")

# Stop the Spark session
spark.stop()

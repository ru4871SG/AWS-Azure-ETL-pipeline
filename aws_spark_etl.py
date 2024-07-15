# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat, lit, regexp_replace, unix_timestamp
from pyspark.sql.window import Window

from botocore.exceptions import ClientError
from dotenv import load_dotenv
from retry import retry
import os


# Load environment variables
load_dotenv()

# AWS configuration
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
dynamodb_table_name = os.getenv("DYNAMODB_TABLE_NAME")

# Create a Spark session
spark = SparkSession.builder \
    .appName("Divvy Trip Data ETL from S3 to DynamoDB") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.dynamodb.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.dynamodb.aws.region", aws_region) \
    .config("spark.dynamodb.access.key", aws_access_key_id) \
    .config("spark.dynamodb.secret.key", aws_secret_access_key) \
    .config("spark.dynamodb.endpoint", f"dynamodb.{aws_region}.amazonaws.com") \
    .config("spark.executor.extraJavaOptions", f"-Dcom.amazonaws.services.dynamodbv2.enableV2=true -Daws.accessKeyId={aws_access_key_id} -Daws.secretKey={aws_secret_access_key}") \
    .config("spark.driver.extraJavaOptions", f"-Dcom.amazonaws.services.dynamodbv2.enableV2=true -Daws.accessKeyId={aws_access_key_id} -Daws.secretKey={aws_secret_access_key}") \
    .config("spark.dynamodb.write.throttle.rate", "5") \
    .config("spark.dynamodb.read.throttle.rate", "5") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Function to read CSV from S3 bucket
def read_csv_from_s3(file_name):
    s3_path = f"s3a://{s3_bucket_name}/{file_name}"
    return spark.read.csv(s3_path, header=True, inferSchema=True)

# Read data from S3 for each month and union the dataframes
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dataframes = [read_csv_from_s3(f"divvy_tripdata_2022{month}.csv") for month in months]
sdf = dataframes[0]
for df in dataframes[1:]:
    sdf = sdf.union(df)

print("Data extracted from S3")

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

# Add partition key and sort key columns to the DataFrame after sorting it
# For the sort key, we will use the 'started_at' column, and use the date format "yyyyMMddHHmmss"
window_spec = Window.orderBy("started_at")
sdf = sdf.withColumn("row_number", F.row_number().over(window_spec)) \
        .withColumn("tripdatapartitionkey", concat(lit("TRIP_"), F.format_string("%07d", F.col("row_number")))) \
        .withColumn("tripdatasortkey", F.date_format("started_at", "yyyyMMddHHmmss"))

print("Data transformation done.")

# Function to write batches to DynamoDB
@retry(exceptions=ClientError, tries=5, delay=2, backoff=2)
def dynamodb_batch(batch_df):
    batch_df.write \
        .format("dynamodb") \
        .option("tableName", dynamodb_table_name) \
        .option("region", aws_region) \
        .option("hashKey", "tripdatapartitionkey") \
        .option("rangeKey", "tripdatasortkey") \
        .mode("append") \
        .save()

# Batch write operation
batch_size = 300
total_rows = sdf.count()

for i in range(1, total_rows + 1, batch_size):
    end = min(i + batch_size - 1, total_rows)
    batch_df = sdf.filter((F.col("row_number") >= i) & (F.col("row_number") <= end))
    dynamodb_batch(batch_df)
    print(f"Batch {(i - 1) // batch_size + 1} (rows {i} to {end}) written to DynamoDB")

print("Data successfully written to DynamoDB")

# Stop the Spark session
spark.stop()

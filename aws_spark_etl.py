# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace, unix_timestamp, monotonically_increasing_id, lit, concat

# Create a Spark session
spark = SparkSession.builder \
    .appName("Divvy Trip Data ETL from AWS S3 Bucket to DynamoDB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# S3 bucket name
bucket_name = "divvydatacsv"

# Function to read CSV from S3
def read_csv_from_s3(file_name):
    s3_path = f"s3a://{bucket_name}/{file_name}"
    return spark.read.csv(s3_path, header=True, inferSchema=True)

# Months to read data from
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

# Read data from S3 for each month and union the dataframes
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


# Add partition key and sort key to the DataFrame
# For the sort key, we will use the started_at column, and use the date format "yyyyMMddHHmmss"
sdf = sdf.withColumn("tripdatapartitionkey", concat(lit("TRIP_"), monotonically_increasing_id())) \
        .withColumn("tripdatasortkey", F.date_format("started_at", "yyyyMMddHHmmss"))

# Write the cleaned data to DynamoDB
sdf.write \
    .format("dynamodb") \
    .option("tableName", "divvy_tripdata_cleaned") \
    .option("region", "us-east-1") \
    .option("hashKey", "tripdatapartitionkey") \
    .option("rangeKey", "tripdatasortkey") \
    .mode("append") \
    .save()

print("Data successfully written to DynamoDB")

# Stop the Spark session
spark.stop()

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace, unix_timestamp

# Create a Spark session
spark = SparkSession.builder \
    .appName("Divvy Bikes - Data Cleaning Using Spark (From AWS S3 Bucket)") \
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
sdf = sdf.withColumn("start_station_name", regexp_replace("start_station_name", "\\*", "")) \
         .withColumn("start_station_name", regexp_replace("start_station_name", "\\(Temp\\)", "")) \
         .withColumn("end_station_name", regexp_replace("end_station_name", "\\*", "")) \
         .withColumn("end_station_name", regexp_replace("end_station_name", "\\(Temp\\)", ""))

# Show the first 5 rows of the cleaned data
sdf.show(5)

# Perform some tests to verify the cleaning process
def test_cleaning():
    # Test 1: Check if 'chargingstx3' station name is cleaned
    test1 = sdf.filter(F.col("start_station_id") == "chargingstx3").select("start_station_name").distinct().collect()
    print("Test 1 result:", test1[0]["start_station_name"] if test1 else "No matching records")

    # Test 2: Check if '13285' station name is cleaned
    test2 = sdf.filter(F.col("end_station_id") == "13285").select("end_station_name").distinct().collect()
    print("Test 2 result:", test2[0]["end_station_name"] if test2 else "No matching records")

    # Test 3: Check if 'DIVVY CASSETTE REPAIR MOBILE STATION' is removed
    test3 = sdf.filter(F.col("end_station_id") == "DIVVY CASSETTE REPAIR MOBILE STATION").count()
    print("Test 3 result:", "Removed" if test3 == 0 else "Still present")

    # Test 4: Check if 'DIVVY 001 - Warehouse test station' is removed
    test4 = sdf.filter(F.col("start_station_id") == "DIVVY 001 - Warehouse test station").count()
    print("Test 4 result:", "Removed" if test4 == 0 else "Still present")

# Run the tests
test_cleaning()

# Stop the Spark session
spark.stop()

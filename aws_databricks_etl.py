# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat, lit, regexp_replace, unix_timestamp
from pyspark.sql.window import Window

from botocore.exceptions import ClientError
from decimal import Decimal
from retry import retry

import boto3

def to_decimal(value):
    return Decimal(str(value)) if value is not None else None

def main():
    # Retrieve secrets
    aws_access_key_id = dbutils.secrets.get(scope="ruddy-scope", key="aws_access_key_id")
    aws_secret_access_key = dbutils.secrets.get(scope="ruddy-scope", key="aws_secret_access_key")
    dynamodb_table_name = dbutils.secrets.get(scope="ruddy-scope", key="dynamodb_table_name")
    aws_region = dbutils.secrets.get(scope="ruddy-scope", key="aws_region")

    spark = SparkSession.builder \
        .appName("Divvy Trip Data ETL using Databricks") \
        .getOrCreate()

    # Read data from Databricks workspace directly
    sdf = spark.table("de_testing_oregon.default.divvy_table_complete")
    # We can drop the '_rescued_data' column (which is automatically added by Databricks), since it's not needed
    sdf = sdf.drop("_rescued_data")

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
    window_spec = Window.orderBy("started_at")
    sdf = sdf.withColumn("row_number", F.row_number().over(window_spec)) \
            .withColumn("tripdatapartitionkey", concat(lit("TRIP_"), F.format_string("%07d", F.col("row_number")))) \
            .withColumn("tripdatasortkey", F.date_format("started_at", "yyyyMMddHHmmss"))

    # Since we use boto3 here, we need to convert datetime columns to strings before writing to DynamoDB
    sdf = sdf.withColumn("started_at", F.date_format("started_at", "yyyy-MM-dd HH:mm:ss"))
    sdf = sdf.withColumn("ended_at", F.date_format("ended_at", "yyyy-MM-dd HH:mm:ss"))

    # Same here, we need to convert double or float columns to decimal
    numeric_columns = ["start_lat", "start_lng", "end_lat", "end_lng"]
    for col in numeric_columns:
        sdf = sdf.withColumn(col, F.udf(to_decimal, StringType())(F.col(col)))

    print("Data cleaning and transformation done.")

    #Function to write batches to DynamoDB
    @retry(exceptions=ClientError, tries=5, delay=2, backoff=2)
    def dynamodb_batch(batch_df):
        try:
            print(f"Attempting to write {batch_df.count()} rows to DynamoDB")
            boto3_session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region
            )
            dynamodb = boto3_session.resource('dynamodb')
            table = dynamodb.Table(dynamodb_table_name)
            for row in batch_df.collect():
                item = {}
                for column in batch_df.columns:
                    item[column] = row[column]
                table.put_item(Item=item)
        except Exception as e:
            print(f"Error writing to DynamoDB: {e}")
            raise

    # Batch write operation
    batch_size = 300
    total_rows = sdf.count()

    for i in range(1, total_rows + 1, batch_size):
        end = min(i + batch_size - 1, total_rows)
        batch_df = sdf.filter((F.col("row_number") >= i) & (F.col("row_number") <= end))
        dynamodb_batch(batch_df)
        print(f"Batch {(i - 1) // batch_size + 1} (rows {i} to {end}) written to DynamoDB")

    print("Data successfully written to DynamoDB")

if __name__ == "__main__":
    main()

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace, when, col, unix_timestamp

def create_spark_session():
    return SparkSession.builder \
        .appName("Divvy Bikes - Data Cleaning Using Spark (From AWS S3 Bucket)") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("fs.s3a.metrics.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.memory.fraction", "0.8") \
        .getOrCreate()

def read_csv_from_s3(spark, bucket_name, file_name):
    s3_path = f"s3a://{bucket_name}/{file_name}"
    return spark.read.csv(s3_path, header=True, inferSchema=True)

def clean_data(sdf):
    # Cache the dataframe after initial load
    sdf = sdf.cache()

    # Data Cleaning Steps
    # 1. Remove leading/trailing whitespace from string columns
    sdf = sdf.select([F.trim(F.col(c)).alias(c) if sdf.schema[c].dataType == StringType() else F.col(c) for c in sdf.columns])

    # 2. Remove rows with any null values
    sdf = sdf.dropna()

    # 3. Calculate ride_length and filter out rides less than 60 seconds
    sdf = sdf.withColumn("ride_length", 
                         unix_timestamp("ended_at") - unix_timestamp("started_at"))
    sdf = sdf.filter(sdf.ride_length >= 60)

    # Cache the dataframe again after major transformations
    sdf = sdf.cache()

    # 4. Clean up station names
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

    # 5. Remove asterisks and "(Temp)" from station names
    sdf = sdf.withColumn("start_station_name", regexp_replace("start_station_name", "\\*", "")) \
             .withColumn("start_station_name", regexp_replace("start_station_name", "\\(Temp\\)", "")) \
             .withColumn("end_station_name", regexp_replace("end_station_name", "\\*", "")) \
             .withColumn("end_station_name", regexp_replace("end_station_name", "\\(Temp\\)", ""))

    return sdf


def process_month(spark, bucket_name, month):
    input_file = f"divvy_tripdata_2022{month}.csv"
    output_file = f"divvy_tripdata_2022{month}_cleaned"

    print(f"Processing {input_file}")

    # Read CSV file from S3
    sdf = read_csv_from_s3(spark, bucket_name, input_file)

    # Clean the data
    sdf = clean_data(sdf)

    # Write the cleaned data back to S3
    output_path = f"s3a://{bucket_name}/{output_file}"
    sdf.write.csv(output_path, header=True, mode="overwrite")
    print(f"Cleaned data has been written to {output_path}")

    # Unpersist the cached DataFrame
    sdf.unpersist()


def main():
    if len(sys.argv) != 2:
        print("Usage: spark-submit script.py <month>")
        print("Month should be in the format '01', '02', ..., '12'")
        sys.exit(1)

    month = sys.argv[1]
    bucket_name = "divvydatacsv"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    process_month(spark, bucket_name, month)

    spark.stop()

if __name__ == "__main__":
    main()

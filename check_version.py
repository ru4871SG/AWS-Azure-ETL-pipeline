from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VersionCheck").getOrCreate()
sc = spark.sparkContext

print(f"Spark version: {sc.version}")
print(f"Hadoop version: {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

spark.stop()

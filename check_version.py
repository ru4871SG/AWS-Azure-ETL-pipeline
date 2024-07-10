# Since I use Spark locally, this script will help to check the version of Spark and Hadoop installed on my machine.
# You can run the Bash script 'check_version_submit_job.sh' to submit this script to the Spark local cluster.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VersionCheck").getOrCreate()
sc = spark.sparkContext

print(f"Spark version: {sc.version}")
print(f"Hadoop version: {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

spark.stop()

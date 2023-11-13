from pyspark.sql import SparkSession
import os


packages = ",".join([
    "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
    "net.snowflake:snowflake-jdbc:3.14.0",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
])

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages " + packages + "  pyspark-shell"

os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/anaconda3/bin/python'


def init_spark():
    return SparkSession.builder \
        .appName("Project App") \
        .config("spark.driver.memory", "12g") \
        .config("spark.jars.packages", packages) \
        .getOrCreate()


baseInputPath = "/Users/hims/Downloads/yelp_dataset/"
baseOutputPath = "/Users/hims/Documents/yelp_project/outputs/"
sample = 0.001

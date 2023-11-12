from pyspark.sql import SparkSession
import os

#



# Create a Spark session with the Snowflake connector JARs
packages = ",".join([
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
            "net.snowflake:snowflake-jdbc:3.14.0"])

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages " + packages + "  pyspark-shell"

os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/anaconda3/bin/python'


def init_spark():
    return SparkSession.builder \
        .appName("Project App") \
        .config("spark.driver.memory", "12g") \
        .config("spark.jars.packages",packages) \
        .getOrCreate()


baseInputPath = "/Users/hims/Downloads/yelp_dataset/"
baseOutputPath = "/tmp/test-1"
sample = 0.005

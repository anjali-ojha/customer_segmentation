import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col


def init_spark():
    os.environ['PYSPARK_SUBMIT_ARGS'] = ",".join([
        '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0',
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'])

    spark = (
        SparkSession
        .builder
        .appName("Read Kafka")
        .config('spark.sql.shuffle.partitions', 4)
        .config('sspark.default.parallelism', 4)
        .config('spark.jars.packages', ','.join([
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
            'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0']))
        .config('spark.jars', ','.join([
            '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar',
            '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar',
            '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/kafka-clients-3.5.0.jar',
            '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar',
            '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/commons-pool2-2.8.0.jar']))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class Consumer:

    def __init__(self, server, topic, schema):
        self.server = server
        self.topic = topic
        self.schema = schema

    def read_from_topic(self, spark):
        print(f"reading data from the {topic = }, {server = }")
        df = (
            spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.server)
            .option("startingOffsets", "earliest")
            .option("subscribe", self.topic)
            .load()
        )
        print(f"is spark is reading the streams from kafka = {df.isStreaming}")
        df.printSchema()
        return df.withColumn("json_string", col("value").cast(StringType()))

    def write_reviews(self, spark, output_path):
        stream_df = self.read_from_topic(spark)
        df_result = stream_df.select(from_json(col("json_string"), self.schema).alias("data"))
        writer = df_result \
            .writeStream.outputMode("append") \
            .format("parquet") \
            .option("path",f"{output_path}/data") \
            .option("checkpointLocation", f"{output_path}/checkpoint") \
            .trigger(processingTime="5 seconds") \
            .start()
        writer.awaitTermination()


schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("cool", LongType(), True),
    StructField("date", StringType(), True),
    StructField("funny", LongType(), True),
    StructField("review_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("text", StringType(), True),
    StructField("useful", LongType(), True),
    StructField("user_id", StringType(), True),
])

if __name__ == "__main__":
    if len(sys.argv) >= 4:
        server = sys.argv[1]
        topic = sys.argv[2]
        output_path = sys.argv[3]
        spark = init_spark()
        consumer = Consumer(server, topic, schema)
        consumer.write_reviews(spark, output_path)
        spark.stop()
    else:
        print("Invalid number of arguments. Please pass the server and topic name")

#%%

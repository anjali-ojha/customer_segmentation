import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col
from commons import *

# def init_spark():
#     os.environ['PYSPARK_SUBMIT_ARGS'] = ",".join([
#         '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0',
#         'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'])
#
#     spark = (
#         SparkSession
#         .builder
#         .appName("Read Kafka")
#         .config('spark.sql.shuffle.partitions', 4)
#         .config('spark.default.parallelism', 4)
#         .config('spark.jars.packages', ','.join([
#             'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
#             'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0']))
#         .config('spark.jars', ','.join([
#             '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar',
#             '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar',
#             '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/kafka-clients-3.5.0.jar',
#             '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar',
#             '/opt/anaconda3/lib/python3.9/site-packages/pyspark/jars/commons-pool2-2.8.0.jar']))
#         .getOrCreate()
#     )
#     spark.sparkContext.setLogLevel("ERROR")
#     return spark


class Consumer:

    def __init__(self, server, output_path):
        self.server = server
        self.output_path = output_path

    def read_from_topic(self, spark, topic):
        print(f"reading data from the {topic = }, {server = }")
        df = (
            spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.server)
            .option("startingOffsets", "earliest")
            .option("subscribe", topic)
            .load()
        )
        print(f"is spark is reading the streams from kafka = {df.isStreaming}")
        df.printSchema()
        return df.withColumn("json_string", col("value").cast(StringType()))

    def write(self, df_result, topic_name):
        writer = df_result \
            .writeStream.outputMode("append") \
            .format("parquet") \
            .option("path", f"{self.output_path}/{topic_name}/data") \
            .option("checkpointLocation", f"{self.output_path}/{topic_name}/checkpoint") \
            .trigger(processingTime="10 seconds") \
            .start()
        writer.awaitTermination()

    def read_checkins(self, spark):
        stream_df = self.read_from_topic(spark, "checkins")
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("date", StringType(), True),
        ])
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data"))
        return df_result

    def read_tips(self, spark):
        topicName = "tips"
        stream_df = self.read_from_topic(spark, topicName)
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("compliment_count", LongType(), True),
            StructField("date", StringType(), True),
            StructField("text", StringType(), True),
            StructField("user_id", StringType(), True),
        ])
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data"))
        self.write(df_result, topicName)

    def read_reviews(self, spark):
        topicName = "reviews"
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
        stream_df = self.read_from_topic(spark, topicName)
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data"))
        self.write(df_result, topicName)


if __name__ == "__main__":
    if len(sys.argv) >= 4:
        server = sys.argv[1]
        topic = sys.argv[2]
        output_path = sys.argv[3]
        spark = init_spark()
        consumer = Consumer(server, output_path)
        consumer.read_reviews(spark)
        consumer.read_tips(spark)
        consumer.read_checkins(spark)
        spark.stop()
    else:
        print("Invalid number of arguments. Please pass the server and topic name")

#%%

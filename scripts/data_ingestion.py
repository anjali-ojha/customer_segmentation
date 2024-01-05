import sys

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from commons import *
from sampling import *
from sentiment import *

global sample

class Consumer:
    """
    - This class will consume the data from kafka and write to the parquet files.
    - While writing to the parquet files, it will also perform some transformations.
    - It will also perform sampling if the sample is specified.
    - it will also perform sentiment analysis and tokenize the words to
    - capture the most frequent words and sentiment of the review.
    """

    def __init__(self, server, output_path):
        self.server = server
        self.output_path = output_path

    def read_from_topic(self, spark, topic):
        """
        Reading Data from the kafka topic.
        """
        print(f"reading data from the topic = ", topic, "server = ", self.server)
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

    def write_stream(self, df_result, topic_name):
        """
        Write stream data to the parquet files.
        it writes data as append mode, to avoid whole data rewrite.
        we are using trigger to write data every 10 seconds.
        """
        writer = df_result \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"{self.output_path}/{topic_name}/data") \
            .option("checkpointLocation", f"{self.output_path}/{topic_name}/checkpoint") \
            .trigger(processingTime="10 seconds") \
            .start()
        writer.awaitTermination()

    def read_checkins(self, spark):
        """
        This method will read the checkins data from the kafka topic.
        it also performs the transformation on the data, by converting the json string to the dataframe.
        """
        topicName = "checkins"
        stream_df = self.read_from_topic(spark, topicName)
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("date", StringType(), True),
        ])
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data"))
        self.write_stream(df_result, topicName)

    def read_tips(self, spark):
        """
        This method will read the tips data from the kafka topic.
        it also performs the transformation on the data, by converting the json string to the dataframe.
        """
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
        self.write_stream(df_result, topicName)

    def process_review_data_df(self, review_df, x):
        """
        This method will process the review data batch.
        each batch will be processed and written to the parquet files.
        a batch is the new data that is read from the kafka topic for the first time.
        it will do following things:
            -  sample the data if the sample is specified.
            - perform sentiment analysis on the review text.
            - tokenize the words and get the most frequent words.
            - write the data to the parquet files.

        As this is a generic method, it will be called for each batch, and more and more functionality can be added to it.
        """
        sampled_users, is_sampled = get_sampled_users_data(spark, sample)
        if is_sampled:
            print("got sampled users ... processing that.")
            sampled_users.printSchema()
            review_df.printSchema()
            review_df = review_df.join(sampled_users, on=["user_id"])

        review_df = review_df \
            .withColumn("date", col("date").cast("timestamp")) \
            .withColumn("sentiment",  get_sentiment(col("text"))) \
            .withColumn("frequent_words", tokenize_and_get_top_words(col("text")))

        review_df.printSchema()
        review_df.repartition(1).write.mode("append").parquet(f"{sample_output_path(sample)}/review")
        print("sample review ares = ", review_df.count())
        return review_df

    def read_reviews(self, spark):
        """
        This method will read the review data from the kafka topic.
        it also performs the transformation on the data, by converting the json string to the dataframe.
        it will use the function process_review_data_df to process each batch.
        """
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
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data")).select("data.*")
        writer = df_result.writeStream.outputMode("append").foreachBatch(self.process_review_data_df).start()
        writer.awaitTermination(30)



if __name__ == "__main__":
    # This is the main method, which will be called when the program will be executed.
    # it will read the arguments from the command line.
    # it will also initialize the spark session.
    # it will also call the consumer class to read the data from the kafka topic.
    # it will call the methods to read the data from the kafka topic.
    # it will also call the methods to write the data to the parquet files.
    if len(sys.argv) >= 4:
        server = sys.argv[1]
        topic = sys.argv[2]
        output_path = sys.argv[3]
        sample = float(sys.argv[4])
        spark = init_spark()
        consumer = Consumer(server, output_path)
        consumer.read_reviews(spark)
        consumer.read_tips(spark)
        consumer.read_checkins(spark)
        spark.stop()
    else:
        print("Invalid number of arguments. Please pass the server and topic name")

#%%

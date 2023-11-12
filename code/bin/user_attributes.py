from pyspark.sql.functions import col, udf
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import explode, create_map
from pyspark.sql.functions import size
from pyspark.sql.types import IntegerType, MapType
from pyspark.sql.types import StringType

from commons import *
from sentiment import *
from storage import *
from data_processing import *


@udf(MapType(StringType(), IntegerType()))
def merge_maps_array(map_array):
    result = {}
    for m in map_array:
        for k, v in m.items():
            result[k] = result.get(k, 0) + v
    return result


def get_customer_agg_value(spark, review_df):
    review_df.createOrReplaceTempView("review")
    return spark.sql("""
        select 
            user_id, 
            min(date) as first_seen, 
            max(date) as last_seen, 
            DATEDIFF(max(date), min(date)) as date_diff,
            count(distinct business_id) as different_business_count,
            avg(stars) as avg_rating,
            min(stars) as min_stars,
            max(stars) as max_stars
        from review
        group by user_id 
    """)


def get_customer_category_counts(review_df, business_df):
    df = review_df.select("user_id", "business_id") \
        .join(business_df.select("business_id", "categories"), on=["business_id"]) \
        .select("user_id", explode("categories").alias("category")) \
        .groupBy("user_id", "category").count() \
        .withColumn("category_map", create_map(col("category"), col("count"))) \
        .groupBy("user_id").agg(collect_list(col("category_map")).alias("category_map")) \
        .withColumn("category_map", merge_maps_array(col("category_map")))

    return df


def get_friends_count(friends_df):
    df = friends_df.select("user_id", size(col("friends")).alias("friends_count"))
    return df


def get_sentiments_count(review_df):
    df = review_df.select("user_id", "sentiment").groupBy("user_id", "sentiment").count() \
        .withColumnRenamed("count", "sentiment_count") \
        .withColumn("sentiment_map", create_map(col("sentiment"), col("sentiment_count"))) \
        .groupBy("user_id").agg(collect_list(col("sentiment_map")).alias("sentiment_map")) \
        .withColumn("sentiment_map", merge_maps_array(col("sentiment_map")))

    return df


def most_frequent_words(review_df):
    return review_df.select("user_id", explode("frequent_words").alias("frequent_words")) \
        .groupBy("user_id", "frequent_words").count() \
        .withColumn("frequent_words_map", create_map(col("frequent_words"), col("count"))) \
        .groupBy("user_id").agg(collect_list(col("frequent_words_map")).alias("frequent_words_map")) \
        .withColumn("frequent_words_map", merge_maps_array(col("frequent_words_map")))


def merge_attributes(spark):
    user_df = process_user_data(spark)
    business_df = process_business_data(spark)
    friends_df = process_friends_data(spark)
    checkin_df = process_checkin_data(spark)
    tip_df = process_tip_data(spark)
    review_df = process_review_data(spark)

    user_agg_df = get_customer_agg_value(spark, review_df)
    user_category_df = get_customer_category_counts(review_df, business_df)
    friends_count_df = get_friends_count(friends_df)
    sentiment_count_df = get_sentiments_count(review_df)
    frequent_words_df = most_frequent_words(review_df)

    complete_user_df = user_df \
        .join(user_agg_df, on=["user_id"]) \
        .join(user_category_df, on=["user_id"]) \
        .join(friends_count_df, on=["user_id"]) \
        .join(sentiment_count_df, on=["user_id"]).cache()

    complete_user_df.printSchema()
    complete_user_df.count()

    complete_user_df.write.mode("overwrite").parquet(f"{sampleOutputPath}/combined")
    return spark.read.parquet(f"{sampleOutputPath}/combined")


if __name__ == "__main__":
    sparkSession = init_spark()
    merged_df = merge_attributes(sparkSession)
    # save_spark_df_to_db(merged_df, "users")
    sparkSession.stop()

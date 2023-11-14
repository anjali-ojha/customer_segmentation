from data_processing import *

from attributes.users_agg import *
from attributes.business import *
from attributes.reviews import *


def merge_attributes(spark):
    # This method will aggregate all the features at a sinple page
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
        .join(sentiment_count_df, on=["user_id"]).cache() \
        .join(frequent_words_df, on=["user_id"]) \

    complete_user_df.printSchema()
    complete_user_df.count()

    complete_user_df.write.mode("overwrite").parquet(f"{sampleOutputPath}/combined")
    return spark.read.parquet(f"{sampleOutputPath}/combined")


if __name__ == "__main__":
    sparkSession = init_spark()
    merged_df = merge_attributes(sparkSession)
    # save_spark_df_to_db(merged_df, "users")
    sparkSession.stop()

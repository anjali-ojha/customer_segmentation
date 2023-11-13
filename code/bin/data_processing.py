from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.sql.types import TimestampType

from commons import *
from project.customer_segmentation.code.bin.data.sampling import *
baseInputPath = baseInputPath
sampleOutputPath = f"{baseOutputPath}/sample={sample}/"


def process_user_data(spark):
    try:
        userDf = spark.read.parquet(f"{sampleOutputPath}/user")
    except Exception as e:

        sampled_users, is_sampled = get_sampled_users_data(spark)
        userDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_user.json')
        if is_sampled:
            userDf = userDf.join(sampled_users, on = ["user_id"])

        userDf = userDf \
            .drop("friends") \
            .withColumn("elite", split(col("elite"), ", ")) \
            .withColumn("yelping_since", col("yelping_since").cast("timestamp"))

        userDf.coalesce(1).write.mode("overwrite").parquet(f"{sampleOutputPath}/user")
        userDf = spark.read.parquet(f"{sampleOutputPath}/user")
        print(f"sample users ares = {userDf.count()}")
    return userDf


def process_business_data(spark):
    try:
        businessDf = spark.read.parquet(f"{sampleOutputPath}/business")
    except Exception as e:

        schema = StructType([
            StructField("address", StringType(), True),
            StructField("attributes", MapType(StringType(), StringType()), True),
            StructField("business_id", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("city", StringType(), True),
            StructField("hours", MapType(StringType(), StringType()), True),
            StructField("is_open", LongType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("name", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("review_count", LongType(), True),
            StructField("stars", DoubleType(), True),
            StructField("state", StringType(), True),
        ])

        sampled_business, is_sampled = get_sampled_business_data(spark)
        businessDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_business.json', schema)
        if is_sampled:
            businessDf = businessDf.join(sampled_business, on = ["business_id"])

        businessDf =  businessDf \
            .withColumn("categories", split(col("categories"), ", "))
        businessDf.write.mode("overwrite").parquet(f"{sampleOutputPath}/business")
        businessDf = spark.read.parquet(f"{sampleOutputPath}/business")
        print(f"sample business ares = {businessDf.count()}")

    return businessDf


def process_friends_data(spark):
    try:
        friendsDf = spark.read.parquet(f"{sampleOutputPath}/friends")
    except Exception as e:

        sampled_users, is_sampled = get_sampled_users_data(spark)
        friendsDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_user.json')
        if is_sampled:
            friendsDf = friendsDf.join(sampled_users, on = ["user_id"])

        friendsDf = friendsDf.select("user_id", split(col("friends"), ", ").alias("friends"))

        friendsDf.printSchema()
        friendsDf.write.mode("overwrite").parquet(f"{sampleOutputPath}/friends")
        friendsDf = spark.read.parquet(f"{sampleOutputPath}/friends")
        print("sample friends ares = ", friendsDf.count())
    return friendsDf


def process_checkin_data(spark):
    try:
        checkinDf = spark.read.parquet(f"{sampleOutputPath}/checkin")
    except Exception as e:

        sampled_business, is_sampled = get_sampled_business_data(spark)
        checkinDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_checkin.json')
        if is_sampled:
            checkinDf = checkinDf.join(sampled_business, on = ["business_id"])

        checkinDf = checkinDf \
            .withColumn("date", expr("transform(split(date, ', '), d -> to_timestamp(d))").cast(ArrayType(TimestampType())))

        checkinDf.printSchema()

        checkinDf.write.mode("overwrite").parquet(f"{sampleOutputPath}/checkin")
        checkinDf = spark.read.parquet(f"{sampleOutputPath}/checkin")
        print("sample checkin ares = ", checkinDf.count())
    return checkinDf


def process_tip_data(spark):
    try:
        tipDf = spark.read.parquet(f"{sampleOutputPath}/tip")
    except Exception as e:

        sampled_users, is_sampled = get_sampled_users_data(spark)
        tipDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_tip.json')
        if is_sampled:
            tipDf = tipDf.join(sampled_users, on = ["user_id"])

        tipDf = tipDf.withColumn("date", col("date").cast("timestamp"))

        tipDf.write.mode("overwrite").parquet(f"{sampleOutputPath}/tip")
        tipDf = spark.read.parquet(f"{sampleOutputPath}/tip")
        print("sample tip ares = ", tipDf.count())
    return tipDf


def process_review_data(spark):
    try:
        reviewDf = spark.read.parquet(f"{sampleOutputPath}/review")
    except Exception as e:
        sampled_users, is_sampled = get_sampled_users_data(spark)
        reviewDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_review.json')
        if is_sampled:
            reviewDf = reviewDf.join(sampled_users, on = ["user_id"])

        reviewDf = reviewDf \
            .withColumn("date", col("date").cast("timestamp")) \
            .withColumn("sentiment",  get_sentiment(col("text"))) \
            .withColumn("frequent_words", tokenize_and_get_top_words(col("text")))

        reviewDf.printSchema()
        reviewDf.write.mode("overwrite").parquet(f"{sampleOutputPath}/review")
        reviewDf = spark.read.parquet(f"{sampleOutputPath}/review")
        print("sample review ares = ", reviewDf.count())
    return reviewDf


if __name__ == "__main__":

    sparkSession = init_spark()

    user_df = process_user_data(sparkSession)
    business_df = process_business_data(sparkSession)
    friends_df = process_friends_data(sparkSession)
    checkin_df = process_checkin_data(sparkSession)
    tip_df = process_tip_data(sparkSession)
    review_df = process_review_data(sparkSession)

    sparkSession.stop()

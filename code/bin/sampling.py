from commons import *
from sentiment import *

baseInputPath = baseInputPath
sampleOutputPath = f"{baseOutputPath}/sample={sample}/"


def get_sampled_users_data(spark):
    if sample != 1:
        try:
            sampled_users = spark.read.parquet(f"{sampleOutputPath}/sampled_user_id")
        except Exception as e:
            sampled_users = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_review.json') \
                .groupBy("user_id").count().orderBy(col("count").desc()).select("user_id").sample(sample)

            sampled_users.write.mode("overwrite").parquet(f"{sampleOutputPath}/sampled_user_id")
            sampled_users = spark.read.parquet(f"{sampleOutputPath}/sampled_user_id")
            print(f"sample users ares = {sampled_users.count()}")
        return sampled_users, True
    else:
        return None, False


def get_sampled_business_data(spark):
    if sample != 1:
        try:
            sampled_business = spark.read.parquet(f"{sampleOutputPath}/sampled_business_id")
        except Exception as e:

            sampled_user, _ = get_sampled_users_data(spark)
            sampled_business =  spark.read.json(f'{baseInputPath}/yelp_academic_dataset_review.json') \
                .join(sampled_user, on = ["user_id"]) \
                .select("business_id").distinct()

            sampled_business.write.mode("overwrite").parquet(f"{sampleOutputPath}/sampled_business_id")
            sampled_business = spark.read.parquet(f"{sampleOutputPath}/sampled_business_id")
            print(f"sample business ares = {sampled_business.count()}")
        return sampled_business, True
    else:
        return None, False


if __name__ == "__main__":
    spark = init_spark()
    get_sampled_users_data(spark)
    get_sampled_business_data(spark)
    spark.stop()

from pyspark.sql.functions import col, udf
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import explode, create_map
from pyspark.sql.functions import size
from pyspark.sql.types import IntegerType, MapType
from pyspark.sql.types import StringType


@udf(MapType(StringType(), IntegerType()))
def merge_maps_array(map_array):
    result = {}
    for m in map_array:
        for k, v in m.items():
            result[k] = result.get(k, 0) + v
    return result


def get_customer_category_counts(review_df, business_df):
    df = review_df.select("user_id", "business_id") \
        .join(business_df.select("business_id", "categories"), on=["business_id"]) \
        .select("user_id", explode("categories").alias("category")) \
        .groupBy("user_id", "category").count() \
        .withColumn("category_map", create_map(col("category"), col("count"))) \
        .groupBy("user_id").agg(collect_list(col("category_map")).alias("category_map")) \
        .withColumn("category_map", merge_maps_array(col("category_map")))

    return df

from pyspark.sql.functions import col
from pyspark.sql.functions import size


def get_friends_count(friends_df):
    """
    This method will get the friends count of the user.
    friends count will be the count of the friends that the customer has.
    it captures how many friends the customer has and it's important to know if the customer is social or not
    and how big their social circle is.
    """
    df = friends_df.select("user_id", size(col("friends")).alias("friends_count"))
    return df


def get_customer_agg_value(spark, review_df):
    """
    This method will get the customer aggregation value.
    customer aggregation value will be the aggregation of the customers different attributes.
    it will compute attributes like:
        - first_seen :  when the customer put their first review
        - last_seen : when the customer put their last review
        - date_diff : how many days the customer has been inactive. (last_seen - first_seen)
                      this can be used for customer retention.
        - different_business_count : number of different business the customer has visited
                                     it captures how customers is using platform.
        - avg_rating : average rating given by the customer
        - min_stars : minimum rating given by the customer
        - max_stars : maximum rating given by the customer

    """
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


from sqlalchemy import create_engine

account_url = "https://fx34478.us-central1.gcp.snowflakecomputing.com"
organization = "ESODLJG"
account = "RU55705"
email = "data228.project@gmail.com"

snowflake_options = {
    "sfURL": "https://fx34478.us-central1.gcp.snowflakecomputing.com",
    "sfUser": "DATA228PROJECT",
    "sfPassword": "Project228",
    "sfWarehouse": "COMPUTE_WH",
    "sfDatabase": "data_228_project",
    "sfSchema": "yelp",
    "sfTable": "test",
    "dbtable": "test"
}


def get_engine():
    engine = create_engine(
        'snowflake://{user}:{password}@{account}'.format(user=snowflake_options["sfUser"],
                                                         password=snowflake_options["sfPassword"],
                                                         account=account)
    )
    return engine


def save_spark_df_to_db(df, table):
    current_conf = snowflake_options
    current_conf['dbtable'] = table
    current_conf['sfTable'] = table
    (
        df.write.format("snowflake")
        .options(**current_conf)
        .mode("overwrite")
        .save(table)
    )



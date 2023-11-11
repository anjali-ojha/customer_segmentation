# Customer Segmentation using Yelp Review Data

### Package Dependency
``` shell
pip install sqlalchemy
pip install ipynb
pip install snowflake-connector-python
pip install snowflake-sqlalchemy
pip install tqdm
pip install nltk
pip install wordCloud

```


### Snowflake connection
```
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
    "dbtable" : "test"
}
```
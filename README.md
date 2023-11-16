# Customer Segmentation using Yelp Review Data


# GIT usage
```
    1 -  clone the repo by using command
             git clone https://github.com/anjali-ojha/customer_segmentation.git
             git checkout miain 
             git pull origin main
             git checkout -b <your_feature_branch_name>  # this will help you to keep the changes in your branch

    2 -  make you code changes ,  once you are done
            git add --all
            git commit -m "message for the commit"
            git push origin <your_feature_branch_name>

    3 -  crate pull request from the UI.
    4 -  Some one will review and merge it.
    
```





### Package Dependency
``` shell
pip install sqlalchemy
pip install ipynb
pip install snowflake-connector-python
pip install snowflake-sqlalchemy
pip install tqdm
pip install nltk
pip install wordCloud

pip install streamlit
pip install watchdog
pip install yfinance

# for this we need python 3.8+
pip install snowflake-snowpark


```


### Snowflake connection
```
account_url = "https://fx34478.us-central1.gcp.snowflakecomputing.com"
organization = "ESODLJG"
account = "RU55705"
email = "data228.project@gmail.com"

snowflake_options = {
    "sfUser": "DATA228PROJECT",
    "sfWarehouse": "COMPUTE_WH",
    "sfDatabase": "data_228_project",
    "sfSchema": "yelp",
    "sfTable": "test",
    "dbtable" : "test"
}
```

### StreamLit App

https://yelp-customer-segmentation.streamlit.app/

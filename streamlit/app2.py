import streamlit as st
import snowflake.connector
import pandas as pd

# Snowflake connection parameters
snowflake_credentials = {

    "account": "ESODLJG-RU55705",
    "user": "DATA228PROJECT",
    "password": "Project228",
    "database": "data_228_project",  # optional
    "schema": "yelp",  # optional
}

# Function to execute SQL query on Snowflake
def execute_query(query):
    with snowflake.connector.connect(**snowflake_credentials) as con:
        with con.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result_df = pd.DataFrame(rows, columns=columns)
    return result_df

# Streamlit app
def main():
    st.title("Snowflake SQL Generator")

    # # Input form for Snowflake connection details
    # st.sidebar.header("Snowflake Connection Details")
    # user = st.sidebar.text_input("Username", "")
    # password = st.sidebar.text_input("Password", "", type="password")
    # account = st.sidebar.text_input("Account URL", "")
    # warehouse = st.sidebar.text_input("Warehouse", "")
    # database = st.sidebar.text_input("Database", "")
    # schema = st.sidebar.text_input("Schema", "")

    # Input form for SQL query filters
    st.sidebar.header("SQL Query Filters")
    table_name = st.sidebar.text_input("Table Name", "")
    filter_column = st.sidebar.text_input("Filter Column", "")
    filter_value = st.sidebar.text_input("Filter Value", "")

    # Connect to Snowflake when the "Connect" button is clicked
    if st.sidebar.button("Connect"):
        snowflake_credentials["user"] = user
        snowflake_credentials["password"] = password
        snowflake_credentials["account"] = account
        snowflake_credentials["warehouse"] = warehouse
        snowflake_credentials["database"] = database
        snowflake_credentials["schema"] = schema

        st.sidebar.success("Connected to Snowflake!")

    # Display filtered data and generate SQL query
    if st.sidebar.button("Generate SQL"):
        # Build and execute the SQL query
        sql_query = f"SELECT * FROM {table_name} WHERE {filter_column} = '{filter_value}'"
        result_df = execute_query(sql_query)

        # Display the result DataFrame
        st.write("Filtered Data:")
        st.write(result_df)

        # Display the generated SQL query
        st.subheader("Generated SQL Query:")
        st.code(sql_query, language="sql")

if __name__ == "__main__":
    main()

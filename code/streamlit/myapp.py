"""
Copyright (c) 2022 Snowflake Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.table import Table
from toolz.itertoolz import pluck

from snowflake.snowpark.functions import col, max, lit, array_contains, cast


from lib.chart_helpers import mk_labels, mk_links
from lib.filterwidget import MyFilter

account_url = "https://fx34478.us-central1.gcp.snowflakecomputing.com"
organization = "ESODLJG"
account = "RU55705"
email = "data228.project@gmail.com"

connection_parameters = {
    "account": "ESODLJG-RU55705",
    "user": "DATA228PROJECT",
    "password": "Project228",
    "database": "data_228_project",  # optional
    "schema": "yelp",  # optional
}


MY_TABLE = "USERS"


def _get_active_filters() -> filter:
    return filter(lambda _: _.is_enabled, st.session_state.filters)


def _is_any_filter_enabled() -> bool:
    return any(pluck("is_enabled", st.session_state.filters))


def _get_human_filter_names(_iter: Iterable) -> Iterable:
    return pluck("human_name", _iter)


# Initialize connection.
def init_connection() -> Session:
    return Session.builder.configs(connection_parameters).create()


@st.cache_data
def convert_df(df: pd.DataFrame):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


def draw_sidebar():
    """Should include dynamically generated filters"""

    with st.sidebar:
        selected_filters = st.multiselect(
            "Select which filters to enable",
            list(_get_human_filter_names(st.session_state.filters)),
            list(_get_human_filter_names(st.session_state.filters)),
        )
        for _f in st.session_state.filters:
            if _f.human_name in selected_filters:
                _f.enable()

        if _is_any_filter_enabled():
            with st.form(key="input_form"):

                for _f in _get_active_filters():
                    _f.create_widget()
                st.session_state.clicked = st.form_submit_button(label="Submit")
        else:
            st.write("Please enable a filter")


def draw_table_data(table_sequence):
    st.header("Dataframe preview")
    print("table_sequence = ", type(table_sequence[-1]), table_sequence[-1].dtypes)
    table_sequence[-1].show()
    # print(table_sequence[-1].select("elite").dtypes)
    # a = table_sequence[-1]
    # a["elite"] = a["elite"].astype(str)
    st.write(table_sequence[-1].sample(n=5).to_pandas().head())


def draw_table_query_sequence(table_sequence: list):
    # Add the SQL statement sequence table
    statement_sequence = """
| number | filter name | query, transformation |
| ------ | ----------- | --------------------- |"""
    st.header("Statement sequence")
    statments = []
    for number, (_label, _table) in enumerate(
            zip(mk_labels(_get_human_filter_names(_get_active_filters())), table_sequence)):
        statments.append(f"""\n| {number+1} | {_label} | ```{_table.queries['queries'][0]}``` |""")

    statement_sequence += "".join(statments[::-1])

    st.markdown(statement_sequence)

    # Materialize the result <=> the button was clicked
    if st.session_state.clicked:
        with st.spinner("Converting results..."):
            st.download_button(
                label="Download as CSV",
                data=convert_df(table_sequence[-1].to_pandas()),
                file_name="customers.csv",
                mime="text/csv",
            )


def draw_main_ui(_session: Session):
    """Contains the logic and the presentation of main section of UI"""
    if _is_any_filter_enabled():

        customers: Table = _session.table(MY_TABLE)
        table_sequence = [customers]

        # _f: MyFilter
        for _f in _get_active_filters():
            # This block generates the sequence of dataframes as continually applying AND filter set by the sidebar
            # The dataframes are to be used in the Sankey chart.

            # First, get the last dataframe in the list
            last_table = table_sequence[-1]
            # Apply the current filter to it
            print(_f)

            new_table = last_table[
                # At this point the filter will be dynamically applied to the dataframe using the API from MyFilter
                _f(last_table)
            ]
            table_sequence += [new_table]
            print(type(table_sequence[-1]))

        draw_table_data(table_sequence)
        draw_table_query_sequence(table_sequence)

    else:
        st.write("Please enable a filter in the sidebar to show transformations")


if __name__ == "__main__":
    # Initialize the filters
    session = init_connection()
    MyFilter.session = session
    MyFilter.table_name = MY_TABLE

    st.session_state.filters = (
        MyFilter(
            human_name="FRIENDS_COUNT",
            table_column="FRIENDS_COUNT",
            widget_id="FRIENDS_COUNT_slider",
            widget_type=st.select_slider,
        ),
        MyFilter(
            human_name="Average Stars",
            table_column="AVERAGE_STARS",
            widget_id="AVERAGE_STARS_slider",
            widget_type=st.select_slider,
        ),
        # MyFilter(
        #     human_name="ELITE customer",
        #     table_column="elite",
        #     widget_id="elite_select",
        #     widget_type=st.multiselect,
        # )
    )

    draw_sidebar()
    draw_main_ui(session)
#%%

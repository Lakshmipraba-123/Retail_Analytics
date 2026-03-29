import streamlit as st
import snowflake.connector
import pandas as pd

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account   = "jo29319.ap-southeast-1",
        user      = "Lipa2312",        # replace with your actual username
        password  = "K2iMAvwG42AKZVW",        # replace with your actual password
        warehouse = "RETAIL_WH",
        database  = "RETAIL_ANALYTICS",
        schema    = "ANALYTICS",
    )

@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows    = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)
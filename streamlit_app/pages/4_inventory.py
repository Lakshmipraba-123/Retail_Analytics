import streamlit as st
import plotly.express as px
from snowflake_conn import run_query

st.title("Inventory alerts")

df = run_query("""
    SELECT
        PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND,
        STOCK_QUANTITY, PRICE_USD, AVG_RATING
    FROM DIM_PRODUCTS
    ORDER BY STOCK_QUANTITY ASC
""")

col1, col2, col3 = st.columns(3)
col1.metric("Out of stock",  int((df["STOCK_QUANTITY"] == 0).sum()))
col2.metric("Low stock (<10)", int((df["STOCK_QUANTITY"] < 10).sum()))
col3.metric("Healthy stock",  int((df["STOCK_QUANTITY"] >= 10).sum()))

st.markdown("---")

tab1, tab2, tab3 = st.tabs(["Out of stock", "Low stock", "All products"])

with tab1:
    st.dataframe(
        df[df["STOCK_QUANTITY"] == 0].reset_index(drop=True),
        use_container_width=True
    )

with tab2:
    low = df[(df["STOCK_QUANTITY"] > 0) & (df["STOCK_QUANTITY"] < 10)]
    st.dataframe(low.reset_index(drop=True), use_container_width=True)

with tab3:
    fig = px.histogram(df, x="STOCK_QUANTITY", nbins=30,
                       color_discrete_sequence=["#1D9E75"])
    fig.update_layout(xaxis_title="Stock quantity", yaxis_title="Products")
    st.plotly_chart(fig, use_container_width=True)
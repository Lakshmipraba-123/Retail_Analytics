import streamlit as st
from snowflake_conn import run_query

st.title("Product lookup")

search = st.text_input("Search product name or ID", placeholder="e.g. laptop, 42")

if search:
    df = run_query(f"""
        SELECT
            p.PRODUCT_ID,
            p.PRODUCT_NAME,
            p.CATEGORY,
            p.BRAND,
            p.PRICE_USD,
            p.DISCOUNT_PCT,
            p.AVG_RATING,
            p.STOCK_QUANTITY,
            m.PREDICTED_UNITS,
            m.ACTUAL_UNITS,
            CASE
                WHEN ABS(m.PREDICTED_UNITS - m.ACTUAL_UNITS) <= 5  THEN 'excellent'
                WHEN ABS(m.PREDICTED_UNITS - m.ACTUAL_UNITS) <= 15 THEN 'good'
                ELSE 'needs_review'
            END AS ACCURACY_BAND,
            ROUND((m.PREDICTED_UNITS - m.ACTUAL_UNITS)
                  / NULLIF(m.ACTUAL_UNITS, 0) * 100, 1) AS PCT_ERROR
        FROM DIM_PRODUCTS p
        LEFT JOIN ML_PREDICTIONS m ON p.PRODUCT_ID = m.PRODUCT_ID
        WHERE LOWER(p.PRODUCT_NAME) LIKE '%{search.lower()}%'
           OR CAST(p.PRODUCT_ID AS VARCHAR) = '{search}'
        LIMIT 20
    """)

    if df.empty:
        st.warning("No products found.")
    else:
        for _, row in df.iterrows():
            with st.expander(f"{row['PRODUCT_NAME']}  —  {row['CATEGORY']}"):
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Price",    f"${row['PRICE_USD']}")
                col2.metric("Discount", f"{row['DISCOUNT_PCT']}%")
                col3.metric("Rating",   row['AVG_RATING'])
                col4.metric("Stock",    int(row['STOCK_QUANTITY']))

                if row['PREDICTED_UNITS'] is not None:
                    col5, col6, col7 = st.columns(3)
                    col5.metric("Predicted units", row['PREDICTED_UNITS'])
                    col6.metric("Actual units",    row['ACTUAL_UNITS'])
                    col7.metric("Accuracy band",   row['ACCURACY_BAND'])
                else:
                    st.info("No ML prediction available for this product.")
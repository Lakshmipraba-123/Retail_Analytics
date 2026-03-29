import streamlit as st
import plotly.express as px
from snowflake_conn import run_query

st.title("Overview")

# ── KPI metrics ───────────────────────────────────────────────────────────────
kpi = run_query("""
    SELECT
        COUNT(DISTINCT ORDER_ID)                        AS total_orders,
        COUNT(DISTINCT PRODUCT_ID)                      AS total_products,
        ROUND(SUM(LINE_TOTAL_USD), 2)                   AS total_revenue,
        ROUND(AVG(DISCOUNT_PCT), 1)                     AS avg_discount_pct
    FROM FACT_ORDERS
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total orders",    int(kpi["TOTAL_ORDERS"][0]))
col2.metric("Total products",  int(kpi["TOTAL_PRODUCTS"][0]))
col3.metric("Total revenue",   f"${kpi['TOTAL_REVENUE'][0]:,.2f}")
col4.metric("Avg discount",    f"{kpi['AVG_DISCOUNT_PCT'][0]}%")

st.markdown("---")

# ── Revenue by category bar chart ────────────────────────────────────────────
st.subheader("Revenue by category")
df = run_query("""
    SELECT
        p.CATEGORY,
        ROUND(SUM(f.LINE_TOTAL_USD), 2) AS revenue
    FROM FACT_ORDERS f
    JOIN DIM_PRODUCTS p ON f.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY p.CATEGORY
    ORDER BY revenue DESC
""")

fig = px.bar(
    df, x="CATEGORY", y="REVENUE",
    color="REVENUE",
    color_continuous_scale="teal",
    labels={"CATEGORY": "Category", "REVENUE": "Revenue (USD)"},
)
fig.update_layout(showlegend=False, xaxis_tickangle=-45)
st.plotly_chart(fig, use_container_width=True)
import streamlit as st
import plotly.express as px
from snowflake_conn import run_query

st.title("Category analysis")

df = run_query("""
    SELECT
        p.CATEGORY,
        COUNT(DISTINCT f.ORDER_ID)               AS ORDERS,
        SUM(f.QUANTITY_ORDERED)                  AS UNITS_SOLD,
        ROUND(SUM(f.LINE_TOTAL_USD), 2)          AS REVENUE,
        ROUND(AVG(COALESCE(p.PRICE_USD, 0)), 2)      AS AVG_PRICE,
        ROUND(AVG(COALESCE(p.DISCOUNT_PCT, 0)), 1)   AS AVG_DISCOUNT,
        ROUND(AVG(COALESCE(p.AVG_RATING, 0)), 2)     AS AVG_RATING
    FROM FACT_ORDERS f
    JOIN DIM_PRODUCTS p ON f.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY p.CATEGORY
    ORDER BY REVENUE DESC
""")

# ── KPI metrics ───────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
col1.metric("Total categories", len(df))
col2.metric("Total revenue",    f"${df['REVENUE'].sum():,.2f}")
col3.metric("Total units sold", int(df['UNITS_SOLD'].sum()))

st.markdown("---")

# ── Revenue by category bar chart ────────────────────────────────────────────
st.subheader("Revenue by category")
fig1 = px.bar(
    df,
    x="CATEGORY", y="REVENUE",
    color="REVENUE",
    color_continuous_scale="teal",
    labels={"CATEGORY": "Category", "REVENUE": "Revenue (USD)"},
)
fig1.update_layout(showlegend=False, xaxis_tickangle=-45)
st.plotly_chart(fig1, use_container_width=True)

# ── Scatter: revenue vs avg rating sized by units sold ────────────────────────
st.subheader("Revenue vs rating")
fig2 = px.scatter(
    df,
    x="AVG_RATING", y="REVENUE",
    size="UNITS_SOLD", color="CATEGORY",
    hover_name="CATEGORY",
    labels={"AVG_RATING": "Avg rating", "REVENUE": "Revenue (USD)"},
)
st.plotly_chart(fig2, use_container_width=True)

# ── Units sold by category ────────────────────────────────────────────────────
st.subheader("Units sold by category")
fig3 = px.bar(
    df.sort_values("UNITS_SOLD", ascending=True),
    x="UNITS_SOLD", y="CATEGORY",
    orientation="h",
    color="UNITS_SOLD",
    color_continuous_scale="teal",
    labels={"UNITS_SOLD": "Units sold", "CATEGORY": ""},
)
fig3.update_layout(showlegend=False)
st.plotly_chart(fig3, use_container_width=True)

# ── Full table ────────────────────────────────────────────────────────────────
st.subheader("Category breakdown")
st.dataframe(df, use_container_width=True)
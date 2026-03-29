import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from snowflake_conn import run_query

st.title("ML Predictions")

df = run_query("""
    SELECT
        CATEGORY,
        ROUND(AVG(PREDICTED_UNITS), 2)                     AS AVG_PREDICTED,
        ROUND(AVG(ACTUAL_UNITS), 2)                        AS AVG_ACTUAL,
        ROUND(AVG(ABS(PREDICTED_UNITS - ACTUAL_UNITS)), 2) AS AVG_ERROR,
        COUNT(*)                                           AS PRODUCT_COUNT
    FROM ML_PREDICTIONS
    GROUP BY CATEGORY
    ORDER BY AVG_PREDICTED DESC
""")

metrics = run_query("""
    SELECT
        ROUND(AVG(MAE), 4)      AS MAE,
        ROUND(AVG(R2_SCORE), 4) AS R2
    FROM ML_PREDICTIONS
""")

# ── Metrics banner ────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
col1.metric("Model MAE",  metrics["MAE"][0])
col2.metric("Model R²",   metrics["R2"][0])
col3.metric("Categories", len(df))

st.markdown("---")

# ── Predicted vs actual grouped bar ──────────────────────────────────────────
st.subheader("Predicted vs actual units — by category")
fig = go.Figure()
fig.add_trace(go.Bar(
    name="Predicted",
    x=df["CATEGORY"],
    y=df["AVG_PREDICTED"],
    marker_color="#1D9E75"
))
fig.add_trace(go.Bar(
    name="Actual",
    x=df["CATEGORY"],
    y=df["AVG_ACTUAL"],
    marker_color="#378ADD"
))
fig.update_layout(barmode="group", xaxis_tickangle=-45)
st.plotly_chart(fig, use_container_width=True)

# ── Accuracy band — computed on the fly ──────────────────────────────────────
st.subheader("Prediction accuracy breakdown")
band_df = run_query("""
    SELECT
        CASE
            WHEN ABS(PREDICTED_UNITS - ACTUAL_UNITS) <= 5  THEN 'excellent'
            WHEN ABS(PREDICTED_UNITS - ACTUAL_UNITS) <= 15 THEN 'good'
            ELSE 'needs_review'
        END AS ACCURACY_BAND,
        COUNT(*) AS PRODUCTS
    FROM ML_PREDICTIONS
    GROUP BY ACCURACY_BAND
""")
fig2 = px.pie(
    band_df,
    names="ACCURACY_BAND",
    values="PRODUCTS",
    color="ACCURACY_BAND",
    color_discrete_map={
        "excellent":    "#1D9E75",
        "good":         "#378ADD",
        "needs_review": "#D85A30",
    }
)
st.plotly_chart(fig2, use_container_width=True)

# ── Error by category ─────────────────────────────────────────────────────────
st.subheader("Average prediction error by category")
fig3 = px.bar(
    df.sort_values("AVG_ERROR"),
    x="AVG_ERROR",
    y="CATEGORY",
    orientation="h",
    color="AVG_ERROR",
    color_continuous_scale="reds",
    labels={"AVG_ERROR": "Avg error (units)", "CATEGORY": ""},
)
fig3.update_layout(showlegend=False)
st.plotly_chart(fig3, use_container_width=True)

# ── Raw table ─────────────────────────────────────────────────────────────────
st.subheader("Full predictions table")
st.dataframe(df, use_container_width=True)
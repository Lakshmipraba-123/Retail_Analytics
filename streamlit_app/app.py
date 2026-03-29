import streamlit as st

st.set_page_config(
    page_title = "Retail Analytics",
    page_icon  = "📦",
    layout     = "wide",
)

st.sidebar.title("📦 Retail Analytics")
st.sidebar.markdown("---")
st.sidebar.markdown("**Data source:** Snowflake `RETAIL_ANALYTICS`")
st.sidebar.markdown("**Model:** RandomForest · Phase 5")

st.title("Welcome to Retail Analytics Platform")
st.markdown("""
Use the sidebar to navigate between pages:
- **Overview** — high level KPIs
- **ML Predictions** — forecast vs actual
- **Category** — revenue breakdown
- **Inventory** — stock alerts
- **Product** — individual product lookup
""")
# Retail Analytics Data Platform

A production-grade end-to-end data engineering project built with
industry-standard tools used at companies like Uber, Airbnb, and Netflix.

---

## Architecture

```
REST API → Python ETL → Apache Airflow → PySpark → Snowflake → Streamlit Dashboard
                                                       ↑
                                               dbt transformations
                                                       ↑
                                            Jenkins CI/CD pipeline
                                                       ↑
                                          ML Forecasting (scikit-learn)
                                          Predictions written back to Snowflake
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.13, REST APIs, pandas |
| Orchestration | Apache Airflow (Docker) |
| Big data processing | PySpark (Java 17) |
| Data warehouse | Snowflake (ap-southeast-1) |
| Data transformation | dbt Core |
| CI/CD | Jenkins |
| Machine learning | scikit-learn — RandomForestRegressor |
| Visualization | Streamlit + Plotly |
| Containerization | Docker |
| Testing | pytest (8 tests) |
| Version control | Git + GitHub |

---

## Project structure

```
retail_analytics/
│
├── dags/
│   └── retail_pipeline.py          Airflow DAG — daily ETL at 6am
│
├── etl/
│   ├── extract_products.py         Products ingestion from DummyJSON API
│   └── extract_orders.py           Orders ingestion from DummyJSON API
│
├── spark/
│   └── jobs/
│       └── pyspark_retail.py       4 PySpark analyses
│
├── ml_forecast.py                  ML training + Snowflake writeback
│
├── streamlit_app/
│   ├── app.py                      Dashboard entry point
│   ├── snowflake_conn.py           Shared Snowflake connection + query cache
│   └── pages/
│       ├── 1_overview.py           KPIs + revenue by category
│       ├── 2_predictions.py        ML predicted vs actual + accuracy breakdown
│       ├── 3_category.py           Category revenue + scatter + units sold
│       ├── 4_inventory.py          Out of stock + low stock alerts
│       └── 5_product.py            Product search + ML prediction lookup
│
├── dbt/
│   └── models/
│       ├── sources.yml
│       ├── category_revenue.sql
│       ├── product_performance.sql
│       └── ml_predictions.sql
│
├── tests/
│   ├── test_products.py            5 ETL unit tests
│   └── test_ml.py                  3 ML unit tests
│
├── Jenkinsfile                     4-stage CI/CD pipeline
├── conftest.py                     pytest path configuration
├── requirements.txt
└── README.md
```

---

## Data pipeline

### Phase 1 — Ingestion
- Extracts product catalogue and order data from DummyJSON REST API
- Implements pagination to fetch all available records
- Cleans and transforms raw JSON into structured CSV using pandas
- Scheduled daily at 6am via Apache Airflow running in Docker containers

### Phase 2 — Processing
- Loads cleaned CSVs into PySpark for distributed processing
- Produces 4 business analytics outputs:
  - Revenue by product category
  - Dead inventory identification
  - Discount impact analysis
  - Order revenue by category

### Phase 3 — Warehousing
Star schema in `RETAIL_ANALYTICS.ANALYTICS`:

| Object | Type | Rows |
|---|---|---|
| `FACT_ORDERS` | Table | 198 |
| `DIM_PRODUCTS` | Table | 194 |
| `AGG_CATEGORY_REVENUE` | Table | — |
| `AGG_DEAD_INVENTORY` | Table | — |
| `AGG_DISCOUNT_IMPACT` | Table | — |
| `AGG_ORDER_ANALYSIS` | Table | — |
| 3 analytical views | Views | — |

### Phase 4 — CI/CD + dbt
Jenkins pipeline with 4 stages:

```
Stage 1: Test         pytest — 8 tests must pass
Stage 2: Deploy ETL   push ETL scripts
Stage 3: Run dbt      3 models (category_revenue, product_performance, ml_predictions)
Stage 4: Run ML       train model + write predictions to Snowflake
```

### Phase 5 — Machine learning
- Model: `RandomForestRegressor` (200 trees, max_depth=10, 5-fold CV)
- Target variable: units sold per product
- 20 engineered features including price tiers, discount rates, stock ratios, and category aggregations
- Predictions written back to `ANALYTICS.ML_PREDICTIONS` (194 rows)
- Artifacts archived by Jenkins: `model.pkl`, `metrics.json`, `feature_importance.png`

**Model performance:**

| Metric | Value |
|---|---|
| MAE | ~3.2 units |
| R² | ~0.81 |
| CV R² (5-fold) | ~0.77 ± 0.04 |
| Categories predicted | 24 |

### Phase 6 — Streamlit dashboard
Five live pages connected directly to Snowflake:

| Page | Content |
|---|---|
| Overview | Total orders, revenue, products, avg discount + bar chart |
| ML Predictions | Predicted vs actual by category + accuracy pie chart |
| Category | Revenue scatter, units sold horizontal bar, full table |
| Inventory | Out of stock + low stock alerts with product details |
| Product | Search by name or ID, see full ML prediction + accuracy band |

---

## Key results
- 194 products and 198 order lines ingested, processed, and warehoused
- RandomForest model achieves R² of ~0.81 predicting units sold per product
- Identified top revenue categories and dead inventory products
- Fully automated pipeline requiring zero manual intervention
- 8 unit tests covering ETL and ML functions

---

## Setup

### Prerequisites
- Python 3.13
- Java 17 (for PySpark)
- Docker Desktop
- Snowflake account
- Jenkins

### Install dependencies

```bash
pip install streamlit plotly pandas snowflake-connector-python[pandas] \
            scikit-learn python-dotenv matplotlib apache-airflow pyspark
```

### Environment variables

Create a `.env` file in the project root:

```
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

### How to run

```bash
# Start Airflow
cd C:\retail_analytics
docker compose up -d

# Run PySpark analysis
python spark/jobs/pyspark_retail.py

# Run ML forecast
python ml_forecast.py

# Run dbt transformations
dbt run --profiles-dir "C:\Users\YourUser\.dbt"

# Run tests
pytest tests/ -v

# Launch dashboard
cd streamlit_app
streamlit run app.py
```

---

## Test results

```
tests/test_products.py::test_extract_products_columns     PASSED
tests/test_products.py::test_extract_products_not_empty   PASSED
tests/test_products.py::test_pagination                   PASSED
tests/test_products.py::test_flatten_nested_json          PASSED
tests/test_products.py::test_csv_output                   PASSED
tests/test_ml.py::test_engineer_features_shape            PASSED
tests/test_ml.py::test_engineer_features_no_nulls         PASSED
tests/test_ml.py::test_model_trains_and_predicts          PASSED

======= 8 passed ===============
```

---

## Key design decisions

**Idempotent ML runs** — `ML_PREDICTIONS` is truncated and reloaded on every Jenkins run so daily executions never produce duplicate rows.

**Snowflake-native connector** — uses `cursor.execute()` + `fetchall()` instead of `pd.read_sql()` to avoid SQLAlchemy dependency and work natively with the Snowflake Python connector.

**dbt views over tables** — `ml_predictions` dbt model is materialised as a view so it always reflects the latest `ML_PREDICTIONS` table data without a separate refresh step.

**Feature leakage awareness** — category and brand aggregation features are computed from the full dataset. Acceptable for daily batch retraining; would need fold-level computation for an online learning setup.

---

## Author

Built by Lakshmi Praba as a portfolio data engineering project covering the full modern data stack from ingestion to live dashboard.
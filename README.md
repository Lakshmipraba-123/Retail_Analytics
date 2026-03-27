# Retail Analytics Data Platform

A production-grade end-to-end data engineering project built with 
industry-standard tools used at companies like Uber, Airbnb, and Netflix.

## Architecture
```
REST API → Python ETL → Apache Airflow → PySpark → Snowflake → Power BI
                                                 ↑
                                          dbt transformations
                                                 ↑
                                        Jenkins CI/CD pipeline
```

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, REST APIs, pandas |
| Orchestration | Apache Airflow |
| Big Data Processing | PySpark |
| Data Warehouse | Snowflake |
| Data Transformation | dbt |
| CI/CD | Jenkins |
| Visualization | Power BI |
| Containerization | Docker |
| Version Control | Git + GitHub |

## Project Structure
```
retail_analytics/
├── dags/                    # Airflow DAG definitions
│   └── retail_pipeline.py   # Daily ETL schedule
├── etl/                     # Python ETL scripts
│   ├── extract_products.py  # Products ingestion
│   └── extract_orders.py    # Orders ingestion
├── spark/
│   └── jobs/                # PySpark transformation jobs
│       └── pyspark_retail.py
├── dbt_retail/              # dbt models and tests
│   ├── models/
│   └── tests/
├── jenkins/                 # CI/CD pipeline definition
│   └── Jenkinsfile
├── tests/                   # Unit tests for ETL scripts
│   ├── test_products.py
│   └── test_orders.py
└── README.md
```

## Data Pipeline

### Phase 1 — Ingestion
- Extracts product catalogue and order data from DummyJSON REST API
- Implements pagination to fetch all available records
- Cleans and transforms raw JSON into structured CSV using pandas
- Scheduled daily at 6am via Apache Airflow

### Phase 2 — Processing  
- Loads cleaned data into PySpark for distributed processing
- Produces 4 business analytics outputs:
  - Revenue by product category
  - Dead inventory identification
  - Discount impact analysis
  - Order revenue by category

### Phase 3 — Warehousing
- Loads all data into Snowflake cloud data warehouse
- Star Schema design: 1 fact table + 3 dimension tables
- 3 analytical views ready for BI consumption

### Phase 4 — CI/CD
- Jenkins pipeline triggers on every GitHub push
- Runs automated unit tests on ETL scripts
- Deploys dbt models to Snowflake if tests pass

## Key Results
- 194 products and 198 order lines ingested and warehoused
- Identified top revenue categories and dead inventory products
- Fully automated pipeline requiring zero manual intervention

## How to Run
```bash
# Start Airflow
cd C:\retail_analytics
docker compose up -d

# Run PySpark job
cd spark/jobs
python pyspark_retail.py

# Run dbt transformations
cd dbt_retail
dbt run
```
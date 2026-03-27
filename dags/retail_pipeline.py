from airflow import DAG #define_pipeline
from airflow.operators.python import PythonOperator #execute_python_tasks
from datetime import datetime, timedelta #handle_time_and_schedule
import sys #handle_time_and_schedule

sys.path.insert(0, "/opt/airflow/etl")  #ETL scripts live( Docker container path)

from extract_products import run_products_etl
from extract_orders   import run_orders_etl # Importing the main functions from two ETL scripts

# ── DEFAULT SETTINGS FOR ALL TASKS ─────────────────────────

default_args = {
    "owner":            "you",       # who owns this pipeline
    "retries":          1,           # if a task fails, retry once
    "retry_delay":      timedelta(minutes=5),  # wait 5 mins before retrying
    "email_on_failure": False,       # we'll add email alerts later
}

# ── DEFINE THE DAG ──────────────────────────────────────────

with DAG(
    dag_id="retail_pipeline",          # name shown in Airflow UI
    description="Daily retail ETL — products and orders",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),   # Airflow uses this as reference point
    schedule_interval="0 6 * * *",    # run every day at 6:00 AM
    catchup=False,                     # don't run for past dates we missed
    tags=["retail", "phase1"],         # labels to find it in the UI
) as dag:
    # ── TASK 1: Extract Products ────────────────────────────
    task_products = PythonOperator(
        task_id="extract_products",        # name shown in Airflow UI
        python_callable=run_products_etl,  # which function to call
    )

    # ── TASK 2: Extract Orders ──────────────────────────────
    task_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=run_orders_etl,
    )

    # ── TASK 3: Log Summary ─────────────────────────────────
    # A simple function to confirm both tasks completed
    def log_summary():
        print("=" * 50)
        print("DAILY RETAIL PIPELINE COMPLETED")
        print(f"Finished at: {datetime.now()}")
        print("Products and orders data refreshed successfully")
        print("=" * 50)

    task_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )
    [task_products, task_orders] >> task_summary
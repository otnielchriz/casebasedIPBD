from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# =========================
# 🔥 TAMBAH PATH SCRAPERS
# =========================
sys.path.append('/opt/airflow/scrapers')

# =========================
# IMPORT FUNCTION
# =========================
from load_pendapatan_warkop import (
    aggregate_income,
    load_income_to_postgres
)

# =========================
# CONFIG DAG
# =========================
default_args = {
    "owner": "warkop_kusuma",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pendapatan_warkop_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["ipbd", "pendapatan", "etl"],
) as dag:

    # =========================
    # TASK 1: AGREGASI
    # =========================
    task_aggregate_income = PythonOperator(
        task_id="aggregate_income_daily",
        python_callable=aggregate_income,
    )

    # =========================
    # TASK 2: LOAD KE DB
    # =========================
    task_load_income = PythonOperator(
        task_id="load_income_to_postgres",
        python_callable=load_income_to_postgres,
    )

    # =========================
    # FLOW
    # =========================
    task_aggregate_income >> task_load_income
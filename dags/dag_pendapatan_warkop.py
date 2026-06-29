from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

SCRAPERS_PATH = "/opt/airflow/scripts/scrapers"

if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from load_pendapatan_warkop import (
    aggregate_income,
    load_income_to_postgres
)

default_args = {
    "owner": "warkop_kusuma",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pendapatan_warkop_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["ipbd", "pendapatan", "etl"],
) as dag:

    task_aggregate_income = PythonOperator(
        task_id="aggregate_income_daily",
        python_callable=aggregate_income,
    )

    task_load_income = PythonOperator(
        task_id="load_income_to_postgres",
        python_callable=load_income_to_postgres,
    )

    trigger_ml_pipeline = TriggerDagRunOperator(
        task_id="trigger_ml_pipeline",
        trigger_dag_id="ml_pipeline",
        wait_for_completion=False,
    )

    task_aggregate_income >> task_load_income >> trigger_ml_pipeline
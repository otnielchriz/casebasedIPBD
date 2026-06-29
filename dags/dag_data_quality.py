from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Path to scrapers
SCRAPERS_PATH = "/opt/airflow/scripts/scrapers"
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from telegram_alert import send_telegram_alert

default_args = {
    'owner': 'zaki',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': send_telegram_alert,
}

def execute_data_quality_validation():
    # Import inside the function to prevent top-level import timeout in Airflow scheduler
    from data_quality_check import run_data_quality_validation
    run_data_quality_validation()

with DAG(
    dag_id='data_quality_validation_pipeline',
    default_args=default_args,
    description='Pipeline untuk memvalidasi kualitas data (Data Governance) menggunakan Great Expectations',
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['governance', 'quality', 'validation', 'greatexpectations'],
) as dag:

    task_validate_data = PythonOperator(
        task_id='run_great_expectations_validation',
        python_callable=execute_data_quality_validation,
    )

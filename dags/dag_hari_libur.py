from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Path generator
GENERATORS_PATH = "/opt/airflow/scripts/generators"
if GENERATORS_PATH not in sys.path:
    sys.path.insert(0, GENERATORS_PATH)

from scrap_hari_libur import run_holiday_etl_airflow

default_args = {
    'owner': 'zaki',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='hari_libur_etl_pipeline',
    default_args=default_args,
    description='Pipeline untuk memproses dan mengunggah data kalender hari libur Indonesia',
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['libur', 'kalender', 'etl'],
) as dag:

    task_run_holiday_etl = PythonOperator(
        task_id='run_holiday_etl',
        python_callable=run_holiday_etl_airflow,
    )

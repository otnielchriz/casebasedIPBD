from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from telegram_alert import send_telegram_alert

def fail_task():
    raise ValueError("Simulasi error untuk pengujian notifikasi Telegram Airflow")

with DAG(
    dag_id='test_telegram_alert_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={'on_failure_callback': send_telegram_alert},
    tags=['test', 'telegram']
) as dag:
    
    test_fail = PythonOperator(
        task_id='simulate_task_failure',
        python_callable=fail_task
    )

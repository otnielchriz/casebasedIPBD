from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

from telegram_alert import send_telegram_alert

default_args = {
    'on_failure_callback': send_telegram_alert,
}

with DAG(
    dag_id="ml_pipeline",
    default_args=default_args,
    description="Retrain model dan generate prediksi pendapatan Warkop Kusuma",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["machine-learning", "kusuma"],
) as dag:

    train_model = BashOperator(
        task_id="train_model",
        bash_command="""
        cd /opt/airflow &&
        python scripts/mechine_learning/train_model.py
        """
    )

    predict_revenue = BashOperator(
        task_id="predict_revenue",
        bash_command="""
        cd /opt/airflow &&
        python scripts/mechine_learning/predict_to_postgres.py
        """
    )

    train_model >> predict_revenue
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ml_pipeline",
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
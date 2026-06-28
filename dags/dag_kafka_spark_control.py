from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'zaki',
    'retries': 0,
}

def log_instruction():
    print("============================================================")
    print("STATUS KONTROL STREAMING KAFKA & PYSPARK")
    print("============================================================")
    print("DAG ini berfungsi sebagai antarmuka untuk menyalakan servis streaming.")
    print("Pastikan container `warkop_kafka` dan `warkop_pyspark` telah berjalan via docker-compose.")

with DAG(
    dag_id='kafka_spark_streaming_control',
    default_args=default_args,
    description='Kontrol untuk menyalakan/mematikan simulasi streaming Kafka & PySpark.',
    schedule=None, # Triggered manually
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['streaming', 'kafka', 'pyspark', 'realtime'],
) as dag:

    instruction_task = PythonOperator(
        task_id='info_streaming',
        python_callable=log_instruction
    )

    # Menjalankan script Kafka Producer sebagai background process (daemon)
    start_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command='nohup python /opt/airflow/scripts/scrapers/kafka_producer_weather.py > /opt/airflow/logs/kafka_producer.log 2>&1 & echo "Producer started in background!"'
    )

    def start_consumer_via_api():
        import requests
        try:
            print("📡 Mengirim request untuk menjalankan PySpark Consumer...")
            res = requests.post("http://warkop_pyspark:5000/start", timeout=15)
            print(f"Response: {res.text}")
        except Exception as e:
            print(f"❌ Gagal memicu Spark Consumer melalui API: {e}")
            raise e

    start_consumer = PythonOperator(
        task_id='start_pyspark_consumer',
        python_callable=start_consumer_via_api
    )

    instruction_task >> start_producer >> start_consumer

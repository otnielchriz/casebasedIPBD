from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from telegram_alert import send_telegram_alert

default_args = {
    'owner': 'zaki',
    'retries': 0,
    'on_failure_callback': send_telegram_alert,
}

def log_instruction():
    print("============================================================")
    print("STATUS KONTROL STREAMING OPEN-METEO (10 MENIT)")
    print("============================================================")
    print("DAG ini mengaktifkan Producer dan Consumer untuk streaming cuaca Open-Meteo.")
    print("Data akan dikirim ke Kafka dan disimpan ke tabel `cuaca_stream_raw`.")

with DAG(
    dag_id='weather_stream_openmeteo_control',
    default_args=default_args,
    description='Kontrol untuk menyalakan/mematikan simulasi streaming Open-Meteo 10-menit.',
    schedule=None, # Manual trigger only
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['streaming', 'kafka', 'openmeteo', 'realtime'],
) as dag:

    instruction_task = PythonOperator(
        task_id='info_streaming',
        python_callable=log_instruction
    )

    # Menjalankan Open-Meteo Kafka Producer sebagai background process
    start_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command='nohup python /opt/airflow/scripts/producers/weather_producer.py > /opt/airflow/logs/kafka_producer.log 2>&1 & echo "Producer started"'
    )

    # Menjalankan Open-Meteo Kafka Consumer sebagai background process
    start_consumer = BashOperator(
        task_id='start_kafka_consumer',
        bash_command='nohup python /opt/airflow/scripts/consumers/weather_consumer.py > /opt/airflow/logs/kafka_consumer.log 2>&1 & echo "Consumer started"'
    )

    instruction_task >> start_producer >> start_consumer

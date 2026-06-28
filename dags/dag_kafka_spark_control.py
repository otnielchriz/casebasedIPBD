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

    # Karena PySpark menggunakan kontainer `bitnami/spark:3.4` yang terpisah,
    # Eksekusi Consumer sebaiknya dijalankan di dalam kontainer Spark tersebut.
    # BashOperator ini berfungsi mengirimkan sinyal eksekusi ke kontainer Spark (jika docker ada di env host,
    # namun karena ini dalam docker network, kita instruksikan user).
    # Namun jika Airflow Worker memiliki java, kita bisa menjalankannya juga.
    start_consumer_instruction = BashOperator(
        task_id='start_pyspark_consumer',
        bash_command='''
        echo "Untuk menyalakan PySpark Consumer, buka terminal lokal Anda dan jalankan:"
        echo "docker exec -it warkop_pyspark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/airflow/scrapers/pyspark_consumer_weather.py"
        echo "Lalu pantau hasilnya di http://localhost:4040"
        '''
    )

    instruction_task >> start_producer >> start_consumer_instruction

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# =========================
# PATH SCRAPER
# =========================
SCRAPERS_PATH = '/opt/airflow/scrapers'
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

# ⚠️ PASTIKAN INI SESUAI FILE KAMU
from scrap_weather_ncep import fetch_weather_forecast_and_load


# =========================
# LOAD FUNCTION
# =========================
def load_csv_to_postgres(**kwargs):

    tanggal_eksekusi = kwargs['logical_date'].strftime('%Y-%m-%d')

    file_path = f'/opt/airflow/data/raw/cuaca_warkop_{tanggal_eksekusi}.csv'

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    df = pd.read_csv(file_path)

    # =========================
    # CLEAN DATA TYPES
    # =========================
    df['waktu'] = pd.to_datetime(df['waktu'], errors='coerce')

    numeric_cols = [
        'kelembapan',
        'cloudiness',
        'suhu',
        'suhu_terasa',
        'kecepatan_angin',
        'curah_hujan'
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # =========================
    # POSTGRES
    # =========================
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')
    engine = pg_hook.get_sqlalchemy_engine()

    print(f"📊 Insert {len(df)} rows ke PostgreSQL")

    df.to_sql(
        'cuaca_historis',
        con=engine,
        if_exists='append',
        index=False
    )

    print("✅ SUCCESS insert ke cuaca_historis")


# =========================
# DAG CONFIG
# =========================
default_args = {
    'owner': 'zaki',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='weather_historical_catchup',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=True,
    tags=['cuaca', 'etl']
) as dag:

    # =========================
    # TASK 1: SCRAPE
    # =========================
    task_scrape_weather = PythonOperator(
        task_id='scrape_weather_daily',
        python_callable=fetch_weather_forecast_and_load,
    )

    # =========================
    # TASK 2: LOAD
    # =========================
    task_load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_csv_to_postgres,
    )

    # =========================
    # DEPENDENCY
    # =========================
    task_scrape_weather >> task_load_postgres
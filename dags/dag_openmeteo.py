from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# =========================
# 🔥 QUICK HACK IMPORT PATH
# =========================
SCRAPERS_PATH = '/opt/airflow/scrapers'
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

# ❗ IMPORT TANPA "scrapers."
from scrap_weather_ncep import fetch_weather_ncep


# =========================
# LOAD FUNCTION
# =========================
def load_csv_to_postgres(**kwargs):
    tanggal_eksekusi = kwargs.get('ds') or kwargs['logical_date'].strftime('%Y-%m-%d')

    file_path = f'/opt/airflow/data/raw/cuaca_warkop_{tanggal_eksekusi}.csv'

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"GAGAL: File {file_path} tidak ditemukan!")

    print(f"📂 Membaca data dari {file_path}")
    df = pd.read_csv(file_path)

    # =========================
    # TAMBAHAN FORMAT TIPE DATA
    # =========================
    df['waktu'] = pd.to_datetime(df['waktu'], errors='coerce')

    df['kelembapan'] = pd.to_numeric(df['kelembapan'], errors='coerce')
    df['cloudiness'] = pd.to_numeric(df['cloudiness'], errors='coerce')

    df['suhu'] = pd.to_numeric(df['suhu'], errors='coerce')
    df['suhu_terasa'] = pd.to_numeric(df['suhu_terasa'], errors='coerce')
    df['kecepatan_angin'] = pd.to_numeric(df['kecepatan_angin'], errors='coerce')
    df['curah_hujan'] = pd.to_numeric(df['curah_hujan'], errors='coerce')

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')
    engine = pg_hook.get_sqlalchemy_engine()

    print(f"📊 Insert {len(df)} rows ke PostgreSQL...")

    df.to_sql(
        'cuaca_historis',
        con=engine,
        if_exists='append',
        index=False
    )

    print("✅ Sukses load data ke PostgreSQL!")


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
    description='Tarik cuaca harian mulai April 2026',
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=True,
    tags=['cuaca', 'open-meteo', 'etl'],
) as dag:

    # =========================
    # TASK 1: SCRAPE
    # =========================
    task_scrape_weather = PythonOperator(
        task_id='scrape_weather_daily_task',
        python_callable=fetch_weather_ncep,
    )

    # =========================
    # TASK 2: LOAD
    # =========================
    task_load_postgres = PythonOperator(
        task_id='load_to_postgres_task',
        python_callable=load_csv_to_postgres,
    )

    task_scrape_weather >> task_load_postgres
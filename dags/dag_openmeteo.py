from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
from sqlalchemy import text

import pandas as pd
import os
import sys


SCRAPERS_PATH = "/opt/airflow/scripts/scrapers"

if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from scrap_weather_ncep import fetch_weather_forecast_to_csv

def load_csv_to_postgres(**kwargs):
    file_path = "/opt/airflow/data/raw/cuaca_warkop.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    df = pd.read_csv(file_path)

    df["waktu"] = pd.to_datetime(df["waktu"], errors="coerce")

    numeric_cols = [
        "weather_code",
        "kelembapan",
        "cloudiness",
        "suhu",
        "suhu_terasa",
        "kecepatan_angin",
        "curah_hujan"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["waktu"])

    start_time = df["waktu"].min()
    end_time = df["waktu"].max()

    pg_hook = PostgresHook(postgres_conn_id="postgres_traffic")
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM cuaca_forecast
            WHERE waktu BETWEEN :start_time AND :end_time
        """), {
            "start_time": start_time,
            "end_time": end_time
        })

    df.to_sql(
        "cuaca_forecast",
        con=engine,
        if_exists="append",
        index=False
    )

    print(f"SUCCESS insert {len(df)} rows ke cuaca_forecast")
    print(f"Range data: {start_time} sampai {end_time}")


default_args = {
    "owner": "zaki",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="weather_forecast_daily",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["cuaca", "forecast", "etl"]
) as dag:

    task_scrape_weather = PythonOperator(
        task_id="scrape_weather_to_csv",
        python_callable=fetch_weather_forecast_to_csv,
    )

    task_load_postgres = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )

    task_scrape_weather >> task_load_postgres
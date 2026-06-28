from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# =========================
# PATH SCRAPER
# =========================
SCRAPERS_PATH = '/opt/airflow/scripts/scrapers'
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from scrap_weather_additional import (
    fetch_weather_sebulan_lalu_and_load,
    fetch_weather_prediksi_2minggu_and_load
)

default_args = {
    'owner': 'zaki',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================
# DAG 1: HISTORICAL DATA 1 MONTH AGO
# Schedule: Weekly (or can be triggered manually)
# ============================================================
with DAG(
    dag_id='cuaca_sukoharjo_sebulan_lalu_pipeline',
    default_args=default_args,
    description='Scraping data historis cuaca 1 bulan lalu via Open-Meteo Archive API',
    schedule='@weekly',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['cuaca', 'etl', 'historis', 'openmeteo'],
) as dag1:

    task_fetch_historis = PythonOperator(
        task_id='fetch_sebulan_lalu',
        python_callable=fetch_weather_sebulan_lalu_and_load,
    )

# ============================================================
# DAG 2: FORECAST 14 DAYS AHEAD
# Schedule: Daily
# ============================================================
with DAG(
    dag_id='cuaca_sukoharjo_prediksi_2minggu_pipeline',
    default_args=default_args,
    description='Scraping prediksi cuaca 14 hari ke depan via Open-Meteo Forecast API',
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['cuaca', 'etl', 'forecast', 'openmeteo'],
) as dag2:

    task_fetch_prediksi = PythonOperator(
        task_id='fetch_prediksi_2minggu',
        python_callable=fetch_weather_prediksi_2minggu_and_load,
    )

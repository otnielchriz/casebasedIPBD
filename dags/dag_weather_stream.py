from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# =========================
# PATH SCRAPER
# =========================
SCRAPERS_PATH = '/opt/airflow/scrapers'
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from scrap_weather_owm import fetch_owm_current_and_load, fetch_owm_batch_backfill

# =========================
# DAG CONFIG — STREAM MODE
# =========================
default_args = {
    'owner': 'zaki',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================
# DAG 1: [DEPRECATED] REAL-TIME STREAM MODE (production)
# ============================================================
with DAG(
    dag_id='weather_stream_owm',
    default_args=default_args,
    description='[DEPRECATED - Ganti Kafka] Real-time cuaca via OWM',
    schedule=None,  # Deprecated: tidak lagi dijadwalkan setiap 5 menit
    start_date=datetime(2026, 4, 1),
    catchup=False,  
    tags=['cuaca', 'etl', 'deprecated'],
) as dag:

    task_stream = PythonOperator(
        task_id='fetch_owm_realtime',
        python_callable=fetch_owm_current_and_load,
    )

# ============================================================
# DAG 2: BATCH BACKFILL MODE (one-time / manual trigger)
# Ambil semua 40 interval (5 hari) sekaligus
# ============================================================
with DAG(
    dag_id='weather_batch_backfill_owm',
    default_args=default_args,
    description='Batch backfill cuaca via OpenWeatherMap (40 intervals = 5 hari)',
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['cuaca', 'etl', 'batch', 'backfill', 'openweathermap'],
) as dag:

    task_backfill = PythonOperator(
        task_id='fetch_owm_batch',
        python_callable=fetch_owm_batch_backfill,
    )

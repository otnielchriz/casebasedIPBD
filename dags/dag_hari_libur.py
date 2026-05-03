from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

SCRAPERS_PATH = '/opt/airflow/scrapers'
if SCRAPERS_PATH not in sys.path:
    sys.path.insert(0, SCRAPERS_PATH)

from scrap_hari_libur import generate_hari_libur

def load_hari_libur_to_postgres():
    path_csv = '/opt/airflow/data/raw/hari_libur_2026.csv'
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')

    # Ganti semua "hari_libur" jadi "hari_libur"
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS hari_libur (
            id                 SERIAL PRIMARY KEY,
            tanggal            DATE UNIQUE,
            nama_hari          VARCHAR(10),
            is_libur           BOOLEAN,
            is_weekend         BOOLEAN,
            is_libur_nasional  BOOLEAN,
            is_cuti_bersama    BOOLEAN,
            keterangan         VARCHAR(100)
        );
    """)
    print("Tabel hari_libur siap.")

    if not os.path.exists(path_csv):
        print("Skip: File tidak ditemukan.")
        return

    df = pd.read_csv(path_csv)
    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            pg_hook.run("""
                INSERT INTO hari_libur
                    (tanggal, nama_hari, is_libur, is_weekend, is_libur_nasional, is_cuti_bersama, keterangan)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tanggal) DO NOTHING
            """, parameters=(
                row['tanggal'],
                row['nama_hari'],
                bool(row['is_libur']),
                bool(row['is_weekend']),
                bool(row['is_libur_nasional']),
                bool(row['is_cuti_bersama']),
                row['keterangan']
            ))
            inserted += 1
        except Exception as e:
            skipped += 1
            print(f"Skip {row['tanggal']}: {e}")

    print(f"Berhasil insert {inserted} hari, skip {skipped} duplikat.")

default_args = {
    'owner': 'zaki',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hari_libur_etl_pipeline',
    default_args=default_args,
    description='Generate Kalender Hari Libur 2026 -> PostgreSQL',
    schedule='0 4 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['warkop', 'hari_libur', 'etl', 'postgres'],
) as dag:

    extract_task = PythonOperator(
        task_id='generate_hari_libur_csv',
        python_callable=generate_hari_libur,
    )

    load_task = PythonOperator(
        task_id='load_hari_libur_to_postgres',
        python_callable=load_hari_libur_to_postgres,
    )

    extract_task >> load_task
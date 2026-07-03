import pandas as pd
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


def aggregate_income(**kwargs):
    file_path = "/opt/airflow/data/raw/Rincian Penjualan-2026-06-01__2026-06-30.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path, sep=';')

    df.columns = df.columns.str.strip().str.lower()

    df['order date'] = pd.to_datetime(df['order date'], errors='coerce')
    df = df.dropna(subset=['order date'])

    latest_month = df['order date'].dt.to_period('M').max()
    df = df[df['order date'].dt.to_period('M') == latest_month]

    df['tanggal'] = df['order date'].dt.date

    if 'net amount' in df.columns:
        income_col = 'net amount'
    else:
        income_col = 'total amount'

    df[income_col] = (
        df[income_col]
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '', regex=False)
    )

    df[income_col] = pd.to_numeric(df[income_col], errors='coerce').fillna(0)

    df_daily = df.groupby('tanggal', as_index=False)[income_col].sum()
    df_daily.rename(columns={income_col: 'total_pendapatan'}, inplace=True)

    output_path = "/opt/airflow/data/raw/pendapatan_harian.csv"
    df_daily.to_csv(output_path, index=False)

    print("AGGREGATE SUCCESS")
    print(df_daily.head())


def load_income_to_postgres(**kwargs):
    file_path = "/opt/airflow/data/raw/pendapatan_harian.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path)

    df['tanggal'] = pd.to_datetime(df['tanggal'], errors='coerce').dt.date

    if df['tanggal'].isnull().any():
        raise ValueError("Ada tanggal invalid di data")

    df['total_pendapatan'] = pd.to_numeric(
        df['total_pendapatan'],
        errors='coerce'
    ).fillna(0)

    df = df.drop_duplicates(subset=['tanggal'], keep='last')

    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')
    engine = pg_hook.get_sqlalchemy_engine()

    sample_date = pd.to_datetime(df['tanggal'].iloc[0])

    start_month = sample_date.replace(day=1).date()

    next_month = (
        sample_date.replace(day=28)
        + pd.Timedelta(days=4)
    ).replace(day=1).date()

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pendapatan_harian (
                tanggal DATE PRIMARY KEY,
                total_pendapatan NUMERIC(15, 2) NOT NULL
            );
        """))
        conn.execute(text("""
            DELETE FROM pendapatan_harian
            WHERE tanggal >= :start_month
              AND tanggal < :next_month
        """), {
            "start_month": start_month,
            "next_month": next_month
        })

    df.to_sql(
        'pendapatan_harian',
        con=engine,
        if_exists='append',
        index=False
    )

    print("SUCCESS: DATA BULAN TERBARU LOADED")
    print(f"Range delete: {start_month} sampai sebelum {next_month}")
    print(df.head())
import pandas as pd
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


def aggregate_income(**kwargs):

    # =========================
    # FILE CSV
    # =========================
    file_path = "/opt/airflow/data/raw/Rincian Penjualan-2026-05-01__2026-05-31.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    # =========================
    # READ CSV
    # =========================
    df = pd.read_csv(file_path, sep=';')

    # =========================
    # CLEAN COLUMN NAME
    # =========================
    df.columns = df.columns.str.strip().str.lower()

    # =========================
    # PARSE DATE
    # =========================
    df['order date'] = pd.to_datetime(df['order date'], errors='coerce')

    df = df.dropna(subset=['order date'])

    # =========================
    # 🔥 FILTER BULAN TERBARU ONLY
    # =========================
    latest_month = df['order date'].dt.to_period('M').max()
    df = df[df['order date'].dt.to_period('M') == latest_month]

    # =========================
    # CREATE TANGGAL
    # =========================
    df['tanggal'] = df['order date'].dt.date

    # =========================
    # PILIH KOLOM PENDAPATAN
    # =========================
    if 'net amount' in df.columns:
        income_col = 'net amount'
    else:
        income_col = 'total amount'

    # =========================
    # CLEAN NUMBER FORMAT
    # =========================
    df[income_col] = (
        df[income_col]
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '', regex=False)
    )

    df[income_col] = pd.to_numeric(df[income_col], errors='coerce').fillna(0)

    # =========================
    # AGGREGATE HARIAN
    # =========================
    df_daily = df.groupby('tanggal', as_index=False)[income_col].sum()

    df_daily.rename(columns={income_col: 'total_pendapatan'}, inplace=True)

    # =========================
    # SAVE CLEAN FILE (ONLY CURRENT MONTH)
    # =========================
    output_path = "/opt/airflow/data/raw/pendapatan_harian.csv"
    df_daily.to_csv(output_path, index=False)

    print("✅ AGGREGATE SUCCESS (ONLY LATEST MONTH)")
    print(df_daily.head())


def load_income_to_postgres(**kwargs):

    file_path = "/opt/airflow/data/raw/pendapatan_harian.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path)

    # =========================
    # CLEAN DATE
    # =========================
    df['tanggal'] = pd.to_datetime(df['tanggal'], errors='coerce').dt.date

    if df['tanggal'].isnull().any():
        raise ValueError("Ada tanggal invalid di data!")

    # =========================
    # DATABASE CONNECTION
    # =========================
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')
    engine = pg_hook.get_sqlalchemy_engine()

    # =========================
    # AMBIL BULAN DARI DATA
    # =========================
    sample_date = df['tanggal'].iloc[0]

    # =========================
    # 🔥 DELETE 1 BULAN SEKALIGUS (ANTI DUPLICATE)
    # =========================
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM pendapatan_harian
            WHERE DATE_TRUNC('month', tanggal) = DATE_TRUNC('month', :tanggal)
        """), {
            "tanggal": sample_date
        })

    # =========================
    # DROP DUPLICATE DI DATAFRAME (SAFETY)
    # =========================
    df = df.drop_duplicates(subset=['tanggal'], keep='last')

    # =========================
    # INSERT DATA BARU
    # =========================
    df.to_sql(
        'pendapatan_harian',
        con=engine,
        if_exists='append',
        index=False
    )

    print("✅ SUCCESS: BULAN TERBARU LOADED + OLD MONTH OVERWRITTEN")
import pandas as pd
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


# =========================
# 📊 AGREGASI PENDAPATAN HARIAN
# =========================
def aggregate_income(**kwargs):

    # =========================
    # FILE CSV TERBARU
    # =========================
    file_path = "/opt/airflow/data/raw/Rincian Penjualan-2026-04-01__2026-05-16.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    # =========================
    # READ CSV
    # =========================
    df = pd.read_csv(file_path, sep=';')

    # =========================
    # CLEAN NAMA KOLOM
    # =========================
    df.columns = df.columns.str.strip().str.lower()

    # =========================
    # FORMAT TANGGAL
    # =========================
    df['order date'] = pd.to_datetime(
        df['order date'],
        errors='coerce'
    )

    # HAPUS TANGGAL INVALID
    df = df.dropna(subset=['order date'])

    # =========================
    # AMBIL TANGGAL
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
    # CLEAN FORMAT ANGKA
    # =========================
    df[income_col] = (
        df[income_col]
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '', regex=False)
    )

    # CONVERT NUMERIC
    df[income_col] = pd.to_numeric(
        df[income_col],
        errors='coerce'
    ).fillna(0)

    # =========================
    # AGREGASI HARIAN
    # =========================
    df_daily = (
        df.groupby(
            'tanggal',
            as_index=False
        )[income_col]
        .sum()
    )

    # RENAME
    df_daily.rename(
        columns={
            income_col: 'total_pendapatan'
        },
        inplace=True
    )

    # =========================
    # SIMPAN CSV
    # =========================
    output_path = "/opt/airflow/data/raw/pendapatan_harian.csv"

    df_daily.to_csv(
        output_path,
        index=False
    )

    print("✅ AGREGASI SUCCESS")
    print(df_daily.head())


# =========================
# 📥 LOAD / UPDATE POSTGRES
# =========================
def load_income_to_postgres(**kwargs):

    file_path = "/opt/airflow/data/raw/pendapatan_harian.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    df = pd.read_csv(file_path)

    # =========================
    # CLEANING DATE
    # =========================
    df['tanggal'] = pd.to_datetime(
        df['tanggal'],
        errors='coerce'
    ).dt.date

    if df['tanggal'].isnull().any():
        raise ValueError("Ada tanggal yang gagal di-parse!")

    # =========================
    # DATABASE CONNECTION
    # =========================
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_traffic'
    )

    engine = pg_hook.get_sqlalchemy_engine()

    # =========================
    # AMBIL RANGE TANGGAL FILE BARU
    # =========================
    start_date = df['tanggal'].min()
    end_date = df['tanggal'].max()

    # =========================
    # HAPUS DATA LAMA DI RANGE TERSEBUT
    # =========================
    with engine.begin() as conn:

        for tanggal in df['tanggal'].unique():

            conn.execute(text("""
                DELETE FROM pendapatan_harian
                WHERE tanggal = :tanggal
            """), {
                "tanggal": tanggal
            })

    # =========================
    # INSERT DATA BARU
    # =========================
    df.to_sql(
        'pendapatan_harian',
        con=engine,
        if_exists='append',
        index=False
    )

    print("✅ Data pendapatan berhasil di-update")
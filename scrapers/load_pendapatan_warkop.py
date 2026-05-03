import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =========================
# 📊 AGREGASI PENDAPATAN HARIAN (FULL RANGE)
# =========================
def aggregate_income(**kwargs):

    file_path = "/opt/airflow/data/raw/Rincian Penjualan-2026-04-01__2026-05-02.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    # =========================
    # READ DATA (SEMICOLON FORMAT POS)
    # =========================
    df = pd.read_csv(file_path, sep=';')

    # =========================
    # CLEAN COLUMN NAME
    # =========================
    df.columns = df.columns.str.strip().str.lower()

    # =========================
    # FIX DATE FORMAT (PENTING BANGET)
    # =========================
    df['order date'] = pd.to_datetime(
        df['order date'],
        dayfirst=True,
        errors='coerce'
    )

    df = df.dropna(subset=['order date'])

    # =========================
    # AMBIL TANGGAL SAJA
    # =========================
    df['tanggal'] = df['order date'].dt.date

    # =========================
    # PILIH KOLOM PENDAPATAN
    # =========================
    if 'net amount' in df.columns:
        income_col = 'net amount'
    else:
        income_col = 'total amount'

    df[income_col] = pd.to_numeric(df[income_col], errors='coerce')

    # =========================
    # AGREGASI HARIAN
    # =========================
    df_daily = df.groupby('tanggal', as_index=False)[income_col].sum()

    df_daily.rename(columns={income_col: 'total_pendapatan'}, inplace=True)

    # =========================
    # 🔥 INI BAGIAN PENTING: FULL RANGE 1 APR - 2 MEI
    # =========================
    full_range = pd.date_range(
        start="2026-04-01",
        end="2026-05-02"
    )

    df_daily['tanggal'] = pd.to_datetime(df_daily['tanggal'])

    df_daily = df_daily.set_index('tanggal').reindex(full_range, fill_value=0)

    df_daily = df_daily.reset_index()
    df_daily.columns = ['tanggal', 'total_pendapatan']

    # =========================
    # SIMPAN OUTPUT FINAL
    # =========================
    output_path = "/opt/airflow/data/raw/pendapatan_harian.csv"

    df_daily.to_csv(output_path, index=False)

    print("✅ SUCCESS - FULL DAILY RANGE GENERATED")
    print(df_daily.head())

# =========================
# 📥 LOAD KE POSTGRES
# =========================
def load_income_to_postgres(**kwargs):

    file_path = "/opt/airflow/data/raw/pendapatan_harian.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    df = pd.read_csv(file_path)

    # =========================
    # CLEANING DATE
    # =========================
    df['tanggal'] = pd.to_datetime(df['tanggal'], errors='coerce').dt.date

    # cek kalau ada yang gagal parsing
    if df['tanggal'].isnull().any():
        raise ValueError("Ada tanggal yang gagal di-parse!")

    # =========================
    # DATABASE CONNECTION
    # =========================
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic')
    engine = pg_hook.get_sqlalchemy_engine()

    # =========================
    # LOAD DATA
    # =========================
    df.to_sql(
        'pendapatan_harian',
        con=engine,
        if_exists='append',
        index=False
    )

    print("✅ Data pendapatan harian berhasil masuk PostgreSQL")
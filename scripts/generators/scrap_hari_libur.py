"""
Generate kalender hari libur Indonesia 2025-2026,
simpan ke CSV, lalu load ke PostgreSQL.

Script ini DI LUAR DAG Airflow.
"""

import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


# =========================
# PATH OUTPUT CSV
# =========================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"


# =========================
# DATA HARI LIBUR
# =========================



LIBUR_2026 = {
    date(2026, 1, 1): ("Tahun Baru 2026 Masehi", "libur_nasional"),
    date(2026, 1, 16): ("Isra Mikraj Nabi Muhammad SAW", "libur_nasional"),
    date(2026, 2, 17): ("Tahun Baru Imlek 2577", "libur_nasional"),
    date(2026, 3, 19): ("Hari Suci Nyepi", "libur_nasional"),
    date(2026, 3, 21): ("Idul Fitri 1447 H (Hari 1)", "libur_nasional"),
    date(2026, 3, 22): ("Idul Fitri 1447 H (Hari 2)", "libur_nasional"),
    date(2026, 4, 3): ("Wafat Yesus Kristus", "libur_nasional"),
    date(2026, 4, 5): ("Kebangkitan Yesus Kristus (Paskah)", "libur_nasional"),
    date(2026, 5, 1): ("Hari Buruh Internasional", "libur_nasional"),
    date(2026, 5, 14): ("Kenaikan Yesus Kristus", "libur_nasional"),
    date(2026, 5, 27): ("Idul Adha 1447 H", "libur_nasional"),
    date(2026, 5, 31): ("Hari Raya Waisak 2570 BE", "libur_nasional"),
    date(2026, 6, 1): ("Hari Lahir Pancasila", "libur_nasional"),
    date(2026, 6, 16): ("Tahun Baru Islam 1448 H", "libur_nasional"),
    date(2026, 8, 17): ("HUT Kemerdekaan RI ke-81", "libur_nasional"),
    date(2026, 8, 25): ("Maulid Nabi Muhammad SAW", "libur_nasional"),
    date(2026, 12, 25): ("Hari Raya Natal", "libur_nasional"),

    date(2026, 2, 16): ("Cuti Bersama Tahun Baru Imlek", "cuti_bersama"),
    date(2026, 3, 18): ("Cuti Bersama Nyepi", "cuti_bersama"),
    date(2026, 3, 20): ("Cuti Bersama Idul Fitri", "cuti_bersama"),
    date(2026, 3, 23): ("Cuti Bersama Idul Fitri", "cuti_bersama"),
    date(2026, 3, 24): ("Cuti Bersama Idul Fitri", "cuti_bersama"),
    date(2026, 5, 15): ("Cuti Bersama Kenaikan Yesus", "cuti_bersama"),
    date(2026, 5, 28): ("Cuti Bersama Idul Adha", "cuti_bersama"),
    date(2026, 12, 24): ("Cuti Bersama Natal", "cuti_bersama"),
}


NAMA_HARI = {
    0: "Senin",
    1: "Selasa",
    2: "Rabu",
    3: "Kamis",
    4: "Jumat",
    5: "Sabtu",
    6: "Minggu",
}


DATA_PER_TAHUN = {
    2026: LIBUR_2026,
}


# =========================
# GENERATE KALENDER
# =========================

def generate_kalender(tahun: int, data_libur: dict) -> pd.DataFrame:
    rows = []

    tanggal = date(tahun, 1, 1)
    akhir = date(tahun, 12, 31)

    while tanggal <= akhir:
        hari_idx = tanggal.weekday()
        nama_hari = NAMA_HARI[hari_idx]

        jenis = data_libur.get(tanggal, ("", ""))[1]
        keterangan = data_libur.get(tanggal, ("", ""))[0]

        is_weekend = hari_idx in (4, 5, 6)
        is_libur_nasional = jenis == "libur_nasional"
        is_cuti_bersama = jenis == "cuti_bersama"
        is_libur = is_weekend or is_libur_nasional or is_cuti_bersama

        if not keterangan and is_weekend:
            keterangan = f"Akhir Pekan ({nama_hari})"

        rows.append({
            "tanggal": tanggal,
            "nama_hari": nama_hari,
            "is_libur": is_libur,
            "is_weekend": is_weekend,
            "is_libur_nasional": is_libur_nasional,
            "is_cuti_bersama": is_cuti_bersama,
            "keterangan": keterangan,
        })

        tanggal += timedelta(days=1)

    return pd.DataFrame(rows)


# =========================
# KONEKSI POSTGRES
# =========================

def get_engine():
    from sqlalchemy import create_engine

    user = os.getenv("DB_USER", "airflow")
    password = os.getenv("DB_PASSWORD", "airflow")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5435")
    database = os.getenv("DB_NAME", "airflow")

    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

    return create_engine(db_url)


# =========================
# CREATE TABLE JIKA BELUM ADA
# =========================

def create_table_if_not_exists(engine):
    from sqlalchemy import text

    ddl = """
    CREATE TABLE IF NOT EXISTS hari_libur (
        tanggal DATE PRIMARY KEY,
        nama_hari TEXT,
        is_libur BOOLEAN,
        is_weekend BOOLEAN,
        is_libur_nasional BOOLEAN,
        is_cuti_bersama BOOLEAN,
        keterangan TEXT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))


# =========================
# SAVE CSV
# =========================

def save_csv(df: pd.DataFrame, tahun: int):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUTPUT_DIR / f"hari_libur_{tahun}.csv"
    df.to_csv(path, index=False)

    print(f"CSV tersimpan: {path}")


# =========================
# LOAD KE POSTGRES
# =========================

def load_to_postgres(df: pd.DataFrame, engine):
    from sqlalchemy import text

    df["tanggal"] = pd.to_datetime(df["tanggal"]).dt.date

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE hari_libur"))

    df.to_sql(
        "hari_libur",
        con=engine,
        if_exists="append",
        index=False
    )

    print(f"PostgreSQL: {len(df)} baris berhasil dimuat ke tabel hari_libur")


# =========================
# MAIN
# =========================

def main():
    engine = get_engine()

    create_table_if_not_exists(engine)

    all_df = []

    for tahun, data_libur in DATA_PER_TAHUN.items():
        df = generate_kalender(tahun, data_libur)

        save_csv(df, tahun)

        all_df.append(df)

    final_df = pd.concat(all_df, ignore_index=True)

    load_to_postgres(final_df, engine)

    print("Selesai: CSV 2025 & 2026 sudah dibuat dan masuk PostgreSQL.")


if __name__ == "__main__":
    main()
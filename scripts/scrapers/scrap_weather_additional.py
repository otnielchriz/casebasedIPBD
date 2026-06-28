import requests
import pandas as pd
import os
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

# =========================
# CONFIG KOTA
# =========================
LAT = -7.7073187
LON = 110.8379588
KOTA = "Sukoharjo"

# =========================
# WMO MAP (kondisi + deskripsi)
# =========================
WMO_MAP = {
    0: ("Clear", "Cerah"),
    1: ("Clouds", "Sebagian Berawan"),
    2: ("Clouds", "Berawan"),
    3: ("Clouds", "Mendung"),
    45: ("Fog", "Berkabut"),
    48: ("Fog", "Kabut"),
    51: ("Drizzle", "Gerimis"),
    53: ("Drizzle", "Gerimis Sedang"),
    55: ("Drizzle", "Gerimis Lebat"),
    56: ("Drizzle", "Gerimis Beku Ringan"),
    57: ("Drizzle", "Gerimis Beku Lebat"),
    61: ("Rain", "Hujan Ringan"),
    63: ("Rain", "Hujan Sedang"),
    65: ("Rain", "Hujan Lebat"),
    66: ("Rain", "Hujan Beku Ringan"),
    67: ("Rain", "Hujan Beku Lebat"),
    71: ("Snow", "Salju Ringan"),
    73: ("Snow", "Salju Sedang"),
    75: ("Snow", "Salju Lebat"),
    77: ("Snow", "Biji Salju"),
    80: ("Rain", "Hujan Lokal Ringan"),
    81: ("Rain", "Hujan Lokal Sedang"),
    82: ("Rain", "Hujan Lokal Lebat"),
    85: ("Snow", "Hujan Salju Ringan"),
    86: ("Snow", "Hujan Salju Lebat"),
    95: ("Thunderstorm", "Badai Petir"),
    96: ("Thunderstorm", "Badai Petir Ringan dengan Hujan Es"),
    99: ("Thunderstorm", "Badai Petir Lebat dengan Hujan Es")
}

def _process_and_save(data, table_name, csv_filename, start_date, end_date):
    """
    Helper function to process the JSON data from Open-Meteo API, 
    save to CSV, and insert into Postgres.
    """
    hourly = data["hourly"]
    
    kondisi = [WMO_MAP.get(code, ("Unknown", "Tidak Diketahui"))[0] for code in hourly["weather_code"]]
    deskripsi = [WMO_MAP.get(code, ("Unknown", "Tidak Diketahui"))[1] for code in hourly["weather_code"]]

    df = pd.DataFrame({
        "waktu": hourly["time"],
        "kota": KOTA,
        "suhu": hourly["temperature_2m"],
        "suhu_terasa": hourly["apparent_temperature"],
        "kelembapan": hourly["relative_humidity_2m"],
        "kondisi": kondisi,
        "deskripsi": deskripsi,
        "kecepatan_angin": hourly["wind_speed_10m"],
        "curah_hujan": hourly["precipitation"],
        "cloudiness": hourly["cloud_cover"]
    })

    # Format time to ISO 8601 without seconds
    df["waktu"] = pd.to_datetime(df["waktu"]).dt.strftime("%Y-%m-%dT%H:%M")

    # =========================
    # SAVE CSV
    # =========================
    folder = "/opt/airflow/data/raw"
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, csv_filename)

    # Merge logic if file exists
    if os.path.exists(file_path):
        df_lama = pd.read_csv(file_path)
        df_gabungan = pd.concat([df_lama, df], ignore_index=True)
        df_gabungan = df_gabungan.drop_duplicates(subset=["waktu"], keep="last")
        df_gabungan = df_gabungan.sort_values("waktu")
        df_gabungan.to_csv(file_path, index=False)
        print(f"♻️ CSV updated: {file_path}")
    else:
        df.to_csv(file_path, index=False)
        print(f"💾 CSV created: {file_path}")

    # =========================
    # POSTGRES UPSERT
    # =========================
    pg_hook = PostgresHook(postgres_conn_id="postgres_traffic")
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        # 1. Create table if not exists
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                waktu TIMESTAMP PRIMARY KEY,
                kota VARCHAR(50),
                suhu FLOAT,
                suhu_terasa FLOAT,
                kelembapan FLOAT,
                kondisi VARCHAR(50),
                deskripsi VARCHAR(100),
                kecepatan_angin FLOAT,
                curah_hujan FLOAT,
                cloudiness FLOAT
            )
        """))

        # 2. Delete data in the specified range to avoid duplicates
        conn.execute(text(f"""
            DELETE FROM {table_name}
            WHERE waktu >= :start AND waktu < :end
        """), {
            "start": start_date,
            "end": end_date + timedelta(days=1)
        })

    # 3. Append new data
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"🚀 SUCCESS insert {len(df)} rows ke {table_name}")


def fetch_weather_sebulan_lalu_and_load(**kwargs):
    """
    Mengambil data historis cuaca 30 hari ke belakang menggunakan Open-Meteo Archive API.
    """
    # Menggunakan logical_date (atau tanggal eksekusi Airflow saat ini)
    logical_date = kwargs.get("logical_date")
    if not logical_date:
        logical_date = datetime.utcnow()
        
    end_date = logical_date.date() - timedelta(days=1)
    start_date = end_date - timedelta(days=30)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": ["temperature_2m", "apparent_temperature", "relative_humidity_2m", "weather_code", "wind_speed_10m", "precipitation", "cloud_cover"],
        "timezone": "Asia/Jakarta"
    }

    print(f"Mulai fetch cuaca historis dari {start_date} hingga {end_date}...")
    res = requests.get(url, params=params)
    res.raise_for_status()
    
    _process_and_save(res.json(), "cuaca_sukoharjo_sebulan_lalu", "cuaca_sukoharjo_sebulan_lalu.csv", start_date, end_date)
    print(f"✅ Historis sebulan lalu berhasil di-load ({start_date} to {end_date})")


def fetch_weather_prediksi_2minggu_and_load(**kwargs):
    """
    Mengambil prediksi cuaca 14 hari ke depan menggunakan Open-Meteo Forecast API.
    """
    # Menggunakan logical_date (atau tanggal eksekusi Airflow saat ini)
    logical_date = kwargs.get("logical_date")
    if not logical_date:
        logical_date = datetime.utcnow()
        
    start_date = logical_date.date()
    end_date = start_date + timedelta(days=14)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": ["temperature_2m", "apparent_temperature", "relative_humidity_2m", "weather_code", "wind_speed_10m", "precipitation", "cloud_cover"],
        "timezone": "Asia/Jakarta"
    }

    print(f"Mulai fetch cuaca prediksi dari {start_date} hingga {end_date}...")
    res = requests.get(url, params=params)
    res.raise_for_status()
    
    _process_and_save(res.json(), "cuaca_sukoharjo_prediksi_2minggu", "cuaca_sukoharjo_prediksi_2minggu.csv", start_date, end_date)
    print(f"✅ Prediksi 2 minggu berhasil di-load ({start_date} to {end_date})")

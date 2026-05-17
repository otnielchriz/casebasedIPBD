import requests
import pandas as pd
import os
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


def fetch_weather_forecast_and_load(**kwargs):

    # =========================
    # RANGE 14 HARI
    # =========================
    start_date = datetime.today().date()
    end_date = start_date + timedelta(days=14)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    LAT = -7.7073187
    LON = 110.8379588
    KOTA = "Node_Warkop_Kusuma"

    # =========================
    # API
    # =========================
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "hourly": [
            "temperature_2m",
            "apparent_temperature",
            "relative_humidity_2m",
            "weather_code",
            "wind_speed_10m",
            "precipitation",
            "cloud_cover"
        ],
        "timezone": "Asia/Jakarta"
    }

    res = requests.get(url, params=params)
    res.raise_for_status()
    data = res.json()

    hourly = data["hourly"]

    # =========================
    # WMO MAP (kondisi + deskripsi)
    # =========================
    wmo = {
        0: ("Clear", "Cerah"),
        1: ("Clouds", "Sebagian Berawan"),
        2: ("Clouds", "Berawan"),
        3: ("Clouds", "Mendung"),
        45: ("Fog", "Berkabut"),
        48: ("Fog", "Kabut"),
        51: ("Drizzle", "Gerimis"),
        61: ("Rain", "Hujan Ringan"),
        63: ("Rain", "Hujan Sedang"),
        65: ("Rain", "Hujan Lebat"),
        80: ("Rain", "Hujan Lokal"),
        95: ("Thunderstorm", "Badai Petir")
    }

    kondisi = [
        wmo.get(code, ("Unknown", "Tidak Diketahui"))[0]
        for code in hourly["weather_code"]
    ]

    deskripsi = [
        wmo.get(code, ("Unknown", "Tidak Diketahui"))[1]
        for code in hourly["weather_code"]
    ]

    # =========================
    # DATAFRAME FINAL (SESUAI CONTOH KAMU)
    # =========================
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

    # =========================
    # FORMAT TIME BIAR SAMA KAYAK CONTOH
    # =========================
    df["waktu"] = pd.to_datetime(df["waktu"]).dt.strftime("%Y-%m-%dT%H:%M")

    # =========================
    # SAVE CSV (1 FILE)
    # =========================
    folder = "/opt/airflow/data/raw"
    os.makedirs(folder, exist_ok=True)

    file_path = os.path.join(
        folder,
        f"cuaca_warkop_{start_date_str}_to_{end_date_str}.csv"
    )

    df.to_csv(file_path, index=False)

    print("💾 CSV saved:", file_path)

    # =========================
    # POSTGRES
    # =========================
    pg_hook = PostgresHook(postgres_conn_id="postgres_traffic")
    engine = pg_hook.get_sqlalchemy_engine()

    # =========================
    # DELETE RANGE (ANTI DUPLICATE)
    # =========================
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM cuaca_historis
            WHERE waktu BETWEEN :start AND :end
        """), {
            "start": start_date,
            "end": end_date + timedelta(days=1)
        })

    # =========================
    # INSERT DB
    # =========================
    df.to_sql(
        "cuaca_historis",
        con=engine,
        if_exists="append",
        index=False
    )

    print("🚀 SUCCESS insert ke DB")
    print(df.head())
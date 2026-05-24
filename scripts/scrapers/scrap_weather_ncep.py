import requests
import pandas as pd
import os
from datetime import datetime, timedelta


def fetch_weather_forecast_to_csv(**kwargs):
    start_date = datetime.today().date()
    end_date = start_date + timedelta(days=14)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    LAT = -7.7073187
    LON = 110.8379588
    KOTA = "Node_Warkop_Kusuma"

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

    df["waktu"] = pd.to_datetime(df["waktu"]).dt.strftime("%Y-%m-%dT%H:%M")

    folder = "/opt/airflow/data/raw"
    os.makedirs(folder, exist_ok=True)

    file_path = os.path.join(folder, "cuaca_warkop.csv")

    if os.path.exists(file_path):
        df_lama = pd.read_csv(file_path)

        df_gabungan = pd.concat([df_lama, df], ignore_index=True)

        df_gabungan = df_gabungan.drop_duplicates(
            subset=["waktu"],
            keep="last"
        )

        df_gabungan = df_gabungan.sort_values("waktu")

        df_gabungan.to_csv(file_path, index=False)

        print("CSV updated:", file_path)

    else:
        df.to_csv(file_path, index=False)
        print("CSV created:", file_path)

    print(df.head())
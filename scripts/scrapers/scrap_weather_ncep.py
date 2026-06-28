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

    df = pd.DataFrame({
        "waktu": hourly["time"],

        "kota": KOTA,

        "weather_code": hourly["weather_code"],

        "suhu": hourly["temperature_2m"],

        "suhu_terasa": hourly["apparent_temperature"],

        "kelembapan": hourly["relative_humidity_2m"],

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
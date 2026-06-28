# ==============================================================================
# ⚠️ DEPRECATED (10-Minute Interval Scraper)
# Gunakan scripts/scrapers/kafka_producer_weather.py dengan interval 10 detik
# ==============================================================================

import json
import time
import requests

from kafka import KafkaProducer
from datetime import datetime
from zoneinfo import ZoneInfo


LAT = -7.7073187
LON = 110.8379588
KOTA = "Node_Warkop_Kusuma"

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "weather_stream"


def fetch_current_weather():

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": LAT,
        "longitude": LON,
        "current": [
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

    res = requests.get(url, params=params, timeout=30)
    res.raise_for_status()

    data = res.json()
    current = data["current"]

    payload = {
        "waktu_ambil": datetime.now(
            ZoneInfo("Asia/Jakarta")
        ).strftime("%Y-%m-%d %H:%M:%S"),

        "kota": KOTA,

        "weather_code": current.get("weather_code"),

        "suhu": current.get("temperature_2m"),
        "suhu_terasa": current.get("apparent_temperature"),
        "kelembapan": current.get("relative_humidity_2m"),

        "kecepatan_angin": current.get("wind_speed_10m"),
        "curah_hujan": current.get("precipitation"),
        "cloudiness": current.get("cloud_cover"),
    }

    return payload


def main():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    while True:

        try:

            payload = fetch_current_weather()

            producer.send(TOPIC, payload)
            producer.flush()

            print("Sent:", payload)

        except Exception as e:
            print("Producer error:", e)

        time.sleep(600)


if __name__ == "__main__":
    main()
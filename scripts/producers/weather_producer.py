import json
import time
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from kafka import KafkaProducer


LAT = -7.7073187
LON = 110.8379588
KOTA = "Node_Warkop_Kusuma"

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "weather_stream"

WMO = {
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
    95: ("Thunderstorm", "Badai Petir"),
}


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
            "cloud_cover",
        ],
        "timezone": "Asia/Jakarta",
    }

    res = requests.get(url, params=params, timeout=30)
    res.raise_for_status()

    data = res.json()
    current = data["current"]

    code = current.get("weather_code")
    kondisi, deskripsi = WMO.get(code, ("Unknown", "Tidak Diketahui"))

    return {
       "waktu_ambil": datetime.now(
            ZoneInfo("Asia/Jakarta")
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "kota": KOTA,
        "suhu": current.get("temperature_2m"),
        "suhu_terasa": current.get("apparent_temperature"),
        "kelembapan": current.get("relative_humidity_2m"),
        "kondisi": kondisi,
        "deskripsi": deskripsi,
        "kecepatan_angin": current.get("wind_speed_10m"),
        "curah_hujan": current.get("precipitation"),
        "cloudiness": current.get("cloud_cover"),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        try:
            payload = fetch_current_weather()
            producer.send(TOPIC, payload)
            producer.flush()

            print("Sent:", payload)

        except Exception as e:
            print("Producer error:", e)

        time.sleep(600)  # 10 menit


if __name__ == "__main__":
    main()
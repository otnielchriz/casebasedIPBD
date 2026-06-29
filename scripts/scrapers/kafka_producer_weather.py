# ==============================================================================
# ⚠️ DEPRECATED (OpenWeatherMap 10-Second Streaming)
# Proyek bermigrasi menggunakan Open-Meteo 10-menit di scripts/producers/weather_producer.py
# ==============================================================================

import json
import time
import os
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer

# =========================
# CONFIG KOTA & KAFKA
# =========================
LAT = -7.7073187
LON = 110.8379588
KAFKA_BROKER = 'kafka:29092' # Nama service docker
TOPIC_NAME = 'weather_stream'
API_URL = "https://api.openweathermap.org/data/2.5/weather"

def _get_api_key():
    """Retrieve OWM API key from environment variables"""
    key = os.getenv("OPENWEATHERMAP_API_KEY", "")
    if not key:
        raise ValueError("OPENWEATHERMAP_API_KEY tidak diset di environment variables!")
    return key

def create_producer():
    """Initialize Kafka Producer with retry mechanism"""
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Berhasil terhubung ke Kafka Broker!")
            return producer
        except Exception as e:
            print(f"⏳ Menunggu Kafka siap... ({retries} retries left). Error: {e}")
            retries -= 1
            time.sleep(5)
    raise Exception("❌ Gagal terhubung ke Kafka Broker setelah beberapa percobaan.")

def fetch_current_weather():
    """Fetch current weather for Sukoharjo from OpenWeatherMap"""
    api_key = _get_api_key()
    params = {
        "lat": LAT,
        "lon": LON,
        "units": "metric",
        "appid": api_key,
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    
    if "dt" not in data:
        raise ValueError("Response OpenWeatherMap tidak memiliki key 'dt'!")
        
    # Convert OWM's epoch timestamp (UTC) to WIB (UTC+7)
    waktu_wib = datetime.utcfromtimestamp(data["dt"]) + timedelta(hours=7)
    waktu_str = waktu_wib.strftime("%Y-%m-%dT%H:%M")
    
    # Format payload conforming to database schema
    payload = {
        "waktu": waktu_str,
        "suhu": data["main"]["temp"],
        "suhu_terasa": data["main"].get("feels_like"),
        "kelembapan": data["main"]["humidity"],
        "weather_code": data["weather"][0]["id"],
        "kecepatan_angin": data["wind"]["speed"],
        "curah_hujan": data.get("rain", {}).get("1h", 0.0),
        "cloudiness": data["clouds"]["all"],
        "sumber": "streaming"
    }
    return payload

def start_streaming():
    producer = create_producer()
    print(f"🚀 Memulai sensor cuaca Sukoharjo (OpenWeatherMap)... Mengirim data ke '{TOPIC_NAME}' setiap 10 detik.")
    
    try:
        while True:
            try:
                weather_data = fetch_current_weather()
                producer.send(TOPIC_NAME, value=weather_data)
                producer.flush()
                print(f"📡 [PRODUCED - OWM] {weather_data['waktu']} | Suhu: {weather_data['suhu']}°C | Kelembapan: {weather_data['kelembapan']}%")
            except Exception as e:
                print(f"⚠️ Gagal mengambil/mengirim data cuaca: {e}")
            time.sleep(10)
    except KeyboardInterrupt:
        print("🛑 Streaming dihentikan oleh user.")
    finally:
        producer.close()

if __name__ == "__main__":
    start_streaming()

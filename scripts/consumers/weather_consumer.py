# ==============================================================================
# ⚠️ DEPRECATED (10-Minute Interval Consumer)
# Gunakan scripts/scrapers/pyspark_consumer_weather.py dengan PySpark
# ==============================================================================

import json
import psycopg2

from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "weather_stream"


DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def insert_weather(data):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO cuaca_stream_raw (
            waktu_ambil,
            kota,
            weather_code,
            suhu,
            suhu_terasa,
            kelembapan,
            kecepatan_angin,
            curah_hujan,
            cloudiness
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (

        data.get("waktu_ambil"),
        data.get("kota"),

        data.get("weather_code"),

        data.get("suhu"),
        data.get("suhu_terasa"),
        data.get("kelembapan"),

        data.get("kecepatan_angin"),
        data.get("curah_hujan"),
        data.get("cloudiness"),
    ))

    conn.commit()

    cur.close()
    conn.close()


def main():

    consumer = KafkaConsumer(
        TOPIC,

        bootstrap_servers=KAFKA_BOOTSTRAP,

        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        group_id="weather-consumer-group",
    )

    print("Consumer running...")

    for msg in consumer:

        data = msg.value

        insert_weather(data)

        print("Inserted:", data)


if __name__ == "__main__":
    main()
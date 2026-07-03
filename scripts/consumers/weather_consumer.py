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


def create_table_if_not_exists():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cuaca_stream_raw (
            waktu_ambil TIMESTAMP NOT NULL,
            kota VARCHAR(100) NOT NULL,
            weather_code INTEGER,
            suhu NUMERIC(5,2),
            suhu_terasa NUMERIC(5,2),
            kelembapan INTEGER,
            kecepatan_angin NUMERIC(6,2),
            curah_hujan NUMERIC(6,2),
            cloudiness INTEGER
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


def main():
    create_table_if_not_exists()
    
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
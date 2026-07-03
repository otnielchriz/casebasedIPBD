# ==============================================================================
# ⚠️ DEPRECATED (OpenWeatherMap PySpark Consumer)
# Proyek bermigrasi menggunakan Open-Meteo 10-menit di scripts/consumers/weather_consumer.py
# ==============================================================================

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# CONFIG KONEKSI
# =========================
KAFKA_BROKER = "kafka:29092"
TOPIC_NAME = "weather_stream"
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
TABLE_NAME = "cuaca_historis"

# =========================
# OWM CODE MAP (Untuk Transformasi)
# =========================
OWM_CODE_MAP = {
    # Thunderstorm
    200: ("Thunderstorm", "Badai Petir (Hujan Ringan)"),
    201: ("Thunderstorm", "Badai Petir"),
    202: ("Thunderstorm", "Badai Petir (Hujan Lebat)"),
    210: ("Thunderstorm", "Badai Petir Ringan"),
    211: ("Thunderstorm", "Badai Petir"),
    212: ("Thunderstorm", "Badai Petir Hebat"),
    221: ("Thunderstorm", "Badai Petir Tidak Beraturan"),
    230: ("Thunderstorm", "Gerimis Badai"),
    231: ("Thunderstorm", "Gerimis Badai"),
    232: ("Thunderstorm", "Gerimis Badai Hebat"),
    # Drizzle
    300: ("Drizzle", "Gerimis Ringan"),
    301: ("Drizzle", "Gerimis"),
    302: ("Drizzle", "Gerimis Lebat"),
    310: ("Drizzle", "Hujan Gerimis Ringan"),
    311: ("Drizzle", "Hujan Gerimis"),
    312: ("Drizzle", "Hujan Gerimis Lebat"),
    313: ("Drizzle", "Hujan + Gerimis"),
    314: ("Drizzle", "Hujan + Gerimis Lebat"),
    321: ("Drizzle", "Gerimis Deras"),
    # Rain
    500: ("Rain", "Hujan Ringan"),
    501: ("Rain", "Hujan Sedang"),
    502: ("Rain", "Hujan Lebat"),
    503: ("Rain", "Hujan Sangat Lebat"),
    504: ("Rain", "Hujan Ekstrem"),
    511: ("Rain", "Hujan Beku"),
    520: ("Rain", "Hujan Lokal Ringan"),
    521: ("Rain", "Hujan Lokal"),
    522: ("Rain", "Hujan Lokal Deras"),
    531: ("Rain", "Hujan Tidak Beraturan"),
    # Snow
    600: ("Snow", "Salju Ringan"),
    601: ("Snow", "Salju"),
    602: ("Snow", "Salju Lebat"),
    611: ("Snow", "Sleet"),
    612: ("Snow", "Sleet Ringan"),
    613: ("Snow", "Sleet Deras"),
    615: ("Snow", "Hujan + Salju Ringan"),
    616: ("Snow", "Hujan + Salju"),
    620: ("Snow", "Salju Lokal Ringan"),
    621: ("Snow", "Salju Lokal"),
    622: ("Snow", "Salju Lokal Deras"),
    # Atmosphere
    701: ("Fog", "Kabut Tipis"),
    711: ("Fog", "Asap"),
    721: ("Fog", "Kabut Asap"),
    731: ("Fog", "Debu"),
    741: ("Fog", "Kabut"),
    751: ("Fog", "Pasir"),
    761: ("Fog", "Debu Tebal"),
    762: ("Fog", "Abu Vulkanik"),
    771: ("Fog", "Angin Kencang"),
    781: ("Fog", "Tornado"),
    # Clear
    800: ("Clear", "Cerah"),
    # Clouds
    801: ("Clouds", "Sebagian Berawan"),
    802: ("Clouds", "Berawan"),
    803: ("Clouds", "Berawan Tebal"),
    804: ("Clouds", "Mendung"),
}

def get_owm_condition(code):
    return OWM_CODE_MAP.get(code, ("Unknown", "Tidak Diketahui"))[0]

def get_owm_description(code):
    return OWM_CODE_MAP.get(code, ("Unknown", "Tidak Diketahui"))[1]

# Kita tidak mendaftarkan UDF PySpark yang kompleks, kita akan menggunakan foreachBatch pandas mapping 
# yang jauh lebih stabil dan ringan tanpa perlu konfigurasi worker kompleks.

def write_to_postgres(df_batch, batch_id):
    """
    Fungsi foreachBatch untuk memproses setiap micro-batch dari Kafka.
    Merubah DataFrame Spark menjadi Pandas, melakukan translasi WMO, dan menyimpan ke Postgres via SQLAlchemy.
    """
    # 1. Convert Spark DataFrame ke Pandas (Aman karena micro-batch streaming berukuran kecil)
    pdf = df_batch.toPandas()
    
    if pdf.empty:
        return
        
    print(f"⚡ [BATCH {batch_id}] Memproses {len(pdf)} baris data...")
    
    # 2. Transformasi (ETL)
    pdf['kota'] = 'Sukoharjo'
    pdf['kondisi'] = pdf['weather_code'].apply(lambda x: get_owm_condition(x))
    pdf['deskripsi'] = pdf['weather_code'].apply(lambda x: get_owm_description(x))
    
    # Format timestamp to standard %Y-%m-%dT%H:%M
    pdf['waktu'] = pd.to_datetime(pdf['waktu']).dt.strftime('%Y-%m-%dT%H:%M')
    
    # Round float values to 2 decimal places to keep data clean and consistent
    for col_name in ['suhu', 'suhu_terasa', 'kelembapan', 'kecepatan_angin', 'curah_hujan', 'cloudiness']:
        if col_name in pdf.columns:
            pdf[col_name] = pdf[col_name].round(2)
            
    # Kolom yang akan dimasukkan ke DB
    final_df = pdf[['waktu', 'kota', 'suhu', 'suhu_terasa', 'kelembapan', 'kondisi', 'deskripsi', 'kecepatan_angin', 'curah_hujan', 'cloudiness', 'sumber']]
    
    # 3. Load ke PostgreSQL
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        # Hapus data dengan waktu yang sama untuk menghindari duplikat (Upsert logic)
        waktu_list = tuple(final_df['waktu'].tolist())
        if len(waktu_list) == 1:
            waktu_str = f"('{waktu_list[0]}')"
        else:
            waktu_str = str(waktu_list)
            
        conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE waktu IN {waktu_str}"))
        
    # Append data baru
    final_df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)
    print(f"✅ [BATCH {batch_id}] Berhasil disimpan ke tabel {TABLE_NAME} di PostgreSQL.")


def start_spark_streaming():
    print("⏳ Memulai PySpark Structured Streaming...")
    
    # Inisialisasi Spark Session dengan package Kafka
    spark = SparkSession.builder \
        .appName("WeatherStreamingConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Schema JSON dari Producer
    schema = StructType([
        StructField("waktu", StringType(), True),
        StructField("suhu", FloatType(), True),
        StructField("suhu_terasa", FloatType(), True),
        StructField("kelembapan", FloatType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("kecepatan_angin", FloatType(), True),
        StructField("curah_hujan", FloatType(), True),
        StructField("cloudiness", FloatType(), True),
        StructField("sumber", StringType(), True)
    ])

    # Membaca aliran dari Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON (Kolom 'value' dari Kafka berisi bytes, cast ke string lalu parse dengan schema)
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Menjalankan stream dengan foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .start()

    print(f"Streaming berjalan. Memantau Kafka topic: {TOPIC_NAME}")
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_streaming()

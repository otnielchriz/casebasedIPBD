import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

# =========================
# OPENWEATHERMAP WEATHER CODES → KONDISI & DESKRIPSI
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

# =========================
# CONFIG
# =========================
LAT = -7.7073187
LON = 110.8379588
KOTA = "Sukoharjo"
API_URL_CURRENT = "https://api.openweathermap.org/data/2.5/weather"
API_URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"


def _get_api_key():
    """Ambil API key dari env var."""
    key = os.getenv("OPENWEATHERMAP_API_KEY", "")
    if not key:
        raise ValueError(
            "OPENWEATHERMAP_API_KEY tidak diset di environment variables. "
            "Tambahkan di .env dan restart docker compose."
        )
    return key


def _convert_kelvin_to_celsius(kelvin):
    """Konversi suhu dari Kelvin ke Celsius."""
    return round(kelvin - 273.15, 2)


def _map_owm_code(code):
    """Map kode cuaca OWM ke kondisi & deskripsi."""
    return OWM_CODE_MAP.get(code, ("Unknown", "Tidak Diketahui"))


def ensure_unique_constraint():
    """
    1. Buat tabel cuaca_historis jika belum ada.
    2. Tambahkan UNIQUE constraint pada kolom 'waktu' untuk ON CONFLICT upsert.
    Idempotent — aman dipanggil berulang kali.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgres_traffic")
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        # 1. CREATE TABLE jika belum ada
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cuaca_historis (
                waktu       TIMESTAMP PRIMARY KEY,
                kota        VARCHAR(50),
                suhu        FLOAT,
                suhu_terasa FLOAT,
                kelembapan  FLOAT,
                kondisi     VARCHAR(50),
                deskripsi   VARCHAR(100),
                kecepatan_angin FLOAT,
                curah_hujan     FLOAT,
                cloudiness      FLOAT,
                sumber      VARCHAR(20) DEFAULT 'current'
            )
        """))
        print("Tabel cuaca_historis siap")

        # 1b. Tambah kolom sumber jika belum ada (migrasi tabel lama)
        conn.execute(text("""
            ALTER TABLE cuaca_historis
            ADD COLUMN IF NOT EXISTS sumber VARCHAR(20) DEFAULT 'current'
        """))
        print("Kolom 'sumber' dipastikan ada")

        # 2. Cek UNIQUE constraint pada kolom 'waktu'
        # PRIMARY KEY sudah UNIQUE, jadi langsung aman
        result = conn.execute(text("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'cuaca_historis'
              AND constraint_type = 'PRIMARY KEY'
              AND constraint_name = 'cuaca_historis_pkey'
        """)).fetchone()

        if result is None:
            # Cek duplikat sebelum add constraint
            dupes = conn.execute(text("""
                SELECT waktu, COUNT(*) FROM cuaca_historis
                GROUP BY waktu HAVING COUNT(*) > 1
            """)).fetchall()

            if dupes:
                conn.execute(text("""
                    DELETE FROM cuaca_historis
                    WHERE ctid NOT IN (
                        SELECT max(ctid) FROM cuaca_historis GROUP BY waktu
                    )
                """))
                print(f"🧹 Dihapus {len(dupes)} duplikat waktu sebelum add constraint")

            conn.execute(text("""
                ALTER TABLE cuaca_historis
                ADD CONSTRAINT cuaca_historis_waktu_key UNIQUE (waktu)
            """))
            print("UNIQUE constraint 'waktu' ditambahkan")
        else:
            print("PRIMARY KEY pada 'waktu' sudah ada, skip.")


def _upsert_rows(conn, rows):
    """Helper: upsert batch rows ke cuaca_historis."""
    for row in rows:
        conn.execute(text("""
            INSERT INTO cuaca_historis
                (waktu, kota, suhu, suhu_terasa, kelembapan,
                 kondisi, deskripsi, kecepatan_angin, curah_hujan, cloudiness,
                 sumber)
            VALUES
                (:waktu, :kota, :suhu, :suhu_terasa, :kelembapan,
                 :kondisi, :deskripsi, :kecepatan_angin, :curah_hujan, :cloudiness,
                 :sumber)
            ON CONFLICT (waktu) DO UPDATE SET
                kota = EXCLUDED.kota,
                suhu = EXCLUDED.suhu,
                suhu_terasa = EXCLUDED.suhu_terasa,
                kelembapan = EXCLUDED.kelembapan,
                kondisi = EXCLUDED.kondisi,
                deskripsi = EXCLUDED.deskripsi,
                kecepatan_angin = EXCLUDED.kecepatan_angin,
                curah_hujan = EXCLUDED.curah_hujan,
                cloudiness = EXCLUDED.cloudiness,
                sumber = EXCLUDED.sumber
        """), row)


def fetch_owm_current_and_load(**kwargs):
    """
    REAL-TIME STREAM: Ambil kondisi cuaca aktual dari OWM Current Weather API.

    Pattern real-time:
    - Run setiap 5 menit (schedule */5 * * * *)
    - Setiap run ambil data cuaca terkini (/data/2.5/weather)
    - Upsert ke DB berdasarkan (waktu) untuk menghindari duplikat
    - Kolom `sumber` = 'current' untuk membedakan dari data forecast
    """
    api_key = _get_api_key()
    logical_date = kwargs.get("logical_date") or datetime.utcnow()

    # =========================
    # PASTIKAN TABLE + KOLOM SUMBER ADA
    # =========================
    ensure_unique_constraint()

    # =========================
    # FETCH CURRENT WEATHER (real-time)
    # =========================
    params = {
        "lat": LAT,
        "lon": LON,
        "units": "metric",
        "appid": api_key,
    }

    res = requests.get(API_URL_CURRENT, params=params)
    res.raise_for_status()
    data = res.json()

    if "dt" not in data:
        raise ValueError("OpenWeatherMap Current Weather response tidak valid")

    # =========================
    # PARSE DATA CURRENT WEATHER
    # =========================
    weather = data["weather"][0]
    owm_code = weather["id"]
    kondisi, deskripsi = _map_owm_code(owm_code)

    waktu_dt = datetime.utcfromtimestamp(data["dt"])
    waktu_str = waktu_dt.strftime("%Y-%m-%dT%H:%M")

    row = {
        "waktu": waktu_str,
        "kota": KOTA,
        "suhu": data["main"]["temp"],
        "suhu_terasa": data["main"].get("feels_like"),
        "kelembapan": data["main"]["humidity"],
        "kondisi": kondisi,
        "deskripsi": deskripsi,
        "kecepatan_angin": data["wind"]["speed"],
        "curah_hujan": data.get("rain", {}).get("1h", 0),
        "cloudiness": data["clouds"]["all"],
        "sumber": "current",
    }

    df = pd.DataFrame([row])
    print(f"Real-time: {waktu_str} | {deskripsi} | {df['suhu'].iloc[0]}°C | sumber=current")

    # =========================
    # SAVE CSV (setiap 5 menit)
    # =========================
    folder = "/opt/airflow/data/raw"
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, f"cuaca_warkop_realtime_{logical_date.strftime('%Y%m%d_%H%M')}.csv")
    df.to_csv(file_path, index=False)
    print(f"CSV saved: {file_path}")

    # =========================
    # UPSERT KE POSTGRES
    # =========================
    pg_hook = PostgresHook(postgres_conn_id="postgres_traffic")
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        _upsert_rows(conn, [row])

    print(f" ✅ SUCCESS upsert data real-time ke cuaca_historis")


def fetch_owm_batch_backfill(**kwargs):
    """
    BATCH MODE: Ambil semua 40 interval (5 hari) untuk backfill/catchup.

    Digunakan saat DAG pertama kali jalan atau catchup=True.
    """
    api_key = _get_api_key()

    # =========================
    # PASTIKAN UNIQUE CONSTRAINT ADA
    # =========================
    ensure_unique_constraint()

    # =========================
    # FETCH SEMUA INTERVAL (40 × 3-jam = 5 hari)
    # =========================
    params = {
        "lat": LAT,
        "lon": LON,
        "units": "metric",
        "cnt": 40,  # SEMUA INTERVAL
        "appid": api_key,
    }

    res = requests.get(API_URL_FORECAST, params=params)
    res.raise_for_status()
    data = res.json()

    if "list" not in data or len(data["list"]) == 0:
        raise ValueError("OpenWeatherMap response kosong atau tidak valid")

    # =========================
    # PARSE SEMUA INTERVAL
    # =========================
    rows = []
    for item in data["list"]:
        weather = item["weather"][0]
        owm_code = weather["id"]
        kondisi, deskripsi = _map_owm_code(owm_code)

        waktu_dt = datetime.utcfromtimestamp(item["dt"])
        waktu_str = waktu_dt.strftime("%Y-%m-%dT%H:%M")

        rows.append({
            "waktu": waktu_str,
            "kota": KOTA,
            "suhu": item["main"]["temp"],
            "suhu_terasa": item["main"].get("feels_like", None),
            "kelembapan": item["main"]["humidity"],
            "kondisi": kondisi,
            "deskripsi": deskripsi,
            "kecepatan_angin": item["wind"]["speed"],
            "curah_hujan": item.get("rain", {}).get("3h", 0),
            "cloudiness": item["clouds"]["all"],
            "sumber": "forecast",
        })

    df = pd.DataFrame(rows)
    print(f"📦 Batch backfill: {len(df)} rows | range: {df['waktu'].iloc[0]} → {df['waktu'].iloc[-1]}")

    # =========================
    # SAVE CSV
    # =========================
    folder = "/opt/airflow/data/raw"
    os.makedirs(folder, exist_ok=True)
    start_date = df["waktu"].iloc[0][:10]
    end_date = df["waktu"].iloc[-1][:10]
    file_path = os.path.join(folder, f"cuaca_warkop_{start_date}_to_{end_date}.csv")
    df.to_csv(file_path, index=False)
    print(f"💾 CSV saved: {file_path}")

    # =========================
    # UPSERT BATCH KE POSTGRES
    # =========================
    pg_hook = PostgresHook(postgres_conn_id="postgres_traffic")
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        _upsert_rows(conn, rows)

    print(f"🚀 SUCCESS upsert {len(df)} rows batch ke cuaca_historis")

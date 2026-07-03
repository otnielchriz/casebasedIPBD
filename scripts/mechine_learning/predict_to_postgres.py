import os
import joblib
import pandas as pd

from datetime import date, timedelta
from sqlalchemy import create_engine, text

# ==========================
# 1. KONFIGURASI DATABASE
# ==========================
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "airflow"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "model_prediksi_pendapatan.pkl")

# ==========================
# 2. LOAD MODEL
# ==========================
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Model tidak ditemukan: {MODEL_PATH}")

model = joblib.load(MODEL_PATH)

features = [
    "rata_suhu",
    "rata_suhu_terasa",
    "rata_kelembapan",
    "rata_kecepatan_angin",
    "rata_curah_hujan",
    "rata_cloudness",
    "day_of_week",
    "day_of_month",
    "bulan",
    "is_weekend",
    "is_libur",
]

# ==========================
# 3. RANGE PREDIKSI
# ==========================
today = date.today()
start_date = today + timedelta(days=1)
end_date = today + timedelta(days=14)

print("Prediksi dibuat pada:", today)
print("Prediksi dari      :", start_date)
print("Prediksi sampai    :", end_date)

# ==========================
# 4. AMBIL DATA CUACA FORECAST
# ==========================
query = """
SELECT
    c.tanggal AS tanggal_prediksi,
    c.rata_suhu,
    c.rata_suhu_terasa,
    c.rata_kelembapan,
    c.rata_kecepatan_angin,
    c.rata_curah_hujan,
    c.rata_cloudness,

    EXTRACT(DOW FROM c.tanggal)::int AS day_of_week,
    EXTRACT(DAY FROM c.tanggal)::int AS day_of_month,
    EXTRACT(MONTH FROM c.tanggal)::int AS bulan,

    COALESCE(h.is_weekend, 0) AS is_weekend,
    COALESCE(h.is_libur, 0) AS is_libur

FROM vw_cuaca_operasional c
LEFT JOIN (
    SELECT
        tanggal,
        MAX(CASE WHEN COALESCE(is_weekend, false) THEN 1 ELSE 0 END) AS is_weekend,
        MAX(CASE WHEN COALESCE(is_libur, false) THEN 1 ELSE 0 END) AS is_libur
    FROM hari_libur
    GROUP BY tanggal
) h
    ON c.tanggal = h.tanggal
WHERE c.tanggal BETWEEN :start_date AND :end_date
ORDER BY c.tanggal;
"""

df = pd.read_sql(
    text(query),
    engine,
    params={
        "start_date": start_date,
        "end_date": end_date,
    },
)

print("\n=== DATA UNTUK PREDIKSI ===")
print(df.head())
print("\nJumlah data:", len(df))

if df.empty:
    print("Tidak ada data cuaca forecast untuk range prediksi.")
    raise SystemExit

df = df.dropna(subset=features)

print("Jumlah data setelah drop null:", len(df))

if df.empty:
    print("Semua data prediksi kosong setelah drop null.")
    raise SystemExit

# ==========================
# 5. PREDIKSI
# ==========================
df["prediksi_pendapatan"] = model.predict(df[features])
df["model_name"] = type(model).__name__

output = df[
    [
        "tanggal_prediksi",
        "rata_suhu",
        "rata_suhu_terasa",
        "rata_kelembapan",
        "rata_kecepatan_angin",
        "rata_curah_hujan",
        "rata_cloudness",
        "day_of_week",
        "day_of_month",
        "bulan",
        "is_weekend",
        "is_libur",
        "prediksi_pendapatan",
        "model_name",
    ]
].copy()

# ==========================
# 6. INSERT / UPDATE POSTGRESQL
# ==========================
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS prediksi_pendapatan (
            id SERIAL PRIMARY KEY,
            tanggal_prediksi DATE NOT NULL UNIQUE,
            rata_suhu NUMERIC(10,2),
            rata_suhu_terasa NUMERIC(10,2),
            rata_kelembapan NUMERIC(10,2),
            rata_kecepatan_angin NUMERIC(10,2),
            rata_curah_hujan NUMERIC(10,2),
            rata_cloudness NUMERIC(10,2),
            day_of_week INTEGER,
            day_of_month INTEGER,
            bulan INTEGER,
            is_weekend INTEGER,
            is_libur INTEGER,
            prediksi_pendapatan NUMERIC(15,2),
            model_name VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS prediksi_pendapatan_history (
            id SERIAL PRIMARY KEY,
            tanggal_generate DATE NOT NULL,
            tanggal_prediksi DATE NOT NULL,
            prediksi_pendapatan NUMERIC(15,2),
            model_name VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tanggal_generate, tanggal_prediksi)
        );
    """))

    # Prediksi aktif selalu fresh 14 hari ke depan
    conn.execute(text("TRUNCATE TABLE prediksi_pendapatan RESTART IDENTITY;"))

    for _, row in output.iterrows():
        data = row.to_dict()
        data["tanggal_generate"] = today

        conn.execute(
            text("""
                INSERT INTO prediksi_pendapatan (
                    tanggal_prediksi,
                    rata_suhu,
                    rata_suhu_terasa,
                    rata_kelembapan,
                    rata_kecepatan_angin,
                    rata_curah_hujan,
                    rata_cloudness,
                    day_of_week,
                    day_of_month,
                    bulan,
                    is_weekend,
                    is_libur,
                    prediksi_pendapatan,
                    model_name
                )
                VALUES (
                    :tanggal_prediksi,
                    :rata_suhu,
                    :rata_suhu_terasa,
                    :rata_kelembapan,
                    :rata_kecepatan_angin,
                    :rata_curah_hujan,
                    :rata_cloudness,
                    :day_of_week,
                    :day_of_month,
                    :bulan,
                    :is_weekend,
                    :is_libur,
                    :prediksi_pendapatan,
                    :model_name
                );
            """),
            data,
        )

        conn.execute(
            text("""
                INSERT INTO prediksi_pendapatan_history (
                    tanggal_generate,
                    tanggal_prediksi,
                    prediksi_pendapatan,
                    model_name
                )
                VALUES (
                    :tanggal_generate,
                    :tanggal_prediksi,
                    :prediksi_pendapatan,
                    :model_name
                )
                ON CONFLICT (tanggal_prediksi)
                DO UPDATE SET
                    tanggal_generate = EXCLUDED.tanggal_generate,
                    prediksi_pendapatan = EXCLUDED.prediksi_pendapatan,
                    model_name = EXCLUDED.model_name,
                    created_at = CURRENT_TIMESTAMP;
            """),
            data,
        )

print("\nPrediksi aktif dan history berhasil disimpan.")
print(output)
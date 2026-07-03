import os
import joblib
import pandas as pd
import numpy as np

from sqlalchemy import create_engine, text
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

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
IMPORTANCE_PATH = os.path.join(BASE_DIR, "feature_importance.csv")

# ==========================
# 2. AMBIL DATASET ML
# ==========================
query = """
SELECT
    tanggal,
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
    total_pendapatan
FROM vw_dataset_ml
WHERE total_pendapatan IS NOT NULL
ORDER BY tanggal;
"""

df = pd.read_sql(query, engine)
df["tanggal"] = pd.to_datetime(df["tanggal"])

print("=== DATA AWAL ===")
print(df.head())
print("\nJumlah data awal:", len(df))

# ==========================
# 3. CLEANING DATA
# ==========================
df = df.dropna()

print("Jumlah data setelah drop null:", len(df))


# ==========================
# 4. SPLIT DATA 80:20 TEMPORAL SPLIT (ADAPTIVE)
# ==========================

df = df.sort_values("tanggal")

unique_dates = sorted(df["tanggal"].unique())

# Calculate 80:20 split dynamically
total_dates = len(unique_dates)
test_size = int(total_dates * 0.2)  # 20% untuk test
test_dates = unique_dates[-test_size:]

train_df = df[~df["tanggal"].isin(test_dates)]
test_df = df[df["tanggal"].isin(test_dates)]

print("\n=== PERIODE DATA (80:20 SPLIT) ===")
print("Train:", train_df["tanggal"].min().date(), "s/d", train_df["tanggal"].max().date())
print("Test :", test_df["tanggal"].min().date(), "s/d", test_df["tanggal"].max().date())
print("Jumlah train:", len(train_df), f"({len(train_df)/len(df)*100:.1f}%)")
print("Jumlah test :", len(test_df), f"({len(test_df)/len(df)*100:.1f}%)")

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

X_train = train_df[features]
y_train = train_df["total_pendapatan"]

X_test = test_df[features]
y_test = test_df["total_pendapatan"]

# ==========================
# 5. MODEL
# ==========================
models = {
    "Random Forest": RandomForestRegressor(
        n_estimators=200,
        max_depth=5,
        min_samples_leaf=2,
        random_state=42
    ),
    "Gradient Boosting": GradientBoostingRegressor(
        n_estimators=150,
        learning_rate=0.05,
        max_depth=2,
        random_state=42
    )
}

results = []

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
    r2 = r2_score(y_test, y_pred)

    print(f"\n=== HASIL EVALUASI {name} ===")
    print(f"MAE  : Rp {mae:,.0f}")
    print(f"RMSE : Rp {rmse:,.0f}")
    print(f"MAPE : {mape:.2f}%")
    print(f"R2   : {r2:.4f}")

    results.append({
        "model_name": name,
        "model": model,
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
        "r2": r2
    })

# ==========================
# 6. PILIH MODEL TERBAIK
# ==========================
best = min(results, key=lambda x: x["mae"])

best_model = best["model"]
best_name = best["model_name"]

print(f"\n=== MODEL TERBAIK: {best_name} ===")
print(f"MAE  : Rp {best['mae']:,.0f}")
print(f"RMSE : Rp {best['rmse']:,.0f}")
print(f"MAPE : {best['mape']:.2f}%")
print(f"R2   : {best['r2']:.4f}")

# ==========================
# 7. FEATURE IMPORTANCE
# ==========================
importance = pd.DataFrame({
    "fitur": features,
    "importance": best_model.feature_importances_
}).sort_values(by="importance", ascending=False)

print("\n=== FEATURE IMPORTANCE ===")
print(importance)

importance.to_csv(IMPORTANCE_PATH, index=False)
joblib.dump(best_model, MODEL_PATH)

print(f"\nModel berhasil disimpan: {MODEL_PATH}")
print(f"Feature importance berhasil disimpan: {IMPORTANCE_PATH}")

# ==========================
# 8. SIMPAN METRICS KE POSTGRESQL
# ==========================
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ml_model_metrics (
            id SERIAL PRIMARY KEY,
            training_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            model_name VARCHAR(100),
            train_start DATE,
            train_end DATE,
            test_start DATE,
            test_end DATE,
            total_train_data INTEGER,
            total_test_data INTEGER,
            mae NUMERIC(15,2),
            rmse NUMERIC(15,2),
            mape NUMERIC(10,2),
            r2 NUMERIC(10,4)
        );
    """))

    conn.execute(
        text("""
            INSERT INTO ml_model_metrics (
                model_name,
                train_start,
                train_end,
                test_start,
                test_end,
                total_train_data,
                total_test_data,
                mae,
                rmse,
                mape,
                r2
            )
            VALUES (
                :model_name,
                :train_start,
                :train_end,
                :test_start,
                :test_end,
                :total_train_data,
                :total_test_data,
                :mae,
                :rmse,
                :mape,
                :r2
            );
        """),
        {
            "model_name": best_name,
            "train_start": train_df["tanggal"].min().date(),
            "train_end": train_df["tanggal"].max().date(),
            "test_start": test_df["tanggal"].min().date(),
            "test_end": test_df["tanggal"].max().date(),
            "total_train_data": len(train_df),
            "total_test_data": len(test_df),
            "mae": float(best["mae"]),
            "rmse": float(best["rmse"]),
            "mape": float(best["mape"]),
            "r2": float(best["r2"]),
        }
    )

print("\nMetrics berhasil disimpan ke tabel ml_model_metrics")
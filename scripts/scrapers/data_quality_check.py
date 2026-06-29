import sys
import pandas as pd
from sqlalchemy import create_engine
import great_expectations as gx

# Use the Docker internal DB connection URI
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

def run_data_quality_validation():
    print("🚀 Starting Data Quality validation via Great Expectations...")
    
    engine = create_engine(DB_URL)
    
    # 1. Validate 'cuaca_historis' table
    print("📋 Validating table 'cuaca_historis'...")
    try:
        df_cuaca = pd.read_sql("SELECT waktu, suhu, kelembapan, curah_hujan FROM cuaca_historis", engine)
    except Exception as e:
        print(f"❌ Failed to read cuaca_historis: {e}")
        raise e
        
    gx_cuaca = gx.dataset.PandasDataset(df_cuaca)
    
    # Rules (Expectations) for weather data
    chk_cuaca_waktu = gx_cuaca.expect_column_values_to_not_be_null("waktu")
    chk_cuaca_suhu = gx_cuaca.expect_column_values_to_be_between("suhu", min_value=-5, max_value=55)
    chk_cuaca_kelembapan = gx_cuaca.expect_column_values_to_be_between("kelembapan", min_value=0, max_value=100)
    chk_cuaca_hujan = gx_cuaca.expect_column_values_to_be_between("curah_hujan", min_value=0, max_value=500)
    
    # 2. Validate 'pendapatan_harian' table
    print("📋 Validating table 'pendapatan_harian'...")
    try:
        df_income = pd.read_sql("SELECT tanggal, total_pendapatan FROM pendapatan_harian", engine)
    except Exception as e:
        print(f"❌ Failed to read pendapatan_harian: {e}")
        raise e
        
    gx_income = gx.dataset.PandasDataset(df_income)
    
    # Rules (Expectations) for daily revenue data
    chk_inc_tgl = gx_income.expect_column_values_to_not_be_null("tanggal")
    chk_inc_val = gx_income.expect_column_values_to_be_between("total_pendapatan", min_value=0, max_value=100000000) # Max 100 Juta
    
    # Log results
    print("\n=== Data Quality Report (Great Expectations) ===")
    print(f"cuaca_historis - waktu not null: {chk_cuaca_waktu.success}")
    print(f"cuaca_historis - suhu between -5 and 55: {chk_cuaca_suhu.success}")
    print(f"cuaca_historis - kelembapan between 0 and 100: {chk_cuaca_kelembapan.success}")
    print(f"cuaca_historis - curah_hujan between 0 and 500: {chk_cuaca_hujan.success}")
    print(f"pendapatan_harian - tanggal not null: {chk_inc_tgl.success}")
    print(f"pendapatan_harian - total_pendapatan >= 0: {chk_inc_val.success}")
    
    # Validate final outcome
    all_success = (
        chk_cuaca_waktu.success and 
        chk_cuaca_suhu.success and 
        chk_cuaca_kelembapan.success and 
        chk_cuaca_hujan.success and 
        chk_inc_tgl.success and 
        chk_inc_val.success
    )
    
    if not all_success:
        failed_tests = []
        if not chk_cuaca_waktu.success: failed_tests.append("cuaca_historis.waktu IS NULL")
        if not chk_cuaca_suhu.success: failed_tests.append("cuaca_historis.suhu out of bounds (-5 to 55)")
        if not chk_cuaca_kelembapan.success: failed_tests.append("cuaca_historis.kelembapan out of bounds (0 to 100)")
        if not chk_cuaca_hujan.success: failed_tests.append("cuaca_historis.curah_hujan out of bounds (0 to 500)")
        if not chk_inc_tgl.success: failed_tests.append("pendapatan_harian.tanggal IS NULL")
        if not chk_inc_val.success: failed_tests.append("pendapatan_harian.total_pendapatan negative or excessive")
        
        err_msg = f"Data Quality Check FAILED! Issues found: {', '.join(failed_tests)}"
        print(f"❌ {err_msg}")
        raise ValueError(err_msg)
        
    print("✅ All Great Expectations validations passed successfully!")

if __name__ == "__main__":
    run_data_quality_validation()

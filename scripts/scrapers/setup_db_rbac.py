import psycopg2
from psycopg2 import sql

def setup_rbac():
    try:
        # Connect to local Postgres using mapped port 5435
        conn = psycopg2.connect(
            host="localhost",
            port=5435,
            database="airflow",
            user="airflow",
            password="airflow"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 1. Create user if not exists
        print("Mengecek dan membuat user 'metabase_user'...")
        cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'metabase_user') THEN
                CREATE USER metabase_user WITH PASSWORD 'metabase_pass';
            END IF;
        END
        $$;
        """)
        
        # 2. Grant connection and schema privileges
        print("Mengatur hak akses database dan schema...")
        cursor.execute("GRANT CONNECT ON DATABASE airflow TO metabase_user;")
        cursor.execute("GRANT USAGE ON SCHEMA public TO metabase_user;")
        
        # 3. Grant select on all existing tables and views
        print("Mengatur hak akses SELECT pada tabel dan view...")
        cursor.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_user;")
        cursor.execute("GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO metabase_user;")
        
        # 4. Alter default privileges for future tables
        print("Mengatur hak akses default untuk tabel baru di masa depan...")
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO metabase_user;")
        
        cursor.close()
        conn.close()
        print("SUCCESS: Konfigurasi RBAC PostgreSQL selesai!")
    except Exception as e:
        print(f"ERROR: Gagal setup RBAC PostgreSQL: {e}")

if __name__ == "__main__":
    setup_rbac()

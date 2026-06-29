select * from  pendapatan_harian
order by tanggal;
select * from  cuaca_forecast
order by waktu;
select * from  hari_libur ;
select * from cuaca_stream_raw;


SELECT
    p.tanggal,
    MAX(p.total_pendapatan) AS total_pendapatan,
    AVG(c.curah_hujan) AS curah_hujan
FROM pendapatan_harian p
LEFT JOIN cuaca_forecast c
    ON p.tanggal = c.waktu::date
GROUP BY p.tanggal
ORDER BY p.tanggal;

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

CREATE TABLE IF NOT EXISTS cuaca_forecast (
    waktu TIMESTAMP NOT NULL,
    kota VARCHAR(100) NOT NULL,
    weather_code INTEGER,
    suhu NUMERIC(5,2),
    suhu_terasa NUMERIC(5,2),
    kelembapan INTEGER,
    kecepatan_angin NUMERIC(6,2),
    curah_hujan NUMERIC(6,2),
    cloudiness INTEGER
);









--========================================
--=========== CREATE VIEW ================
--========================================


-- ========= DASHBOARD ANALITYC ===========
CREATE OR REPLACE VIEW vw_cuaca_operasional AS
SELECT
    waktu::date AS tanggal,

    AVG(suhu) AS rata_suhu,
    AVG(suhu_terasa) AS rata_suhu_terasa,
    AVG(kelembapan) AS rata_kelembapan,
    AVG(kecepatan_angin) AS rata_kecepatan_angin,
    AVG(curah_hujan) AS rata_curah_hujan,
    AVG(cuaca_forecast.cloudiness) AS rata_cloudness,

    MAX(curah_hujan) AS max_curah_hujan,

    CASE
        WHEN COALESCE(AVG(curah_hujan), 0) = 0 THEN 'Tidak Hujan'
        WHEN AVG(curah_hujan) <= 2.5 THEN 'Hujan Ringan'
        WHEN AVG(curah_hujan) <= 10 THEN 'Hujan Sedang'
        ELSE 'Hujan Lebat'
    END AS kategori_hujan,

    CASE
        WHEN AVG(suhu) < 25 THEN 'Dingin (<25)'
        WHEN AVG(suhu) < 26 THEN 'Sedikit Dingin (25-26)'
        WHEN AVG(suhu) <= 27 THEN 'Normal (26-27)'
        ELSE 'Panas (>27)'
    END AS kategori_suhu

FROM cuaca_forecast
WHERE waktu::time >= '16:00:00'
  AND waktu::time <= '23:59:59'
GROUP BY waktu::date;


-- ========= VIEW ML ===========


CREATE VIEW vw_dataset_ml AS
WITH pendapatan AS (
    SELECT
        tanggal,
        MAX(total_pendapatan) AS total_pendapatan
    FROM pendapatan_harian
    GROUP BY tanggal
),
cuaca AS (
    SELECT
        tanggal,
        MAX(rata_suhu) AS rata_suhu,
        MAX(rata_suhu_terasa) AS rata_suhu_terasa,
        MAX(rata_kelembapan) AS rata_kelembapan,
        MAX(rata_kecepatan_angin) AS rata_kecepatan_angin,
        MAX(rata_curah_hujan) AS rata_curah_hujan,
        MAX(rata_cloudness) AS rata_cloudness
    FROM vw_cuaca_operasional
    GROUP BY tanggal
),
hari AS (
    SELECT
        tanggal,
        MAX(CASE WHEN COALESCE(is_weekend, false) THEN 1 ELSE 0 END) AS is_weekend,
        MAX(CASE WHEN COALESCE(is_libur, false) THEN 1 ELSE 0 END) AS is_libur
    FROM hari_libur
    GROUP BY tanggal
)
SELECT
    p.tanggal,

    c.rata_suhu,
    c.rata_suhu_terasa,
    c.rata_kelembapan,
    c.rata_kecepatan_angin,
    c.rata_curah_hujan,
    c.rata_cloudness,

    EXTRACT(DOW FROM p.tanggal)::int AS day_of_week,
    EXTRACT(DAY FROM p.tanggal)::int AS day_of_month,
    EXTRACT(MONTH FROM p.tanggal)::int AS bulan,

    COALESCE(h.is_weekend, 0) AS is_weekend,
    COALESCE(h.is_libur, 0) AS is_libur,

    p.total_pendapatan
FROM pendapatan p
LEFT JOIN cuaca c
    ON p.tanggal = c.tanggal
LEFT JOIN hari h
    ON p.tanggal = h.tanggal;

SELECT *
FROM vw_dataset_ml
order by tanggal;

-- Buat Table Untuk Prediksi Pendapatan
CREATE TABLE prediksi_pendapatan (
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


SELECT *
FROM prediksi_pendapatan
ORDER BY tanggal_prediksi asc;



-- Histori Prediksi Pendapatan
CREATE TABLE prediksi_pendapatan_history (
    id SERIAL PRIMARY KEY,
    tanggal_generate DATE NOT NULL,
    tanggal_prediksi DATE NOT NULL UNIQUE,
    prediksi_pendapatan NUMERIC(15,2),
    model_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT *
FROM prediksi_pendapatan_history
ORDER BY tanggal_prediksi asc;




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

SELECT *
FROM ml_model_metrics
ORDER BY training_date DESC;




-- ========= VIEW STREAM ================
CREATE OR REPLACE VIEW vw_cuaca_stream AS
SELECT
    waktu_ambil,
    kota,
    weather_code,
    suhu,
    suhu_terasa,
    kelembapan,
    kecepatan_angin,
    curah_hujan,
    cloudiness,

    CASE
        WHEN weather_code = 0 THEN 'Clear'
        WHEN weather_code IN (1,2,3) THEN 'Clouds'
        WHEN weather_code IN (45,48) THEN 'Fog'
        WHEN weather_code IN (51,53,55,56,57) THEN 'Drizzle'
        WHEN weather_code IN (61,63,65,66,67,80,81,82) THEN 'Rain'
        WHEN weather_code IN (95,96,99) THEN 'Thunderstorm'
        ELSE 'Unknown'
    END AS kondisi,

    CASE
        WHEN weather_code = 0 THEN 'Cerah'
        WHEN weather_code = 1 THEN 'Cerah Berawan'
        WHEN weather_code = 2 THEN 'Berawan Sebagian'
        WHEN weather_code = 3 THEN 'Mendung'
        WHEN weather_code = 45 THEN 'Berkabut'
        WHEN weather_code = 48 THEN 'Kabut Tebal'
        WHEN weather_code = 51 THEN 'Gerimis Ringan'
        WHEN weather_code = 53 THEN 'Gerimis Sedang'
        WHEN weather_code = 55 THEN 'Gerimis Lebat'
        WHEN weather_code = 61 THEN 'Hujan Ringan'
        WHEN weather_code = 63 THEN 'Hujan Sedang'
        WHEN weather_code = 65 THEN 'Hujan Lebat'
        WHEN weather_code = 80 THEN 'Hujan Lokal Ringan'
        WHEN weather_code = 81 THEN 'Hujan Lokal Sedang'
        WHEN weather_code = 82 THEN 'Hujan Lokal Lebat'
        WHEN weather_code = 95 THEN 'Badai Petir'
        WHEN weather_code = 96 THEN 'Badai Petir dengan Hujan Es Ringan'
        WHEN weather_code = 99 THEN 'Badai Petir dengan Hujan Es Lebat'
        ELSE 'Tidak Diketahui'
    END AS deskripsi
FROM cuaca_stream_raw;

select * from vw_cuaca_stream



SELECT
    tanggal,
    total_pendapatan
FROM pendapatan_harian
WHERE tanggal BETWEEN '2026-05-24' AND '2026-05-31'
ORDER BY tanggal;

select * from v_analisis_warkop;

-- View untuk data cuaca terurut (siap pakai di Metabase tanpa menulis SQL tambahan)
CREATE OR REPLACE VIEW v_cuaca_historis_urut AS
SELECT * FROM cuaca_historis 
ORDER BY waktu ASC;

select * from v_cuaca_historis_urut;

-- ==========================================
-- METADATA & DATA GOVERNANCE (RPS Poin 9)
-- ==========================================

-- Deskripsi Kepemilikan Tabel (Owner Metadata)
COMMENT ON TABLE cuaca_historis IS 'Tabel data cuaca historis Sukoharjo. Owner: Zaki (Infrastruktur & Core Pipeline)';
COMMENT ON TABLE pendapatan_harian IS 'Tabel data pendapatan harian Warkop Kusuma. Owner: warkop_kusuma';
COMMENT ON TABLE hari_libur IS 'Tabel kalender hari libur Indonesia 2026. Owner: Zaki (Infrastruktur & Core Pipeline)';
COMMENT ON TABLE prediksi_pendapatan IS 'Tabel hasil ramalan pendapatan warkop 14 hari ke depan. Owner: Otniel (ML & Analytics)';

-- Deskripsi Kolom (Column Metadata)
COMMENT ON COLUMN cuaca_historis.waktu IS 'Waktu pencatatan cuaca (format WIB)';
COMMENT ON COLUMN cuaca_historis.suhu IS 'Suhu udara rata-rata dalam derajat Celsius';
COMMENT ON COLUMN pendapatan_harian.total_pendapatan IS 'Total omzet bersih warkop pada tanggal tersebut (rupiah)';
COMMENT ON COLUMN hari_libur.is_libur IS 'Flag penanda hari libur (True untuk weekend/libur nasional/cuti bersama)';

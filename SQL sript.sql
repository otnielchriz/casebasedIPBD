select * from  pendapatan_harian
order by tanggal as
select * from  cuaca_forecast;
select * from  hari_libur ;
select * from cuaca_stream_raw;

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
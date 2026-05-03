select * from  pendapatan_harian; 
select * from  cuaca_historis;
select * from  hari_libur ;



--========================================
--=========== CREATE VIEW ================
--========================================


drop view v_analisis_warkop  
drop view v_cuaca_harian 

--Menjadikan seluruh elemen cuaca menjadi 1 skor
CREATE OR REPLACE VIEW v_cuaca_harian AS
SELECT
    waktu::date AS tanggal,

    ROUND(AVG(suhu)::numeric, 2) AS avg_suhu,
    ROUND(AVG(suhu_terasa)::numeric, 2) AS avg_suhu_terasa,
    ROUND(AVG(kelembapan)::numeric, 2) AS avg_kelembapan,
    ROUND(AVG(kecepatan_angin)::numeric, 2) AS avg_angin,
    ROUND(SUM(curah_hujan)::numeric, 2) AS total_hujan,
    ROUND(AVG(cloudiness)::numeric, 2) AS avg_cloudiness,

    MODE() WITHIN GROUP (ORDER BY kondisi) AS kondisi_dominan,
    MODE() WITHIN GROUP (ORDER BY deskripsi) AS deskripsi_dominan

FROM cuaca_historis
WHERE EXTRACT(HOUR FROM waktu) BETWEEN 14 AND 23
GROUP BY waktu::date;

select * from  v_cuaca_harian


SELECT
    p.tanggal,
    p.total_pendapatan,

    c.avg_suhu,
    c.avg_suhu_terasa,
    c.avg_kelembapan,
    c.avg_angin,
    c.total_hujan,
    c.avg_cloudiness,
    c.kondisi_dominan,

    h.is_libur,
    h.is_weekend

FROM pendapatan_harian p
LEFT JOIN v_cuaca_harian c ON p.tanggal = c.tanggal
LEFT JOIN hari_libur h ON p.tanggal = h.tanggal;




-- Menggabungkan antara Fakror Cauaca, Hari libur, dan Penghasilan untuk menghasilkan insight utama
CREATE OR REPLACE VIEW v_analisis_warkop AS
SELECT
    p.tanggal,
    p.total_pendapatan,

    -- ===== HARI =====
    h.is_weekend,
    CASE
        WHEN h.is_weekend THEN 'Weekend'
        ELSE 'Weekday'
    END AS tipe_hari,

    h.is_libur,
    h.keterangan,

    -- ===== CUACA =====
    c.avg_suhu,
    c.avg_kelembapan,
    c.avg_angin,
    c.total_hujan,
    c.avg_cloudiness,
    c.kondisi_dominan,
    c.deskripsi_dominan,

    -- ===== BUCKET CUACA (biar gampang analisis) =====
    CASE
        WHEN c.total_hujan = 0 THEN 'Tidak Hujan'
        WHEN c.total_hujan <= 2 THEN 'Hujan Ringan'
        WHEN c.total_hujan <= 5 THEN 'Hujan Sedang'
        ELSE 'Hujan Lebat'
    END AS kategori_hujan,

    CASE
        WHEN c.avg_suhu < 26 THEN 'Dingin'
        WHEN c.avg_suhu BETWEEN 26 AND 27 THEN 'Hangat'
        WHEN c.avg_suhu BETWEEN 27 AND 28 THEN 'Normal'
        ELSE 'Panas'
    END AS kategori_suhu

FROM pendapatan_harian p
LEFT JOIN hari_libur h
    ON p.tanggal = h.tanggal
LEFT JOIN v_cuaca_harian c
    ON p.tanggal = c.tanggal;

select * from v_analisis_warkop
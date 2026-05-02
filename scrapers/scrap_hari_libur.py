import pandas as pd
import os
from datetime import date, timedelta
import pytz

WIB = pytz.timezone('Asia/Jakarta')

# Data resmi SKB 3 Menteri 2026
HARI_LIBUR_NASIONAL_2026 = {
    # Libur Nasional
    date(2026, 1,  1) : 'Tahun Baru 2026 Masehi',
    date(2026, 1, 16) : 'Isra Mikraj Nabi Muhammad SAW',
    date(2026, 2, 17) : 'Tahun Baru Imlek 2577',
    date(2026, 3, 19) : 'Hari Suci Nyepi',
    date(2026, 3, 21) : 'Idul Fitri 1447 H (Hari 1)',
    date(2026, 3, 22) : 'Idul Fitri 1447 H (Hari 2)',
    date(2026, 4,  3) : 'Wafat Yesus Kristus',
    date(2026, 4,  5) : 'Kebangkitan Yesus Kristus (Paskah)',
    date(2026, 5,  1) : 'Hari Buruh Internasional',
    date(2026, 5, 14) : 'Kenaikan Yesus Kristus',
    date(2026, 5, 27) : 'Idul Adha 1447 H',
    date(2026, 5, 31) : 'Hari Raya Waisak 2570 BE',
    date(2026, 6,  1) : 'Hari Lahir Pancasila',
    date(2026, 6, 16) : 'Tahun Baru Islam 1448 H',
    date(2026, 8, 17) : 'HUT Kemerdekaan RI ke-81',
    date(2026, 8, 25) : 'Maulid Nabi Muhammad SAW',
    date(2026, 12,25) : 'Hari Raya Natal',

    # Cuti Bersama
    date(2026, 2, 16) : 'Cuti Bersama Tahun Baru Imlek',
    date(2026, 3, 18) : 'Cuti Bersama Nyepi',
    date(2026, 3, 20) : 'Cuti Bersama Idul Fitri',
    date(2026, 3, 23) : 'Cuti Bersama Idul Fitri',
    date(2026, 3, 24) : 'Cuti Bersama Idul Fitri',
    date(2026, 5, 15) : 'Cuti Bersama Kenaikan Yesus Kristus',
    date(2026, 5, 28) : 'Cuti Bersama Idul Adha',
    date(2026, 12,24) : 'Cuti Bersama Natal',
}

def generate_hari_libur(tahun=2026):
    output_dir = '/opt/airflow/data/raw'
    os.makedirs(output_dir, exist_ok=True)

    NAMA_HARI = {
        0: 'Senin', 1: 'Selasa', 2: 'Rabu',
        3: 'Kamis', 4: 'Jumat', 5: 'Sabtu', 6: 'Minggu'
    }

    hasil = []
    tanggal = date(tahun, 1, 1)
    akhir_tahun = date(tahun, 12, 31)

    while tanggal <= akhir_tahun:
        hari = tanggal.weekday()
        nama_hari = NAMA_HARI[hari]

        is_weekend = hari in [4, 5, 6]  # Jumat, Sabtu, Minggu
        is_libur_nasional = tanggal in HARI_LIBUR_NASIONAL_2026
        is_cuti_bersama = 'Cuti Bersama' in HARI_LIBUR_NASIONAL_2026.get(tanggal, '')
        is_libur = is_weekend or is_libur_nasional

        keterangan = HARI_LIBUR_NASIONAL_2026.get(tanggal, '')
        if not keterangan and is_weekend:
            keterangan = f'Akhir Pekan ({nama_hari})'

        hasil.append({
            'tanggal'          : tanggal.strftime('%Y-%m-%d'),
            'nama_hari'        : nama_hari,
            'is_libur'         : is_libur,
            'is_weekend'       : is_weekend,
            'is_libur_nasional': is_libur_nasional,
            'is_cuti_bersama'  : is_cuti_bersama,
            'keterangan'       : keterangan,
        })

        tanggal += timedelta(days=1)

    df = pd.DataFrame(hasil)
    path = os.path.join(output_dir, 'hari_libur_2026.csv')
    df.to_csv(path, index=False)

    print(f"Berhasil generate {len(df)} hari kalender 2026!")
    print(f"Total hari libur     : {df['is_libur'].sum()}")
    print(f"Total libur nasional : {df['is_libur_nasional'].sum()}")
    print(f"Total cuti bersama   : {df['is_cuti_bersama'].sum()}")
    print(f"Total weekend        : {df['is_weekend'].sum()}")

    return df

if __name__ == "__main__":
    generate_hari_libur()
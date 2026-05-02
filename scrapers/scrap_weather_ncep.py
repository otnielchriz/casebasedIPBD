import requests
import pandas as pd
import os

def fetch_weather_ncep(**kwargs):
    tanggal_eksekusi = kwargs['ds']
    
    LAT = -7.7073187
    LON = 110.8379588
    KOTA = 'Node_Warkop_Kusuma' # Sesuai variabel kotamu
    
    print(f"Menarik data cuaca lengkap untuk tanggal: {tanggal_eksekusi}")

    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # KITA TAMBAHKAN SEMUA PARAMETER YANG KAMU BUTUHKAN DI SINI
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": tanggal_eksekusi,
        "end_date": tanggal_eksekusi,
        "hourly": [
            "temperature_2m",         # suhu
            "apparent_temperature",   # suhu_terasa
            "relative_humidity_2m",   # kelembapan
            "weather_code",           # untuk kondisi & deskripsi
            "wind_speed_10m",         # kecepatan_angin
            "precipitation",          # curah_hujan (mm)
            "cloud_cover"             # cloudiness (%)
        ],
        "timezone": "Asia/Jakarta"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    hourly_data = data['hourly']
    
    # --- KAMUS PENERJEMAH KODE CUACA (WMO MAPPING) ---
    # Format: kode_wmo: ("Kondisi_Utama", "Deskripsi_Detail")
    wmo_mapping = {
        0:  ("Clear", "Cerah"),
        1:  ("Clouds", "Sebagian Berawan"),
        2:  ("Clouds", "Berawan"),
        3:  ("Clouds", "Mendung"),
        45: ("Fog", "Berkabut"),
        48: ("Fog", "Kabut Rime"),
        51: ("Drizzle", "Gerimis Ringan"),
        53: ("Drizzle", "Gerimis Sedang"),
        55: ("Drizzle", "Gerimis Lebat"),
        61: ("Rain", "Hujan Ringan"),
        63: ("Rain", "Hujan Sedang"),
        65: ("Rain", "Hujan Lebat"),
        71: ("Snow", "Salju Ringan"),
        80: ("Rain", "Hujan Deras Lokal"),
        95: ("Thunderstorm", "Badai Petir Ringan/Sedang"),
        99: ("Thunderstorm", "Badai Petir Lebat")
    }

    # Terjemahkan kode WMO dari API ke dalam bentuk teks (default "Unknown" jika tidak ada di kamus)
    kondisi_list = [wmo_mapping.get(code, ("Unknown", "Tidak Diketahui"))[0] for code in hourly_data['weather_code']]
    deskripsi_list = [wmo_mapping.get(code, ("Unknown", "Tidak Diketahui"))[1] for code in hourly_data['weather_code']]

    # BENTUK DATAFRAME SESUAI NAMA KOLOM REQUEST-MU
    df = pd.DataFrame({
        'waktu': hourly_data['time'],
        'kota': KOTA,
        'suhu': hourly_data['temperature_2m'],
        'suhu_terasa': hourly_data['apparent_temperature'],
        'kelembapan': hourly_data['relative_humidity_2m'],
        'kondisi': kondisi_list,
        'deskripsi': deskripsi_list,
        'kecepatan_angin': hourly_data['wind_speed_10m'],
        'curah_hujan': hourly_data['precipitation'],
        'cloudiness': hourly_data['cloud_cover']
    })
    
    folder_path = '/opt/airflow/data/raw'
    os.makedirs(folder_path, exist_ok=True)
    
    file_path = os.path.join(folder_path, f'cuaca_warkop_{tanggal_eksekusi}.csv')
    df.to_csv(file_path, index=False)
    
    print(f"Data lengkap tanggal {tanggal_eksekusi} berhasil disimpan di {file_path}")
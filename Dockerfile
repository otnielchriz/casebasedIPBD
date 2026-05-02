# Gunakan image resmi Airflow
FROM apache/airflow:3.1.8

USER root

# Install Google Chrome (Sangat berguna untuk backup scraping IG via Selenium)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install requirements untuk TomTom API & IG
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
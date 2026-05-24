# AGENTS.md — casebasedIPBD

## Project Overview
Airflow 3.1.8 ETL pipeline + Metabase dashboard for "Warkop Kusuma" — analyzing the relationship between weather, holidays, and daily revenue. Python-only, Dockerized.

## Directory Structure
- `dags/` — Airflow DAG definitions (3 pipelines)
- `scrapers/` — ETL logic imported by DAGs (weather, holidays, income)
- `data/raw/` — CSV outputs from scrapers, manually uploaded sales data
- `config/airflow.cfg` — local Airflow config
- `docker-compose.yaml` — full stack: Airflow (CeleryExecutor), Postgres, Redis, Metabase
- `Dockerfile` — extends Airflow image with Google Chrome + `requirements.txt` deps
- `SQL sript.sql` — analysis views (`v_analisis_warkop`, `v_cuaca_harian`)

## Key Commands
```bash
# Start entire stack (Airflow + Postgres + Redis + Metabase)
docker compose up -d

# Stop
docker compose down

# Access Airflow UI
http://localhost:8080  (login: airflow / airflow)

# Access Metabase
http://localhost:3000

# Flower (Celery monitor) — optional profile
docker compose --profile flower up -d  # port 5555
```

## Postgres Connection Setup (in Airflow UI)
After starting, create a connection in Airflow Admin → Connections:
- **Connection ID**: `postgres_traffic` (used by ALL DAGs)
- **Host**: `postgres` (inside Docker) or `localhost` (external)
- **Port**: `5432` (internal) / `5435` (mapped to host)
- **Login**: `airflow` / **Password**: `airflow`
- **Database**: `airflow`

## DAGs
| DAG ID | File | Schedule | Catchup | Owner |
|---|---|---|---|---|---|
| `pendapatan_warkop_pipeline` | `dags/dag_pendapatan_warkop.py` | @daily | No | warkop_kusuma |
| `weather_historical_catchup` | `dags/dag_openmeteo.py` | @daily | Yes | zaki |
| `hari_libur_etl_pipeline` | `dags/dag_hari_libur.py` | 0 4 * * * | No | zaki |
| `weather_stream_owm` | `dags/dag_weather_stream.py` | `*/5 * * * *` | No | zaki |
| `weather_batch_backfill_owm` | `dags/dag_weather_stream.py` | Manual only | No | zaki |

All DAGs use `sys.path.append('/opt/airflow/scrapers')` to import scraper modules — this is the container path, not the repo root.

## Weather Pipeline — Real-time Streaming (OpenWeatherMap Current API)

**Completely real-time** using OWM Current Weather API:

- **`weather_stream_owm`** — **Real-time mode**. Runs every 5 minutes (`*/5 * * * *`). Fetches actual current weather from OWM Current Weather API (`/data/2.5/weather`). Kolom `sumber = 'current'` membedakan data real-time dari forecast. Uses `ON CONFLICT` upsert.
- **`weather_batch_backfill_owm`** — One-time backfill. Fetches all 40 forecast intervals (5 days) via Forecast API, menulis dengan `sumber = 'forecast'`. Manual trigger only.

**Setup required:**
1. Get free API key from https://openweathermap.org/api (1M calls/month free tier — polling tiap 5 menit cuma ~8.640 calls/bulan, aman)
2. Add `OPENWEATHERMAP_API_KEY=your_key` to `.env`
3. Restart `docker compose` — env var auto-injected via `env_file` in docker-compose

**First run:** DAG auto-adds kolom `sumber` dan memastikan `cuaca_historis` table siap (idempotent).

**Legacy pipeline** (`dag_openmeteo.py`) still exists for backward compatibility but is deprecated.

## Database Tables
- `pendapatan_harian` — daily aggregated revenue
- `cuaca_historis` — real-time weather data (sumber='current') + forecast backfill (sumber='forecast')
- `hari_libur` — 2026 calendar with holiday/weekend flags

## Important Gotchas
- **Hardcoded container paths**: Scrapers write to `/opt/airflow/data/raw/` — not relative paths
- **Income file is manual**: `Rincian Penjualan-YYYY-MM-DD__YYYY-MM-DD.csv` must be placed in `data/raw/` manually. The DAG reads the May 2026 file specifically
- **No tests**: This project has no test framework. Verify by running DAGs and checking DB tables
- **Metabase on port 3000**: If port conflict, check README before changing
- **Postgres port mapping**: Internal `5432` maps to host `5435` (not default)
- **Dockerfile installs Chrome**: Needed for Selenium-based Instagram scraping (backup method)
- **No npm/Node**: Pure Python project — use `requirements.txt` for any new deps

## Dependencies
From `requirements.txt`: selenium, beautifulsoup4, instaloader, requests, pandas, psycopg2-binary, sqlalchemy

## Airflow Version
`apache/airflow:3.1.8` — uses new CeleryExecutor architecture with separate api-server, scheduler, dag-processor, and worker containers

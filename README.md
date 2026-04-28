# 🌤️ France Weather Pipeline

A end-to-end data engineering starter project that walks through the full DE lifecycle — from API ingestion to automated scheduling — using real weather data.

## Overview

```
Open-Meteo API (free · no sign-up required)
        ↓
extract.py (fetch weather data for Paris / Lyon / Marseille)
        ↓
transform.py (clean · deduplicate · derive new fields)
        ↓
PostgreSQL raw_weather (raw data layer)
        ↓
PostgreSQL clean_weather (cleaned data layer)
        ↓
Airflow DAG (automated daily schedule at 06:00)
        ↓
Metabase / matplotlib (visualization)
```

## Tech Stack

| Component | Purpose |
|-----------|---------|
| Python 3.12 | Data extraction and transformation scripts |
| PostgreSQL 15 | Storage with a two-layer architecture |
| Apache Airflow 2.9 | Workflow orchestration and scheduling |
| Docker Compose | One-command local environment setup |
| Open-Meteo API | Weather data source (free, no API key needed) |

## Project Structure

```
france-weather-pipeline/
├── docker-compose.yml      # Airflow + PostgreSQL stack
├── .env                    # Environment variables (do not commit)
├── dags/
│   └── weather_dag.py      # Airflow DAG definition
├── scripts/
│   ├── extract.py          # Step 1: call the Open-Meteo API
│   ├── transform.py        # Step 2: clean and classify data
│   └── load.py             # Step 3: write to PostgreSQL
├── sql/
│   └── init.sql            # Table definitions (auto-runs on container start)
└── viz/
    └── plot_weather.py     # Temperature trend chart with matplotlib
```

## Getting Started

### Prerequisites

- Docker (installed and running)
- Python 3.10+ (for local visualization only)

### 1. Start all services

```bash
# Initialize the Airflow database and admin account (run once only)
docker compose up airflow-init

# Start all services in the background
docker compose up -d

# Verify all containers are running
docker compose ps
```

### 2. Open the Airflow UI

Visit `http://localhost:8080` in your browser.

- Username: `airflow`
- Password: `airflow`

### 3. Trigger the DAG

In the Airflow UI:

1. Locate `france_weather_pipeline`
2. Toggle the switch on the left to unpause it
3. Click the ▶ button to trigger a manual run

### 4. Verify the data

```bash
# Connect to the weather database (password: weather123)
psql -h localhost -p 5433 -U weather -d weather

-- Inspect the cleaned data
SELECT city, date, temp_max_c, temp_min_c, weather_category
FROM clean_weather
ORDER BY date DESC, city;

-- Inspect the raw data
SELECT city, date, temp_max, fetched_at
FROM raw_weather
ORDER BY fetched_at DESC
LIMIT 10;
```

### 5. Visualize locally

```bash
pip install psycopg2-binary pandas matplotlib
python viz/plot_weather.py
```

## Database Design

### raw_weather (raw layer)

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| city | VARCHAR | City name |
| date | DATE | Observation date |
| temp_max | FLOAT | Maximum temperature |
| temp_min | FLOAT | Minimum temperature |
| precip_mm | FLOAT | Precipitation (mm) |
| wind_max | FLOAT | Maximum wind speed (km/h) |
| fetched_at | TIMESTAMP | Time the record was fetched |

> The raw layer is **append-only**. Records are never updated, preserving the full history of every ingestion run.

### clean_weather (cleaned layer)

| Column | Type | Description |
|--------|------|-------------|
| city | VARCHAR | City name (composite PK) |
| date | DATE | Observation date (composite PK) |
| temp_max_c | FLOAT | Cleaned maximum temperature |
| temp_min_c | FLOAT | Cleaned minimum temperature |
| temp_range_c | FLOAT | Daily temperature range (derived) |
| precip_mm | FLOAT | Precipitation (NULL replaced with 0) |
| wind_max_kmh | FLOAT | Maximum wind speed |
| weather_category | VARCHAR | Category: clear / drizzle / rainy / stormy |
| updated_at | TIMESTAMP | Last updated timestamp |

> The cleaned layer uses `UPSERT` for **idempotent writes** — re-running the pipeline never creates duplicate rows.

## DAG Configuration

```
extract → transform → load
```

| Setting | Value |
|---------|-------|
| Schedule | Daily at 06:00 (Europe/Paris) |
| Retries on failure | Up to 2, with a 5-minute delay |
| Backfill | Disabled (`catchup=False`) |
| Inter-task data passing | XCom |

## Weather Classification Rules

| Category | Condition |
|----------|-----------|
| `stormy` | Wind speed > 60 km/h |
| `rainy` | Precipitation > 10 mm |
| `drizzle` | Precipitation > 0.5 mm |
| `clear` | Everything else |

## Stopping and Cleanup

```bash
# Stop all containers (data is preserved)
docker compose down

# Stop and delete all data volumes (irreversible)
docker compose down -v
```

## What You Learn From This Project

| Concept | Where it appears |
|---------|-----------------|
| REST API ingestion | `extract.py` |
| Data cleaning and validation | `transform.py` |
| Two-layer data architecture (raw vs. clean) | `init.sql`, `load.py` |
| Idempotent writes with UPSERT | `load.py` |
| DAG authoring and task dependencies | `weather_dag.py` |
| Inter-task communication | XCom in `weather_dag.py` |
| Containerised orchestration | `docker-compose.yml` |
| Downstream consumption | `viz/plot_weather.py` |

## Roadmap

```
This project (foundations)
        ↓
Introduce dbt (replace transform.py with SQL models)
        ↓
Migrate to the cloud (AWS S3 + RDS  or  GCP BigQuery)
        ↓
Add data quality checks (Great Expectations / dbt tests)
        ↓
Capstone project (Reddit comment sentiment pipeline)
```

## Data Source

Weather data is provided by [Open-Meteo](https://open-meteo.com/) — completely free, no account or API key required.

Cities covered: Paris · Lyon · Marseille

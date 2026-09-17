# Spotify Data Pipeline — End-to-End ETL Project

A local, cost-free implementation of a cloud-style data engineering pipeline: extraction, transformation, warehousing, orchestration, and visualization — built entirely on a Windows machine without requiring an AWS account.

## Architecture

```
Kaggle Dataset  ─┐
                  ▼
        [Extract: Python script]  ──►  MinIO (raw bucket)
                                            │
                                            ▼
        [Transform: local PySpark]  ──►  MinIO (transformed bucket)
                                            │
                                            ▼
                                    Snowflake (data warehouse)
                                            │
                                            ▼
                                       Power BI (dashboard)

        Orchestrated end-to-end by: Apache Airflow (Docker)
```

### Why this differs from a "textbook" AWS architecture

This project was originally scoped around AWS Lambda, S3, and Glue. Partway through, a real-world constraint surfaced: AWS requires a credit card for account creation, with no exception for free-tier or learning use. Rather than stall the project, each AWS-managed service was swapped for a **local, API-compatible equivalent** that preserves the same architectural role and largely the same code:

| Original (AWS)   | Used instead     | Why it's a fair substitute |
|-------------------|-------------------|------------------------------|
| AWS S3            | MinIO (Docker)    | Implements the same S3 API — `boto3` code is nearly identical |
| AWS Lambda        | Local Python script (`lambda_handler` shape) | Same handler signature (`event`, `context`); portable to real Lambda later |
| AWS Glue          | Local PySpark     | Glue *is* managed Spark — this runs the same Spark transformation logic without AWS's wrapper |
| Spotify Web API   | Kaggle dataset (`maharshipandya/-spotify-tracks-dataset`) | Spotify deprecated public access to audio-features/analysis endpoints in Nov 2024, and now requires a Premium account to even register a developer app (as of March 2026) |

Snowflake, Power BI, and Airflow/Docker required no substitution — they matched the original design as-is.

## Tech stack

- **Python 3** — extraction and transformation logic
- **MinIO** — S3-compatible object storage (raw + transformed zones)
- **Apache PySpark 3.x** — data cleaning and transformation
- **Snowflake** — cloud data warehouse (free trial tier)
- **Apache Airflow 3.3.1** — pipeline orchestration (Docker, CeleryExecutor)
- **Docker Desktop** — containerization for MinIO and Airflow
- **Power BI Desktop** — dashboarding and visualization

## Project structure

```
spotify-pipeline/
├── data/                        # Downloaded Kaggle dataset (gitignored)
├── venv/                        # Python virtual environment (gitignored)
├── docker-compose.yml           # MinIO service definition
├── create_buckets.py            # Provisions raw + transformed buckets in MinIO
├── extract_lambda.py            # "Lambda-style" extraction: Kaggle CSV -> MinIO raw
├── transform_glue.py            # "Glue-style" transformation: PySpark cleaning -> MinIO transformed
├── load_snowflake.py            # Loads transformed data into Snowflake via internal stage
├── .env                         # Snowflake credentials (gitignored, never committed)
├── .gitignore
└── airflow-orchestration/
    ├── docker-compose.yaml      # Official Airflow 3.3.1 compose stack
    ├── Dockerfile                # Extends apache/airflow:3.3.1 with Java + Python deps
    ├── dags/
    │   └── spotify_pipeline_dag.py   # Orchestrates extract -> transform -> load
    └── scripts/                 # Mounted copies of the three pipeline scripts
```

## Pipeline stages

### 1. Extraction (`extract_lambda.py`)
Reads the Kaggle dataset and uploads it to the MinIO `spotify-raw-data` bucket, partitioned by ingestion date (`raw/YYYY/MM/DD/...`) — mirroring how production pipelines timestamp incoming batches. Written in the AWS Lambda handler shape (`def lambda_handler(event, context)`) so it could be redeployed to real Lambda with minimal changes if AWS access becomes available later.

### 2. Transformation (`transform_glue.py`)
Downloads the latest raw file, loads it into a local Spark session, and applies:
- Deduplication (`dropDuplicates`)
- Null-handling on essential fields (`dropna`)
- Tolerant numeric casting (`try_cast`, not `cast`) — bad/malformed values become `null` instead of crashing the job, matching how production Glue jobs handle messy upstream data
- Derived columns: `duration_minutes` (from milliseconds) and `popularity_tier` (High/Medium/Low bucketing)
- Filtering of invalid rows (zero-duration tracks)

Writes the cleaned result to the MinIO `spotify-transformed-data` bucket.

### 3. Loading (`load_snowflake.py`)
Downloads the transformed CSV, uploads it to a Snowflake **internal stage** via `PUT`, then bulk-loads it into the `spotify_tracks` table via `COPY INTO`. Uses `TRUNCATE` before each load to keep re-runs idempotent, and `ON_ERROR = 'CONTINUE'` so malformed rows are skipped rather than failing the whole load.

### 4. Orchestration (Airflow)
A single DAG (`spotify_pipeline`) chains the three scripts as sequential `BashOperator` tasks:

```
extract_from_kaggle_to_raw >> transform_raw_to_cleaned >> load_cleaned_to_snowflake
```

Runs inside a custom Airflow image (Java + PySpark + boto3 + Snowflake connector installed on top of `apache/airflow:3.3.1`), with the host's MinIO instance reached via Docker Desktop's `host.docker.internal` hostname.

### 5. Visualization (Power BI)
Connects to Snowflake via the native Snowflake connector (requires the Snowflake ODBC driver installed separately). Dashboard includes: average popularity by genre, danceability vs. energy scatter (colored by genre), popularity tier breakdown, average duration by genre, summary KPI cards, and a genre slicer for interactive filtering.

## Setup

1. Install Python 3, Git, Docker Desktop, and Power BI Desktop
2. Create a free Kaggle account and download an API token to `%USERPROFILE%\.kaggle\kaggle.json`
3. `pip install boto3 pandas kaggle pyspark snowflake-connector-python python-dotenv`
4. Download the dataset: `kaggle datasets download -d maharshipandya/-spotify-tracks-dataset -p .\data --unzip`
5. Start MinIO: `docker compose up -d`
6. Run `create_buckets.py`, then `extract_lambda.py`, then `transform_glue.py` to validate each stage manually
7. Create a free Snowflake trial account, set up the warehouse/database/table (see `snowflake_setup.sql`), and populate `.env` with your credentials
8. Run `load_snowflake.py` to validate the load
9. Set up Airflow (`airflow-orchestration/`): build the custom image, initialize the metadata DB, start the stack, and trigger the `spotify_pipeline` DAG from `http://localhost:8080`
10. Install the Snowflake ODBC driver, connect Power BI Desktop to Snowflake, and build the dashboard

## Notable issues hit and fixed during this build

- **Windows shell path escaping** — an early `$HOME` typed into the wrong shell context created a broken literal folder path; resolved by rebuilding the project in a clean, short path with no special characters
- **Docker Desktop virtualization failures** — required enabling the WSL2/Virtual Machine Platform Windows features and confirming BIOS-level virtualization was on
- **MinIO removed from Docker Hub** — MinIO archived its open-source Docker Hub distribution; resolved by pulling from `quay.io/minio/minio` instead
- **Malformed CSV values crashing Spark** (`CAST_INVALID_INPUT`) — an artist name containing unusual quoting broke strict `.cast("double")`; resolved with `try_cast` plus explicit CSV quote/escape options
- **`PermissionError` on file rename inside Docker on Windows** — boto3's atomic rename-after-download pattern fails on Windows bind-mounted volumes; resolved by downloading into the container's native `/tmp` via `tempfile.gettempdir()` instead of the mounted scripts folder
- **`COPY INTO` silently loading 0 rows** — Snowflake's `PUT` auto-compresses and renames files by default (`.gz` suffix); resolved with `AUTO_COMPRESS = FALSE` and referencing the staged file by basename only

## Future improvements

- Swap MinIO/local-Lambda/local-Glue back to real AWS S3/Lambda/Glue if AWS access becomes available — the code is written to make this a low-effort migration
- Move Snowflake credentials to Airflow's built-in encrypted Connections instead of a `.env` file mounted into the container
- Switch Snowflake authentication from password to key-pair auth
- Add a schedule (e.g. daily) to the Airflow DAG instead of manual triggering
- Add data-quality checks (e.g. Great Expectations) between the transform and load stages

# E2E Self-Healing Data Pipeline Framework

## Overview

A robust, platform-agnostic data engineering framework built to autonomously handle unpredictable schema evolution (drift) and ensure idempotent data ingestion. This pipeline demonstrates enterprise-grade "schema-on-read" capabilities, seamlessly adapting to incoming data mutations across both local relational databases (PostgreSQL) and cloud data warehouses (Google BigQuery & Snowflake).

## Core Capabilities

* **Auto-DDL & Schema-on-Read:** Targets require no pre-configured schemas. Tables are built dynamically based on the incoming data payload.
* **Evolutionary & Additive Schema Drift:** Automatically detects when incoming data types widen (e.g., integers changing to strings) or when new columns appear.
* *PostgreSQL:* Dynamically issues `ALTER TABLE` commands.
* *BigQuery:* Rewrites the table in-place to safely cast types, bypassing strict cloud data type limitations.


* **Idempotent Execution:** Primary key constraints and `MERGE`/`UPSERT` logic guarantee zero duplicate rows, even if the same file is processed multiple times.
* **Automated Orchestration:** A centralized runner manages data generation and sequential cross-platform ingestion with built-in error handling.

## Project Structure

* `generate_data.py` - Synthesizes mock CSV batches simulating baseline loads, schema mutations, and duplicate data.
* `run_pipeline.py` - The master orchestrator that sequentially executes the end-to-end workflow.
* `ingest_to_postgres.py` - Producer-consumer engine for PostgreSQL.
* `bq_ingestion.py` - Producer-consumer engine for Google BigQuery.
* `snowflake_ingestion.py` - Template script for Snowflake integration.
* `config_drift_test.json` - Configuration parameters for the mock data generator.
* `PIPELINE_ARCHITECTURE.md` - Detailed architectural breakdown of the pipeline logic.

## Prerequisites

* **Python 3.8+**
* **PostgreSQL** running locally (or remotely) with a database named `pipeline_test_db`.
* **Google Cloud Project** with BigQuery enabled and a dataset named `pipeline_test_db`.
* **GCP Service Account Key** named `gcp-key.json` saved in the root directory.

## Installation

1. **Clone the repository:**
```bash
git clone https://github.com/vinski-dev/data-pipeline-framework.git
cd data-pipeline-framework

```


2. **Create and activate a virtual environment:**
```bash
python -m venv venv
source venv/Scripts/activate  # On Windows Git Bash

```


3. **Install dependencies:**
```bash
pip install pandas psycopg2-binary google-cloud-bigquery pandas-gbq db-dtypes pyarrow snowflake-connector-python[pandas]

```



## Usage

You can run the entire pipeline end-to-end with a single command. The orchestrator will generate the data, ingest it to PostgreSQL, and then ingest it to BigQuery while handling all schema drift and idempotency checks automatically.

```bash
python run_pipeline.py

```

### What happens during execution?

1. **Phase 1 (Generation):** Creates `01_baseline.csv`, `02_drift_and_updates.csv` (adds `loyalty_tier`, changes `age` to string), and `03_idempotency_retry.csv` (duplicate batch).
2. **Phase 2 (PostgreSQL):** Ingests all three files sequentially, mutating the local `users_raw` table on the fly and merging duplicates.
3. **Phase 3 (BigQuery):** Ingests all three files sequentially, catching strict type violations, rebuilding the cloud table to cast `INT64` to `STRING`, and merging duplicates.

import os
from datetime import datetime

def generate_documentation():
    filename = "PIPELINE_ARCHITECTURE.md"
    
    doc_content = f"""# Automated Data Pipeline Framework
*Last Updated: {datetime.now().strftime('%Y-%m-%d')}*

## Overview
A robust, self-healing data pipeline built to handle unpredictable schema evolution and ensure idempotent data ingestion across local relational databases (PostgreSQL) and cloud data warehouses (Google BigQuery).

## Architecture Components

### 1. The Producer: `generate_data.py`
Generates mock CSV batches to simulate real-world data flows:
* **`01_baseline.csv`**: Initial data load (100 records).
* **`02_drift_and_updates.csv`**: Simulates additive drift (new columns like `loyalty_tier`) and evolutionary drift (type widening, e.g., `age` changing from integer to string).
* **`03_idempotency_retry.csv`**: A duplicate batch to test safe upsert logic.

### 2. The Consumers
* **PostgreSQL (`ingest_to_postgres.py`)**: Utilizes `psycopg2` and `pandas` to dynamically generate `CREATE TABLE`, `ALTER TABLE`, and `INSERT ... ON CONFLICT` SQL statements.
* **BigQuery (`bq_ingestion.py`)**: Uses the Google Cloud BigQuery API. Employs `MERGE` statements and on-the-fly table recreation to safely bypass BigQuery's strict type-casting limitations during evolutionary drift.

## Core Capabilities

1. **Auto-DDL & Schema-on-Read**: Targets require no pre-configured schemas. Tables are built dynamically based on the incoming data payload.
2. **Evolutionary Schema Drift**: Automatically detects when incoming data types widen. In PostgreSQL, it dynamically issues `ALTER TABLE` commands. In BigQuery, it rewrites the table in place to safely cast types before merging.
3. **Idempotent Execution**: Primary key constraints and UPSERT/MERGE logic guarantee zero duplicate rows, even if the same file is processed multiple times.

## Execution Guide

### Generate Source Data
```bash
python generate_data.py --config config_drift_test.json
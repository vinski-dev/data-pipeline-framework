import subprocess
import sys
import time

def run_step(command, description):
    print(f"\n[{time.strftime('%H:%M:%S')}] 🚀 {description}")
    print(f"Executing: {' '.join(command)}")
    
    # Run the command and wait for it to finish
    result = subprocess.run(command)
    
    if result.returncode != 0:
        print(f"\n❌ FATAL: Pipeline failed during -> {description}")
        print("Halting execution.")
        sys.exit(1)
    
    print("✅ Step Completed Successfully!")

def main():
    print("="*60)
    print("🔥 STARTING END-TO-END DATA PIPELINE ORCHESTRATION 🔥")
    print("="*60)

    # PHASE 1: Data Generation
    run_step(
        ["python", "generate_data.py", "--config", "config_drift_test.json"],
        "Phase 1: Generating Mock Source Data"
    )

    csv_files = [
        "mock_source_data/01_baseline.csv",
        "mock_source_data/02_drift_and_updates.csv",
        "mock_source_data/03_idempotency_retry.csv"
    ]

    # PHASE 2: PostgreSQL Ingestion
    print("\n" + "="*60)
    print("🐘 PHASE 2: INGESTING TO POSTGRESQL")
    print("="*60)
    for file_path in csv_files:
        run_step(
            ["python", "ingest_to_postgres.py", "--file", file_path], 
            f"Postgres Ingestion: {file_path.split('/')[-1]}"
        )

    # PHASE 3: BigQuery Ingestion
    print("\n" + "="*60)
    print("☁️ PHASE 3: INGESTING TO GOOGLE BIGQUERY")
    print("="*60)
    for file_path in csv_files:
        run_step(
            ["python", "bq_ingestion.py", "--file", file_path], 
            f"BigQuery Ingestion: {file_path.split('/')[-1]}"
        )

    print("\n" + "="*60)
    print("🎉 ALL PIPELINE PHASES COMPLETED SUCCESSFULLY! 🎉")
    print("="*60)

if __name__ == "__main__":
    main()
# ==============================================================================
# Script Name: Config-Driven Synthetic Data Generator
# Description: Generates realistic mock data (CSV/NDJSON) to rigorously test 
#              idempotent data ingestion pipelines (Snowflake, BigQuery).
#
# Features Tested:
#   1. Partition Pruning: Simulates CDC updates by advancing 'updated_at' to 
#      today while leaving 'created_at' in the past.
#   2. Subtractive Drift: Dynamically drops columns from the payload.
#   3. Additive Drift: Dynamically injects brand-new columns.
#   4. Evolutionary Drift: Widens data types (e.g., Integer to String).
#   5. Idempotency: Generates exact duplicate payloads for replay testing.
#
# Usage: 
#   python generate_data.py --config config_drift_test.json --outdir ./mock_data
# ==============================================================================

import argparse
import json
import os
import pandas as pd
from faker import Faker
import random
from datetime import datetime

# Initialize Faker and seed for deterministic, reproducible testing
fake = Faker()
Faker.seed(101)
random.seed(101)

def generate_base_record(record_id, is_historical=False):
    """Generates a single base record with appropriate timestamps."""
    if is_historical:
        # Simulate historical data from 1 to 2 years ago
        dt = fake.date_time_between(start_date='-2y', end_date='-1y')
    else:
        # Simulate new data arriving today
        dt = datetime.now()

    return {
        "id": record_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "age": random.randint(18, 65),  # Starts as an Integer
        "created_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": dt.strftime("%Y-%m-%d %H:%M:%S")
    }

def apply_drift(df, drift_config):
    """Applies schema drift rules defined in the JSON configuration."""
    if not drift_config:
        return df

    # 1. Subtractive Drift
    for col in drift_config.get("drop_columns", []):
        if col in df.columns:
            df = df.drop(columns=[col])

    # 2. Additive Drift
    for col, dtype in drift_config.get("add_columns", {}).items():
        if dtype == "string":
            df[col] = [random.choice(["Gold", "Silver", "Bronze"]) for _ in range(len(df))]
        elif dtype == "int":
            df[col] = [random.randint(1, 100) for _ in range(len(df))]

    # 3. Evolutionary Drift
    for col, new_type in drift_config.get("change_types", {}).items():
        if new_type == "string" and col in df.columns:
            # Inject string values into an integer column
            df[col] = [random.choice(["Opted Out", "Unknown", str(x)]) for x in df[col]]

    return df

def main():
    # ---------------------------------------------------------
    # 1. CLI ARGUMENT PARSING (Environment Variables)
    # ---------------------------------------------------------
    parser = argparse.ArgumentParser(description="Synthetic Data Generator")
    parser.add_argument("--config", type=str, required=True, 
                        help="Path to the JSON configuration file (Required)")
    parser.add_argument("--outdir", type=str, default="mock_source_data", 
                        help="Output directory (Default: ./mock_source_data)")
    
    args = parser.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    # ---------------------------------------------------------
    # 2. LOAD CONFIGURATION (Business Rules)
    # ---------------------------------------------------------
    if not os.path.exists(args.config):
        raise FileNotFoundError(f"Configuration file not found: {args.config}")
        
    with open(args.config, 'r') as f:
        config = json.load(f)

    out_format = config.get("settings", {}).get("output_format", "csv")
    print(f"🚀 Loaded Config: {args.config}")
    print(f"⚙️ Target Format: {out_format.upper()} | Output Directory: {args.outdir}/\n")

    # ---------------------------------------------------------
    # 3. EXECUTE GENERATION BATCHES
    # ---------------------------------------------------------
    master_state = pd.DataFrame()
    last_batch_df = pd.DataFrame()
    current_id = 0

    for batch in config.get("batches", []):
        prefix = batch["file_prefix"]
        
        # USE CASE: Idempotency Replay (Export identical file)
        if batch.get("duplicate_previous", False):
            file_name = f"{args.outdir}/{prefix}.{out_format}"
            if out_format == "csv": last_batch_df.to_csv(file_name, index=False)
            else: last_batch_df.to_json(file_name, orient="records", lines=True)
            print(f"✅ Generated Duplicate: {file_name} (Idempotency Test)")
            continue

        batch_data = []

        # USE CASE: Partition Pruning (Update existing historical records)
        if not master_state.empty and batch.get("update_percentage", 0.0) > 0:
            num_updates = int(len(master_state) * batch["update_percentage"])
            update_sample = master_state.sample(n=max(1, num_updates)).copy()
            
            # Advance updated_at to TODAY to trigger partition pruning
            update_sample["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_sample["email"] = [f"updated_{fake.word()}@test.com" for _ in range(len(update_sample))]
            batch_data.extend(update_sample.to_dict('records'))

        # Generate New Records
        for _ in range(batch.get("new_records", 0)):
            current_id += 1
            batch_data.append(generate_base_record(current_id, is_historical=batch.get("is_historical", False)))

        df_batch = pd.DataFrame(batch_data)

        if not df_batch.empty:
            # USE CASE: Apply Schema Drift Rules
            df_batch = apply_drift(df_batch, batch.get("drift_rules", {}))

            # Save File
            file_name = f"{args.outdir}/{prefix}.{out_format}"
            if out_format == "csv":
                df_batch.to_csv(file_name, index=False)
            elif out_format == "json":
                df_batch.to_json(file_name, orient="records", lines=True)
            
            print(f"✅ Generated Batch: {file_name} ({len(df_batch)} records)")
            
            # Update internal memory for the next loop
            if master_state.empty:
                master_state = df_batch
            else:
                master_state.set_index("id", inplace=True)
                df_batch_idx = df_batch.set_index("id")
                # --- ADD THESE 3 LINES TO FIX THE DTYPE CRASH ---
                for col in df_batch_idx.columns:
                    if col in master_state.columns and master_state[col].dtype != df_batch_idx[col].dtype:
                        master_state[col] = master_state[col].astype(df_batch_idx[col].dtype)
                # ------------------------------------------------

                # Now Pandas will safely update the widened string columns   
                master_state.update(df_batch_idx)
                new_records = df_batch_idx[~df_batch_idx.index.isin(master_state.index)]
                master_state = pd.concat([master_state, new_records]).reset_index()

            last_batch_df = df_batch.copy()

    print("\n🎉 All data generated successfully. Ready for pipeline ingestion!")

if __name__ == "__main__":
    main()
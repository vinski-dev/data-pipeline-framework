import pandas as pd
from google.cloud import bigquery
import uuid
import logging
import os

from google.cloud.exceptions import NotFound



# 1. Point Google Cloud to your downloaded JSON key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

# 2. Set your Project ID and Dataset
PROJECT_ID = "bq-demo-123-508711" # <-- CHANGE THIS
DATASET_ID = "pipeline_test_db"
TABLE_ID = "users_raw"

# 3. Initialize the client
client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_bq_type(pandas_dtype):
    dtype_str = str(pandas_dtype).lower()
    if 'int' in dtype_str: return 'INT64'
    if 'float' in dtype_str: return 'FLOAT64'
    if 'datetime' in dtype_str: return 'TIMESTAMP'
    if 'bool' in dtype_str: return 'BOOL'
    return 'STRING'

def ingest_to_bigquery(df, project_id, dataset_id, table_name, primary_key, partition_col="updated_at"):
    if primary_key not in df.columns or partition_col not in df.columns:
        raise ValueError(f"Payload must contain primary_key '{primary_key}' and partition_col '{partition_col}'")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    
    # ---------------------------------------------------------
    # 1. TABLE CREATION (With Holy Trinity & Pruning Logic)
    # ---------------------------------------------------------
    try:
        client.get_table(table_ref)
    except Exception:
        logging.info(f"Creating raw table '{table_name}'...")
        schema = [bigquery.SchemaField(c, get_bq_type(df[c].dtype)) for c in df.columns]
        # Append database-generated audited timestamp
        schema.append(bigquery.SchemaField("ingested_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"))
        
        table = bigquery.Table(table_ref, schema=schema)
        
        # Partition by the source-modified date
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_col
        )
        # Cluster by primary key for rapid MERGE lookups
        table.clustering_fields = [primary_key]
        client.create_table(table)

    # ---------------------------------------------------------
    # 2. SCHEMA DRIFT (Additive)
    # ---------------------------------------------------------
    table = client.get_table(table_ref)
    db_cols = {field.name for field in table.schema}
    
    new_fields = []
    for col in df.columns:
        if col not in db_cols:
            logging.warning(f"Additive Drift: Adding column '{col}'")
            new_fields.append(bigquery.SchemaField(col, get_bq_type(df[col].dtype)))
            
    if new_fields:
        table.schema = table.schema + new_fields
        client.update_table(table, ["schema"])

    # ---------------------------------------------------------
    # 3. AUTO-EXPIRING STAGING TABLE
    # ---------------------------------------------------------
    temp_table_id = f"{table_name}_staging_{uuid.uuid4().hex[:8]}"
    temp_ref = f"{project_id}.{dataset_id}.{temp_table_id}"
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, temp_ref, job_config=job_config).result()
    
    # Vaporize staging table automatically after 1 hour
    temp_table = client.get_table(temp_ref)
    temp_table.expires = pd.Timestamp.utcnow() + pd.Timedelta(hours=1)
    client.update_table(temp_table, ["expires"])

    # ---------------------------------------------------------
    # 4. IDEMPOTENT MERGE (With Partition Pruning & Auditing)
    # ---------------------------------------------------------
    cols_list = list(df.columns)
    
    # Subtractive drift handled by dynamically building these lists based ONLY on incoming columns
    update_set = ", ".join([f"t.{c} = s.{c}" for c in cols_list if c != primary_key])
    insert_cols = ", ".join(cols_list)
    insert_vals = ", ".join([f"s.{c}" for c in cols_list])

    merge_sql = f"""
        MERGE `{table_ref}` t USING `{temp_ref}` s
        ON t.{primary_key} = s.{primary_key} 
           -- Cost Saver: Only scan records modified in the last 7 days
           --AND t.{partition_col} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        WHEN MATCHED THEN 
            UPDATE SET {update_set}, t.ingested_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT ({insert_cols}, ingested_at) VALUES ({insert_vals}, CURRENT_TIMESTAMP())
    """
    
    client.query(merge_sql).result()
    logging.info(f"Successfully upserted data into '{table_name}'.")

if __name__ == "__main__":
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="Run Ingestion Pipeline")
    parser.add_argument("--file", type=str, required=True, help="Path to the CSV file to ingest")
    args = parser.parse_args()

    # 1. Read the specific file passed from the command line
    print(f"Reading file: {args.file}...")
    df = pd.read_csv(args.file)

    # 2. Set up your database connection (Add your actual credentials here)
    # conn = snowflake.connector.connect(...) 
    # OR client = bigquery.Client()

    # 3. Call the ingestion function we built earlier
    # ingest_to_snowflake(df, conn, table_name="USERS", primary_key="ID", partition_col="UPDATED_AT")
    # 2. Define your BigQuery tables
    # (Ensure PROJECT_ID and DATASET_ID are defined at the top of your script)
    # 2. Define your BigQuery tables
    target_table = f"{PROJECT_ID}.{DATASET_ID}.users_raw"
    staging_table = f"{PROJECT_ID}.{DATASET_ID}.users_raw_staging"

    # 3. Check if target table exists
    try:
        client.get_table(target_table)
        target_exists = True
    except NotFound:
        target_exists = False

    if not target_exists:
        # BASELINE RUN: Table doesn't exist, create and load directly
        print(f"INFO: Target table '{target_table}' not found. Loading baseline...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_EMPTY")
        client.load_table_from_dataframe(df, target_table, job_config=job_config).result()
        print("INFO: Successfully loaded baseline data.")
        
    else:
        # DRIFT & UPSERT RUNS
        # DRIFT & UPSERT RUNS
        print("INFO: Loading data into staging table...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, staging_table, job_config=job_config).result()

        # 1. Detect and explicitly handle Type Widening (e.g., INT64 -> STRING)
        target_schema = client.get_table(target_table).schema
        staging_schema = client.get_table(staging_table).schema
        
        for stg_field in staging_schema:
            tgt_field = next((f for f in target_schema if f.name == stg_field.name), None)
            if tgt_field and tgt_field.field_type == 'INTEGER' and stg_field.field_type == 'STRING':
                print(f"WARNING: Evolutionary Drift: Widening '{tgt_field.name}' to STRING")
                
                # BigQuery Workaround: Recreate the table in place with the casted column
                alter_sql = f"""
                CREATE OR REPLACE TABLE `{target_table}` AS
                SELECT * EXCEPT(`{tgt_field.name}`),
                       CAST(`{tgt_field.name}` AS STRING) AS `{tgt_field.name}`
                FROM `{target_table}`
                """
                client.query(alter_sql).result()

        # 2. Trick: Safely add brand new columns (like loyalty_tier)
        evolve_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )
        client.load_table_from_dataframe(df.head(0), target_table, job_config=evolve_config).result()

        print("INFO: Executing MERGE into target table...")
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"S.{col}" for col in df.columns])
        update_cols = ", ".join([f"{col} = S.{col}" for col in df.columns if col != 'id'])

        merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        client.query(merge_sql).result()
        print("INFO: Successfully upserted data into BigQuery.")
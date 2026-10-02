import argparse
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to the CSV file")
    args = parser.parse_args()

    print(f"Reading file: {args.file}...")
    df = pd.read_csv(args.file)
    
    # Snowflake best practice: uppercase all column names
    df.columns = [c.upper() for c in df.columns]

    # 1. Initialize Snowflake Connection (UPDATE THESE)
    conn = snowflake.connector.connect(
        user='YOUR_USERNAME',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT_IDENTIFIER', # e.g., 'xy12345.us-east-2.aws'
        warehouse='COMPUTE_WH',            # Default warehouse
        database='PIPELINE_TEST_DB',
        schema='PUBLIC'
    )
    cursor = conn.cursor()

    target_table = "USERS_RAW"
    staging_table = "USERS_RAW_STAGING"

    try:
        # 2. Check if the target table exists
        cursor.execute(f"SHOW TABLES LIKE '{target_table}'")
        target_exists = cursor.fetchone() is not None

        if not target_exists:
            # BASELINE RUN: Auto-create and load target table
            print(f"INFO: Target table '{target_table}' not found. Loading baseline...")
            success, nchunks, nrows, _ = write_pandas(conn, df, target_table, auto_create_table=True)
            print(f"INFO: Successfully loaded baseline data ({nrows} rows).")
        
        else:
            # DRIFT & UPSERT RUNS
            print("INFO: Loading data into staging table...")
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            write_pandas(conn, df, staging_table, auto_create_table=True)

            # 3. Handle Additive and Evolutionary Schema Drift
            cursor.execute(f"DESCRIBE TABLE {target_table}")
            target_cols = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.execute(f"DESCRIBE TABLE {staging_table}")
            staging_cols = {row[0]: row[1] for row in cursor.fetchall()}

            for stg_col, stg_type in staging_cols.items():
                if stg_col not in target_cols:
                    print(f"WARNING: Additive Drift: Adding column '{stg_col}'")
                    cursor.execute(f"ALTER TABLE {target_table} ADD COLUMN {stg_col} {stg_type}")
                elif target_cols[stg_col].startswith('NUMBER') and stg_type.startswith('VARCHAR'):
                    print(f"WARNING: Evolutionary Drift: Widening '{stg_col}' to VARCHAR")
                    # Snowflake allows direct type alteration from Number to Varchar
                    cursor.execute(f"ALTER TABLE {target_table} ALTER COLUMN {stg_col} SET DATA TYPE VARCHAR")

            # 4. Execute the MERGE (Upsert)
            print("INFO: Executing MERGE into target table...")
            insert_cols = ", ".join(df.columns)
            insert_vals = ", ".join([f"S.{col}" for col in df.columns])
            update_cols = ", ".join([f"{col} = S.{col}" for col in df.columns if col != 'ID'])

            merge_sql = f"""
            MERGE INTO {target_table} T
            USING {staging_table} S
            ON T.ID = S.ID
            WHEN MATCHED THEN
                UPDATE SET {update_cols}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            cursor.execute(merge_sql)
            print("INFO: Successfully upserted data into Snowflake.")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
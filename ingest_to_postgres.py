import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_pg_type(pandas_dtype):
    """Maps Pandas data types to PostgreSQL data types."""
    dtype_str = str(pandas_dtype).lower()
    if 'int' in dtype_str: return 'BIGINT'
    if 'float' in dtype_str: return 'DOUBLE PRECISION'
    if 'datetime' in dtype_str: return 'TIMESTAMP'
    if 'bool' in dtype_str: return 'BOOLEAN'
    return 'TEXT' # Text is preferred over VARCHAR in Postgres for dynamic lengths

def ingest_to_postgres(df, conn, table_name, primary_key):
    # Enforce lowercase for PostgreSQL naming standards
    df.columns = [c.lower() for c in df.columns]
    primary_key = primary_key.lower()
    table_name = table_name.lower()
    
    if primary_key not in df.columns:
        raise ValueError(f"Payload must contain primary_key '{primary_key}'")

    cursor = conn.cursor()
    
    try:
        # ---------------------------------------------------------
        # 1. TABLE CREATION (With Explicit Primary Key & Audit Column)
        # ---------------------------------------------------------
        # Check if table exists
        cursor.execute("SELECT EXISTS(SELECT FROM pg_tables WHERE tablename = %s);", (table_name,))
        if not cursor.fetchone()[0]:
            logging.info(f"Creating raw table '{table_name}'...")
            cols_ddl = [f"{c} {get_pg_type(df[c].dtype)}" for c in df.columns]
            
            # PostgreSQL REQUIRES a Primary Key constraint to use ON CONFLICT (Upsert)
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    {', '.join(cols_ddl)},
                    PRIMARY KEY ({primary_key}),
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

        # ---------------------------------------------------------
        # 2. SCHEMA DRIFT (Additive & Evolutionary)
        # ---------------------------------------------------------
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s;
        """, (table_name,))
        db_columns = {row[0]: row[1] for row in cursor.fetchall()}
        
        for col in df.columns:
            target_type = get_pg_type(df[col].dtype)
            
            if col not in db_columns:
                logging.warning(f"Additive Drift: Adding {col}")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {target_type};")
            # If incoming data is TEXT but database is numeric/other, safely widen it
            elif target_type == 'TEXT' and db_columns[col] != 'text':
                logging.warning(f"Evolutionary Drift: Widening {col} to TEXT")
                # USING clause safely casts existing database values to the new type
                cursor.execute(f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE TEXT USING {col}::TEXT;")

        # ---------------------------------------------------------
        # 3. TEMPORARY STAGING TABLE
        # ---------------------------------------------------------
        temp_table = f"{table_name}_staging"
        
        # Create a true temp table bound only to this specific database session
        cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} (LIKE {table_name} INCLUDING ALL) ON COMMIT DROP;")
        cursor.execute(f"ALTER TABLE {temp_table} DROP COLUMN ingested_at;")
        
        # High-speed bulk insert into the temporary table
        cols_list = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        insert_temp_sql = f"INSERT INTO {temp_table} ({', '.join(cols_list)}) VALUES %s"
        execute_values(cursor, insert_temp_sql, values)

        # ---------------------------------------------------------
        # 4. IDEMPOTENT UPSERT (Using ON CONFLICT)
        # ---------------------------------------------------------
        # Subtractive drift is naturally handled: we only update columns present in this payload
        update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols_list if c != primary_key])
        insert_cols = ", ".join(cols_list)

        upsert_sql = f"""
            INSERT INTO {table_name} ({insert_cols}, ingested_at)
            SELECT {insert_cols}, CURRENT_TIMESTAMP FROM {temp_table}
            ON CONFLICT ({primary_key}) 
            DO UPDATE SET 
                {update_set}, 
                ingested_at = CURRENT_TIMESTAMP;
        """
        cursor.execute(upsert_sql)
        
        # Commit the transaction (this also automatically drops the temporary table)
        conn.commit()
        logging.info(f"Successfully upserted data into '{table_name}'.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Ingestion failed: {e}")
        raise e
    finally:
        cursor.close()

# ==========================================
# CLI Execution Logic
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Postgres Ingestion Pipeline")
    parser.add_argument("--file", type=str, required=True, help="Path to the CSV file to ingest")
    args = parser.parse_args()

    print(f"Reading file: {args.file}...")
    df = pd.read_csv(args.file)

    # Convert timestamps properly before passing to psycopg2
    for col in df.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            df[col] = pd.to_datetime(df[col])

    # Establish Postgres connection
    # Replace with your actual database credentials
    connection = psycopg2.connect(
        host="localhost",
        database="analytics_lab",
        user="postgres",
        password="14myNmax2021",
        port="5432"
    )

    try:
        ingest_to_postgres(df, connection, table_name="users_raw", primary_key="id")
    finally:
        connection.close()
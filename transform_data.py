from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp
import os
from dotenv import load_dotenv

# Load connection details
load_dotenv()

def get_session():
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    return Session.builder.configs(connection_parameters).create()

def run_pipeline():
    session = get_session()
    print("❄️  Connected to Snowflake!")

    # 1. Read the RAW table into a DataFrame
    # (This doesn't download data; it creates a lazy SQL plan)
    df_raw = session.table("SALES_RAW")
    
    initial_count = df_raw.count()
    print(f"📉 Raw Row Count: {initial_count}")

    # 2. Perform Transformations
    # Remove rows where Product is NULL
    df_clean = df_raw.filter(col("PRODUCT").is_not_null())
    
    # Remove rows where Amount is negative (Dirty data)
    df_clean = df_clean.filter(col("AMOUNT") > 0)
    
    # Add a timestamp column
    df_clean = df_clean.with_column("PROCESSED_AT", current_timestamp())

    # 3. Write to the CLEAN table
    # mode="append" adds new rows. mode="overwrite" replaces the table.
    df_clean.write.mode("append").save_as_table("SALES_CLEAN")
    
    final_count = df_clean.count()
    print(f"📈 Clean Row Count: {final_count}")
    print(f"🗑️  Dropped {initial_count - final_count} bad rows.")
    print("✅ Pipeline Finished Successfully!")

if __name__ == "__main__":
    run_pipeline()
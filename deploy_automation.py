from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, current_timestamp
import os
from dotenv import load_dotenv

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

# This is the function that will run INSIDE Snowflake
def sales_pipeline_logic(session: Session) -> str:
    # 1. Load Data from Azure Stage into RAW (The Ingestion Step)
    # We use SQL here because COPY INTO is fastest for ingestion
    session.sql("""
        COPY INTO SALES_RAW
        FROM @AZURE_SALES_STAGE
        FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
        ON_ERROR = 'CONTINUE'
    """).collect()

    # 2. Transform (The Cleaning Step)
    df_raw = session.table("SALES_RAW")
    
    # Filter bad data
    df_clean = df_raw.filter(col("PRODUCT").is_not_null())
    df_clean = df_clean.filter(col("AMOUNT") > 0)
    df_clean = df_clean.with_column("PROCESSED_AT", current_timestamp())

    # 3. Write to Clean
    df_clean.write.mode("append").save_as_table("SALES_CLEAN")

    # 4. Clear Raw (Optional: To keep landing zone empty for next batch)
    # session.sql("TRUNCATE TABLE SALES_RAW").collect()
    
    return "✅ Pipeline executed successfully inside Snowflake!"

if __name__ == "__main__":
    session = get_session()
    print("🚀 Deploying Stored Procedure to Snowflake...")
    
    # This registers the python function above as a Stored Procedure named 'PROCESS_SALES_DATA'
    session.sproc.register(
        func=sales_pipeline_logic,
        name="PROCESS_SALES_DATA",
        replace=True,
        is_permanent=True,
        stage_location="@AZURE_SALES_STAGE", # We store the python code in your existing stage
        packages=["snowflake-snowpark-python"]
    )
    
    print("✅ Deployment Complete! You can now call PROCESS_SALES_DATA() in SQL.")
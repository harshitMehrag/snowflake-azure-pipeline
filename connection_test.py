import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from azure.storage.blob import BlobServiceClient

load_dotenv()

def check_connections():
    print("🚀 Starting Connectivity Check...")

    # 1. Check Snowflake
    try:
        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA")
        }
        session = Session.builder.configs(connection_parameters).create()
        print("✅ Snowflake Connection: SUCCESS")
        session.close()
    except Exception as e:
        print(f"❌ Snowflake Connection: FAILED - {e}")

    # 2. Check Azure
    try:
        connect_str = os.getenv("AZURE_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        # List containers to prove we have access
        containers = blob_service_client.list_containers()
        print("✅ Azure Connection: SUCCESS (Containers found: " + ", ".join([c['name'] for c in containers]) + ")")
    except Exception as e:
        print(f"❌ Azure Connection: FAILED - {e}")

if __name__ == "__main__":
    check_connections()
import pandas as pd
import random
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import io

load_dotenv()

# Configuration
NUM_ROWS = 1000
FILE_NAME = f"sales_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
CONTAINER_NAME = "snowflake-stage"

def generate_data():
    print(f"🛠️  Generating {NUM_ROWS} rows of synthetic data...")
    
    products = ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Headphones', None] # None simulates dirty data
    cities = ['Chirkunda', 'Dhanbad', 'Ranchi', 'Kolkata', 'Delhi', 'Mumbai']
    
    data = []
    for i in range(NUM_ROWS):
        date = datetime.now() - timedelta(days=random.randint(0, 30))
        prod = random.choice(products)
        city = random.choice(cities)
        qty = random.randint(1, 10)
        # Introduce some negative prices (dirty data to clean later)
        price = random.uniform(-50, 2000) if random.random() < 0.05 else random.uniform(10, 2000)
        
        data.append([i, date.date(), prod, city, qty, round(price, 2)])
        
    df = pd.DataFrame(data, columns=['transaction_id', 'date', 'product', 'city', 'quantity', 'amount'])
    return df

def upload_to_azure(df):
    try:
        connect_str = os.getenv("AZURE_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        # Convert DataFrame to CSV in memory
        output = io.StringIO()
        df.to_csv(output, index=False)
        data = output.getvalue()
        
        # Upload
        print(f"🚀 Uploading {FILE_NAME} to Azure...")
        blob_client = container_client.get_blob_client(FILE_NAME)
        blob_client.upload_blob(data, overwrite=True)
        print("✅ Upload Successful!")
        
    except Exception as e:
        print(f"❌ Error uploading to Azure: {e}")

if __name__ == "__main__":
    df = generate_data()
    upload_to_azure(df)
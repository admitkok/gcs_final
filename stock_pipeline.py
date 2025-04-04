import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import os
from dotenv import load_dotenv
import time
import requests
from google.api_core.exceptions import ServiceUnavailable

# image-processing@plasma-bounty-454012-d0.iam.gserviceaccount.com

def upload_to_bigquery(df, project_id, dataset_id, table_id, max_retries=3):
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    for attempt in range(max_retries):
        try:
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for job to complete
            print(f"Successfully uploaded data to {table_ref}")
            return
        except ServiceUnavailable:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff
                print(f"BigQuery service unavailable. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise


def fetch_stock_data():
    """Fetch MSFT stock data for the last day with retry logic"""
    max_retries = 3
    retry_delay = 5  # Increased delay between retries
    
    for attempt in range(max_retries):
        try:
            msft = yf.Ticker("MSFT")
            data = msft.history(period="1d", interval="1d")
            
            if data.empty:
                raise ValueError("No data received from yfinance")
            
            print(f"Successfully fetched {len(data)} rows of data")
            return data
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Network error on attempt {attempt + 1}: {str(e)}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Network error after {max_retries} attempts: {str(e)}")
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to fetch stock data after {max_retries} attempts: {str(e)}")

def process_data(df):
    """Process the stock data"""
    if df is None or df.empty:
        raise ValueError("No data to process")
    
    df = df.reset_index()
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    
    required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return df[required_columns]

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Upload processed data to BigQuery"""
    if df is None or df.empty:
        raise ValueError("No data to upload")
    
    credentials = service_account.Credentials.from_service_account_file(
        "plasma-bounty-454012-d0-ec28c2efcc46.json"
    )
    
    client = bigquery.Client(credentials=credentials, project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    
    print(f"Successfully uploaded data to {table_ref}")

def main():
    load_dotenv()
    project_id = os.getenv("GCP_PROJECT_ID", "plasma-bounty-454012-d0")
    dataset_id = "alex"
    table_id = "final"
    
    try:
        print("Fetching MSFT stock data...")
        stock_data = fetch_stock_data()
        
        print("Processing data...")
        processed_data = process_data(stock_data)
        
        print("Uploading to BigQuery...")
        upload_to_bigquery(processed_data, project_id, dataset_id, table_id)
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    while True:
        main()
        print("Waiting for 1 minute...")
        time.sleep(60)  # Waits for 60 seconds before running again
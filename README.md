# MSFT Stock Data Pipeline

This project creates an automated pipeline that:
1. Fetches Microsoft (MSFT) stock data using yfinance
2. Processes the data into a CSV format
3. Uploads the processed data to Google Cloud Storage

## Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account
- Google Cloud Storage bucket (chess_hs_bucket-1)
- Google Cloud credentials

## Setup

1. Clone this repository:
```bash
git clone <your-repo-url>
cd <repo-directory>
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up Google Cloud credentials:
   - Create a service account in Google Cloud Console
   - Download the JSON key file
   - Set the environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

4. Create a `.env` file in the project root:
```
GCS_BUCKET_NAME=chess_hs_bucket-1
```

## Usage

Run the pipeline:
```bash
python stock_pipeline.py
```

The script will:
- Fetch the last 30 days of MSFT stock data
- Process it into a CSV format
- Upload it to your GCS bucket with a timestamp in the filename

## Output

The processed data will be uploaded to your GCS bucket as CSV files with the naming format:
`msft_stock_data_YYYYMMDD_HHMMSS.csv`

Each CSV file contains the following columns:
- Date
- Open
- High
- Low
- Close
- Volume 
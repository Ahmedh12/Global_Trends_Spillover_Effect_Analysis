import yfinance as yf
from pyspark.sql import SparkSession
from .util import START_DATE, END_DATE
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Stock Data Downloader") \
    .getOrCreate()

# Directory to save raw data
RAW_DATA_DIR = "raw/"

# List of indices to download
INDICES = {
    "global": {
        "S&P 500": "^GSPC",
        "Dow Jones": "^DJI",
        "FTSE 100": "^FTSE",
        "Nikkei 225": "^N225"
    },
    "egyptian": {
        "EGX 30": "^CASE30"
    }
}

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_data(ticker, index_name):
    print(f"Downloading data for {index_name} ({ticker})...")
    try:
        # Fetch data using yfinance
        data = yf.download(ticker, start=START_DATE, end=END_DATE)
        # Reset index to include the Date column
        data.reset_index(inplace=True)
        # Convert to Spark DataFrame
        df = spark.createDataFrame(data)
        return df
    except Exception as e:
        print(f"Failed to download data for {index_name}: {e}")
        return None

def save_data(df, file_path):
    if df is not None:
        df.write.csv(file_path, header=True, mode="overwrite")
        print(f"Data saved to {file_path}")

def main():
    create_directory(RAW_DATA_DIR)

    for index_name, ticker in INDICES["global"].items():
        file_path = os.path.join(RAW_DATA_DIR, f"{index_name.replace(' ', '_').lower()}")
        df = download_data(ticker, index_name)
        save_data(df, file_path)

    for index_name, ticker in INDICES["egyptian"].items():
        file_path = os.path.join(RAW_DATA_DIR, f"{index_name.replace(' ', '_').lower()}")
        df = download_data(ticker, index_name)
        save_data(df, file_path)

if __name__ == "__main__":
    main()

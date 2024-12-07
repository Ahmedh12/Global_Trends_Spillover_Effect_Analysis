import yfinance as yf
from pyspark.sql import SparkSession
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

# Date range for historical data
START_DATE = "2010-01-01"
END_DATE = "2023-12-31"

def create_directory(directory):
    """
    Creates the directory if it doesn't exist.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_data(ticker, index_name):
    """
    Downloads historical data for a given ticker symbol.
    Returns a PySpark DataFrame.
    """
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
    """
    Saves a PySpark DataFrame to a CSV file.
    """
    if df is not None:
        df.write.csv(file_path, header=True, mode="overwrite")
        print(f"Data saved to {file_path}")

def main():
    # Ensure the raw data directory exists
    create_directory(RAW_DATA_DIR)

    # Download and save global indices data
    for index_name, ticker in INDICES["global"].items():
        file_path = os.path.join(RAW_DATA_DIR, f"{index_name.replace(' ', '_').lower()}")
        df = download_data(ticker, index_name)
        save_data(df, file_path)

    # Download and save Egyptian index data
    for index_name, ticker in INDICES["egyptian"].items():
        file_path = os.path.join(RAW_DATA_DIR, f"{index_name.replace(' ', '_').lower()}")
        df = download_data(ticker, index_name)
        save_data(df, file_path)

if __name__ == "__main__":
    main()

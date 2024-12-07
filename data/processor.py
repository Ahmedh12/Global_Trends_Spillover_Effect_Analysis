import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import  col
from pyspark.sql.types import DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Unified Data Processing Script") \
    .getOrCreate()

spark.conf.set("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Concurrent GC")
spark.conf.set("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Concurrent GC")

# Define directories
raw_data_dir = "raw"
processed_data_dir = "processed"

def unify_column_names(df):
    """
    Standardizes column names across multiple datasets to a unified schema:
    (Date, Close, High, Low, Open, Volume).
    Drops 'Adj Close' and 'Change %' if present and ensures unified column names.
    """
    # Drop 'Adj Close' if it exists in the dataframe
    for column in df.columns:
        if 'Adj Close' in column:
            df = df.drop(column)

    # Drop 'Change %' if it exists in the dataframe
    if 'Change %' in df.columns:
        df = df.drop('Change %')

    # Rename columns based on the provided patterns
    rename_dict = {
        "Close": "Close",  # 'Close' remains 'Close'
        "High": "High",  # 'High' remains 'High'
        "Low": "Low",  # 'Low' remains 'Low'
        "Open": "Open",  # 'Open' remains 'Open'
        "Volume": "Volume",  # 'Volume' remains 'Volume'
        "Vol.": "Volume",  # Handle 'Vol.' from second dataset
        "Price": "Close",  # 'Price' maps to 'Close' in the second dataset
        "Date": "Date"  # 'Date' remains 'Date'
    }

    # Apply renaming
    for col_name in df.columns:
        for name in rename_dict.keys():
            if name in col_name:
                new_name = rename_dict[name]
                if new_name:  # Don't rename if it's None (i.e., dropping 'Change %')
                    df = df.withColumnRenamed(col_name, new_name)

    return df


def convert_to_float(value):
    """
    Converts a string value like '31.88M' to float (in this case, multiplying by 1 million).
    """
    if isinstance(value, str):
        value = value.replace(',', '')  # Remove commas if any

        # Regex to capture the number and suffix for large numbers
        match = re.match(r"([0-9.]+)([a-zA-Z]+)?", value)
        if match:
            number, suffix = match.groups()
            number = float(number)

            if suffix == 'M':
                return number * 1e6
            elif suffix == 'K':
                return number * 1e3
            elif suffix == 'B':
                return number * 1e9
            return number

        return float(value)  # If no suffix, convert directly

    return float(value)


def clean_and_convert(df):
    """
    Cleans and converts data types of the dataframe.
    Converts numeric columns to appropriate numeric types
    and handles null values using PySpark built-in functions.
    """
    # Convert 'Date' to proper date format if it's not already in DateType
    df = df.withColumn("Date", F.to_date("Date", "MM/dd/yyyy"))

    # List of columns to process
    columns_to_convert = ["Close", "Low", "High", "Open", "Volume"]

    is_string = False
    for column , dtype in df.dtypes:
        if column in columns_to_convert:
            if dtype == "string":
                is_string = True
            else:
                df = df.withColumn(column, col(column).cast(DoubleType()))

    if is_string :
        df = df.toPandas()

        df.replace("",None,inplace=True)
        df.dropna(axis=0, how='any', inplace=True)

        for column in columns_to_convert:
            df.loc[:, column] = df[column].apply(convert_to_float)

        df = spark.createDataFrame(df)
    else:
        df = df.dropna()

    df = df.orderBy("Date")

    return df


def main():
    # Process each subfolder in raw_data_dir
    for folder_name in os.listdir(raw_data_dir):
        folder_path = os.path.join(raw_data_dir, folder_name)

        if os.path.isdir(folder_path):
            # Create corresponding folder in processed_data_dir
            processed_folder_path = os.path.join(processed_data_dir, folder_name)
            os.makedirs(processed_folder_path, exist_ok=True)

            # Process each CSV file in the folder
            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)

                    # Load the CSV file
                    print(f"Processing file: {file_path}")
                    df = spark.read.csv(file_path, header=True, inferSchema=True)
                    df = unify_column_names(df)
                    df = clean_and_convert(df)

                    #Save File
                    df.coalesce(1).write.csv(file_path.replace(raw_data_dir,processed_data_dir), header=True, mode='overwrite')

    print("Data processing complete.")

if __name__ == "__main__":
    main()

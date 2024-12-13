import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, last
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import pandas as pd

from constants import  ColumnNames

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Unified Data Processing Script") \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")



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
        "Close": "Close",  # ColumnNames.Close.name remains ColumnNames.Close.name
        "High": "High",  # 'High' remains 'High'
        "Low": "Low",  # 'Low' remains 'Low'
        "Open": "Open",  # 'Open' remains 'Open'
        "Volume": "Volume",  # 'Volume' remains 'Volume'
        "Vol.": "Volume",  # Handle 'Vol.' from second dataset
        "Price": "Close",  # 'Price' maps to ColumnNames.Close.name in the second dataset
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


def forward_fill_spark(df):
    start_date = df.agg(F.min("Date").alias("start_date")).collect()[0]["start_date"]
    end_date = df.agg(F.max("Date").alias("end_date")).collect()[0]["end_date"]

    date_range = pd.date_range(start=start_date, end=end_date, freq='D').date
    full_date_df = spark.createDataFrame([(str(date),) for date in date_range], ["Date"])

    df = df.withColumn("Date", to_date(df["Date"]))

    df = full_date_df.join(df, "Date", how="left")

    window_spec = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    for column in df.columns:
        if column != "Date":  # Skip the 'Date' column
            df = df.withColumn(column, last(column, True).over(window_spec))

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

def get_exchange_rates_df(start_date, end_date, currency_pair="EGPUSD=X"):
    import yfinance as yf
    import pandas as pd

    ticker = yf.Ticker(currency_pair)
    exchange_rate_data = ticker.history(start=start_date, end=end_date)

    exchange_rate_data.reset_index(inplace=True)

    exchange_rate_data['Date'] = pd.to_datetime(exchange_rate_data['Date']).dt.date

    exchange_rate_data = exchange_rate_data[['Date', 'Close', 'Open', 'High', 'Low']]
    exchange_rate_data.rename(columns={
        'Date': 'ExchangeRate_Date',
        'Close': 'ExchangeRate_Close',
        'Open': 'ExchangeRate_Open',
        'High': 'ExchangeRate_High',
        'Low': 'ExchangeRate_Low'
    }, inplace=True)

    date_range = pd.date_range(start=start_date, end=end_date, freq='D').date

    exchange_rate_data = exchange_rate_data.set_index('ExchangeRate_Date').reindex(date_range)

    exchange_rate_data.fillna(method='ffill', inplace=True)

    exchange_rate_data.reset_index(inplace=True)

    exchange_rate_data.rename(columns={'index': 'ExchangeRate_Date'}, inplace=True)

    return exchange_rate_data

def convert_currency(spark_df, column_names):
    start_date = spark_df.agg(F.min("Date").alias("start_date")).collect()[0]["start_date"]
    end_date = spark_df.agg(F.max("Date").alias("end_date")).collect()[0]["end_date"]
    exchange_rate_data = get_exchange_rates_df(start_date = start_date, end_date = end_date)

    exchange_rate_sdf = spark.createDataFrame(exchange_rate_data)

    spark_df = spark_df.withColumn("Date", to_date(col("Date")))


    # Join the Spark DataFrame with the exchange rate data on Date
    spark_df = spark_df.join(
        exchange_rate_sdf.withColumnRenamed('ExchangeRate_Date', 'Date'),
        on="Date",
        how="left"
    )

    for column in column_names:
        if column != 'Date':  # Skip the 'Date' column
            exchange_rate_column = f"ExchangeRate_{column}"
            if exchange_rate_column in exchange_rate_sdf.columns:
                spark_df = spark_df.withColumn(column, col(column) * col(exchange_rate_column))

    drop_columns = [f"ExchangeRate_{metric}" for metric in ['Close', 'Open', 'High', 'Low']]
    spark_df = spark_df.drop(*drop_columns)


    return spark_df

def main():
    for folder_name in os.listdir(raw_data_dir):
        folder_path = os.path.join(raw_data_dir, folder_name)

        if os.path.isdir(folder_path):
            processed_folder_path = os.path.join(processed_data_dir, folder_name)
            os.makedirs(processed_folder_path, exist_ok=True)

            # Process each CSV file in the folder
            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)

                    print(f"Processing file: {file_path}")
                    df = spark.read.csv(file_path, header=True, inferSchema=True)
                    df = unify_column_names(df)
                    df = clean_and_convert(df)
                    df = forward_fill_spark(df)
                    if "EGX" in file_name:
                        df = convert_currency(df,[ColumnNames.Close.name, ColumnNames.Open.name,
                                                  ColumnNames.High.name, ColumnNames.Low.name])
                    #Save File
                    df = df.orderBy("Date")
                    df.coalesce(1).write.csv(file_path.replace(raw_data_dir,processed_data_dir), header=True, mode='overwrite')

    print("Data processing complete.")

if __name__ == "__main__":
    main()
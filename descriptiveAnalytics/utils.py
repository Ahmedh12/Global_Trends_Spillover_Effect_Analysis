from pyspark.sql.functions import mean, stddev, min, max, col, skewness
import matplotlib.pyplot as plt
import pandas as pd


def calculate_summary_stats(df, columns):
    metrics = [mean(col(c)).alias(f"{c}_mean") for c in columns] + \
              [stddev(col(c)).alias(f"{c}_stddev") for c in columns] + \
              [min(col(c)).alias(f"{c}_min") for c in columns] + \
              [max(col(c)).alias(f"{c}_max") for c in columns] + \
              [skewness(col(c)).alias(f"{c}_skewness") for c in columns]

    return df.select(metrics)


def plot_numpy_array(data, column_names, date_col, price_cols, title="Price Trends"):
    try:
        df = pd.DataFrame(data, columns=column_names)
    except ValueError as e:
        raise ValueError(f"Data and column names mismatch: {e}")

    # Ensure the date column is in datetime format
    try:
        df[date_col] = pd.to_datetime(df[date_col])
    except Exception as e:
        raise ValueError(f"Error in converting {date_col} to datetime: {e}")

    # Debug column names and types

    # Ensure price columns are numeric
    for col in price_cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")
        if not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col],errors='coerce')
            # raise ValueError(f"Column '{col}' is not numeric.")

    # Initialize the plot
    plt.figure(figsize=(15, 8))

    # Plot each price column
    for col in price_cols:
        print()
        plt.plot(df[date_col].to_numpy(), df[col].to_numpy(), label=col)

    # Add labels, title, and legend
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid()
    plt.show()

#the std-dev of the returns over a specified window
def plot_rolling_volatility(data, column_names, date_col, price_cols, window=30, title="Rolling Volatility"):
    # Convert array to DataFrame for easier handling
    try:
        df = pd.DataFrame(data, columns=column_names)
    except ValueError as e:
        raise ValueError(f"Data and column names mismatch: {e}")

    # Ensure the date column is in datetime format
    try:
        df[date_col] = pd.to_datetime(df[date_col])
    except Exception as e:
        raise ValueError(f"Error in converting {date_col} to datetime: {e}")

    # Ensure price columns are numeric
    for col in price_cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")
        if not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Initialize the plot
    plt.figure(figsize=(15, 8))

    # Calculate and plot rolling volatility for each price column
    for col in price_cols:
        rolling_std_col = f"{col}_rolling_std"
        df[rolling_std_col] = df[col].rolling(window=window).std()
        plt.plot(df[date_col].to_numpy(), df[rolling_std_col].to_numpy(), label=f"{col} {window}-day Volatility")

    # Add labels, title, and legend
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Volatility")
    plt.yscale('log')
    plt.legend()
    plt.grid()
    plt.show()

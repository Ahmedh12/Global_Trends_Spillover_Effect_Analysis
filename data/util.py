from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from enum import Enum , auto

import os

DATA_FOLDERS = [
    "../data/processed/dow_jones",
    "../data/processed/ftse_100",
    "../data/processed/nikkei_225",
    "../data/processed/s&p_500",
    "../data/processed/EGX_30"
]

class ColumnNames(Enum):
    Date    = auto()
    Open    = auto()
    Close   = auto()
    Low     = auto()
    High    = auto()
    Volume  = auto()


class Indices(Enum):
    sp_500      = auto()
    nikkei_225  = auto()
    ftse_100    = auto()
    dow_Jones   = auto()
    EGX_30      = auto()


def load_data_from_folder(folder_path: str):
    # Create or get the existing Spark session
    spark = SparkSession.builder.appName("DataLoader").getOrCreate()

    # List all subdirectories (assuming each subdirectory contains the CSV files)
    subdirs = [os.path.join(folder_path, subdir) for subdir in os.listdir(folder_path) if
               os.path.isdir(os.path.join(folder_path, subdir))]

    # Load all CSV files from each subdirectory
    dfs = []
    for subdir in subdirs:
        # Find all CSV files in the subdirectory
        csv_files = [os.path.join(subdir, f) for f in os.listdir(subdir) if f.endswith(".csv")]

        # Read each CSV file into a DataFrame and append to the list
        for file in csv_files:
            df = spark.read.option("header", "true").csv(file)
            dfs.append(df)
    # Union all DataFrames together into a single DataFrame
    if dfs:
        final_df = dfs[0]
        for df in dfs[1:]:
            final_df = final_df.union(df)
        return final_df
    else:
        return None


def rename_columns(df, prefix):
    return df.select([col(c).alias(f"{prefix}_{c}") if c != "Date" else col(c) for c in df.columns])

def cast_columns_to_double(df):
    for col_name in df.columns:
        if ColumnNames.Date.name not in col_name:
            df =  df.withColumn(col_name, col(col_name).cast(DoubleType()))
    return df
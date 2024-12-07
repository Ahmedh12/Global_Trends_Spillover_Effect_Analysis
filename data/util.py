from pyspark.sql import SparkSession
import os


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

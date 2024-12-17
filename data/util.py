from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.linalg import DenseVector
from pyspark.sql.functions import col ,udf, lit
from .constants import *
import os




def load_data_from_folder(folder_path: str):
    spark = SparkSession.builder.appName("DataLoader").getOrCreate()

    subdirs = [os.path.join(folder_path, subdir) for subdir in os.listdir(folder_path) if
               os.path.isdir(os.path.join(folder_path, subdir))]

    dfs = []
    for subdir in subdirs:
        csv_files = [os.path.join(subdir, f) for f in os.listdir(subdir) if f.endswith(".csv")]
         
        for file in csv_files:
            df = spark.read.option("header", "true").csv(file)
            dfs.append(df)

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


def scale_df(df, columns_to_scale=None,
             for_regression = False):
    if columns_to_scale is None:
        columns_to_scale = [col for col in df.columns if "Date" not in col]

    assembler = VectorAssembler(inputCols=columns_to_scale, outputCol="features")
    df_assembled = assembler.transform(df)

    scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)

    scaled_columns = ['scaled_' + col_name for col_name in columns_to_scale]
    def extract_element(vector, index):
        return float(vector[index]) if isinstance(vector, DenseVector) else None

    extract_element_udf = udf(extract_element, DoubleType())

    for i, col_name in enumerate(scaled_columns):
        df_scaled = df_scaled.withColumn(col_name, extract_element_udf("scaled_features",lit(i)))

    if not for_regression:
        df_scaled = df_scaled.drop("features", "scaled_features")

    return df_scaled

if __name__ == "__main__":
    pass


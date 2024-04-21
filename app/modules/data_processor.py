# data_processor.py
import os
from app.config.credentials import DatabaseConfig
from pyspark.sql.functions import regexp_extract


class DataSanitizer:
    @staticmethod
    def sanitize_txt(txt_df):
        sanitized_df = txt_df.select(
            regexp_extract("value", r"\[(.*?)\]", 1).alias("register_time"),
            regexp_extract("value", r"(\bERROR\b|\bWARNING\b)", 1).alias("severity"),
            regexp_extract("value", r"sensor\[(\d+)\]", 1).cast("integer").alias("sensor_id"),
            regexp_extract("value", r"temperature\s+([\d.-]+)", 1).cast("double").alias("temperature"),
            regexp_extract("value", r"vibration\s+([\d.-]+)", 1).cast("double").alias("vibration")
        )
        return sanitized_df


class DataReader:
    def __init__(self, spark):
        self.spark = spark

    def read_files(self, files_directory):
        dfs = {}
        for filename in os.listdir(files_directory):
            if filename.endswith(".json") or filename.endswith(".csv") or filename.endswith(".txt"):
                file_name = os.path.splitext(filename)[0]  # Remove a extensão do nome do arquivo
                file_path = os.path.join(files_directory, filename)
                if filename.endswith(".txt"):
                    df = self.spark.read.text(file_path)
                    df = DataSanitizer.sanitize_txt(df)
                else:
                    df = self.spark.read.option("multiline", "true").json(file_path) if filename.endswith(".json") \
                        else self.spark.read.csv(file_path, header=True, inferSchema=True) if filename.endswith(".csv") \
                        else None
                if df is not None:
                    dfs[file_name] = df.alias(f"{file_name}_df")
        return dfs


class DataWriter:
    def __init__(self, spark):
        self.spark = spark

    def write_to_database(self, dfs, db_schema):
        for df_name, df in dfs.items():
            table_name = f"{db_schema}.{df_name}" 
            df.write.jdbc(DatabaseConfig.DATABASE_URL, table_name, mode="overwrite", properties=DatabaseConfig.DATABASE_PROPERTIES)




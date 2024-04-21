# credentials.py
from pyspark.sql import SparkSession

class DatabaseConfig:
    DATABASE_URL = "jdbc:postgresql://postgres:5432/shape"
    DATABASE_PROPERTIES = {
        "user": "shape",
        "password": "shape",
        "driver": "org.postgresql.Driver",
    }

    @staticmethod
    def get_files_directory():
        return "app/files/"

def get_spark_session():
    spark = SparkSession.builder \
        .appName("ShapeDocker") \
        .config("spark.driver.extraClassPath", "app/drivers/postgresql-42.2.25.jar") \
        .getOrCreate()
    return spark



from pyspark.sql import SparkSession
import os

def get_spark():
    project_root = os.path.abspath(os.path.join("/opt/airflow", ".."))
    warehouse_path = os.path.join(project_root, "data")

    spark = SparkSession.builder \
        .appName("credit-lending") \
        .master("local[*]") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .getOrCreate()

    return spark
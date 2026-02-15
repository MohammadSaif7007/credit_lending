from pyspark.sql import functions as F

def parse_float(df, column):
    return df.withColumn(column, F.col(column).cast("double"))

def add_ingestion_date(df):
    return df.withColumn("ingestion_date", F.current_timestamp())
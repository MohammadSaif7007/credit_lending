from pyspark.sql import functions as F

def drop_nulls(df, column):
    return df.filter(F.col(column).isNotNull())

def filter_positive(df, column):
    return df.filter(F.col(column) > 0)
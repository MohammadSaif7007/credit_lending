from pyspark.sql import functions as F

def parse_float(df, *columns):
    """
    Cast one or more columns to double/float type.
    :param df: Spark DataFrame
    :param columns: str or list of column names
    """
    if isinstance(columns, str):
        columns = [columns]
    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        df = df.withColumn(column, F.col(column).cast("double"))
    return df


def parse_int(df, *columns):
    """
    Cast one or more columns to integer type.
    :param df: Spark DataFrame
    :param columns: str or list of column names
    """
    if isinstance(columns, str):
        columns = [columns]
    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        df = df.withColumn(column, F.col(column).cast("integer"))
    return df

def rename_columns(df, rename_mapping):
    """
    Rename columns using a dictionary: {"old_name": "new_name"}
    """
    for old_name, new_name in rename_mapping.items():
        if old_name not in df.columns:
            raise ValueError(f"Column {old_name} not found in DataFrame")
        df = df.withColumnRenamed(old_name, new_name)
    return df

def map_column_values(df, column, mapping_dict, default=None):
    """
    Map values in a column based on mapping dictionary.
    
    :param df: Spark DataFrame
    :param column: column name to map
    :param mapping_dict: dictionary with {old_value: new_value}
    :param default: default value if not in mapping
    """
    if column not in df.columns:
        raise ValueError(f"Column {column} not found in DataFrame")

    mapping_expr = F.create_map([F.lit(x) for x in sum(mapping_dict.items(), ())])
    
    if default is not None:
        df = df.withColumn(column, F.coalesce(mapping_expr.getItem(F.col(column)), F.lit(default)))
    else:
        df = df.withColumn(column, mapping_expr.getItem(F.col(column)))
    
    return df

def round_column(df, columns, decimals=2):
    """
    Round one or more numeric columns to specified decimal places.
    
    :param df: Spark DataFrame
    :param columns: str or list of column names
    :param decimals: number of decimal places (default=2)
    """
    if isinstance(columns, str):
        columns = [columns]
    
    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        df = df.withColumn(column, F.round(F.col(column), decimals))
    
    return df

def add_ingestion_date(df):
    """
    Add a current timestamp ingestion_date column.
    """
    return df.withColumn("ingestion_date", F.current_timestamp())
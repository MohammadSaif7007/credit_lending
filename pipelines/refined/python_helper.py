from pyspark.sql import functions as F


# ---------------------------------------------------------
# Remove rows with NULL values in given columns
# ---------------------------------------------------------
def remove_rows_with_null(df, *columns):
    """
    Remove rows where any of the specified columns is NULL.
    :param df: Spark DataFrame
    :param columns: str or list of column names
    """
    if isinstance(columns, str):
        columns = [columns]

    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        df = df.filter(F.col(column).isNotNull())

    return df


# ---------------------------------------------------------
# Remove rows with empty string values
# ---------------------------------------------------------
def remove_rows_with_empty_values(df, *columns):
    """
    Remove rows where specified columns contain empty strings.
    :param df: Spark DataFrame
    :param columns: str or list of column names
    """
    if isinstance(columns, str):
        columns = [columns]

    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        df = df.filter(F.col(column) != "")

    return df


# ---------------------------------------------------------
# Remove rows where column values are not in allowed list
# ---------------------------------------------------------
def remove_rows_with_unmapped_values(df, **column_rules):
    """
    Remove rows where column values are not in allowed_values.

    YAML example:
    args:
      gender: [0, 1]
      corporate: ["Retail", "SME", "Corporate", "Other"]
    """

    for column, allowed_values in column_rules.items():

        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        if allowed_values:
            df = df.filter(F.col(column).isin(allowed_values))

    return df


# ---------------------------------------------------------
# Remove rows where numeric column <= 0
# - function: remove_negative_or_zero
#   args:
#     columns: ["amount", "balance"]
# ---------------------------------------------------------
def remove_negative_or_zero(df, columns):
    """
    Remove rows where numeric columns are <= 0.
    :param df: Spark DataFrame
    :param columns: list of column names
    """

    if isinstance(columns, str):
        columns = [columns]

    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        df = df.filter(F.col(column) > 0)

    return df


# ---------------------------------------------------------
# Remove rows where value exceeds another column
# Example: balance > amount
# - function: remove_rows_exceeding_reference
#   args:
#     columns: ["balance"]
#     reference_column: "amount"
# ---------------------------------------------------------
def remove_rows_exceeding_reference(df, columns, reference_column):
    """
    Remove rows where column > reference_column.
    Example: balance > amount
    """

    if isinstance(columns, str):
        columns = [columns]

    if reference_column not in df.columns:
        raise ValueError(
            f"Reference column '{reference_column}' not found in DataFrame"
        )

    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        df = df.filter(F.col(column) <= F.col(reference_column))

    return df
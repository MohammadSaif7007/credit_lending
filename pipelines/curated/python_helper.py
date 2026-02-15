from pyspark.sql.functions import col

class CuratedHelper:
    """
    Common reusable transformation utilities for curated layer.
    These methods operate directly on self.df.
    """

    def retain_columns(self, columns: list):
        missing_cols = [c for c in columns if c not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
        self.df = self.df.select(*columns)

    def rename_columns(self, rename_mapping: dict):
        missing_cols = [c for c in rename_mapping.keys() if c not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found for renaming: {missing_cols}")
        for old_name, new_name in rename_mapping.items():
            self.df = self.df.withColumnRenamed(old_name, new_name)

    def drop_columns(self, columns: list):
        existing_cols = [c for c in columns if c in self.df.columns]
        if existing_cols:
            self.df = self.df.drop(*existing_cols)

    def cast_columns(self, cast_mapping: dict):
        for col_name, dtype in cast_mapping.items():
            if col_name not in self.df.columns:
                raise ValueError(f"Column {col_name} not found for casting")
            self.df = self.df.withColumn(col_name, col(col_name).cast(dtype))
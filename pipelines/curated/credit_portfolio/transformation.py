from pyspark.sql.functions import col
from pipelines.curated.python_helper import CuratedHelper


class CreditPortfolioCurator(CuratedHelper):
    def __init__(self, df, dependencies=None, config=None):
        """
        df: base table as self.df
        dependencies: dict with {logical_name: Spark DataFrame}
        """
        self.df = df
        self.config = config or {}
        self.dependencies = dependencies or {}
    
    def cfg(self, key, default=None):
        return self.config.get(key, default)
    
    def step_agg_transactions(self):
        # Base table transformations
        self.df.withColumn("credit_score_adj", col("amount") * 1.1)

    def step_join_dependencies(self):
        customer_df = self.dependencies.get("customer")

        if customer_df is None:
            raise ValueError("Customer dependency not provided")

        self.df = (
            self.df
            .join(customer_df, on="transaction_id", how="left")
        )

    def execute(self):
        self.step_agg_transactions()
        self.step_join_dependencies()
        self.retain_columns(self.cfg("retain_columns"))

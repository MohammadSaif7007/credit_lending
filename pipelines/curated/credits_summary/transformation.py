from pyspark.sql.functions import col, sum as Fsum, avg
from pipelines.curated.python_helper import CuratedHelper


class CreditsSummaryCurator(CuratedHelper):
    def __init__(self, df, dependencies=None, config=None):
        """
        df: base DataFrame (refined credits)
        dependencies: dict of upstream tables (not needed here)
        config: optional config dictionary
        """
        self.df = df
        self.dependencies = dependencies or {}
        self.config = config or {}

    def cfg(self, key, default=None):
        return self.config.get(key, default)

    # --------------------------
    # Aggregate metrics per client
    # --------------------------
    def step_aggregate_credits(self):
        self.df = (
            self.df.groupBy("client_id")
            .agg(
                Fsum("amount").alias("total_amount"),
                avg("amount").alias("average_payment"),
                )
        )

    # --------------------------
    # Default flag based on business rule
    # Since balance is not available, we can use a simple rule:
    # If total_amount > 10000, mark as 1 (example), else 0
    # --------------------------
    def step_default_flag(self):
        self.df = self.df.withColumn(
            "default_flag",
            (col("total_amount") > 10000).cast("int")
        )

    def execute(self):
        self.step_aggregate_credits()
        self.step_default_flag()
        self.retain_columns(self.cfg("retain_columns"))
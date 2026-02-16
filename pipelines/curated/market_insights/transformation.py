from pyspark.sql.functions import col, avg, max as Fmax, min as Fmin
from pipelines.curated.python_helper import CuratedHelper


class MarketInsightsCurator(CuratedHelper):
    def __init__(self, df, dependencies=None, config=None):
        self.df = df
        self.dependencies = dependencies or {}
        self.config = config or {}

    def cfg(self, key, default=None):
        return self.config.get(key, default)

    def step_avg_price_metrics(self):
        self.df = (
            self.df.groupBy("symbol")
            .agg(
                avg("price").alias("avg_price"),
                Fmax("price").alias("max_price"),
                Fmin("price").alias("min_price"),
                avg("change_pct").alias("avg_change_pct"),
            )
        )

    def step_volatility_index(self):
        self.df = self.df.withColumn("volatility_index", col("max_price") - col("min_price"))

    def execute(self):
        self.step_avg_price_metrics()
        self.step_volatility_index()
        self.retain_columns(self.cfg("retain_columns"))
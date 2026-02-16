from pyspark.sql.functions import col, sum as Fsum, countDistinct
from pipelines.curated.python_helper import CuratedHelper


class CollateralsSummaryCurator(CuratedHelper):
    def __init__(self, df, dependencies=None, config=None):
        self.df = df
        self.dependencies = dependencies or {}
        self.config = config or {}

    def cfg(self, key, default=None):
        return self.config.get(key, default)

    def step_aggregate_collaterals(self):
        self.df = (
            self.df.groupBy("client_id")
            .agg(
                Fsum("value").alias("total_collateral_value"),
                countDistinct("type").alias("num_collateral_types"),
            )
        )

    def step_diversification_index(self):
        self.df = self.df.withColumn(
            "collateral_diversification_index", col("num_collateral_types") / col("total_collateral_value")
        )

    def execute(self):
        self.step_aggregate_collaterals()
        self.step_diversification_index()
        self.retain_columns(self.cfg("retain_columns"))
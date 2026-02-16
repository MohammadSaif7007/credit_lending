from pyspark.sql.functions import col, sum as Fsum, avg, when
from pipelines.curated.python_helper import CuratedHelper

class ClientSummaryCurator(CuratedHelper):
    def __init__(self, df, dependencies=None, config=None):
        self.df = df
        self.dependencies = dependencies or {}
        self.config = config or {}

    def cfg(self, key, default=None):
        return self.config.get(key, default)

    # --------------------------
    # Aggregate loan info per client
    # --------------------------
    def step_aggregate_loans(self):
        self.df = (
            self.df.groupBy("client_id", "name", "income")
            .agg(
                Fsum("loan_count").alias("total_loans"),
                avg("loan_count").alias("average_loan_count"),
            )
        )

    def step_debt_to_income_ratio(self):
        self.df = self.df.withColumn("debt_to_income_ratio", col("total_loans") / col("income"))

    def step_high_risk_flag(self):
        self.df = self.df.withColumn(
            "high_risk_flag", when(col("debt_to_income_ratio") > 1, 1).otherwise(0)
        )

    def execute(self):
        self.step_aggregate_loans()
        self.step_debt_to_income_ratio()
        self.step_high_risk_flag()
        self.retain_columns(self.cfg("retain_columns"))
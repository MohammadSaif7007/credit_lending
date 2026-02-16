from pyspark.sql.functions import col, sum as Fsum, avg, max as Fmax, countDistinct, when
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
    
    # --------------------------
    # Aggregations per client
    # --------------------------
    def step_aggregate_metrics(self):
        self.df = (
            self.df.groupBy("client_id")
            .agg(
                Fsum("amount").alias("total_loan_amount"),
                Fsum("balance").alias("total_balance"),
                avg("amount").alias("average_loan_amount"),
                Fmax("amount").alias("max_loan_amount"),
                countDistinct("status").alias("loan_status_count")
            )
        )

    # --------------------------
    # Join with dependencies
    # --------------------------
    def step_join_dependencies(self):
        client_df = self.dependencies.get("client")
        collat_df = self.dependencies.get("collaterals")

        if client_df is None:
            raise ValueError("Client dependency not provided")
        if collat_df is None:
            raise ValueError("Collateral dependency not provided")

        # Join client info
        self.df = self.df.join(client_df, on="client_id", how="left")

        # Join collateral info
        collat_agg = (
            collat_df.groupBy("client_id")
            .agg(
                Fsum("value").alias("total_collateral_value"),
                countDistinct("type").alias("collateral_diversification_index")
            )
        )
        self.df = self.df.join(collat_agg, on="client_id", how="left")

    # --------------------------
    # Derived metrics
    # --------------------------
    def step_derive_metrics(self):
        self.df = self.df.withColumn(
            "debt_to_income_ratio", col("total_loan_amount") / col("income")
        )
        self.df = self.df.withColumn(
            "collateral_coverage_ratio", col("total_collateral_value") / col("total_balance")
        )
        self.df = self.df.withColumn(
            "high_risk_flag",
            when((col("debt_to_income_ratio") > 0.5) | (col("collateral_coverage_ratio") < 1), 1).otherwise(0)
        )

    # --------------------------
    # Retain only needed columns
    # --------------------------
    def retain_columns(self):
        cols_to_keep = self.cfg("retain_columns")
        if cols_to_keep:
            self.df = self.df.select(*cols_to_keep)

    # --------------------------
    # Execute all steps
    # --------------------------
    def execute(self):
        self.step_aggregate_metrics()
        self.step_join_dependencies()
        self.step_derive_metrics()
        self.retain_columns()

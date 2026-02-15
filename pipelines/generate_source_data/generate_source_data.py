import os
from pyspark.sql.functions import col, current_timestamp, rand
from framework.spark_session import get_spark  # reuse framework SparkSession

def generate_source_data():
    spark = get_spark()  # reuse existing session

    base_path = "/opt/data/source"
    os.makedirs(base_path, exist_ok=True)

    # ----------------------
    # Clients dataset
    # ----------------------
    clients = spark.range(1000).withColumnRenamed("id", "client_id")
    clients = clients.withColumn("name", col("client_id").cast("string"))
    clients = clients.withColumn("age", (rand()*50 + 20).cast("int"))
    clients = clients.withColumn("gender", (rand()*2).cast("int"))
    clients = clients.withColumn("ingestion_date", current_timestamp())
    clients.write.mode("overwrite").parquet(os.path.join(base_path, "clients"))

    # ----------------------
    # Collaterals dataset
    # ----------------------
    collaterals = spark.range(1500).withColumnRenamed("id", "collateral_id")
    collaterals = collaterals.withColumn("client_id", (rand()*1000).cast("int"))
    collaterals = collaterals.withColumn("type", (rand()*3).cast("int"))
    collaterals = collaterals.withColumn("value", (rand()*50000 + 5000).cast("double"))
    collaterals = collaterals.withColumn("ingestion_date", current_timestamp())
    collaterals.write.mode("overwrite").parquet(os.path.join(base_path, "collaterals"))

    # ----------------------
    # Credits dataset
    # ----------------------
    credits = spark.range(2000).withColumnRenamed("id", "credit_id")
    credits = credits.withColumn("client_id", (rand()*1000).cast("int"))
    credits = credits.withColumn("amount", (rand()*20000 + 1000).cast("double"))
    credits = credits.withColumn("balance", (rand()*20000).cast("double"))
    credits = credits.withColumn("status", (rand()*2).cast("int"))
    credits = credits.withColumn("ingestion_date", current_timestamp())
    credits.write.mode("overwrite").parquet(os.path.join(base_path, "credits"))

    # ----------------------
    # Market dataset
    # ----------------------
    market = spark.range(500).withColumnRenamed("id", "market_id")
    market = market.withColumn("symbol", col("market_id").cast("string"))
    market = market.withColumn("price", (rand()*500 + 10).cast("double"))
    market = market.withColumn("change_pct", (rand()*10 - 5).cast("double"))
    market.write.mode("overwrite").parquet(os.path.join(base_path, "market"))

    print("✅ Source data generated using framework SparkSession!")
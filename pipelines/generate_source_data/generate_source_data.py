import os
import random
from pyspark.sql.functions import (col, 
                                   current_timestamp,
                                   round,
                                   lit,
                                   when,
                                   rand,
                                   udf
)
from pyspark.sql.types import StringType
from framework.spark_session import get_spark

def generate_source_data(base_path="/opt/data/source"):
    """
    Generate sample source data for Clients, Collaterals, Credits, and Market datasets.
    Data has realistic values and relationships for analytics and ML use cases.
    """
    spark = get_spark()  # reuse existing SparkSession
    os.makedirs(base_path, exist_ok=True)

    # ----------------------
    # Clients dataset
    # ----------------------
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Evan", "Fiona", "George", "Hannah", "Ian", "Julia",
                   "Kevin", "Laura", "Michael", "Nina", "Oscar", "Paula", "Quentin", "Rachel", "Steve", "Tina"]
    last_names = ["Smith", "Johnson", "Lee", "Patel", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor",
                  "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"]

    def random_name():
        return random.choice(first_names) + " " + random.choice(last_names)

    name_udf = udf(lambda x: random_name(), StringType())
    
    clients = spark.range(200).withColumnRenamed("id", "client_id")
    clients = clients.withColumn("name", name_udf(col("client_id")))
    clients = clients.withColumn("age", (rand()*50 + 20).cast("int"))
    clients = clients.withColumn("gender", (rand()*2).cast("int"))

    # Address mapping
    cities = ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven", "Groningen", "Maastricht", "Leiden"]
    clients = clients.withColumn("address", (col("client_id") % len(cities)).cast("int"))
    clients = clients.withColumn("address", when(col("address")==0, lit("Amsterdam"))
                                   .when(col("address")==1, lit("Rotterdam"))
                                   .when(col("address")==2, lit("The Hague"))
                                   .when(col("address")==3, lit("Utrecht"))
                                   .when(col("address")==4, lit("Eindhoven"))
                                   .when(col("address")==5, lit("Groningen"))
                                   .when(col("address")==6, lit("Maastricht"))
                                   .otherwise(lit("Leiden")))

    # Corporate info
    corporates = ["None", "ABN Corp", "Other"]
    clients = clients.withColumn("corporate", (rand()*3).cast("int"))
    clients = clients.withColumn("corporate", when(col("corporate")==0, lit("None"))
                                             .when(col("corporate")==1, lit("ABN Corp"))
                                             .otherwise(lit("Other")))

    clients = clients.withColumn("income", round(rand()*100000 + 20000,2))
    clients = clients.withColumn("loan_count", (rand()*5).cast("int"))
    clients = clients.withColumn("ingestion_date", current_timestamp())
    clients.write.mode("overwrite").parquet(os.path.join(base_path, "clients"))

    # ----------------------
    # Collaterals dataset
    # ----------------------
    collateral_types = {0: "Mortgage Registration", 1: "Lombard Loans", 2: "Company Equipment", 3: "Cash Savings/Deposits"}
    collaterals = spark.range(500).withColumnRenamed("id", "collateral_id")
    collaterals = collaterals.withColumn("client_id", (rand()*1000).cast("int"))
    collaterals = collaterals.withColumn("type", (rand()*4).cast("int"))
    collaterals = collaterals.withColumn("type", when(col("type")==0, lit("Mortgage Registration"))
                                              .when(col("type")==1, lit("Lombard Loans"))
                                              .when(col("type")==2, lit("Company Equipment"))
                                              .otherwise(lit("Cash Savings/Deposits")))
    collaterals = collaterals.withColumn("value", round(rand()*50000 + 5000,2))
    collaterals = collaterals.withColumn("ingestion_date", current_timestamp())
    collaterals.write.mode("overwrite").parquet(os.path.join(base_path, "collaterals"))

    # ----------------------
    # Credits dataset
    # ----------------------
    credits = spark.range(1000).withColumnRenamed("id", "credit_id")
    credits = credits.withColumn("client_id", (rand()*1000).cast("int"))
    credits = credits.withColumn("amount", round(rand()*20000 + 1000,2))
    credits = credits.withColumn("balance", round(rand()*20000,2))
    credits = credits.withColumn("status", (rand()*2).cast("int"))
    credits = credits.withColumn("ingestion_date", current_timestamp())
    credits.write.mode("overwrite").parquet(os.path.join(base_path, "credits"))

    # ----------------------
    # Market dataset
    # ----------------------
    market = spark.range(200).withColumnRenamed("id", "market_id")
    market = market.withColumn("symbol", col("market_id").cast("string"))
    market = market.withColumn("price", round(rand()*500 + 10,2))
    market = market.withColumn("change_pct", round(rand()*10 - 5,2))
    market = market.withColumn("ingestion_date", current_timestamp())
    market.write.mode("overwrite").parquet(os.path.join(base_path, "market"))

    print("Source data generated for Clients, Collaterals, Credits, and Market datasets!")
# framework/engine.py
import os
import sys
import argparse
from pyspark.sql import functions as F

sys.path.append("/opt")

from framework.spark_session import get_spark
from framework.config_loader import load_config
from framework.plugin_loader import load_helper, load_transform


def get_paths(layer: str, table: str):
    """Returns base_path for pipeline and output_dir for parquet dynamically"""
    project_root = os.path.abspath(os.path.join("/opt/airflow", ".."))
    base_path = f"/opt/pipelines/{layer}/{table}"
    output_dir = os.path.join(project_root, "data", layer, table)
    return base_path, output_dir


def read_input(layer: str, config: dict):
    """
    Read base input table depending on layer.

    - parsed/refined → expects `input_path`
    - curated → expects `input_tables.baseTable`
    """

    spark = get_spark()

    # Determine input path based on layer
    if layer == "curated":
        input_path = config.get("input_tables", {}).get("baseTable")
    else:
        input_path = config.get("input_path")

    if not input_path:
        raise ValueError(f"Base input path missing in config for {layer}")

    # # Read CSV for parsed/refined if needed, else parquet
    # if layer in ["parsed", "refined"] and input_path.endswith(".csv"):
    #     df = spark.read.option("header", True).csv(input_path)
    # else:
    #     df = spark.read.parquet(input_path)
    df = spark.read.parquet(input_path)
    return df, spark


def write_output(df, output_dir: str):
    """Add partition columns and write partitioned parquet"""
    if "ingestion_date" not in df.columns:
        df = df.withColumn("ingestion_date", F.current_timestamp())

    df = df.withColumn("year", F.year("ingestion_date")) \
           .withColumn("month", F.month("ingestion_date")) \
           .withColumn("day", F.dayofmonth("ingestion_date")) 

    df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_dir)
    return df


def run_validations(df, table_name: str, validations: list, stage="Pre-Write"):
    """Run SQL-based validations"""
    # 1. Check if DataFrame exists
    if df is None:
        raise ValueError(f"[{stage} VALIDATION] DataFrame for {table_name} is None")

    # 2. Check if DataFrame has rows without converting to RDD
    if not df.head(1):  # returns [] if empty
        raise ValueError(f"[{stage} VALIDATION] DataFrame for {table_name} is empty")
    
    df.createOrReplaceTempView(table_name)
    for rule in validations:
        query = f"SELECT * FROM {table_name} WHERE {rule['condition']}"
        failed_count = df.sql_ctx.sql(query).count()
        print(f"[{stage} VALIDATION] {rule['name']} (severity={rule['severity']}): {failed_count} rows failed")
        if rule["severity"].lower() == "fail" and failed_count > 0:
            raise Exception(f"Validation {rule['name']} failed with {failed_count} rows")


def transform_df(layer: str, table: str, df, config):
    """Apply transformations on df depending on layer"""
    if layer in ["parsed", "refined"]:
        helper = load_helper(layer)
        for step in config.get("steps", []):
            func = getattr(helper, step["function"])
            args = step.get("args", [])
            print(f"[INFO] Applying {step['function']} with args {args} on {layer}/{table}")
            if isinstance(args, dict):
                df = func(df, **args)   # for dict-based args
            elif isinstance(args, list):
                df = func(df, *args)    # for list-based args
    elif layer == "curated":
        spark = get_spark()
        transform_module = load_transform(table)

    # Pick the class whose name ends with "Curator"
        transform_class = None
        for cls in transform_module.__dict__.values():
            if isinstance(cls, type) and cls.__name__.endswith("Curator"):
                transform_class = cls
                break

        if not transform_class:
            raise ValueError(f"No transformation class found for table {table}")

        # Load dependencies
        dependencies = {}
        for dep_name, dep_path in config.get("input_tables", {}).get("dependencies", {}).items():
            dependencies[dep_name] = spark.read.parquet(dep_path)

        # Pass config as well if needed
        transform_instance = transform_class(df, dependencies=dependencies, config=config)
        transform_instance.execute()
        df = transform_instance.df
    return df


def execute_transform(layer: str, table: str):
    """Run transformations only with pre-write validation"""
    print(f"[INFO] Starting transformation for {layer}/{table}")
    base_path, output_dir = get_paths(layer, table)
    config = load_config(os.path.join(base_path, "config.yaml"))

    df, spark = read_input(layer, config)
    df = transform_df(layer, table, df, config)

    # Pre-write validation
    run_validations(df, table, config.get("validations", []), stage="Pre-Write")

    # Write partitioned parquet
    df = write_output(df, output_dir)
    print(f"[INFO] Transformation for {layer}/{table} completed. Output at {output_dir}")
    spark.stop()


def execute_validation(layer: str, table: str):
    """Run post-write validations"""
    print(f"[INFO] Starting validation for {layer}/{table}")
    _, output_dir = get_paths(layer, table)
    config = load_config(os.path.join(f"/opt/pipelines/{layer}/{table}", "config.yaml"))
    spark = get_spark()
    df = spark.read.parquet(output_dir)
    run_validations(df, table, config.get("validations", []), stage="Post-Write")
    print(f"[INFO] Validation for {layer}/{table} completed successfully!")
    spark.stop()


def execute_etl(layer: str, table: str):
    """Wrapper to run transform + validation sequentially"""
    execute_transform(layer, table)
    execute_validation(layer, table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("layer")
    parser.add_argument("table")
    parser.add_argument("--mode", choices=["transform", "validate", "etl"], default="etl")
    args = parser.parse_args()

    if args.mode == "transform":
        execute_transform(args.layer, args.table)
    elif args.mode == "validate":
        execute_validation(args.layer, args.table)
    else:
        execute_etl(args.layer, args.table)
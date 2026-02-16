import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import sys

# Add /opt so framework package can be imported
sys.path.append("/opt")

from framework.engine import execute_transform, execute_validation
from pipelines.generate_source_data.generate_source_data import generate_source_data

# -----------------------------------------------------
# Use the DAG file's folder to locate dependencies.json
# -----------------------------------------------------
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
DEPENDENCIES_FILE = os.path.join(DAG_FOLDER, "dependencies.json")

# Load dependencies from JSON
with open(DEPENDENCIES_FILE) as f:
    raw_dependencies = json.load(f)

# Convert JSON keys to tuples for Airflow DAG usage
dependencies = {}
for key, upstreams in raw_dependencies.items():
    layer, table = key.split(".")
    dependencies[(layer, table)] = [tuple(up.split(".")) for up in upstreams]

# Define all layer/table combinations
layers_tables = list(dependencies.keys())

with DAG(
    dag_id="credit_lending_dag",
    start_date=datetime(2026, 2, 12),
    schedule=None,
    catchup=False,
    description="Credit Lending ETL DAG with dynamic dependencies",
) as dag:

    # -------------------
    # Source generation task
    # -------------------
    source_task = PythonOperator(
        task_id="generate_source_data",
        python_callable=generate_source_data
    )

    # Dictionary to store TaskGroup objects for reference
    task_groups = {}

    # Create TaskGroups for each table
    for layer, table in layers_tables:
        with TaskGroup(group_id=f"{layer}_{table}_group") as tg:
            transform_task = PythonOperator(
                task_id=f"{layer}_{table}_transform",
                python_callable=execute_transform,
                op_args=[layer, table]
            )

            validation_task = PythonOperator(
                task_id=f"{layer}_{table}_validation",
                python_callable=execute_validation,
                op_args=[layer, table]
            )

            transform_task >> validation_task
            task_groups[(layer, table)] = tg

    # Set inter-layer dependencies dynamically
    for table_key, upstream_tables in dependencies.items():
        for upstream_key in upstream_tables:
            task_groups[upstream_key] >> task_groups[table_key]

    # Set source_task upstream for all parsed tables
    for layer, table in layers_tables:
        if layer == "parsed":
            source_task >> task_groups[(layer, table)]
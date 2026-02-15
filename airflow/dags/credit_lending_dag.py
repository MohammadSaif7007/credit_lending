from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import sys

# Add /opt so framework package can be imported
sys.path.append("/opt")

from framework.engine import execute_transform, execute_validation
from pipelines.generate_source_data.generate_source_data import generate_source_data

with DAG(
    dag_id="credit_lending_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Credit Lending ETL DAG with dynamic dependencies",
) as dag:
    
    # Source generation task
    source_task = PythonOperator(
        task_id="generate_source_data",
        python_callable=generate_source_data
    )

    # Define all layer/table combinations
    layers_tables = [
        ("parsed", "clients"),
        # ("parsed", "transactions"),
        # ("parsed", "customer"),
        # ("refined", "transactions"),
        # ("refined", "customer"),
        # ("curated", "credit_portfolio"),
    ]

    # Define dependencies: key = table, value = list of upstream tables
    dependencies = {
        ("parsed", "clients"): [],
        # ("parsed", "transactions"): [],
        # ("parsed", "customer"): [],
        # ("refined", "transactions"): [("parsed", "transactions")],
        # ("refined", "customer"): [("parsed", "customer")],
        # ("curated", "credit_portfolio"): [
        #     ("refined", "transactions"),
        #     ("refined", "customer")
        # ],
    }

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

            # Transform -> Validation
            transform_task >> validation_task

            # Store the group
            task_groups[(layer, table)] = tg

    # Set inter-layer dependencies dynamically
    for table_key, upstream_tables in dependencies.items():
        for upstream_key in upstream_tables:
            task_groups[upstream_key] >> task_groups[table_key]

    # Dynamically set source_task upstream for all parsed tables
    for layer, table in layers_tables:
        if layer == "parsed":
            source_task >> task_groups[(layer, table)]
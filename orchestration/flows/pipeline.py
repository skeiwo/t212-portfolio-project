from orchestration.tasks.extract import get_open_positions, get_orders_history
from orchestration.tasks.load import load_to_db

from prefect import flow
from prefect_dbt.cli import DbtCoreOperation

@flow(name="etl-pipeline")
def etl_pipeline_flow():
    # --- Ingestion ---
    positions = get_open_positions()
    load_to_db(positions, schema = "t212", table_name =  "raw_open_positions")

    orders = get_orders_history()
    load_to_db(orders, schema =  "t212", table_name = "raw_orders_history")

    # --- Transformation ---
    DbtCoreOperation(
        commands=["dbt run --select staging", "dbt test --select staging"],
        project_dir="transform/",
        profiles_dir="transform/"
    ).run()


if __name__ == "__main__":
    etl_pipeline_flow()
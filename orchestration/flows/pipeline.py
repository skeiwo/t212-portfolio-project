from orchestration.tasks.extract import get_open_positions, get_orders_history
from orchestration.tasks.load import load_to_db

from prefect import flow

@flow(name="etl-pipeline")
def raw_ingestion_flow():
    positions = get_open_positions()
    load_to_db(positions, "raw_open_positions")

    orders = get_orders_history()
    load_to_db(orders, "raw_orders_history")


if __name__ == "__main__":
    raw_ingestion_flow()
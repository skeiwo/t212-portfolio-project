from orchestration.tasks.extract import get_open_positions, get_orders_history, get_exchange_rates, get_dividends, get_tradable_stocks
from orchestration.tasks.load import load_to_db

from prefect import flow
from prefect_dbt.cli import DbtCoreOperation

@flow(name="etl-pipeline")
def etl_pipeline_flow():
    # --- Ingestion ---
    positions = get_open_positions.submit()
    orders = get_orders_history.submit()
    dividends = get_dividends.submit()
    ex_rates = get_exchange_rates.submit()
    tradable_stocks = get_tradable_stocks.submit()

    load_to_db(positions, schema = "t212_raw", table_name =  "raw_open_positions")
    load_to_db(orders, schema =  "t212_raw", table_name = "raw_orders_history")
    load_to_db(dividends, schema = "t212_raw", table_name = "raw_dividends")
    load_to_db(ex_rates, schema = "t212_raw", table_name = "raw_exchange_rates")
    load_to_db(tradable_stocks, schema = "t212_raw", table_name = "raw_tradable_stocks")
    # --- Transformation ---
    DbtCoreOperation(
        commands=[
            "dbt seed",
            "dbt run --select stg",
            "dbt test --select stg",
            "dbt run --select int",
            "dbt test --select int",
        ],
        project_dir="transform/",
        profiles_dir="transform/"
    ).run()


if __name__ == "__main__":
    etl_pipeline_flow()
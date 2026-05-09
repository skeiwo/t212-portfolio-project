from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from orchestration.tasks.extract import get_open_positions, get_orders_history, get_dividends, get_tradable_stocks, get_historical_prices
from orchestration.tasks.load import load_to_db


@flow(name="etl-pipeline")
def etl_pipeline_flow():
    sources = [
        (get_open_positions,   "raw_open_positions",   "WRITE_TRUNCATE"),
        (get_orders_history,   "raw_orders_history",   "WRITE_APPEND"),
        (get_dividends,        "raw_dividends",        "WRITE_APPEND"),
        (get_tradable_stocks,  "raw_tradable_stocks",  "WRITE_TRUNCATE"),
        (get_historical_prices,"raw_historical_prices","WRITE_APPEND"),
    ]

    load_futures = []
    for extract_task, table_name, write_disposition in sources:
        rows = extract_task.submit()
        load_future = load_to_db.submit(rows, schema="t212_raw", table_name=table_name, write_disposition=write_disposition)
        load_futures.append(load_future)

    for f in load_futures:
        f.wait()


    settings = PrefectDbtSettings(project_dir="transform/", profiles_dir="transform/")
    runner = PrefectDbtRunner(settings=settings)
    runner.invoke(["build", "--select", "stg", "int", "marts"])


if __name__ == "__main__":
    etl_pipeline_flow()
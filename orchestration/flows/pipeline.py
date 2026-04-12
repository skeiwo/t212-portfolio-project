from orchestration.tasks.extract import get_open_positions
from orchestration.tasks.load import load_to_db

from prefect import flow

@flow(name="etl-pipeline")
def etl_pipeline():
    df_extract = get_open_positions()
    transform = load_to_db(df_extract)


if __name__ == "__main__":
    etl_pipeline()
from extract import get_open_positions
from prefect import flow

@flow(name="etl-pipeline")
def etl_pipeline():
    extract = get_open_positions()
    return extract

result = etl_pipeline()
print(result)

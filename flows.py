from tasks import get_numbers, transform_numbers, load_numbers
from prefect import flow

@flow(name="etl-pipeline")
def etl_pipeline():
    extract = get_numbers()
    transform = transform_numbers(extract)
    load = load_numbers(transform)
    print(load)

etl_pipeline()
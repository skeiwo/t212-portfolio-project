from dotenv import load_dotenv
from prefect import task
from google.cloud import bigquery
from google.oauth2 import service_account
from orchestration.utils import _get_logger

import os

load_dotenv()



@task(retries=1, retry_delay_seconds=30)
def load_to_db(rows: list[dict], schema: str, table_name: str) -> None:
    BIGQUERY_CREDENTIALS = service_account.Credentials.from_service_account_file(os.getenv("GCP_CREDENTIALS"))
    BIGQUERY_CLIENT = bigquery.Client(credentials = BIGQUERY_CREDENTIALS, project = os.getenv("GCP_PROJECT"))
    
    logger = _get_logger()
    logger.info("Starting load_to_db ingestion")

    if not rows:
        logger.info("No rows to load, skipping")
        return

    table_id = f"{os.getenv("GCP_PROJECT")}.{schema}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    logger.info("Loading %d rows to %s", len(rows), table_id)
    job = BIGQUERY_CLIENT.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()  # wait for completion and raise on failure
    logger.info("Load complete")
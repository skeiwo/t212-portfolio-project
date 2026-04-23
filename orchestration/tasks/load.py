from dotenv import load_dotenv
from prefect import task
from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

import os
import pandas as pd

load_dotenv()

BIGQUERY_CREDENTIALS = service_account.Credentials.from_service_account_file(os.getenv("GCP_CREDENTIALS"))
BIGQUERY_CLIENT = bigquery.Client(credentials = BIGQUERY_CREDENTIALS, project = os.getenv("GCP_PROJECT"))


@task(retries=1, retry_delay_seconds=30)
def load_to_db(rows: list[dict], table_name: str) -> None:
    try:
        logger = get_run_logger()
    except MissingContextError:
        logger = logging.getLogger(__name__)
    logger.info("Starting load_to_db ingestion")

    if not rows:
        logger.info("No rows to load, skipping")
        return

    table_id = f"{os.getenv('GCP_PROJECT')}.{os.getenv('GCP_SCHEMA')}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    logger.info("Loading %d rows to %s", len(rows), table_id)
    job = BIGQUERY_CLIENT.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()  # wait for completion and raise on failure
    logger.info("Load complete")
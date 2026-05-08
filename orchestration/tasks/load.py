from dotenv import load_dotenv
from prefect import task
from google.cloud import bigquery
from google.oauth2 import service_account
from orchestration.utils import _get_logger

import os

load_dotenv()


@task(retries=1, retry_delay_seconds=30)
def load_to_db(rows: list[dict], schema: str, table_name: str, write_disposition: str = "WRITE_TRUNCATE") -> None:
    credentials = service_account.Credentials.from_service_account_file(os.getenv("GCP_CREDENTIALS"))
    client = bigquery.Client(credentials=credentials, project=os.getenv("GCP_PROJECT"))

    logger = _get_logger()
    logger.info("Starting load_to_db ingestion")

    if not rows:
        logger.info("No rows to load, skipping")
        return

    table_id = f"{os.getenv('GCP_PROJECT')}.{schema}.{table_name}"

    disposition = getattr(bigquery.WriteDisposition, write_disposition)
    job_config = bigquery.LoadJobConfig(write_disposition=disposition)

    logger.info("Loading %d rows to %s with %s", len(rows), table_id, write_disposition)
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    logger.info("Load complete")
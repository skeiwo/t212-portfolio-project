from dotenv import load_dotenv
from prefect import task
from google.cloud import bigquery
from google.oauth2 import service_account

import os
import pandas as pd

load_dotenv()

BIGQUERY_CREDENTIALS = service_account.Credentials.from_service_account_file(os.getenv("GCP_CREDENTIALS"))
BIGQUERY_CLIENT = bigquery.Client(credentials = BIGQUERY_CREDENTIALS, project = os.getenv("GCP_PROJECT"))
TABLE_ID = f"{os.getenv("GCP_PROJECT")}.{os.getenv("GCP_SCHEMA")}.L0_open_positions"

@task
def load_to_db(df: pd.DataFrame) -> None:
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = BIGQUERY_CLIENT.load_table_from_dataframe(df, TABLE_ID, job_config = job_config)
        print("Data loaded to db")
    except Exception as e:
        raise Exception(f"Error: {str(e)}")
    return None
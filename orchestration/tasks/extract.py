import base64
import json
import os
import requests
import time
import yfinance as yf

from google.cloud import bigquery
from google.oauth2 import service_account
from orchestration.utils import _get_logger
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from prefect import task

load_dotenv()

# AUTHENTICATION
T212_API_KEY = os.getenv("T212_API_KEY")
T212_API_SECRET = os.getenv("T212_API_SECRET")
T212_CREDENTIALS = f"{T212_API_KEY}:{T212_API_SECRET}"
ENCODED_CREDENTIALS = base64.b64encode(T212_CREDENTIALS.encode("utf-8")).decode("utf-8")
HEADERS = {"Authorization": f"Basic {ENCODED_CREDENTIALS}"}
BASE_URL = os.getenv("T212_BASE_URL")

def _get_isins_from_bq() -> list[str]:
    credentials = service_account.Credentials.from_service_account_file(os.getenv("GCP_CREDENTIALS"))
    client = bigquery.Client(credentials=credentials, project=os.getenv("GCP_PROJECT"))
    sql = "select isin from `t212_marts.dim_instruments`"
    query_result = client.query(sql)
    return [row[0] for row in query_result.result()]


def _get_tickers_from_isins(isins: list[str]) -> list[tuple[str, str]]:
    pairs = [(isin, yf.utils.get_ticker_by_isin(isin)) for isin in isins]
    return [(isin, ticker) for isin, ticker in pairs if ticker]


@task
def get_open_positions() -> list[dict]:
    rows = []
    extract_timestamp = datetime.now(timezone.utc).isoformat()

    logger = _get_logger()
    logger.info("Starting open positions extract")

    url = f"{BASE_URL}/api/v0/equity/positions"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()


    data = response.json()
    for position in data:
        ticker = position.get("instrument").get("ticker")
        rows.append({
            "extract_timestamp": extract_timestamp,
            "record_id": str(ticker) if ticker is not None else None,
            "payload": json.dumps(position),
        })

    logger.info("Extract complete: %d positions", len(rows))
    return rows


@task
def get_orders_history() -> list[dict]:
    rows = []
    extract_timestamp = datetime.now(timezone.utc).isoformat()

    logger = _get_logger()
    logger.info("Starting orders history extract")
    
    url = f"{BASE_URL}/api/v0/equity/history/orders"
    params = {"limit": 20}

    response = requests.get(url, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    for item in data.get("items", []):
        order_id = item.get("order", {}).get("id")
        created_at = item.get("order", {}).get("createdAt")
        rows.append({
            "extract_timestamp": extract_timestamp,
            "record_id": str(order_id) if order_id is not None else None,
            "record_created_at": created_at,
            "payload": json.dumps(item),
        })

    logger.info("Extracted %d orders", len(rows))
    return rows

@task
def get_dividends() -> list[dict]:
    rows = []
    extract_timestamp = datetime.now(timezone.utc).isoformat()
    
    logger = _get_logger()
    logger.info("Starting dividends history extract")
    
    url = f"{BASE_URL}/api/v0/equity/history/dividends"
    params = {"limit": 20}

    response = requests.get(url, headers=HEADERS, timeout=30, params=params)
    response.raise_for_status()
    data = response.json()

    for item in data.get("items", []):
        reference_id = item.get("reference")
        created_at = item.get("paidOn")
        rows.append({
            "extract_timestamp": extract_timestamp,
            "record_id": reference_id,
            "record_created_at": created_at,
            "payload": json.dumps(item),
        })
    
    logger.info("Extracted %d dividends", len(rows))

    return rows


@task
def get_tradable_stocks() -> list[dict]:
    rows = []
    extract_timestamp = datetime.now(timezone.utc).isoformat()
    
    logger = _get_logger()
    logger.info("Starting tradable stock extract")

    url = f"{BASE_URL}/api/v0/equity/metadata/instruments"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    data = response.json()
    for stock in data:
        rows.append({
            "extract_timestamp": extract_timestamp,
            "isin": stock.get("isin"),
            "created_at": stock.get("addedOn"),
            "payload": json.dumps(stock)
        })
    
    logger.info("Extracted %d tradable_stocks", len(rows))
    return rows


@task(retries=2, retry_delay_seconds=30)
def get_historical_prices() -> list[dict]:
    logger = _get_logger()
    extract_timestamp = datetime.now(timezone.utc).isoformat()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    today = datetime.now(timezone.utc).date()
    rows = []

    isins = _get_isins_from_bq()
    instruments = _get_tickers_from_isins(isins)

    logger.info("Starting historical prices extract for %d tickers for %s", len(instruments), yesterday)

    for isin, ticker in instruments:
        try:
            history = yf.Ticker(ticker).history(start=yesterday, end=today, auto_adjust=False)
        except Exception as e:
            logger.warning("Failed to fetch prices for %s (%s): %s", ticker, isin, e)
            continue

        if history.empty:
            logger.warning("No price data returned for %s (%s), skipping", ticker, isin)
            continue

        for ts, row in history.iterrows():
            row_date = ts.date()
            rows.append({
                "extract_timestamp": extract_timestamp,
                "record_id": f"{ticker}_{row_date.isoformat()}",
                "record_date": row_date.isoformat(),
                "isin": isin,
                "ticker": ticker,
                "payload": json.dumps({
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "adj_close": row["Adj Close"],
                    "volume": int(row["Volume"]),
                }),
            })

        time.sleep(1)

    logger.info("Extract complete: %d rows fetched", len(rows))
    return rows
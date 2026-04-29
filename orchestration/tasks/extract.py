import base64
import json
import os
import requests
import time

from orchestration.utils import _get_logger
from datetime import datetime, timezone
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
    params = {"limit": 50}

    while url:
        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code == 429:
            time_to_wait = int(response.headers.get("retry-after"))
            logger.warning(f"Rate limited, waiting {time_to_wait} seconds")
            time.sleep(time_to_wait)
            continue

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

        next_page = data.get("nextPagePath")
        if next_page:
            url = next_page if next_page.startswith("http") else f"{BASE_URL}{next_page}"
            params = None
        else:
            url = None

    logger.info("Extracted %d orders", len(rows))
    return rows

@task
def get_exchange_rates() -> list[dict]:
    rows = []
    extract_timestamp = datetime.now(timezone.utc).isoformat()

    logger = _get_logger()
    logger.info("Starting exchange rates extract")

    url = "https://api.frankfurter.app/latest?from=EUR&to=CZK,USD,GBP,CHF"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    rows.append({
        "extract_timestamp": extract_timestamp,
        "base": data.get("base"),
        "record_id": data.get("date"),
        "payload": json.dumps(data.get("rates")),
    })

    logger.info("Extracted %d exchange_rates", len(rows))
    return rows

@task
def get_dividends():
    rows = []
    extract_timestamp = datetime.now(timezone.utc).isoformat()
    
    logger = _get_logger()
    logger.info("Starting dividends history extract")
    
    url = f"{BASE_URL}/api/v0/equity/history/dividends"
    params = {"limit": 50}
    
    while url:
        response = requests.get(url, headers=HEADERS, timeout=30, params=params)
        
        if response.status_code == 429:
            time_to_wait = int(response.headers.get("retry-after"))
            logger.warning(f"Rate limited, waiting {time_to_wait} seconds")
            time.sleep(time_to_wait)
            continue

        response.raise_for_status()
        data = response.json()
    

        for item in data.get("items", []):
            reference_id = item.get("reference", {})
            created_at = item.get("paidOn", {})
            rows.append({
                "extract_timestamp": extract_timestamp,
                "record_id": reference_id,
                "record_created_at": created_at,
                "payload": json.dumps(item),
            })

        next_page = data.get("nextPagePath")
        if next_page:
            url = next_page if next_page.startswith("http") else f"{BASE_URL}{next_page}"
            params = None
        else:
            url = None
    
    logger.info("Extracted %d dividends", len(rows))
    return rows


@task
def get_tradable_stocks() -> list[dict]:
    rows = []
    extract_timestamp = datetime.now(timezone.utc).isoformat()
    
    logger = _get_logger()
    logger.info("Starting tradable stock extract")

    url = "https://live.trading212.com/api/v0/equity/metadata/instruments"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    data = response.json()
    for stock in data:
        rows.append({
            "extract_timestamp": extract_timestamp,
            "isin": stock.get("isin", {}),
            "created_at": stock.get("addedOn", {})
        })
    
    logger.info("Extracted %d tradable_stocks", len(rows))
    return rows
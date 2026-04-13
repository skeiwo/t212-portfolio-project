import base64
import numpy as np
import os
import pandas as pd
import requests
import time

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
def get_open_positions() -> dict:
    url = f"{BASE_URL}/api/v0/equity/positions"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
    except (requests.Timeout, requests.ConnectionError) as e:       # These exceptions are not caught by response.raise_for_status()
        raise requests.RequestException(f"Request failed while fetching positions from {url}") from e
    
    try:
        positions_data = response.json()
        output_list = []
        for idx, position in enumerate(positions_data):
            instrument = position.get("instrument")
            wallet_impact = position.get("walletImpact")
            
            if not instrument or not wallet_impact:
                raise ValueError(f"Missing required fields in position {idx}")
            
            output_list.append({
                "ticker": instrument.get("ticker"),
                "name": instrument.get("name"),
                "isin": instrument.get("isin"),
                "createdAt": position.get("createdAt"),
                "quantity": position.get("quantity"),
                "quantityAvailableForTrading": position.get("quantityAvailableForTrading"),
                "quantityInPies": position.get("quantityInPies"),
                "currentPrice": position.get("currentPrice"),
                "averagePricePaid": position.get("averagePricePaid"),
                "currency": wallet_impact.get("currency"),
                "totalCost": wallet_impact.get("totalCost"),
                "currentValue": wallet_impact.get("currentValue"),
                "unrealizedProfitLoss": wallet_impact.get("unrealizedProfitLoss"),
                "fxImpact": wallet_impact.get("fxImpact"),
            })
        
        # Create DataFrame and add timestamp
        df = pd.DataFrame(output_list)
        df["extractTimestamp"] = pd.Timestamp.now("UTC")
        df["extractTimestamp"] = df["extractTimestamp"].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") else x)
    
    except Exception as e:
        raise ValueError(f"Error at index {idx}: {e}") from e
    
    return df
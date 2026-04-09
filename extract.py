import pandas as pd
import requests
import base64
import os
from dotenv import load_dotenv

load_dotenv()


pd.set_option("display.max.rows", None, "display.max.columns", None, "display.max.colwidth", None)

# AUTHENTICATION
T212_API_KEY = os.getenv("T212_API_KEY")
T212_API_SECRET = os.getenv("T212_API_SECRET")
T212_CREDENTIALS = f"{T212_API_KEY}:{T212_API_SECRET}"
ENCODED_CREDENTIALS = base64.b64encode(T212_CREDENTIALS.encode("utf-8")).decode("utf-8")
HEADERS = {"Authorization": f"Basic {ENCODED_CREDENTIALS}"}
BASE_URL = os.getenv("T212_BASE_URL")


# GET ALL POSITIONS
output_list = []
url = f"{BASE_URL}/api/v0/equity/positions"
response = requests.get(url, headers=HEADERS)

print(f"Status Code: {response.status_code}")
if response.status_code == 200:
    print(f"Success! {response.json()}")
else:
    print(f"Error: {response.text}")

for position in response.json():
    output_list.append({
        "ticker": position.get("instrument").get("ticker"),
        "name": position.get("instrument").get("name"),
        "isin": position.get("instrument").get("isin"),

        "createdAt": position.get("createdAt"),
        "quantity": position.get("quantity"),
        "quantityAvailableForTrading": position.get("quantityAvailableForTrading"),
        "quantityInPies": position.get("quantityInPies"),
        "currentPrice": position.get("currentPrice"),
        "averagePricePaid": position.get("averagePricePaid"),

        "currency": position.get("walletImpact").get("currency"),
        "totalCost": position.get("walletImpact").get("totalCost"),
        "currentValue": position.get("walletImpact").get("currentValue"),
        "unrealizedProfitLoss": position.get("walletImpact").get("unrealizedProfitLoss"),
        "fxImpact": position.get("walletImpact").get("fxImpact"),
    })
df = pd.DataFrame(output_list)
df["extract_timestamp"] = pd.Timestamp.now('UTC')
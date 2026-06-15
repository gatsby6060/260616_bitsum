import os
import time
import requests
import uuid
import hashlib
import jwt
from urllib.parse import urlencode

def load_env_file():
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.env"))
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip()

load_env_file()

BITHUMB_ACCESS_KEY = os.getenv("BITHUMB_ACCESS_KEY", "")
BITHUMB_SECRET_KEY = os.getenv("BITHUMB_SECRET_KEY", "")

def create_bithumb_headers(params: dict = None) -> dict:
    if not BITHUMB_ACCESS_KEY or not BITHUMB_SECRET_KEY:
        return {}
    payload = {
        "access_key": BITHUMB_ACCESS_KEY,
        "nonce": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000)
    }
    if params:
        query_string = urlencode(params).encode("utf-8")
        query_hash = hashlib.sha512(query_string).hexdigest()
        payload["query_hash"] = query_hash
        payload["query_hash_alg"] = "SHA512"
    jwt_token = jwt.encode(payload, BITHUMB_SECRET_KEY, algorithm="HS256")
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

def get_current_price(ticker):
    url = f"https://api.bithumb.com/public/ticker/{ticker}_KRW"
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("status") == "0000":
            return float(data["data"]["closing_price"])
    return 0.0

def get_balance_and_evaluate():
    url = "https://api.bithumb.com/v1/accounts"
    headers = create_bithumb_headers()
    if not headers:
        print("API Key not found")
        return
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        print("Bithumb Accounts Evaluation:")
        for item in data:
            curr = item.get("currency")
            balance = float(item.get("balance", 0.0))
            locked = float(item.get("locked", 0.0))
            avg_buy_price = float(item.get("avg_buy_price", 0.0))
            
            qty = balance - locked
            if curr == "KRW":
                print(f"KRW -> Balance: {balance:,.2f}, Locked: {locked:,.2f}, Available: {qty:,.2f}")
            else:
                price = get_current_price(curr)
                eval_val = qty * price
                pnl_pct = 0.0
                if avg_buy_price > 0:
                    pnl_pct = (price - avg_buy_price) / avg_buy_price * 100
                print(f"{curr} -> Qty: {qty:.8f}, Avg Buy: {avg_buy_price:,.2f}, Current: {price:,.2f}, Value: {eval_val:,.2f} KRW, PnL: {pnl_pct:+.2f}%")
    else:
        print("Response:", resp.text)

if __name__ == "__main__":
    get_balance_and_evaluate()

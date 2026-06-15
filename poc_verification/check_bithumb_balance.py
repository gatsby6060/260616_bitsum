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

def get_orders_history():
    url = "https://api.bithumb.com/v1/orders"
    params = {
        "state": "done",
        "limit": 5
    }
    headers = create_bithumb_headers(params)
    query_string = urlencode(params)
    full_url = f"{url}?{query_string}"
    
    resp = requests.get(full_url, headers=headers)
    if resp.status_code == 200:
        import json
        print(json.dumps(resp.json(), indent=2))

if __name__ == "__main__":
    get_orders_history()

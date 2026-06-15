import time
import uuid
import hashlib
import jwt
import requests
from urllib.parse import urlencode, quote

def create_bithumb_headers(access_key: str, secret_key: str, params: dict = None) -> dict:
    payload = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000)
    }

    if params:
        # 빗썸 특화 query_hash 생성 규칙:
        # 1. 알파벳 사전순으로 파라미터 키-값 쌍 정렬
        sorted_params = sorted(params.items())
        
        # 2. 공백 문자가 +가 아닌 %20으로 표준 인코딩 되도록 quote_via=quote 사용
        # doseq=True는 배열 파라미터(예: uuids[]) 대응용
        query_string = urlencode(sorted_params, doseq=True, quote_via=quote).encode("utf-8")
        
        # 3. SHA-512 해싱
        query_hash = hashlib.sha512(query_string).hexdigest()
        payload["query_hash"] = query_hash
        payload["query_hash_alg"] = "SHA512"
        print(f"[Debug] Query String to hash: {query_string.decode('utf-8')}")
        print(f"[Debug] Calculated Hash: {query_hash}")

    # 4. HS256 알고리즘 서명 및 JWT 발급
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

def test_public_apis():
    print("=== [1] 빗썸 Public REST API 검증 ===")
    base_url = "https://api.bithumb.com"
    
    # 1. Ticker 조회
    ticker_url = f"{base_url}/v1/ticker?markets=KRW-BTC"
    print(f"Requesting ticker for KRW-BTC...")
    try:
        resp = requests.get(ticker_url)
        print(f"Response Status: {resp.status_code}")
        if resp.status_code == 200:
            ticker_data = resp.json()
            print(f"BTC Current Price: {ticker_data[0]['trade_price']:,} KRW")
            print("Ticker Public API 검증 성공!\n")
        else:
            print(f"Fail Response: {resp.text}\n")
    except Exception as e:
        print(f"Error requesting ticker: {e}\n")

    # 2. Orderbook 조회
    orderbook_url = f"{base_url}/v1/orderbook?markets=KRW-BTC"
    print(f"Requesting orderbook for KRW-BTC...")
    try:
        resp = requests.get(orderbook_url)
        print(f"Response Status: {resp.status_code}")
        if resp.status_code == 200:
            ob_data = resp.json()
            print(f"Top Ask (매도): {ob_data[0]['orderbook_units'][0]['ask_price']:,} KRW")
            print(f"Top Bid (매수): {ob_data[0]['orderbook_units'][0]['bid_price']:,} KRW")
            print("Orderbook Public API 검증 성공!\n")
        else:
            print(f"Fail Response: {resp.text}\n")
    except Exception as e:
        print(f"Error requesting orderbook: {e}\n")

def test_private_api_auth():
    print("=== [2] 빗썸 JWT 인증 및 Private API 검증 ===")
    
    # 가짜 API Key/Secret Key 설정 (실제 주문 체결 사고 방지 및 인증 반환 확인용)
    fake_access_key = "FAKE_ACCESS_KEY_FOR_TESTING_12345"
    fake_secret_key = "FAKE_SECRET_KEY_FOR_TESTING_67890"
    
    base_url = "https://api.bithumb.com"
    chance_url = f"{base_url}/v1/orders/chance"
    params = {
        "market": "KRW-BTC"
    }
    
    # CASE 1: 정상적인 query_hash와 가짜 Key 전송
    # 기대 응답: invalid_access_key (인증 해싱은 통과했으나 키가 틀림)
    print("\n--- CASE 1: 정상 query_hash + 가짜 API Key 테스트 ---")
    headers = create_bithumb_headers(fake_access_key, fake_secret_key, params)
    
    try:
        resp = requests.get(chance_url, params=params, headers=headers)
        print(f"Response Status: {resp.status_code}")
        print(f"Response Body: {resp.text}")
        
        resp_json = resp.json()
        error_name = resp_json.get("error", {}).get("name", "")
        
        if error_name == "invalid_access_key":
            print(">> [성공] query_hash 서명 규칙 검증 완료! (invalid_query_hash 에러 없이 API Key 오류 반환)")
            case1_ok = True
        else:
            print(f">> [실패] 예상치 못한 에러 반환: {error_name}")
            case1_ok = False
            
    except Exception as e:
        print(f"Error: {e}")
        case1_ok = False

    # CASE 2: 불일치하는 query_hash 전송 (query_hash를 만들 때 사용한 파라미터와 실제 GET 요청 파라미터가 다름)
    # 기대 응답: invalid_query_hash (해시 불일치 에러가 Access Key 체크 이전에 먼저 탐지되어야 함)
    print("\n--- CASE 2: 불일치 query_hash + 가짜 API Key 테스트 ---")
    mismatched_params = {
        "market": "KRW-ETH"  # 해시를 만들 때는 ETH로 만들었으나
    }
    # 실제 요청은 BTC로 전송함
    headers = create_bithumb_headers(fake_access_key, fake_secret_key, mismatched_params)
    
    try:
        resp = requests.get(chance_url, params=params, headers=headers)
        print(f"Response Status: {resp.status_code}")
        print(f"Response Body: {resp.text}")
        
        resp_json = resp.json()
        error_name = resp_json.get("error", {}).get("name", "")
        
        if error_name == "invalid_query_hash":
            print(">> [성공] query_hash 불일치 감지 검증 완료! (invalid_query_hash 정상 반환)")
            case2_ok = True
        else:
            print(f">> [실패] 해시 불일치가 탐지되지 않고 다음 단계 에러 발생: {error_name}")
            case2_ok = False
            
    except Exception as e:
        print(f"Error: {e}")
        case2_ok = False

    print("\n=== 인증 검증 종합 결과 ===")
    if case1_ok and case2_ok:
        print("결과: [합격] 빗썸 JWT query_hash 인코딩 및 해싱 메커니즘이 실제 빗썸 검증 엔진과 완벽하게 일치합니다!")
    else:
        print("결과: [불합격] 빗썸 인증 세부 사양이 일치하지 않습니다.")


if __name__ == "__main__":
    test_public_apis()
    test_private_api_auth()

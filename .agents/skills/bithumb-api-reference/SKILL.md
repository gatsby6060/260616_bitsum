---
name: bithumb-api-reference
description: 빗썸(Bithumb) 거래소의 REST API 및 WebSocket 명세를 참조하여 자동매매 시스템을 연동하고 구축하는 스킬입니다. Public API(시세, 호가, 캔들), Private API(자산 조회, 주문, 취소, TWAP), JWT 인증 토큰 생성(query_hash SHA-512 해싱 규칙), WebSocket 실시간 스트리밍(Public/Private) 구현 시 반드시 이 스킬과 하위 references 문서를 참고하십시오.
---

# Bithumb API Reference Skill

이 스킬은 빗썸 거래소의 최신 API(v2.1.5 기준) 규격을 준수하여 자동매매 봇이나 트레이딩 시스템을 설계 및 구현할 때, AI 모델과 개발자가 참조할 수 있는 완벽한 가이드북입니다. 빗썸 API의 비표준적 인증 규칙, 파라미터 제약조건, 실시간 데이터 명세를 쉽게 파악할 수 있도록 구성되었습니다.

## 핵심 연동 요약

```
[개발자/에이전트] 
   │
   ├─ JWT 인증 토큰 생성 (Secret Key HS256 서명, 파라미터 SHA-512 해싱)
   │
   ├─ REST API 요청 (Base URL: https://api.bithumb.com)
   │     ├─ Public: Ticker, Orderbook, Candles, Trades
   │     └─ Private: Accounts, Order Chance, Orders (Limit/Market/TWAP), Cancel
   │
   └─ WebSocket 연결 (Base URL: wss://ws-api.bithumb.com)
         ├─ Public (/websocket/v1): ticker, orderbook, trade
         └─ Private (/websocket/v1/private): myasset, myorder
```

## 상세 가이드 및 레퍼런스 문서 목록

AI 모델이 빗썸 연동 모듈을 구현하거나 버그를 디버깅할 때, 아래의 문서들을 순차적으로 읽어 적용하게 하십시오.

1. **[인증 가이드 (jwt_auth.md)](references/jwt_auth.md)**: 빗썸 Private API에 필수적인 JWT 토큰의 Payload 구성 방식과 파라미터 해싱(`query_hash` 생성 및 쿼리 스트링 정렬) 규칙, 그리고 Python/Node.js 완성형 코드를 제공합니다.
2. **[Public API 명세 (public_api.md)](references/public_api.md)**: 시장가 조회를 위한 Ticker, Orderbook, Candlestick(분/일/주/월), 최근 체결 내역 등의 엔드포인트와 요청 파라미터, 응답 포맷을 다룹니다.
3. **[Private API 명세 (private_api.md)](references/private_api.md)**: 계좌 잔고 조회, 주문 가능 정보 조회, 지정가/시장가 주문 요청, 일괄(다건) 주문 및 취소, TWAP(시간분할) 주문, 입출금 API 명세를 제공합니다.
4. **[WebSocket 스트리밍 명세 (websocket.md)](references/websocket.md)**: 실시간 호가 및 시세 정보 수신을 위한 웹소켓 구독 메시지 규격과 수신 데이터 구조를 다룹니다.

## 빠른 시작 예제 (Python)

빗썸 REST API 호출에 필요한 공통 Request Wrapper 클래스 구조 예시입니다.

```python
import time
import uuid
import hashlib
import jwt
import requests
from urllib.parse import urlencode

class BithumbClient:
    def __init__(self, access_key: str, secret_key: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.base_url = "https://api.bithumb.com"

    def _generate_headers(self, params: dict = None) -> dict:
        payload = {
            "access_key": self.access_key,
            "nonce": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000)
        }
        
        if params:
            # 빗썸의 해싱 규칙에 따라 쿼리 스트링으로 변환 후 SHA-512 해싱
            # 주의: 파라미터가 딕셔너리일 때 순서 정렬이 깨지지 않도록 함
            query_string = urlencode(params).encode("utf-8")
            query_hash = hashlib.sha512(query_string).hexdigest()
            payload["query_hash"] = query_hash
            payload["query_hash_alg"] = "SHA512"

        jwt_token = jwt.encode(payload, self.secret_key, algorithm="HS256")
        return {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }

    def get_accounts(self):
        url = f"{self.base_url}/v1/accounts"
        headers = self._generate_headers()
        response = requests.get(url, headers=headers)
        return response.json()

    def place_order(self, market: str, side: str, volume: str, price: str, ord_type: str):
        url = f"{self.base_url}/v2/orders"
        # POST 요청의 body 파라미터도 JWT query_hash 생성 대상임
        params = {
            "market": market,
            "side": side,          # 'bid' (매수) or 'ask' (매도)
            "volume": volume,
            "price": price,
            "ord_type": ord_type   # 'limit' (지정가) or 'price'/'market' (시장가)
        }
        headers = self._generate_headers(params)
        response = requests.post(url, json=params, headers=headers)
        return response.json()
```

## 주요 개발 시 주의사항

1. **API 호출 수 제한**: 빗썸 API는 안정적인 운영을 위해 계정당 호출 속도를 제한하고 있습니다. (Public 초당 10~20회, Private 초당 10회 수준, 상세 내용은 `references/private_api.md` 및 공식 가이드 참고)
2. **소수점 제한**: 주문 요청 시 `volume` 및 `price`는 문자열(`str`)로 전달하는 것이 정밀도 손실을 예방하는 데 유리합니다. 마켓별 소수점 자릿수 제한 규칙이 적용됩니다.
3. **네트워크 에러 대응**: WebSocket 연결 시 빗썸 서버의 유휴 상태 해제(Ping-Pong) 프로토콜을 반드시 주기적으로 전송해야 끊김 현상을 예방할 수 있습니다.

## API 최신 동기화 가이드

빗썸 거래소의 API 변경 사항(엔드포인트 추가, 파라미터 규격 수정 등)을 이 스킬의 레퍼런스 가이드에 실시간으로 다시 반영하여 빌드할 수 있는 스크립트를 제공합니다.

* **동기화 스크립트 경로**: [parse_bithumb_apis.py](scripts/parse_bithumb_apis.py)
* **작동 메커니즘**:
  1. `https://apidocs.bithumb.com/llms.txt` 인덱스로부터 최신 레퍼런스 페이지 목록을 갱신 수집합니다.
  2. 각 API 명세 문서 본문에 포함된 OpenAPI 3.1 JSON 규격을 다운로드 및 파싱합니다.
  3. 추출한 최신 정보로 `references/public_api.md` 및 `references/private_api.md` 문서를 다시 자동 빌드합니다.
* **실행 명령**:
  ```bash
  python .agents/skills/bithumb-api-reference/scripts/parse_bithumb_apis.py
  ```


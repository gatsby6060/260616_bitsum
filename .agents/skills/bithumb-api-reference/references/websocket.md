# 빗썸 WebSocket API 레퍼런스

빗썸 WebSocket API는 실시간 시세 변동, 거래 체결 내역, 호가 잔량 정보 및 내 자산/주문 상태의 변경 사항을 실시간 스트리밍으로 수신하기 위해 사용됩니다.
REST API 대비 네트워크 오버헤드가 적고 빠른 반응 속도를 보장하므로, **초단타 트레이딩이나 실시간 UI 모니터링 시 필수적으로 사용**해야 합니다.

---

## 1. WebSocket Base URL

빗썸은 데이터 성격에 따라 Public(인증 불필요)과 Private(인증 필요) 두 개의 웹소켓 엔드포인트를 나누어 제공합니다.

| 구분 | WebSocket URL | 인증 필요 여부 | 제공 데이터 유형 (Channels) |
| :--- | :--- | :--- | :--- |
| **Public** | `wss://ws-api.bithumb.com/websocket/v1` | ❌ 없음 | `ticker`, `orderbook`, `trade` |
| **Private** | `wss://ws-api.bithumb.com/websocket/v1/private` | ✅ JWT 인증 필요 | `myasset` (내 자산), `myorder` (내 주문/체결) |

---

## 2. Public WebSocket 가이드

Public 웹소켓은 별도의 토큰 없이 연결 수립 즉시 구독하고자 하는 메시지 포맷을 전송하여 실시간 데이터를 수신할 수 있습니다.

### A. 구독 요청 포맷 (Subscribe Message)
웹소켓 연결이 열린 후(`on_open`), 서버에 아래 형식의 JSON 배열을 전송합니다.

```json
[
  {
    "ticket": "test-uuid-1234"
  },
  {
    "type": "ticker",
    "codes": ["KRW-BTC", "KRW-ETH"],
    "isOnlySnapshot": false,
    "isOnlyRealtime": false
  },
  {
    "type": "orderbook",
    "codes": ["KRW-BTC"]
  },
  {
    "type": "trade",
    "codes": ["KRW-BTC"]
  }
]
```

* `ticket`: 클라이언트가 수신 데이터를 식별하기 위해 지정하는 고유 식별자(UUID 등 권장). 수신 데이터의 `ticket` 필드에 고대로 반환됩니다.
* `type`: 데이터 종류 (`"ticker"` / `"orderbook"` / `"trade"`)
* `codes`: 구독할 마켓 코드 리스트
* `isOnlySnapshot`: 스냅샷(현재 누적 상태) 데이터만 받고 연결을 끊을지 여부
* `isOnlyRealtime`: 실시간 업데이트만 수신할지 여부

### B. 수신 데이터 포맷

#### 1) 체결 (trade)
```json
{
  "type": "trade",
  "code": "KRW-BTC",
  "trade_price": 85500000.0,
  "trade_volume": 0.012,
  "ask_bid": "BID",
  "prev_closing_price": 85000000.0,
  "change_price": 500000.0,
  "trade_date": "2026-06-08",
  "trade_time": "11:05:20",
  "trade_timestamp": 1780916720000,
  "timestamp": 1780916720123,
  "sequential_id": 1780916720000001,
  "stream_type": "REALTIME",
  "ticket": "test-uuid-1234"
}
```

#### 2) 호가 (orderbook)
```json
{
  "type": "orderbook",
  "code": "KRW-BTC",
  "timestamp": 1780916720500,
  "total_ask_size": 12.345,
  "total_bid_size": 20.123,
  "orderbook_units": [
    {
      "ask_price": 85510000.0,
      "bid_price": 85500000.0,
      "ask_size": 1.05,
      "bid_size": 4.12
    }
  ],
  "ticket": "test-uuid-1234"
}
```

---

## 3. Private WebSocket 가이드

Private 웹소켓은 연결 즉시 **JWT 토큰 인증 절차**를 통과해야 데이터를 구독할 수 있습니다.

### A. 인증 및 구독 흐름

1. `wss://ws-api.bithumb.com/websocket/v1/private` 엔드포인트로 웹소켓을 연결합니다.
2. 연결 성공 직후, 가장 먼저 **인증 프레임(Authorization Frame)**을 서버로 보냅니다. (인증 토큰은 `jwt_auth.md` 가이드에 맞춰 파라미터 없이 생성한 JWT 토큰을 그대로 사용합니다.)
3. 인증이 성공적으로 완료되면 서버로부터 `{"type": "authorization", "status": "success"}` 응답을 받습니다.
4. 이후 원하는 개인 채널(`myasset` 또는 `myorder`)을 구독 요청합니다.

#### 단계 2: 인증 프레임 전송 예시
```json
{
  "type": "authorization",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

#### 단계 4: 개인 정보 구독 요청 예시
```json
[
  {
    "ticket": "private-uuid-5678"
  },
  {
    "type": "myasset"
  },
  {
    "type": "myorder"
  }
]
```

### B. 수신 데이터 포맷

#### 1) 내 자산 정보 (myasset)
내 지갑의 잔고가 변동될 때마다(체결, 입출금 등으로 인해) 실시간으로 통지됩니다.
```json
{
  "type": "myasset",
  "currency": "BTC",
  "balance": "0.12500000",
  "locked": "0.01000000",
  "timestamp": 1780916725000,
  "ticket": "private-uuid-5678"
}
```

#### 2) 내 주문 및 체결 정보 (myorder)
내 주문의 상태 변동(접수, 체결 중, 완료, 취소 등) 정보를 수신합니다.
```json
{
  "type": "myorder",
  "code": "KRW-BTC",
  "uuid": "cdd9fd27-e9a3-455b-bb4f-801211111111",
  "side": "bid",
  "ord_type": "limit",
  "price": "85000000.0",
  "state": "trade",
  "volume": "0.01",
  "remaining_volume": "0.005",
  "executed_volume": "0.005",
  "trades": [
    {
      "trade_price": "85000000.0",
      "trade_volume": "0.005",
      "trade_uuid": "e12a45bc-6789-0123-4567-890123456789",
      "created_at": "2026-06-08T11:05:25+09:00"
    }
  ],
  "timestamp": 1780916725456,
  "ticket": "private-uuid-5678"
}
```

---

## 4. 커넥션 생명 주기 및 연결 유지 (Ping-Pong) (매우 중요)

빗썸 웹소켓 연결이 중도에 아무 소식 없이 해제되거나 좀비 상태(Zombie Connection)가 되는 것을 막으려면 **핑퐁 핸드셰이크(Ping-Pong Handshake)** 처리가 필수적입니다.

* **서버발 Ping**: 빗썸 웹소켓 서버는 연결 유지를 위해 클라이언트에 주기적으로(대략 120초 간격) 텍스트 프레임 `"PING"`을 전송합니다.
* **클라이언트의 대응**: 클라이언트는 `"PING"` 프레임을 수신할 때마다, 즉시 서버에 `"PONG"` 문자열 프레임 또는 빈 Pong 프레임을 돌려보내야 합니다. 응답하지 않을 경우 서버에 의해 연결이 강제 종료됩니다.
* **클라이언트발 Ping (옵션)**: 연결 신뢰성을 더욱 높이기 위해 클라이언트 측에서도 주기적으로 핑을 날리고 일정 시간 동안 응답이 없으면 재연결(`reconnect`) 프로세스를 트리거하도록 구현하는 것을 권장합니다.

### Python 구현 가이드 예시 (websocket-client 기반)

```python
import websocket
import json
import threading
import time

def on_message(ws, message):
    # 서버로부터 PING이 오는 경우 즉시 PONG으로 응답
    if message == "PING":
        ws.send("PONG")
        return
        
    data = json.loads(message)
    print("Received:", data)

def on_open(ws):
    # 구독 메시지 전송
    sub_msg = [
        {"ticket": "test-uuid"},
        {"type": "trade", "codes": ["KRW-BTC"]}
    ]
    ws.send(json.dumps(sub_msg))

# 연결 실행
ws = websocket.WebSocketApp(
    "wss://ws-api.bithumb.com/websocket/v1",
    on_message=on_message,
    on_open=on_open
)
ws.run_forever()
```

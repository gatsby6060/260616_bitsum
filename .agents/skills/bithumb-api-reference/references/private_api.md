# 빗썸 Private API 레퍼런스

Private API 호출 시에는 헤더에 JWT 인증 토큰과 파라미터 해시(`query_hash`)를 필수로 실어 보내야 합니다.

* **공통 Base URL**: `https://api.bithumb.com`

---

## API 목록 목차

* [TWAP - 주문 요청](#twap---주문-요청) (`POST /twap`)
* [TWAP - 주문 취소](#twap---주문-취소) (`DELETE /twap`)
* [TWAP - 주문 내역 조회](#twap---주문-내역-조회) (`GET /twap`)
* [API 키 리스트 조회](#api-키-리스트-조회) (`GET /api_keys`)
* [입출금 현황](#입출금-현황) (`GET /status/wallet`)
* [개별 입금 조회](#개별-입금-조회) (`GET /deposit`)
* [개별 입금 주소 조회](#개별-입금-주소-조회) (`GET /deposits/coin_address`)
* [원화 입금 리스트 조회](#원화-입금-리스트-조회) (`GET /deposits/krw`)
* [원화 입금](#원화-입금) (`POST /deposits/krw`)
* [코인 입금 리스트 조회](#코인-입금-리스트-조회) (`GET /deposits`)
* [입금 주소 생성 요청](#입금-주소-생성-요청) (`POST /deposits/generate_coin_address`)
* [전체 입금 주소 조회](#전체-입금-주소-조회) (`GET /deposits/coin_addresses`)
* [전체 자산 조회](#전체-자산-조회) (`GET /accounts`)
* [개별 주문 조회](#개별-주문-조회) (`GET /order`)
* [다건 주문 요청](#다건-주문-요청) (`POST /orders/batch`)
* [다건 주문 취소 접수](#다건-주문-취소-접수) (`POST /orders/cancel`)
* [주문 가능 정보 조회](#주문-가능-정보-조회) (`GET /orders/chance`)
* [주문 리스트 조회](#주문-리스트-조회) (`GET /orders`)
* [주문 요청](#주문-요청) (`POST /orders`)
* [주문 취소 접수](#주문-취소-접수) (`DELETE /order`)
* [가상 자산 출금 요청](#가상-자산-출금-요청) (`POST /withdraws/coin`)
* [가상 자산 출금 취소](#가상-자산-출금-취소) (`DELETE /withdraws/coin`)
* [개별 출금 조회](#개별-출금-조회) (`GET /withdraw`)
* [원화 출금 리스트 조회](#원화-출금-리스트-조회) (`GET /withdraws/krw`)
* [원화 출금 요청](#원화-출금-요청) (`POST /withdraws/krw`)
* [출금 가능 정보](#출금-가능-정보) (`GET /withdraws/chance`)
* [코인 출금 리스트 조회](#코인-출금-리스트-조회) (`GET /withdraws`)
* [출금 허용 주소 리스트 조회](#출금-허용-주소-리스트-조회) (`GET /withdraws/coin_addresses`)

---

## TWAP - 주문 요청

TWAP 주문을 요청합니다.

* **HTTP Request**: `POST https://api.bithumb.com/v1/twap`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `body (json)` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `side` | `body (json)` | `string` | ✅ 필수 | 주문 종류 |
| `volume` | `body (json)` | `string` | 선택 | 주문 수량 (매도시 필수) |
| `price` | `body (json)` | `string` | 선택 | 주문 가격(매수시 필수) |
| `duration` | `body (json)` | `string` | ✅ 필수 | 주문 시간(twap 주문이 진행되는 시간) - 초(min 300, max 43200) |
| `frequency` | `body (json)` | `string` | ✅ 필수 | 주문 간격 - 초 |

### 응답 예시 (200 OK)

```json
{
  "algo_order_id": "TWAP-A01B02C03D04E05F06"
}
```

---

## TWAP - 주문 취소

TWAP 주문 취소를 요청합니다.

* **HTTP Request**: `DELETE https://api.bithumb.com/v1/twap`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `algo_order_id` | `query` | `string` | ✅ 필수 | 취소할 TWAP 주문 ID |

### 응답 예시 (200 OK)

```json
{
  "algo_order_id": "TWAP-A01B02C03D04E05F06"
}
```

---

## TWAP - 주문 내역 조회

TWAP 주문 목록을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/twap`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `query` | `string` | 선택 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `uuids` | `query` | `array` | 선택 | TWAP 주문 ID 목록 |
| `state` | `query` | `string` | 선택 | 주문 상태 - `progress`: 진행중 (default) - `done`: 주문 완료 - `cancel`: 취소 |
| `next_key` | `query` | `string` | 선택 | 다음 페이지 조회를 위한 커서 값 |
| `limit` | `query` | `integer` | 선택 | 개수 제한(max 100) |
| `order_by` | `query` | `string` | 선택 | 조회 결과 정렬 방식 - `asc`: 오래된 주문 순 - `desc`: 최신 주문 순(default) |

### 응답 예시 (200 OK)

```json
{
  "has_next": true,
  "next_key": "NDMyMjM2fEdCRVQtS1JXfDYyNDc3YjYxLWEwZjItNDY1OC04ZGVhLTFkMjQyYjIxZGFmZQ==",
  "orders": [
    {
      "uuid": "TWAP-001-PROGRESS-BID",
      "side": "bid",
      "price": "92500000",
      "state": "progress",
      "market": "KRW-BTC",
      "created_at": "2025-12-04T10:00:00+09:00",
      "volume": "1.0",
      "total_order_count": 60,
      "total_trades_count": 10,
      "progress_count": 25,
      "total_executed_amount": "2312500000",
      "total_executed_volume": "0.25",
      "avg_trade_price": "92500000.000",
      "wallet_id": "0000000000-00-0000"
    },
    {
      "uuid": "TWAP-002-CANCEL-ASK",
      "side": "ask",
      "price": "5000",
      "state": "cancel",
      "market": "KRW-XRP",
      "created_at": "2025-12-03T09:00:00+09:00",
      "volume": "1000",
      "total_order_count": 120,
      "total_trades_count": 5,
      "progress_count": 15,
      "total_executed_amount": "25000000",
      "total_executed_volume": "5000",
      "avg_trade_price": "5000.0",
      "canceled_at": "2025-12-03T09:15:00+09:00",
      "cancel_type": "user"
    }
  ]
}
```

---

## API 키 리스트 조회

API 키 리스트와 만료 일자를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/api_keys`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

보내는 파라미터가 없습니다.

### 응답 예시 (200 OK)

```json
[
  {
    "access_key": "59683c90185742d69fd8fa1bc0cf27785c392afaa56ece",
    "expire_at": "2025-06-11T09:00:00+09:00"
  },
  {
    "access_key": "3e97926e9b75a6aeb637d2c172a292588502daccfb5cab",
    "expire_at": "2025-06-12T09:00:00+09:00"
  },
  {
    "access_key": "400e5bcb69440e7ace08fd7991340c271683f20dba9a6e",
    "expire_at": "2025-06-12T09:00:00+09:00"
  }
]
```

---

## 입출금 현황

입출금 현황과 블록 상태를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/status/wallet`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

보내는 파라미터가 없습니다.

### 응답 예시 (200 OK)

```json
[
  {
    "currency": "BTC",
    "wallet_state": "working",
    "block_state": "normal",
    "block_height": 852086,
    "block_updated_at": "2024-07-14T13:43:57+09:00",
    "block_elapsed_minutes": 2,
    "net_type": "BTC",
    "network_name": "Bitcoin"
  },
  {
    "currency": "ETH",
    "wallet_state": "working",
    "block_state": "normal",
    "block_height": 20302440,
    "block_updated_at": "2024-07-14T13:45:27+09:00",
    "block_elapsed_minutes": 0,
    "net_type": "ETH",
    "network_name": "Ethereum"
  }
]
```

---

## 개별 입금 조회

개별 입금 내역을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/deposit`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `query` | `string` | ✅ 필수 | 화폐 심볼 |
| `uuid` | `query` | `string` | 선택 | 입금의 고유 ID |
| `txid` | `query` | `string` | 선택 | 입금 TXID |

### 응답 예시 (200 OK)

```json
{
  "type": "deposit",
  "uuid": "200012694",
  "currency": "BTC",
  "net_type": "BTC",
  "txid": "20200313145947.40057",
  "state": "DEPOSIT_ACCEPTED",
  "created_at": "2020-03-13T15:00:35+09:00",
  "done_at": "2020-03-13T15:00:35+09:00",
  "amount": "100",
  "fee": "0",
  "transaction_type": null
}
```

---

## 개별 입금 주소 조회

가상자산별 입금 주소를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/deposits/coin_address`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `query` | `string` | ✅ 필수 | 화폐 심볼  예시) BTC |
| `net_type` | `query` | `string` | ✅ 필수 | 입금 네트워크(ex.BTC, DASH) |

### 응답 예시 (200 OK)

```json
{
  "currency": "XRP",
  "net_type": "XRP",
  "deposit_address": "rs6qtDrs8qp5qLkUxuzQdxFh5eGrQbL3Ee",
  "secondary_address": "1004775304"
}
```

---

## 원화 입금 리스트 조회

원화 입금 목록을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/deposits/krw`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `state` | `query` | `string` | 선택 | 입금 상태 - `PROCESSING` : 처리 중 - `ACCEPTED` : 완료 - `CANCELLED` : 취소됨 |
| `uuids` | `query` | `array` | 선택 | 입금 고유 ID 목록 |
| `txids` | `query` | `array` | 선택 | 입금 TXID의 목록 |
| `page` | `query` | `integer` | 선택 | 페이지 수 (default: 1) |
| `limit` | `query` | `integer` | 선택 | 개수 제한(max 100) |
| `order_by` | `query` | `string` | 선택 | 조회 결과 정렬 방식 - `asc`: 오래된 주문 순 - `desc`: 최신 주문 순(default) |

### 응답 예시 (200 OK)

```json
"\n  {\n    \"type\": \"deposit\",\n    \"uuid\": \"15371593\",\n    \"currency\": \"KRW\",\n    \"txid\": \"1596126\",\n    \"state\": \"ACCEPTED\",\n    \"created_at\": \"2024-07-06T15:14:37+09:00\",\n    \"done_at\": \"2024-07-08T15:19:40+09:00\",\n    \"amount\": \"20000\",\n    \"fee\": \"0\",\n    \"transaction_type\": \"default\"\n  },\n  {\n    \"type\": \"deposit\",\n    \"uuid\": \"15371592\",\n    \"currency\": \"KRW\",\n    \"txid\": \"1596125\",\n    \"state\": \"ACCEPTED\",\n    \"created_at\": \"2024-07-06T15:14:06+09:00\",\n    \"done_at\": \"2024-07-08T15:19:39+09:00\",\n    \"amount\": \"10000\",\n    \"fee\": \"0\",\n    \"transaction_type\": \"default\"\n  }\n]"
```

---

## 원화 입금

원화 입금을 요청합니다.

* **HTTP Request**: `POST https://api.bithumb.com/v1/deposits/krw`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `amount` | `body (json)` | `string` | ✅ 필수 | 입금액 |
| `two_factor_type` | `body (json)` | `string` | ✅ 필수 | 2차 인증 수단 - `kakao`: 카카오 인증 |

### 응답 예시 (200 OK)

성공 시 빈 객체 또는 표준 성공 메시지가 반환됩니다.

---

## 코인 입금 리스트 조회

가상자산 입금 목록을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/deposits`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `query` | `string` | 선택 | 화폐 심볼  예시) BTC |
| `state` | `query` | `string` | 선택 | 입금 상태  [입금신청] - 입금대기: `REQUESTED_PENDING` - 반환신청대기: `REQUESTED_SYSTEM_REJECTED` - 입금신청대기: `REQUESTED_PROCESSING` - 입금심사중: `REQUESTED_PROCESSING` - 입금심사반려: `REQUESTED_ADMIN_REJECTED`  [입금] - 입금대기: `DEPOSIT_PROCESSING` - 입금완료: `DEPOSIT_ACCEPTED` - 입금취소: `DEPOSIT_CANCELLED`  [반환신청] - 반환심사대기: `REFUNDING_PENDING` - 반환취소: `REFUNDING_SYSTEM_REJECTED` - 반환심사중: `REFUNDING_PROCESSING` - 반환취소: `REFUNDING_ADMIN_REJECTED` - 반환완료: `REFUNDING_ACCEPTED`  [반환 신청 건 출금] - 반환승인: `REFUNDED_PROCESSING` - 반환완료: `REFUNDED_ACCEPTED` - 반환취소: `REFUNDED_CANCELLED` |
| `uuids` | `query` | `array` | 선택 | 입금 고유 ID 목록 |
| `txids` | `query` | `array` | 선택 | 입금 TXID의 목록 |
| `page` | `query` | `integer` | 선택 | 페이지 수 (default: 1) |
| `limit` | `query` | `integer` | 선택 | 개수 제한(max 100) |
| `order_by` | `query` | `string` | 선택 | 조회 결과 정렬 방식 - `asc`: 오래된 주문 순 - `desc`: 최신 주문 순(default) |

### 응답 예시 (200 OK)

```json
[
  {
    "type": "deposit",
    "uuid": "202620152",
    "currency": "SHIB",
    "net_type": null,
    "txid": "20240709163030.72335",
    "state": "DEPOSIT_ACCEPTED",
    "created_at": "2024-07-09T16:31:08+09:00",
    "done_at": "2024-07-09T16:31:08+09:00",
    "amount": "100000",
    "fee": "0",
    "transaction_type": null
  },
  {
    "type": "deposit",
    "uuid": "202611907",
    "currency": "SANTOS",
    "net_type": null,
    "txid": "20240701005602.90593",
    "state": "DEPOSIT_ACCEPTED",
    "created_at": "2024-07-01T00:56:23+09:00",
    "done_at": "2024-07-01T00:56:23+09:00",
    "amount": "1000000",
    "fee": "0",
    "transaction_type": null
  }
]
```

---

## 입금 주소 생성 요청

입금 주소 생성을 요청합니다.

* **HTTP Request**: `POST https://api.bithumb.com/v1/deposits/generate_coin_address`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `body (json)` | `string` | ✅ 필수 | 화폐 심볼  예시) BTC |
| `net_type` | `body (json)` | `string` | ✅ 필수 | 입금 네트워크(ex.BTC, DASH) |

### 응답 예시 (200 OK)

성공 시 빈 객체 또는 표준 성공 메시지가 반환됩니다.

---

## 전체 입금 주소 조회

전체 입금 주소를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/deposits/coin_addresses`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

보내는 파라미터가 없습니다.

### 응답 예시 (200 OK)

```json
[
  {
    "currency": "BTC",
    "net_type": "BTC",
    "deposit_address": "13qdmpBMQVnwF8UGCARw7mfrP6rernoGUW",
    "secondary_address": null
  },
  {
    "currency": "ETH",
    "net_type": "ETH",
    "deposit_address": "0x059be8ad9620c1aefd50c30545cb60ab984a3676",
    "secondary_address": null
  },
  {
    "currency": "XRP",
    "net_type": "XRP",
    "deposit_address": "rs6qtDrs8qp5qLkUxuzQdxFh5eGrQbL3Ee",
    "secondary_address": "1004775304"
  }
]
```

---

## 전체 자산 조회

보유 중인 자산 정보를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/accounts`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

보내는 파라미터가 없습니다.

### 응답 예시 (200 OK)

```json
[
  {
    "currency": "KRW",
    "balance": "10981650.10635",
    "locked": "98246",
    "avg_buy_price": "0",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  },
  {
    "currency": "BTC",
    "balance": "124.45272908",
    "locked": "0",
    "avg_buy_price": "36340973",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  },
  {
    "currency": "ETH",
    "balance": "106993.5839313",
    "locked": "0",
    "avg_buy_price": "23993",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  }
]
```

---

## 개별 주문 조회

개별 주문 내역을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/order`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `uuid` | `query` | `string` | 선택 | 주문의 고유 ID |
| `client_order_id` | `query` | `string` | 선택 | 서버에서 부여하는 주문 ID(`uuid`)와 별도로 주문 생성 시 사용자가 직접 지정한 고유 ID  - 허용 문자: 영문 대/소문자, 숫자, -, _ - 길이: 1–36자 |

### 응답 예시 (200 OK)

```json
{
  "uuid": "C0101000000001799231",
  "side": "bid",
  "ord_type": "limit",
  "price": "83000000",
  "state": "done",
  "market": "KRW-BTC",
  "created_at": "2024-07-09T16:32:23+09:00",
  "volume": "1",
  "remaining_volume": "0",
  "reserved_fee": "207500",
  "remaining_fee": "0",
  "paid_fee": "207500",
  "locked": "0",
  "executed_volume": "1",
  "executed_funds": "83000000",
  "trades_count": 1,
  "stp_type": "cancel_taker",
  "trades": [
    {
      "market": "KRW-BTC",
      "uuid": "C0101000000001713006",
      "price": "83000000",
      "volume": "1",
      "funds": "83000000",
      "side": "bid",
      "created_at": "2024-07-09T16:32:23+09:00"
    }
  ]
}
```

---

## 다건 주문 요청

여러 건의 주문을 한 번의 요청으로 생성합니다. 요청당 최대 20건까지 가능합니다.

* **HTTP Request**: `POST https://api.bithumb.com/v2/orders/batch`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `batch_orders` | `body (json)` | `array` | ✅ 필수 | 다건 주문 목록(min 1, max 20) |

### 응답 예시 (200 OK)

```json
{
  "batch_orders_response": [
    {
      "client_order_id": "20260106-test00",
      "order_id": "C0101000007410713262",
      "market": "KRW-BTC",
      "side": "bid",
      "order_type": "limit",
      "created_at": "2026-01-06T12:08:11+09:00",
      "stp_type": "cancel_taker"
    },
    {
      "client_order_id": "20260106-test01",
      "order_id": "C0101000007410713263",
      "market": "KRW-BTC",
      "side": "bid",
      "order_type": "limit",
      "created_at": "2026-01-06T12:08:11+09:00",
      "stp_type": "cancel_taker"
    },
    {
      "client_order_id": "20260106-test6",
      "name": "cross_trading",
      "message": "제출하신 주문은 귀하가 기존에 제출하신 주문과 체결될 수 있어 취소되었습니다. 제출된 주문ID: [C0101000007410713241, C0101000007410713242, C0101000007410713243]"
    }
  ]
}
```

---

## 다건 주문 취소 접수

여러 개의 주문을 일괄 취소 요청합니다. 요청당 최대 30건까지 처리할 수 있습니다.

* **HTTP Request**: `POST https://api.bithumb.com/v2/orders/cancel`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `order_ids` | `body (json)` | `array` | 선택 | 취소할 주문의 고유 ID(구 `uuid`) 목록(max 30) |
| `client_order_ids` | `body (json)` | `array` | 선택 | 서버에서 부여하는 주문 ID(구 `uuid`)와 별도로 주문 생성 시 사용자가 직접 지정한 고유 ID 목록(max 30)  - 허용 문자: 영문 대/소문자, 숫자, -, _ - 길이: 1–36자 |

### 응답 예시 (200 OK)

```json
{
  "success": [
    {
      "order_id": "C0101000007410713533",
      "client_order_id": "20260210-test1",
      "created_at": "2026-02-10T13:56:38+09:00"
    }
  ],
  "fail": [
    {
      "order_id": "C0101000007410713315",
      "error": {
        "name": "order_not_found",
        "message": "주문을 찾지 못했습니다."
      }
    },
    {
      "order_id": "C0101000007410713314",
      "error": {
        "name": "order_not_found",
        "message": "주문을 찾지 못했습니다."
      }
    },
    {
      "order_id": "C0101000007410713317",
      "error": {
        "name": "order_not_found",
        "message": "주문을 찾지 못했습니다."
      }
    }
  ]
}
```

---

## 주문 가능 정보 조회

거래 대상 페어별 주문 가능 정보를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/orders/chance`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `query` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |

### 응답 예시 (200 OK)

```json
{
  "bid_fee": "0.0025",
  "ask_fee": "0.0025",
  "maker_bid_fee": "0.0025",
  "maker_ask_fee": "0.0025",
  "market": {
    "id": "BTC-ETH",
    "name": "ETH/BTC",
    "order_types": [
      "limit"
    ],
    "order_sides": [
      "ask",
      "bid"
    ],
    "bid_types": [
      "limit",
      "price"
    ],
    "ask_types": [
      "limit",
      "market"
    ],
    "bid": {
      "currency": "BTC",
      "price_unit": "0.00000001",
      "min_total": "0.0005"
    },
    "ask": {
      "currency": "ETH",
      "price_unit": "0.00000001",
      "min_total": "0.0005"
    },
    "max_total": "5",
    "state": "active"
  },
  "bid_account": {
    "currency": "BTC",
    "balance": "124.45372908",
    "locked": "0.0001",
    "avg_buy_price": "36341394",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  },
  "ask_account": {
    "currency": "ETH",
    "balance": "106993.5839313",
    "locked": "0",
    "avg_buy_price": "23993",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  }
}
```

---

## 주문 리스트 조회

주문 목록을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/orders`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `query` | `string` | 선택 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `state` | `query` | `string` | 선택 | 주문 상태 - `wait`: 체결 대기(default)  - `watch`: 주문 대기 - `done`: 체결 완료 - `cancel`: 주문 취소 |
| `states` | `query` | `array` | 선택 | 주문 상태 목록 - 일반주문(`wait`, `done`, `cancel`)과 자동주문(`watch`)은 혼합하여 조회하실 수 없습니다. |
| `uuids` | `query` | `array` | 선택 | 주문 고유 ID 목록(max 100) |
| `client_order_ids` | `query` | `array` | 선택 | 서버에서 부여하는 주문 ID(`uuid`)와 별도로 주문 생성 시 사용자가 직접 지정한 고유 ID 목록(max 100) |
| `page` | `query` | `integer` | 선택 | 페이지네이션 응답에서 조회할 페이지 번호 |
| `limit` | `query` | `integer` | 선택 | 조회할 주문 개수 |
| `order_by` | `query` | `string` | 선택 | 조회 결과 정렬 방식 - `asc`: 오래된 주문 순 - `desc`: 최신 주문 순(default) |

### 응답 예시 (200 OK)

```json
[
  {
    "uuid": "C0101000000001799625",
    "side": "ask",
    "ord_type": "limit",
    "price": "84001000",
    "state": "wait",
    "market": "KRW-BTC",
    "created_at": "2024-07-12T16:30:01+09:00",
    "volume": "0.2",
    "remaining_volume": "0.2",
    "reserved_fee": "0",
    "remaining_fee": "0",
    "paid_fee": "0",
    "locked": "0.2",
    "executed_volume": "0",
    "executed_funds": "0",
    "trades_count": 0,
    "stp_type": "cancel_taker"
  },
  {
    "uuid": "C0661000000000760010",
    "side": "ask",
    "ord_type": "limit",
    "price": "1055",
    "state": "wait",
    "market": "KRW-GMT",
    "created_at": "2024-07-10T20:00:02+09:00",
    "volume": "16",
    "remaining_volume": "11",
    "reserved_fee": "0",
    "remaining_fee": "0",
    "paid_fee": "0.52",
    "locked": "11",
    "executed_volume": "5",
    "executed_funds": "5275",
    "trades_count": 1,
    "stp_type": "cancel_taker"
  }
]
```

---

## 주문 요청

주문을 요청합니다.

* **HTTP Request**: `POST https://api.bithumb.com/v2/orders`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `body (json)` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `side` | `body (json)` | `string` | ✅ 필수 | 주문 종류 - `bid`: 매수 - `ask`: 매도 |
| `price` | `body (json)` | `string` | 선택 | 주문 가격(지정가 주문, 시장가 매수 주문 시 필수) - 지정가 주문: 주문 단가 - 시장가 매수 주문: 주문 총액 |
| `volume` | `body (json)` | `string` | 선택 | 주문 수량(지정가 주문, 시장가 매도 주문 시 필수) |
| `order_type` | `body (json)` | `string` | ✅ 필수 | 주문 방식(구 `ord_type`) - `limit`: 지정가 - `price`: 시장가(매수) - `market`: 시장가(매도) |
| `client_order_id` | `body (json)` | `string` | 선택 | 서버에서 부여하는 주문 ID(`order_id`)와 별도로 주문 생성 시 사용자가 직접 지정하는 고유 ID  - 허용 문자: 영문 대/소문자, 숫자, -, _ - 길이: 1–36자 |

### 응답 예시 (200 OK)

성공 시 빈 객체 또는 표준 성공 메시지가 반환됩니다.

---

## 주문 취소 접수

주문 취소를 요청합니다.

* **HTTP Request**: `DELETE https://api.bithumb.com/v2/order`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `order_id` | `query` | `string` | 선택 | 주문의 고유 ID(구 `uuid`) |
| `client_order_id` | `query` | `string` | 선택 | 서버에서 부여하는 주문 ID(`order_id`)와 별도로 주문 생성 시 사용자가 직접 지정한 고유 ID  - 허용 문자: 영문 대/소문자, 숫자, -, _ - 길이: 1–36자 |

### 응답 예시 (200 OK)

```json
{
  "order_id": "C0101000000001799625",
  "created_at": "2024-07-12T16:30:01+09:00"
}
```

---

## 가상 자산 출금 요청

가상 자산 출금을 요청합니다.

* **HTTP Request**: `POST https://api.bithumb.com/v1/withdraws/coin`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `body (json)` | `string` | ✅ 필수 | 화폐 심볼  예시) BTC |
| `net_type` | `body (json)` | `string` | ✅ 필수 | 출금 네트워크  예시) BTC, DASH |
| `amount` | `body (json)` | `string` | ✅ 필수 | 출금 수량 |
| `address` | `body (json)` | `string` | ✅ 필수 | 출금 가능 주소에 등록된 출금 주소 |
| `secondary_address` | `body (json)` | `string` | 선택 | 2차 출금 주소(필요한 디지털 자산에 한해서) |
| `exchange_name` | `body (json)` | `string` | 선택 | 출금 거래소명(영문) |
| `receiver_type` | `body (json)` | `string` | 선택 | 수취인 유형 - `personal`: 개인 - `corporation`: 법인 |
| `receiver_ko_name` | `body (json)` | `string` | 선택 | 수취인 국문명(개인 : 개인 국문명, 법인 : 법인 대표자 국문명) |
| `receiver_en_name` | `body (json)` | `string` | 선택 | 수취인 영문명(개인 : 개인 영문명, 법인 : 법인 대표자 영문명) |
| `receiver_corp_ko_name` | `body (json)` | `string` | 선택 | 법인 국문명(수취인 법인인 경우 필수) |
| `receiver_corp_en_name` | `body (json)` | `string` | 선택 | 법인 영문명(수취인 법인인 경우 필수) |

### 응답 예시 (200 OK)

성공 시 빈 객체 또는 표준 성공 메시지가 반환됩니다.

---

## 가상 자산 출금 취소

가상 자산 출금을 취소합니다.

* **HTTP Request**: `DELETE https://api.bithumb.com/v1/withdraws/coin`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `withdrawal_id` | `query` | `string` | ✅ 필수 | 출금의 고유 ID |

### 응답 예시 (200 OK)

```json
{
  "type": "withdraw",
  "withdrawal_id": "200377211",
  "currency": "BTC",
  "net_type": "BTC",
  "state": "PROCESSING",
  "created_at": "2024-07-14T14:54:24+09:00",
  "amount": "0.00010000",
  "fee": "0",
  "krw_amount": "8400"
}
```

---

## 개별 출금 조회

개별 출금 내역을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/withdraw`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `query` | `string` | ✅ 필수 | 화폐 심볼  예시) BTC |
| `uuid` | `query` | `string` | 선택 | 출금의 고유 ID |
| `txid` | `query` | `string` | 선택 | 출금의 트랜잭션 ID |

### 응답 예시 (200 OK)

```json
{
  "type": "withdraw",
  "uuid": "200359853",
  "currency": "MTL",
  "net_type": "MTL_ETH",
  "txid": "0x28d331ddca9fb3b5a413737b3062289db4995dfeaddb96c4d82abd591fe17a52",
  "state": "DONE",
  "created_at": "2024-06-28T15:13:10+09:00",
  "done_at": "2024-06-28T15:17:17+09:00",
  "amount": "0.6113",
  "fee": "0.1",
  "transaction_type": null
}
```

---

## 원화 출금 리스트 조회

원화 출금 목록을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/withdraws/krw`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `state` | `query` | `string` | 선택 | 출금 상태 - `PROCESSING`: 처리 중 - `DONE`: 완료 - `CANCELLED`: 취소됨 |
| `uuids` | `query` | `array` | 선택 | 출금 고유 ID 목록 |
| `txids` | `query` | `array` | 선택 | 출금 TXID의 목록 |
| `page` | `query` | `integer` | 선택 | 페이지 수 |
| `limit` | `query` | `integer` | 선택 | 개수 제한(max: 100) |
| `order_by` | `query` | `string` | 선택 | 조회 결과 정렬 방식 - `asc`: 오래된 주문 순 - `desc`: 최신 주문 순(default) |

### 응답 예시 (200 OK)

```json
[
  {
    "type": "withdraw",
    "uuid": "12703781",
    "currency": "KRW",
    "net_type": null,
    "txid": "1596146",
    "state": "DONE",
    "created_at": "2024-07-06T17:36:22+09:00",
    "done_at": "2024-07-06T17:36:39+09:00",
    "amount": "6000",
    "fee": "1000",
    "transaction_type": "default"
  },
  {
    "type": "withdraw",
    "uuid": "12703780",
    "currency": "KRW",
    "net_type": null,
    "txid": "1596145",
    "state": "DONE",
    "created_at": "2024-07-06T17:35:58+09:00",
    "done_at": "2024-07-06T17:36:18+09:00",
    "amount": "6000",
    "fee": "1000",
    "transaction_type": "default"
  }
]
```

---

## 원화 출금 요청

등록된 출금 계좌로 원화 출금을 요청합니다.

* **HTTP Request**: `POST https://api.bithumb.com/v1/withdraws/krw`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `amount` | `body (json)` | `string` | ✅ 필수 | 출금액 |
| `two_factor_type` | `body (json)` | `string` | ✅ 필수 | 2차 인증 수단 - `kakao` : 카카오 인증 |

### 응답 예시 (200 OK)

성공 시 빈 객체 또는 표준 성공 메시지가 반환됩니다.

---

## 출금 가능 정보

해당 통화의 출금 가능 정보를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/withdraws/chance`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `query` | `string` | ✅ 필수 | 화폐 심볼  예시) BTC |
| `net_type` | `query` | `string` | ✅ 필수 | 출금 네트워크  예시) BTC, DASH |

### 응답 예시 (200 OK)

```json
{
  "member_level": {
    "security_level": null,
    "fee_level": null,
    "email_verified": null,
    "identity_auth_verified": null,
    "bank_account_verified": null,
    "two_factor_auth_verified": null,
    "locked": null,
    "wallet_locked": null
  },
  "currency": {
    "code": "BTC",
    "withdraw_fee": "0.000108",
    "is_coin": true,
    "wallet_state": "working",
    "wallet_support": [
      "deposit",
      "withdraw"
    ]
  },
  "account": {
    "currency": "BTC",
    "balance": "124.45282908",
    "locked": "0",
    "avg_buy_price": "36341011",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  },
  "withdraw_limit": {
    "currency": "BTC",
    "onetime": "6.01",
    "daily": "160",
    "remaining_daily": "160.00000000",
    "remaining_daily_fiat": null,
    "fiat_currency": null,
    "minimum": "0.0001",
    "fixed": 8,
    "withdraw_delayed_fiat": null,
    "can_withdraw": true,
    "remaining_daily_krw": null
  }
}
```

---

## 코인 출금 리스트 조회

가상자산 출금 목록을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/withdraws`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `query` | `string` | 선택 | 화폐 심볼  예시) BTC |
| `state` | `query` | `string` | 선택 | 출금 상태 - `PROCESSING`: 처리 중 - `DONE`: 완료 - `CANCELLED`: 취소됨 |
| `uuids` | `query` | `array` | 선택 | 출금 고유 ID 목록 |
| `txids` | `query` | `array` | 선택 | 출금 TXID의 목록 |
| `page` | `query` | `integer` | 선택 | 페이지 수 |
| `limit` | `query` | `integer` | 선택 | 개수 제한(max 100) |
| `order_by` | `query` | `string` | 선택 | 조회 결과 정렬 방식 - `asc`: 오래된 주문 순 - `desc`: 최신 주문 순(default) |

### 응답 예시 (200 OK)

```json
[
  {
    "type": "withdraw",
    "uuid": "200347674",
    "currency": "TRX",
    "net_type": null,
    "txid": "20240425231724.50893",
    "state": "DONE",
    "created_at": "2024-04-25T23:19:28+09:00",
    "done_at": "2024-04-25T23:19:28+09:00",
    "amount": "99988545780.9056",
    "fee": "0",
    "transaction_type": null
  },
  {
    "type": "withdraw",
    "uuid": "200347279",
    "currency": "TRX",
    "net_type": null,
    "txid": "20240425182026.6693405",
    "state": "DONE",
    "created_at": "2024-04-25T18:22:11+09:00",
    "done_at": "2024-04-25T18:22:11+09:00",
    "amount": "10000000",
    "fee": "0",
    "transaction_type": null
  }
]
```

---

## 출금 허용 주소 리스트 조회

등록된 출금 허용 주소(100만원 이상 출금 가능한 주소) 리스트를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/withdraws/coin_addresses`
* **인증 요구사항**: `Bearer JWT` (필수)

### 요청 파라미터 명세

보내는 파라미터가 없습니다.

### 응답 예시 (200 OK)

```json
[
  {
    "currency": "ETH",
    "net_type": "ETH",
    "network_name": "Ethereum",
    "withdraw_address": "0x569ece3d6cd807a31b1a2d85ebfee79f89fe0b87",
    "secondary_address": null,
    "exchange_name": "vv",
    "owner_type": "personal",
    "owner_ko_name": "홍길동",
    "owner_en_name": null,
    "owner_corp_ko_name": null,
    "owner_corp_en_name": null
  },
  {
    "currency": "ETH",
    "net_type": "ETH",
    "network_name": "Ethereum",
    "withdraw_address": "0x562ece3d6cd807a31b1a5d85ebfee79f78fe0b26",
    "secondary_address": null,
    "exchange_name": "Binance",
    "owner_type": "personal",
    "owner_ko_name": null,
    "owner_en_name": "GIL DONG HONG",
    "owner_corp_ko_name": null,
    "owner_corp_en_name": null
  }
]
```

---

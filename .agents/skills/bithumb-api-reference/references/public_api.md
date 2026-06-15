# 빗썸 Public API 레퍼런스

Public API는 인증(JWT) 없이 호출할 수 있는 시장 데이터 조회 API입니다.

* **공통 Base URL**: `https://api.bithumb.com`

---

## API 목록 목차

* [체결(Trade) 내역 조회](#체결(trade)-내역-조회) (`GET /trades/ticks`)
* [현재가(Ticker) 조회](#현재가(ticker)-조회) (`GET /ticker`)
* [호가(Orderbook) 조회](#호가(orderbook)-조회) (`GET /orderbook`)
* [거래 대상 목록 조회](#거래-대상-목록-조회) (`GET /market/all`)
* [경보제 조회](#경보제-조회) (`GET /market/virtual_asset_warning`)
* [공지사항 조회](#공지사항-조회) (`GET /notices`)
* [입출금 수수료 조회](#입출금-수수료-조회) (`GET /fee/inout/{currency}`)
* [분(Minute) 캔들 조회](#분(minute)-캔들-조회) (`GET /candles/minutes/{unit}`)
* [월(Month) 캔들 조회](#월(month)-캔들-조회) (`GET /candles/months`)
* [일(Day) 캔들 조회](#일(day)-캔들-조회) (`GET /candles/days`)
* [주(Week) 캔들 조회](#주(week)-캔들-조회) (`GET /candles/weeks`)

---

## 체결(Trade) 내역 조회

최근 체결된 거래 내역을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/trades/ticks`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `query` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `to` | `query` | `string` | 선택 | 조회 기준 시각(KST). 해당 시각 이전 데이터를 조회하며, 00:00:00–23:59:59 사이로 입력 가능  형식) HHmmss 또는 HH:mm:ss  미입력 시 `daysAgo`도 입력하지 않았을 경우 현재 시각 기준 최근 데이터 조회, `daysAgo` 입력했을 경우 해당 일자의 00:00:00 기준 최근 데이터 조회 |
| `count` | `query` | `integer` | 선택 | 조회할 내역 수(1–500) |
| `cursor` | `query` | `string` | 선택 | 페이지네이션을 위한 커서 값. 이전 응답의 `sequential_id` 값을 입력하여 다음 페이지를 조회합니다. |
| `daysAgo` | `query` | `integer` | 선택 | 과거 체결 데이터 조회 범위(1–7일). 미입력 시 현재 시점 기준 최근 데이터 조회 |

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "trade_date_utc": "2018-04-18",
    "trade_time_utc": "10:19:58",
    "timestamp": 1524046798000,
    "trade_price": 8616000,
    "trade_volume": 0.03060688,
    "prev_closing_price": 8450000,
    "change_price": 166000,
    "ask_bid": "ASK"
  }
]
```

---

## 현재가(Ticker) 조회

요청 시점 종목의 스냅샷이 제공됩니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/ticker`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `markets` | `query` | `string` | ✅ 필수 | 조회할 거래 대상 페어의 고유 심볼. 여러 개 조회할 경우 쉼표(,)로 구분합니다.  예시) KRW-BTC, BTC-ETH |

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "trade_date": "20180418",
    "trade_time": "102340",
    "trade_date_kst": "20180418",
    "trade_time_kst": "192340",
    "trade_timestamp": 1524047020000,
    "opening_price": 8450000,
    "high_price": 8679000,
    "low_price": 8445000,
    "trade_price": 8621000,
    "prev_closing_price": 8450000,
    "change": "RISE",
    "change_price": 171000,
    "change_rate": 0.0202366864,
    "signed_change_price": 171000,
    "signed_change_rate": 0.0202366864,
    "trade_volume": 0.02467802,
    "acc_trade_price": 108024804862.58253,
    "acc_trade_price_24h": 232702901371.09308,
    "acc_trade_volume": 12603.53386105,
    "acc_trade_volume_24h": 27181.31137002,
    "highest_52_week_price": 28885000,
    "highest_52_week_date": "2018-01-06",
    "lowest_52_week_price": 4175000,
    "lowest_52_week_date": "2017-09-25",
    "timestamp": 1524047026072
  }
]
```

---

## 호가(Orderbook) 조회

지정한 거래 페어의 호가 정보를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/orderbook`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `markets` | `query` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼. 여러 개 조회할 경우 쉼표(,)로 구분합니다.  예시) KRW-BTC, BTC-ETH |

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "timestamp": 1529910247984,
    "total_ask_size": 8.83621228,
    "total_bid_size": 2.43976741,
    "orderbook_units": [
      {
        "ask_price": 6956000,
        "bid_price": 6954000,
        "ask_size": 0.24078656,
        "bid_size": 0.00718341
      },
      {
        "ask_price": 6958000,
        "bid_price": 6953000,
        "ask_size": 1.12919,
        "bid_size": 0.11500074
      },
      {
        "ask_price": 6960000,
        "bid_price": 6952000,
        "ask_size": 0.08614137,
        "bid_size": 0.19019028
      },
      {
        "ask_price": 6962000,
        "bid_price": 6950000,
        "ask_size": 0.0837203,
        "bid_size": 0.28201649
      },
      {
        "ask_price": 6964000,
        "bid_price": 6949000,
        "ask_size": 0.501885,
        "bid_size": 0.01822085
      },
      {
        "ask_price": 6965000,
        "bid_price": 6946000,
        "ask_size": 1.12517189,
        "bid_size": 0.0002
      },
      {
        "ask_price": 6968000,
        "bid_price": 6945000,
        "ask_size": 2.89900477,
        "bid_size": 0.03597913
      },
      {
        "ask_price": 6970000,
        "bid_price": 6944000,
        "ask_size": 0.2044231,
        "bid_size": 0.39291445
      },
      {
        "ask_price": 6972000,
        "bid_price": 6939000,
        "ask_size": 2.55280097,
        "bid_size": 0.12963816
      },
      {
        "ask_price": 6974000,
        "bid_price": 6937000,
        "ask_size": 0.01308832,
        "bid_size": 1.2684239
      }
    ]
  }
]
```

---

## 거래 대상 목록 조회

빗썸에서 제공하는 거래 대상 페어(Trading Pairs) 목록과 관련 정보를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/market/all`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `isDetails` | `query` | `boolean` | 선택 | `isDetails=true`로 요청하면 '유의 종목' 여부(`market_warning`)를 반환합니다.    '주의 종목' 여부는 [경보제 조회 API](doc:경보제-조회)를 참고하시기 바랍니다. |

### 응답 예시 (200 OK)

```json
"[\n    {\n        \"market\": \"KRW-BTC\",\n        \"korean_name\": \"비트코인\",\n        \"english_name\": \"Bitcoin\"\n    },\n    ...\n]"
```

---

## 경보제 조회

투자 주의가 필요한 거래 페어와 경보 유형 및 단계를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/market/virtual_asset_warning`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

보내는 파라미터가 없습니다.

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "warning_type": "PRICE_SUDDEN_FLUCTUATION",
    "warning_step": "CAUTION",
    "end_date": "2026-01-15 18:00:00"
  },
  {
    "market": "KRW-ETH",
    "warning_type": "TRADING_VOLUME_SUDDEN_FLUCTUATION",
    "warning_step": "WARNING",
    "end_date": "2026-01-16 12:30:00"
  },
  {
    "market": "BTC-XRP",
    "warning_type": "PRICE_DIFFERENCE_HIGH",
    "warning_step": "DANGER",
    "end_date": "2026-01-17 09:00:00"
  }
]
```

---

## 공지사항 조회

빗썸 거래소 공지사항을 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/notices`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `count` | `query` | `integer` | 선택 | 조회할 공지사항 수(min 1, max 20) |

### 응답 예시 (200 OK)

```json
"[\n  {\n    \"categories\": [\n      \"이벤트\",\n      \"안내\"\n    ],\n    \"title\": \"마이쉘(SHELL) 원화 마켓 추가 기념 에어드랍 이벤트\",\n    \"pc_url\": \"https://feed.bithumb.com/notice/1647203\",\n    \"published_at\": \"2025-02-28 15:00:00\",\n    \"modified_at\": \"2025-02-28 14:29:56\"\n  },\n  {\n    \"categories\": [\n      \"마켓추가\"\n    ],\n    \"title\": \"마이쉘(SHELL) 원화 마켓 추가\",\n    \"pc_url\": \"https://feed.bithumb.com/notice/1647199\",\n    \"published_at\": \"2025-02-28 11:12:14\",\n    \"modified_at\": \"2025-02-28 15:10:59\"\n  },\n  {\n    \"categories\": [\n      \"점검\"\n    ],\n    \"title\": \"정부24 점검으로 인한 주민등록증 진위확인 서비스 일시 중단 안내\",\n    \"pc_url\": \"https://feed.bithumb.com/notice/1647039\",\n    \"published_at\": \"2025-02-20 16:00:00\",\n    \"modified_at\": \"2025-02-20 15:06:07\"\n  },\n]"
```

---

## 입출금 수수료 조회

빗썸에 상장된 가상자산의 입출금 수수료 정보를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v2/fee/inout/{currency}`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `currency` | `path` | `string` | ✅ 필수 | 화폐 심볼(ALL로 요청 시 전체 자산에 대한 수수료 정보 반환)  예시) BTC, ETH |

### 응답 예시 (200 OK)

```json
[
  {
    "name": "비트코인",
    "currency": "BTC",
    "networks": [
      {
        "net_name": "Bitcoin",
        "deposit_fee_quantity": "0.00006",
        "deposit_minimum_quantity": "2",
        "withdraw_fee_quantity": "0.0002",
        "withdraw_rate": null,
        "withdraw_fee_min": null,
        "withdraw_fee_max": null,
        "withdraw_minimum_quantity": "0.002"
      }
    ]
  },
  {
    "name": "이더리움",
    "currency": "ETH",
    "networks": [
      {
        "net_name": "Ethereum",
        "deposit_fee_quantity": "0.001",
        "deposit_minimum_quantity": "5",
        "withdraw_fee_quantity": "0.005",
        "withdraw_rate": null,
        "withdraw_fee_min": null,
        "withdraw_fee_max": null,
        "withdraw_minimum_quantity": "0.001"
      },
      {
        "net_name": "Arbitrum One",
        "deposit_fee_quantity": "0",
        "deposit_minimum_quantity": "0",
        "withdraw_fee_quantity": "0.00000022",
        "withdraw_rate": null,
        "withdraw_fee_min": null,
        "withdraw_fee_max": null,
        "withdraw_minimum_quantity": "1.00001"
      },
      {
        "net_name": "Optimism",
        "deposit_fee_quantity": "10",
        "deposit_minimum_quantity": "5",
        "withdraw_fee_quantity": "0.005",
        "withdraw_rate": null,
        "withdraw_fee_min": null,
        "withdraw_fee_max": null,
        "withdraw_minimum_quantity": "0.01"
      }
    ]
  }
]
```

---

## 분(Minute) 캔들 조회

지정한 거래 페어의 분 단위 캔들 데이터를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/candles/minutes/{unit}`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `unit` | `path` | `integer` | ✅ 필수 | 캔들 분 단위 |
| `market` | `query` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `to` | `query` | `string` | 선택 | 조회 기준 시각(KST). 해당 시각의 캔들은 제외되며, 미입력 시 가장 최근 캔들 기준으로 조회합니다.  형식) yyyy-MM-dd HH:mm:ss 또는 yyyy-MM-ddTHH:mm:ss |
| `count` | `query` | `integer` | 선택 | 조회할 캔들 개수(max 200) |

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "candle_date_time_utc": "2018-04-18T10:16:00",
    "candle_date_time_kst": "2018-04-18T19:16:00",
    "opening_price": 8615000,
    "high_price": 8618000,
    "low_price": 8611000,
    "trade_price": 8616000,
    "timestamp": 1524046594584,
    "candle_acc_trade_price": 60018891.90054,
    "candle_acc_trade_volume": 6.96780929,
    "unit": 1
  }
]
```

---

## 월(Month) 캔들 조회

지정한 거래 페어의 월 단위 캔들 데이터를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/candles/months`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `query` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `to` | `query` | `string` | 선택 | 조회 기준 시각(KST). 해당 시각의 캔들은 제외되며, 미입력 시 가장 최근 캔들 기준으로 조회합니다.  형식) yyyy-MM-dd HH:mm:ss 또는 yyyy-MM-ddTHH:mm:ss |
| `count` | `query` | `integer` | 선택 | 조회할 캔들 개수(max 200) |

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "candle_date_time_utc": "2018-04-16T00:00:00",
    "candle_date_time_kst": "2018-04-16T09:00:00",
    "opening_price": 8665000,
    "high_price": 8840000,
    "low_price": 8360000,
    "trade_price": 8611000,
    "timestamp": 1524046708995,
    "candle_acc_trade_price": 466989414916.1301,
    "candle_acc_trade_volume": 54410.56660813,
    "first_day_of_period": "2018-04-16"
  }
]
```

---

## 일(Day) 캔들 조회

지정한 거래 페어의 일 단위 캔들 데이터를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/candles/days`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `query` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `to` | `query` | `string` | 선택 | 조회 기준 시각(KST). 해당 시각의 캔들은 제외되며, 미입력 시 가장 최근 캔들 기준으로 조회합니다.  형식) yyyy-MM-dd HH:mm:ss 또는 yyyy-MM-ddTHH:mm:ss |
| `count` | `query` | `integer` | 선택 | 조회할 캔들 개수(max 200) |
| `convertingPriceUnit` | `query` | `string` | 선택 | 원화 마켓이 아닌 다른 마켓의 일봉 요청 시, 종가를 지정한 화폐 단위로 환산하여 `converted_trade_price` 필드로 반환.  - 현재는 `KRW`만 지원합니다. |

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "candle_date_time_utc": "2018-04-18T00:00:00",
    "candle_date_time_kst": "2018-04-18T09:00:00",
    "opening_price": 8450000,
    "high_price": 8679000,
    "low_price": 8445000,
    "trade_price": 8626000,
    "timestamp": 1524046650532,
    "candle_acc_trade_price": 107184005903.68721,
    "candle_acc_trade_volume": 12505.93101659,
    "prev_closing_price": 8450000,
    "change_price": 176000,
    "change_rate": 0.0208284024
  }
]
```

---

## 주(Week) 캔들 조회

지정한 거래 페어의 주 단위 캔들 데이터를 조회합니다.

* **HTTP Request**: `GET https://api.bithumb.com/v1/candles/weeks`
* **인증 요구사항**: 없음

### 요청 파라미터 명세

| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |
|---|---|---|---|---|
| `market` | `query` | `string` | ✅ 필수 | 거래 대상 페어의 고유 심볼  예시) KRW-BTC |
| `to` | `query` | `string` | 선택 | 조회 기준 시각(KST). 해당 시각의 캔들은 제외되며, 미입력 시 가장 최근 캔들 기준으로 조회합니다.  형식) yyyy-MM-dd HH:mm:ss 또는 yyyy-MM-ddTHH:mm:ss |
| `count` | `query` | `integer` | 선택 | 조회할 캔들 개수(max 200) |

### 응답 예시 (200 OK)

```json
[
  {
    "market": "KRW-BTC",
    "candle_date_time_utc": "2018-04-16T00:00:00",
    "candle_date_time_kst": "2018-04-16T09:00:00",
    "opening_price": 8665000,
    "high_price": 8840000,
    "low_price": 8360000,
    "trade_price": 8611000,
    "timestamp": 1524046708995,
    "candle_acc_trade_price": 466989414916.1301,
    "candle_acc_trade_volume": 54410.56660813,
    "first_day_of_period": "2018-04-16"
  }
]
```

---

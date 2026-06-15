---
name: auto-trading-builder
description: 비트코인 및 주식 자동매매 시스템의 모듈형 아키텍처를 설계하고 구축하는 스킬입니다. 종목별 독립 스레드 처리, 동적 전략 로딩, 복합 전략 조합(AND/OR/VOTE), 리스크 관리(손절/익절), WebSocket 실시간 데이터 수신, Thread-Safe 잔고 관리 기능을 포함한 고성능 트레이딩 시스템 구축 시 사용합니다. 자동매매, 알고리즘 트레이딩, 퀀트 트레이딩 시스템 개발 요청 시 반드시 이 스킬을 먼저 읽으십시오.
---

# Auto Trading Builder

고성능 모듈형 자동매매 시스템을 구축하기 위한 설계 가이드, 템플릿, 참조 문서를 제공합니다.

## 핵심 아키텍처 (5계층)

```
① WebSocket Listener  → ② DataDispatcher → ③ TickerWorker × N
                                                ├─ CompositeStrategy (AND/OR/VOTE)
                                                └─ RiskManager (손절/익절)
                                            → ④ 공유 주문 큐 → OrderExecutor
                                            ↕ (Lock)
                                            ⑤ AccountManager
```

전체 아키텍처 상세 설명: [architecture.md](references/architecture.md)

## 템플릿 파일 목록

| 파일 | 역할 |
|---|---|
| `templates/core_engine.py` | BaseStrategy, BaseRiskManager, CompositeStrategy, TickerWorker, DataDispatcher, AccountManager, TradingEngine 전체 포함 |
| `templates/strategy_loader.py` | importlib 기반 동적 전략 로딩, UI용 scan(), CompositeStrategy 조립 |
| `templates/example_strategies.py` | RsiStrategy, MacdStrategy, DisparityStrategy, BollingerStrategy 예시 구현 |
| `templates/websocket_listener.py` | 업비트/바이낸스 WebSocket 연결, 재연결 로직, DataDispatcher 연동 |
| `templates/risk_managers.py` | 전체 리스크 관리자 구현체 모음 (손절/익절/트레일링/물타기/불타기/드로우다운 등) |

## 구축 절차

### 1. 프로젝트 디렉토리 구성

```
my_trading_bot/
├── core_engine.py          ← templates/core_engine.py 복사
├── strategy_loader.py      ← templates/strategy_loader.py 복사
├── websocket_listener.py   ← templates/websocket_listener.py 복사
├── strategies/
│   ├── rsi_strategy.py
│   ├── macd_strategy.py
│   └── disparity_strategy.py
├── user_config.json        ← 종목별 전략 설정
└── main.py
```

### 2. 전략 파일 작성 규칙

모든 전략 파일은 `BaseStrategy`를 상속받고 반드시 `NAME`과 `PARAMS`를 정의한다.

```python
from core_engine import BaseStrategy

class MyStrategy(BaseStrategy):
    NAME = "전략이름"          # UI에 표시될 이름
    PARAMS = {"key": default}  # UI에서 조정 가능한 파라미터

    def generate_signal(self, data: dict) -> str:
        # 반환값: 'BUY' | 'SELL' | 'HOLD'
        ...
```

`strategies/` 폴더에 파일을 추가하면 `StrategyLoader.scan()`이 자동 탐색한다.

### 3. 설정 파일 (user_config.json)

설정 스키마 및 전체 예시: [config_schema.md](references/config_schema.md)

핵심 구조:
```json
{
  "BTC": {
    "strategies": ["RSI", "이격도"],
    "logic": "AND",
    "params": { "RSI": {"period": 14} },
    "risk": { "stop_loss_pct": 0.03, "take_profit_pct": 0.06 }
  }
}
```

### 4. main.py 조립 패턴

```python
from core_engine import TradingEngine, AccountManager
from strategy_loader import StrategyLoader
from websocket_listener import WebSocketListener
from example_strategies import StopLossRiskManager
import json

# 설정 로드
with open("user_config.json") as f:
    config = json.load(f)

# 엔진 조립
account = AccountManager(initial_balance=10_000_000)
engine  = TradingEngine(account)
loader  = StrategyLoader("strategies")

for ticker, cfg in config.items():
    engine.register_ticker(
        ticker=ticker,
        strategy=loader.build_composite(cfg["strategies"], cfg["logic"], cfg.get("params")),
        risk_manager=StopLossRiskManager(**cfg["risk"]),
    )

# 실행
engine.start()
ws = WebSocketListener(engine.dispatcher, url="wss://...", subscribe_msg=[...])
ws.start()
```

## 주요 설계 원칙

**Thread Safety**: TickerWorker 내부 상태는 해당 스레드만 접근한다. AccountManager 등 공유 자원은 반드시 `threading.Lock`을 사용한다.

**Backpressure**: `queue.Queue(maxsize=100)` 설정 후 큐가 가득 찼을 때 가장 오래된 데이터를 드롭한다 (`push_data()` 참조).

**WebSocket 콜백 경량화**: `on_message` 내에서 전략 계산을 절대 수행하지 않는다. 큐에 `put()` 후 즉시 반환한다.

**Graceful Shutdown**: `threading.Event`를 사용하여 `engine.stop()` 호출 시 모든 워커가 안전하게 종료된다.

**백테스트 호환**: WebSocketListener를 CsvDataFeeder로, OrderExecutor를 VirtualOrderExecutor로 교체하면 동일한 Strategy/RiskManager 코드로 백테스트가 가능하다.

## 리스크 관리자 (Risk Manager)

리스크 관리 전체 레퍼런스: [risk_management.md](references/risk_management.md)

### 포지션 레벨

| 클래스 | 설명 | 핵심 파라미터 |
|---|---|---|
| `StopLossRiskManager` | 고정 손절/익절 | `stop_loss_pct`, `take_profit_pct` |
| `TrailingStopRiskManager` | 최고가 추적 동적 손절 | `trail_pct` |
| `TrailingTakeProfitRiskManager` | 익절 목표 도달 후 트레일링 전환 ★ | `take_profit_pct`, `trail_pct`, `dynamic_trail` |
| `TimedExitRiskManager` | 시간 초과 시 청산 | `max_hold_hours` |

### 분할 매수 전략

| 클래스 | 설명 | 핵심 파라미터 |
|---|---|---|
| `AveragingDownRiskManager` | 물타기 — 하락 시 추가 매수, 평균단가 낮추기 | `drop_trigger_pct`, `max_add_count`, `hard_stop_pct` |
| `PyramidingRiskManager` | 불타기 — 상승 시 추가 매수, 추세 이익 극대화 | `rise_trigger_pct`, `max_add_count`, `trail_stop_pct` |

> **물타기 규칙**: `hard_stop_pct` 없이 절대 사용 금지. 하락이 지속되면 계좌 폭발 위험.
> **불타기 규칙**: `trail_stop_pct`(트레일링 스탑)와 반드시 세트로 사용. 역피라미드 비율 권장.

### 분할 매수 신호 처리 (`ADD_BUY`)

물타기/불타기는 `BUY/SELL/HOLD` 외에 `ADD_BUY` 신호를 반환한다.
OrderExecutor에서 아래 로직으로 평균단가를 재계산해야 한다.

```python
if signal == "ADD_BUY":
    add_amount = position["invested_amount"] * add_ratio
    new_qty    = add_amount / current_price
    total_qty  = position["quantity"] + new_qty
    total_cost = position["total_cost"] + add_amount
    position["avg_price"]  = total_cost / total_qty  # 평균단가 재계산
    position["add_count"] += 1
```

### 포트폴리오 레벨

| 클래스 | 설명 | 핵심 파라미터 |
|---|---|---|
| `DrawdownGuard` | 전고점 대비 손실 한도 초과 시 거래 중단 | `max_drawdown_pct` |
| `DailyLossGuard` | 일일 손실 한도 초과 시 당일 거래 중단 | `daily_loss_limit_pct` |
| `PositionSizer` | 종목별 투자 금액 결정 (고정/켈리/변동성) | `method`, `fixed_ratio` |

### 리스크 관리자 평가 우선순위

```
1순위: DailyLossGuard    — 일일 한도 초과 → 즉시 거래 중단
2순위: DrawdownGuard     — 드로우다운 한도 초과 → 거래 중단
3순위: RiskManager.check() — 포지션 손절/익절/추가매수
4순위: 전략 신호         — BUY / SELL / HOLD
```

## 필수 패키지

```bash
pip install websocket-client  # WebSocket 연결
pip install pandas-ta          # 지표 계산 (RSI, MACD 등)
# 또는
pip install ta-lib
```

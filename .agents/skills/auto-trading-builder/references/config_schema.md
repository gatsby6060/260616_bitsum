# 사용자 설정 스키마 (user_config.json)

UI에서 사용자가 저장한 설정을 JSON으로 직렬화하여 엔진에 전달하는 표준 형식이다.

## 스키마 구조

```json
{
  "TICKER_CODE": {
    "strategies": ["전략명1", "전략명2"],
    "logic": "AND | OR | VOTE",
    "params": {
      "전략명1": { "파라미터키": 값 },
      "전략명2": { "파라미터키": 값 }
    },
    "risk": {
      "type": "StopLoss | TrailingStop | TrailingTakeProfit | AveragingDown | Pyramiding | TimedExit",
      // 타입별 세부 파라미터는 아래 예시 서는 참조
    }
  }
}
```

## 전체 예시

### 고정 손절/익절 (StopLoss)

```json
{
  "BTC": {
    "strategies": ["RSI", "이격도"],
    "logic": "AND",
    "params": {
      "RSI":  { "period": 14, "oversold": 30, "overbought": 70 },
      "이격도": { "period": 20, "buy_threshold": -5.0, "sell_threshold": 5.0 }
    },
    "risk": {
      "type": "StopLoss",
      "stop_loss_pct": 0.03,
      "take_profit_pct": 0.06
    }
  }
}
```

### 트레일링 익절 (TrailingTakeProfit) ★

```json
{
  "ETH": {
    "strategies": ["MACD"],
    "logic": "OR",
    "params": {
      "MACD": { "fast": 12, "slow": 26, "signal_period": 9 }
    },
    "risk": {
      "type": "TrailingTakeProfit",
      "stop_loss_pct": 0.03,
      "take_profit_pct": 0.06,
      "trail_pct": 0.04,
      "dynamic_trail": true
    }
  }
}
```

### 물타기 (AveragingDown)

```json
{
  "XRP": {
    "strategies": ["RSI", "볼린저밴드"],
    "logic": "AND",
    "params": {
      "RSI": { "period": 14, "oversold": 30, "overbought": 70 }
    },
    "risk": {
      "type": "AveragingDown",
      "drop_trigger_pct": 0.05,
      "max_add_count": 3,
      "add_ratio": 0.5,
      "hard_stop_pct": 0.20,
      "take_profit_pct": 0.03
    }
  }
}
```

### 불타기 (Pyramiding)

```json
{
  "005930": {
    "strategies": ["RSI", "MACD"],
    "logic": "VOTE",
    "params": {
      "RSI":  { "period": 9, "oversold": 25, "overbought": 75 },
      "MACD": { "fast": 12, "slow": 26, "signal_period": 9 }
    },
    "risk": {
      "type": "Pyramiding",
      "rise_trigger_pct": 0.05,
      "max_add_count": 3,
      "add_ratio": 0.5,
      "trail_stop_pct": 0.03
    }
  }
}
```

## 설정 로드 코드 패턴

```python
import json
from core_engine import TradingEngine, AccountManager, StopLossRiskManager
from strategy_loader import StrategyLoader

def build_engine_from_config(config_path: str, initial_balance: float) -> TradingEngine:
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    account = AccountManager(initial_balance)
    engine  = TradingEngine(account)
    loader  = StrategyLoader("strategies")

    for ticker, cfg in config.items():
        composite = loader.build_composite(
            selected_names=cfg["strategies"],
            logic=cfg["logic"],
            params=cfg.get("params", {}),
        )
        risk_mgr = StopLossRiskManager(**cfg["risk"])
        engine.register_ticker(ticker, composite, risk_mgr)

    return engine
```

## 전략별 data 딕셔너리 필수 키

| 전략 | 필수 키 | 설명 |
|---|---|---|
| RSI | `rsi` | RSI 값 (0~100) |
| MACD | `macd`, `macd_signal` | MACD 라인, 시그널 라인 |
| 이격도 | `disparity` | (현재가 - MA) / MA × 100 |
| 볼린저밴드 | `price`, `bb_upper`, `bb_lower` | 현재가, 상단/하단 밴드 |

지표 계산은 `ta-lib` 또는 `pandas-ta` 라이브러리를 활용하거나,
WebSocket 수신 후 별도 지표 계산 모듈에서 data 딕셔너리에 추가하여 전달한다.

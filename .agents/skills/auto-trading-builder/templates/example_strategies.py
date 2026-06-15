"""
전략 파일 예시 모음
────────────────────
각 전략을 별도 파일로 분리하여 strategies/ 디렉토리에 저장한다.
모든 전략은 BaseStrategy를 상속받고 NAME / PARAMS 클래스 변수를 정의한다.

파일 분리 예:
  strategies/rsi_strategy.py       ← RsiStrategy
  strategies/macd_strategy.py      ← MacdStrategy
  strategies/disparity_strategy.py ← DisparityStrategy
  strategies/bollinger_strategy.py ← BollingerStrategy
"""

from core_engine import BaseStrategy
from typing import Any, Dict, List


# ══════════════════════════════════════════════════════════════
# RSI 전략
# ══════════════════════════════════════════════════════════════
class RsiStrategy(BaseStrategy):
    """
    RSI(상대강도지수) 기반 전략.
    data 딕셔너리에 'rsi' 키가 포함되어야 한다.
    """
    NAME = "RSI"
    PARAMS = {"period": 14, "oversold": 30, "overbought": 70}

    def generate_signal(self, data: Dict[str, Any]) -> str:
        rsi = data.get("rsi", 50)
        if rsi < self.params["oversold"]:
            return "BUY"
        if rsi > self.params["overbought"]:
            return "SELL"
        return "HOLD"


# ══════════════════════════════════════════════════════════════
# MACD 전략
# ══════════════════════════════════════════════════════════════
class MacdStrategy(BaseStrategy):
    """
    MACD 골든크로스/데드크로스 전략.
    data 딕셔너리에 'macd', 'macd_signal' 키가 포함되어야 한다.
    """
    NAME = "MACD"
    PARAMS = {"fast": 12, "slow": 26, "signal_period": 9}

    def generate_signal(self, data: Dict[str, Any]) -> str:
        macd = data.get("macd", 0)
        signal = data.get("macd_signal", 0)
        if macd > signal:
            return "BUY"
        if macd < signal:
            return "SELL"
        return "HOLD"


# ══════════════════════════════════════════════════════════════
# 이격도 전략
# ══════════════════════════════════════════════════════════════
class DisparityStrategy(BaseStrategy):
    """
    이격도(현재가 vs 이동평균 괴리율) 전략.
    data 딕셔너리에 'disparity' 키가 포함되어야 한다.
    disparity = (현재가 - MA) / MA * 100
    """
    NAME = "이격도"
    PARAMS = {"period": 20, "buy_threshold": -5.0, "sell_threshold": 5.0}

    def generate_signal(self, data: Dict[str, Any]) -> str:
        disparity = data.get("disparity", 0)
        if disparity < self.params["buy_threshold"]:
            return "BUY"
        if disparity > self.params["sell_threshold"]:
            return "SELL"
        return "HOLD"


# ══════════════════════════════════════════════════════════════
# 볼린저밴드 전략
# ══════════════════════════════════════════════════════════════
class BollingerStrategy(BaseStrategy):
    """
    볼린저밴드 돌파 전략.
    data 딕셔너리에 'price', 'bb_upper', 'bb_lower' 키가 포함되어야 한다.
    """
    NAME = "볼린저밴드"
    PARAMS = {"period": 20, "std_dev": 2.0}

    def generate_signal(self, data: Dict[str, Any]) -> str:
        price = data.get("price", 0)
        upper = data.get("bb_upper", float("inf"))
        lower = data.get("bb_lower", 0)
        if price < lower:
            return "BUY"
        if price > upper:
            return "SELL"
        return "HOLD"


# ══════════════════════════════════════════════════════════════
# 손절/익절 리스크 관리자
# ══════════════════════════════════════════════════════════════
from core_engine import BaseRiskManager

class StopLossRiskManager(BaseRiskManager):
    """
    고정 비율 손절/익절 리스크 관리자.
    포지션이 없을 때는 모든 BUY 신호를 허용한다.
    """

    def __init__(self, stop_loss_pct: float = 0.03, take_profit_pct: float = 0.06):
        self.stop_loss_pct = stop_loss_pct        # 기본 3% 손절
        self.take_profit_pct = take_profit_pct    # 기본 6% 익절

    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        avg_price = position.get("avg_price", 0)
        pnl_pct = position.get("pnl_pct", 0)
        quantity = position.get("quantity", 0)

        # 미보유 상태: BUY만 허용
        if quantity == 0 or avg_price == 0:
            return signal == "BUY"

        # 손절 조건: 강제 SELL 허용
        if pnl_pct <= -self.stop_loss_pct:
            return signal == "SELL"

        # 익절 조건: 강제 SELL 허용
        if pnl_pct >= self.take_profit_pct:
            return signal == "SELL"

        # 일반 상태: 전략 신호 그대로 허용
        return True

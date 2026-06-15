"""
리스크 관리자 템플릿 모음
─────────────────────────────────────────────────────────────────
포함 클래스:
  포지션 레벨:
    - StopLossRiskManager          : 고정 손절/익절
    - TrailingStopRiskManager      : 트레일링 스탑
    - TrailingTakeProfitRiskManager: 익절 목표 도달 후 트레일링 전환 ★
    - TimedExitRiskManager         : 시간 기반 손절

  분할 매수:
    - AveragingDownRiskManager     : 물타기 (하락 시 추가 매수)
    - PyramidingRiskManager        : 불타기 (상승 시 추가 매수)

  포트폴리오 레벨:
    - DrawdownGuard                : 최대 드로우다운 제한
    - DailyLossGuard               : 일일 손실 한도

  유틸리티:
    - PositionSizer                : 포지션 사이징 계산기

반환 신호 규칙:
  'HOLD'      — 현재 포지션 유지
  'SELL'      — 청산 (손절 / 익절 / 트레일링)
  'HARD_STOP' — 강제 전량 청산 (물타기 최종 손절)
  'ADD_BUY'   — 추가 매수 (물타기 / 불타기)
"""

import threading
import time
from datetime import datetime, date
from typing import Any, Dict, Optional

from core_engine import BaseRiskManager


# ══════════════════════════════════════════════════════════════
# 1. 포지션 레벨 리스크
# ══════════════════════════════════════════════════════════════

class StopLossRiskManager(BaseRiskManager):
    """
    고정 손절/익절 리스크 관리자.
    진입가 대비 고정 비율에서 강제 청산한다.

    사용 예:
        risk = StopLossRiskManager(stop_loss_pct=0.03, take_profit_pct=0.06)
    """

    def __init__(self, stop_loss_pct: float = 0.03, take_profit_pct: float = 0.06):
        self.stop_loss_pct    = stop_loss_pct
        self.take_profit_pct  = take_profit_pct

    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        """기존 BaseRiskManager 인터페이스 호환용 — check()를 우선 사용할 것."""
        result = self.check(position.get("ticker", ""), position["current_price"], position)
        return result != "SELL"

    def check(self, ticker: str, current_price: float, position: Dict[str, Any]) -> str:
        avg_price = position.get("avg_price", 0)
        if avg_price == 0:
            return "HOLD"

        # 손절: 평균단가 대비 −N%
        if current_price <= avg_price * (1 - self.stop_loss_pct):
            return "SELL"

        # 익절: 평균단가 대비 +N%
        if current_price >= avg_price * (1 + self.take_profit_pct):
            return "SELL"

        return "HOLD"


# ──────────────────────────────────────────────────────────────

class TrailingStopRiskManager(BaseRiskManager):
    """
    트레일링 스탑 리스크 관리자.
    최고가를 추적하며 손절선을 동적으로 상향 조정한다.

    사용 예:
        risk = TrailingStopRiskManager(trail_pct=0.03)
    """

    def __init__(self, trail_pct: float = 0.03):
        self.trail_pct   = trail_pct
        self._peak: Dict[str, float] = {}

    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        result = self.check(position.get("ticker", ""), position["current_price"], position)
        return result != "SELL"

    def check(self, ticker: str, current_price: float, position: Dict[str, Any]) -> str:
        if position.get("quantity", 0) == 0:
            self._peak.pop(ticker, None)
            return "HOLD"

        # 최고가 갱신
        peak = self._peak.get(ticker, current_price)
        self._peak[ticker] = max(peak, current_price)

        # 트레일링 스탑 계산
        trail_stop = self._peak[ticker] * (1 - self.trail_pct)
        if current_price <= trail_stop:
            self._peak.pop(ticker, None)
            return "SELL"

        return "HOLD"


# ──────────────────────────────────────────────────────────────

class TrailingTakeProfitRiskManager(BaseRiskManager):
    """
    트레일링 익절 리스크 관리자. ★ 핵심 기법
    고정 익절과 트레일링 스탑을 결합한다.

    동작 흐름:
      [WATCHING] 모드
        → 손절선(stop_loss_pct) 이탈 시  : SELL (손절)
        → 익절 목표(take_profit_pct) 도달 : [TRAILING] 모드 전환
      [TRAILING] 모드
        → 가격 상승 시                   : 최고가 갱신, 계속 추적
        → 최고가 대비 trail_pct 하락 시  : SELL (트레일링 익절)

    고급: 수익률에 따라 trail_pct를 동적으로 조절 (단계별 트레일).
      profit >= 30% → trail 2% (타이트)
      profit >= 15% → trail 3%
      profit >=  6% → trail 5% (여유)

    사용 예:
        risk = TrailingTakeProfitRiskManager(
            stop_loss_pct=0.03,
            take_profit_pct=0.06,
            trail_pct=0.04,
            dynamic_trail=True,   # 단계별 트레일 활성화
        )
    """

    # 단계별 트레일 구간 정의 (수익률: 트레일 간격)
    DYNAMIC_TRAIL_TABLE = [
        (0.30, 0.02),
        (0.15, 0.03),
        (0.06, 0.05),
    ]

    def __init__(
        self,
        stop_loss_pct:   float = 0.03,
        take_profit_pct: float = 0.06,
        trail_pct:       float = 0.04,
        dynamic_trail:   bool  = False,
    ):
        self.stop_loss_pct    = stop_loss_pct
        self.take_profit_pct  = take_profit_pct
        self.trail_pct        = trail_pct
        self.dynamic_trail    = dynamic_trail

        self._state: Dict[str, str]   = {}   # ticker → 'WATCHING' | 'TRAILING'
        self._peak:  Dict[str, float] = {}   # ticker → 최고가

    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        result = self.check(position.get("ticker", ""), position["current_price"], position)
        return result not in ("SELL", "HARD_STOP")

    def check(self, ticker: str, current_price: float, position: Dict[str, Any]) -> str:
        avg_price = position.get("avg_price", 0)
        if avg_price == 0:
            self._reset(ticker)
            return "HOLD"

        # 상태 초기화
        if ticker not in self._state:
            self._state[ticker] = "WATCHING"
            self._peak[ticker]  = current_price

        state = self._state[ticker]

        # ── WATCHING 모드 ──────────────────────────────────────────
        if state == "WATCHING":

            # 손절 조건
            if current_price <= avg_price * (1 - self.stop_loss_pct):
                self._reset(ticker)
                return "SELL"

            # 익절 목표 도달 → 트레일링 모드 전환
            if current_price >= avg_price * (1 + self.take_profit_pct):
                self._state[ticker] = "TRAILING"
                self._peak[ticker]  = current_price
                print(f"[{ticker}] 🔥 트레일링 모드 전환! 기준가: {current_price:,.0f}")
                return "HOLD"

        # ── TRAILING 모드 ──────────────────────────────────────────
        elif state == "TRAILING":

            # 최고가 갱신
            if current_price > self._peak[ticker]:
                self._peak[ticker] = current_price
                print(f"[{ticker}] 📈 신고가 갱신: {current_price:,.0f}")

            # 트레일 간격 결정
            trail = self._resolve_trail_pct(current_price, avg_price)
            trail_stop = self._peak[ticker] * (1 - trail)

            if current_price <= trail_stop:
                self._reset(ticker)
                return "SELL"

        return "HOLD"

    def _resolve_trail_pct(self, current_price: float, avg_price: float) -> float:
        """단계별 동적 트레일 or 고정 트레일 반환."""
        if not self.dynamic_trail or avg_price == 0:
            return self.trail_pct
        profit_pct = (current_price - avg_price) / avg_price
        for threshold, trail in self.DYNAMIC_TRAIL_TABLE:
            if profit_pct >= threshold:
                return trail
        return self.trail_pct

    def _reset(self, ticker: str) -> None:
        self._state.pop(ticker, None)
        self._peak.pop(ticker, None)


# ──────────────────────────────────────────────────────────────

class TimedExitRiskManager(BaseRiskManager):
    """
    시간 기반 손절 리스크 관리자.
    일정 시간 이상 포지션을 보유하면 수익 무관하게 청산한다.

    사용 예:
        risk = TimedExitRiskManager(max_hold_hours=24)
    """

    def __init__(self, max_hold_hours: float = 24.0):
        self.max_hold_seconds = max_hold_hours * 3600

    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        result = self.check(position.get("ticker", ""), position.get("current_price", 0), position)
        return result != "SELL"

    def check(self, ticker: str, current_price: float, position: Dict[str, Any]) -> str:
        entry_time = position.get("entry_time")
        if entry_time is None:
            return "HOLD"

        hold_seconds = (datetime.now() - entry_time).total_seconds()
        if hold_seconds >= self.max_hold_seconds:
            return "SELL"

        return "HOLD"


# ══════════════════════════════════════════════════════════════
# 2. 분할 매수 전략 (물타기 / 불타기)
# ══════════════════════════════════════════════════════════════

class AveragingDownRiskManager(BaseRiskManager):
    """
    물타기 리스크 관리자.
    손실 중인 포지션에 추가 매수하여 평균단가를 낮춘다.
    횡보장 / 반등 기대 시 유효하다.

    반환 신호:
      'ADD_BUY'   — 추가 매수 실행
      'HARD_STOP' — 최종 손절선 이탈 → 전량 강제 청산
      'SELL'      — 평균단가 회복 후 익절

    ⚠️ hard_stop_pct 없이 사용 금지. 반드시 최대 추가 횟수와 최종 손절선을 설정할 것.

    사용 예:
        risk = AveragingDownRiskManager(
            drop_trigger_pct=0.05,
            max_add_count=3,
            add_ratio=0.5,
            hard_stop_pct=0.20,
            take_profit_pct=0.03,
        )
    """

    def __init__(
        self,
        drop_trigger_pct: float = 0.05,
        max_add_count:    int   = 3,
        add_ratio:        float = 0.5,
        hard_stop_pct:    float = 0.20,
        take_profit_pct:  float = 0.03,
    ):
        self.drop_trigger_pct = drop_trigger_pct
        self.max_add_count    = max_add_count
        self.add_ratio        = add_ratio
        self.hard_stop_pct    = hard_stop_pct
        self.take_profit_pct  = take_profit_pct

    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        result = self.check(position.get("ticker", ""), position["current_price"], position)
        return result not in ("SELL", "HARD_STOP")

    def check(self, ticker: str, current_price: float, position: Dict[str, Any]) -> str:
        entry_price = position.get("entry_price", position.get("avg_price", 0))
        avg_price   = position.get("avg_price", entry_price)
        add_count   = position.get("add_count", 0)

        if entry_price == 0:
            return "HOLD"

        # ❌ 최종 손절선: 최초 진입가 기준 (물타기가 실패했을 때의 안전망)
        if current_price <= entry_price * (1 - self.hard_stop_pct):
            return "HARD_STOP"

        # 📉 추가 매수 조건: N% 하락마다 한 번씩
        drop_pct = (entry_price - current_price) / entry_price
        expected_drop = self.drop_trigger_pct * (add_count + 1)

        if drop_pct >= expected_drop and add_count < self.max_add_count:
            return "ADD_BUY"

        # ✅ 평균단가 기준 익절
        if current_price >= avg_price * (1 + self.take_profit_pct):
            return "SELL"

        return "HOLD"


# ──────────────────────────────────────────────────────────────

class PyramidingRiskManager(BaseRiskManager):
    """
    불타기(피라미딩) 리스크 관리자.
    수익 중인 포지션에 추가 매수하여 추세 이익을 극대화한다.
    강한 상승 추세에서 유효하다. 트레일링 스탑과 반드시 함께 사용한다.

    역피라미드 원칙:
        추가 매수 금액을 점점 줄여(50%씩) 평균단가 상승을 최소화한다.
        1차: 100만원 → 2차: 50만원 → 3차: 25만원

    반환 신호:
      'ADD_BUY' — 추가 매수 실행
      'SELL'    — 트레일링 스탑 이탈 → 전량 청산

    사용 예:
        risk = PyramidingRiskManager(
            rise_trigger_pct=0.05,
            max_add_count=3,
            add_ratio=0.5,
            trail_stop_pct=0.03,
        )
    """

    def __init__(
        self,
        rise_trigger_pct: float = 0.05,
        max_add_count:    int   = 3,
        add_ratio:        float = 0.5,
        trail_stop_pct:   float = 0.03,
    ):
        self.rise_trigger_pct = rise_trigger_pct
        self.max_add_count    = max_add_count
        self.add_ratio        = add_ratio
        self.trail_stop_pct   = trail_stop_pct

        self._peak: Dict[str, float] = {}

    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        result = self.check(position.get("ticker", ""), position["current_price"], position)
        return result != "SELL"

    def check(self, ticker: str, current_price: float, position: Dict[str, Any]) -> str:
        entry_price = position.get("entry_price", position.get("avg_price", 0))
        add_count   = position.get("add_count", 0)

        if entry_price == 0:
            return "HOLD"

        # 최고가 추적
        peak = self._peak.get(ticker, entry_price)
        self._peak[ticker] = max(peak, current_price)

        # 🛡️ 트레일링 스탑 (포지션 보유 중일 때만)
        if add_count > 0:
            trail_stop = self._peak[ticker] * (1 - self.trail_stop_pct)
            if current_price <= trail_stop:
                self._peak.pop(ticker, None)
                return "SELL"

        # 🔥 추가 매수 조건: N% 상승마다 한 번씩
        rise_pct = (current_price - entry_price) / entry_price
        expected_rise = self.rise_trigger_pct * (add_count + 1)

        if rise_pct >= expected_rise and add_count < self.max_add_count:
            return "ADD_BUY"

        return "HOLD"


# ══════════════════════════════════════════════════════════════
# 3. 포트폴리오 레벨 리스크
# ══════════════════════════════════════════════════════════════

class DrawdownGuard:
    """
    최대 드로우다운 제한기.
    전고점 대비 손실이 한도를 초과하면 거래를 중단한다.

    TickerWorker 외부에서 TradingEngine 레벨에서 관리한다.

    사용 예:
        guard = DrawdownGuard(max_drawdown_pct=0.20)
        if not guard.check(current_balance):
            engine.stop()
    """

    def __init__(self, max_drawdown_pct: float = 0.20):
        self.max_drawdown_pct = max_drawdown_pct
        self._peak_balance: float = 0.0

    def check(self, current_balance: float) -> bool:
        """True: 거래 허용 / False: 드로우다운 한도 초과 → 거래 중단"""
        self._peak_balance = max(self._peak_balance, current_balance)
        if self._peak_balance == 0:
            return True
        drawdown = (current_balance - self._peak_balance) / self._peak_balance
        if drawdown <= -self.max_drawdown_pct:
            print(f"[DrawdownGuard] ⚠️ 드로우다운 한도 초과: {drawdown:.1%}")
            return False
        return True


# ──────────────────────────────────────────────────────────────

class DailyLossGuard:
    """
    일일 손실 한도 관리기.
    하루 시작 잔고 대비 손실이 한도를 초과하면 당일 거래를 중단한다.

    자정이 되면 자동으로 일일 기준 잔고를 리셋한다.

    사용 예:
        guard = DailyLossGuard(daily_loss_limit_pct=0.05)
        if not guard.check(current_balance):
            engine.pause_until_next_day()
    """

    def __init__(self, daily_loss_limit_pct: float = 0.05):
        self.daily_loss_limit_pct = daily_loss_limit_pct
        self._start_balance: Optional[float] = None
        self._today: Optional[date]          = None

    def check(self, current_balance: float) -> bool:
        """True: 거래 허용 / False: 일일 한도 초과 → 당일 거래 중단"""
        today = datetime.now().date()

        # 날짜 변경 시 리셋
        if self._today != today:
            self._today         = today
            self._start_balance = current_balance

        if self._start_balance is None or self._start_balance == 0:
            return True

        daily_loss = (current_balance - self._start_balance) / self._start_balance
        if daily_loss <= -self.daily_loss_limit_pct:
            print(f"[DailyLossGuard] 🛑 일일 손실 한도 초과: {daily_loss:.1%}")
            return False
        return True


# ══════════════════════════════════════════════════════════════
# 4. 유틸리티 — 포지션 사이징
# ══════════════════════════════════════════════════════════════

class PositionSizer:
    """
    포지션 사이징 계산기.
    한 종목에 투자할 금액을 결정한다.

    사용 예:
        sizer = PositionSizer(method="kelly")
        amount = sizer.calculate(
            total_balance=10_000_000,
            win_rate=0.55,
            avg_profit=0.06,
            avg_loss=0.03,
        )
    """

    def __init__(self, method: str = "fixed", fixed_ratio: float = 0.10):
        """
        method:
          'fixed'       — 전체 자산의 고정 비율
          'kelly'       — 켈리 공식 (Half Kelly 적용)
          'volatility'  — 변동성 기반 (ATR 필요)
        fixed_ratio:
          method='fixed'일 때 사용하는 비율 (기본 10%)
        """
        self.method      = method
        self.fixed_ratio = fixed_ratio

    def calculate(
        self,
        total_balance: float,
        win_rate:      float = 0.5,
        avg_profit:    float = 0.06,
        avg_loss:      float = 0.03,
        atr:           float = 0.0,
        risk_per_trade: float = 0.01,
    ) -> float:
        """투자 금액 반환."""
        if self.method == "fixed":
            return total_balance * self.fixed_ratio

        elif self.method == "kelly":
            b = avg_profit / avg_loss if avg_loss > 0 else 1
            p = win_rate
            q = 1 - win_rate
            kelly = (b * p - q) / b
            kelly = max(0, kelly)  # 음수 방지
            # Half Kelly: 과레버리지 방지
            return total_balance * (kelly * 0.5)

        elif self.method == "volatility":
            # 목표 위험 금액 / ATR → 매수 수량
            if atr == 0:
                return total_balance * 0.10
            risk_amount = total_balance * risk_per_trade
            return risk_amount / atr  # 수량 반환 (금액 아님)

        return total_balance * self.fixed_ratio

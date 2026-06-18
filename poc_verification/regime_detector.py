import numpy as np
from typing import Optional

# 코인별 사이클 프로파일 (일봉 회귀·회기 앵커·월봉 회복 주기)
TICKER_CYCLE_PROFILE = {
    "BTC": {
        "regression_days": 800,
        "cycle_anchor_days": 800,
        "recovery_days": 365,
        "peak_lookback_days": 1095,
        "bear_drawdown_pct": 25.0,
        "bottoming_anchor_band_pct": 15.0,
    },
    "ETH": {
        "regression_days": 400,
        "cycle_anchor_days": 400,
        "recovery_days": 365,
        "peak_lookback_days": 730,
        "bear_drawdown_pct": 25.0,
        "bottoming_anchor_band_pct": 12.0,
    },
    "XRP": {
        "regression_days": 400,
        "cycle_anchor_days": 400,
        "recovery_days": 1095,
        "peak_lookback_days": 1460,
        "bear_drawdown_pct": 30.0,
        "bottoming_anchor_band_pct": 15.0,
    },
}


def _slope_rate_pct(prices: list, window: int, price_now: float) -> float:
    if len(prices) < window or price_now <= 0:
        return 0.0
    segment = prices[-window:]
    slope, _ = np.polyfit(np.arange(len(segment)), segment, 1)
    return float((slope / price_now) * 100)


def _linear_fair_value(prices: list, period: int) -> tuple:
    segment = prices[-period:]
    x = np.arange(len(segment))
    slope, intercept = np.polyfit(x, segment, 1)
    fair = float(slope * (len(segment) - 1) + intercept)
    return fair, float(slope)


def _segment_slope_pct_per_day(prices: list) -> float:
    if len(prices) < 2:
        return 0.0
    slope, _ = np.polyfit(np.arange(len(prices)), prices, 1)
    ref = float(prices[-1])
    return float((slope / ref) * 100) if ref > 0 else 0.0


def _current_cycle_angle_pct_per_month(prices: list, lookback: int = 400) -> float:
    """최근 저점 이후 회복(또는 하락) 구간의 회귀각(%/월)을 계산합니다."""
    n = len(prices)
    if n < 90:
        return 0.0
    window = min(lookback, n)
    segment = prices[-window:]
    trough_idx = int(np.argmin(segment))
    leg = segment[trough_idx:]
    if len(leg) >= 14:
        return _segment_slope_pct_per_day(leg) * 30
    return _segment_slope_pct_per_day(prices[-min(90, n):]) * 30


def _detect_historical_bull_cycles(prices: list, min_rise_pct: float = 20.0, min_duration_days: int = 60) -> list:
    """일봉 저점→고점 상승 구간별 회귀각(%/월)을 추출해 과거 사이클 목록을 만듭니다."""
    n = len(prices)
    if n < 365:
        return []

    lookback = 60
    troughs = []
    for i in range(lookback, n - 30):
        before = prices[i - lookback : i + 1]
        after = prices[i : min(i + 30, n)]
        if prices[i] != min(before):
            continue
        if prices[i] > min(after) * 1.03:
            continue
        if troughs and i - troughs[-1] < 90:
            if prices[i] < prices[troughs[-1]]:
                troughs[-1] = i
            continue
        troughs.append(i)

    cycles = []
    for idx, trough_idx in enumerate(troughs):
        end_idx = troughs[idx + 1] if idx + 1 < len(troughs) else n - 1
        segment = prices[trough_idx : end_idx + 1]
        if len(segment) < min_duration_days:
            continue

        peak_rel = int(np.argmax(segment))
        peak_idx = trough_idx + peak_rel
        trough_price = float(prices[trough_idx])
        peak_price = float(prices[peak_idx])
        if trough_price <= 0:
            continue

        rise_pct = (peak_price - trough_price) / trough_price * 100
        duration_days = peak_idx - trough_idx
        if rise_pct < min_rise_pct or duration_days < min_duration_days:
            continue

        leg = prices[trough_idx : peak_idx + 1]
        slope_daily = _segment_slope_pct_per_day(leg)
        cycles.append(
            {
                "cycle_number": len(cycles) + 1,
                "trough_idx": int(trough_idx),
                "peak_idx": int(peak_idx),
                "rise_pct": float(rise_pct),
                "duration_days": int(duration_days),
                "angle_pct_per_day": float(slope_daily),
                "angle_pct_per_month": float(slope_daily * 30),
                "days_ago_trough": int(n - 1 - trough_idx),
            }
        )

    return cycles[-8:]


def _match_current_to_historical_cycles(cycles: list, current_angle_pct_per_month: float) -> Optional[dict]:
    if not cycles:
        return None

    best = min(cycles, key=lambda c: abs(c["angle_pct_per_month"] - current_angle_pct_per_month))
    diff = abs(best["angle_pct_per_month"] - current_angle_pct_per_month)
    spread = max(abs(c["angle_pct_per_month"]) for c in cycles) or 5.0
    similarity = max(0.0, min(100.0, 100.0 - (diff / max(spread, 5.0)) * 100.0))

    return {
        "matched_cycle_number": int(best["cycle_number"]),
        "total_cycles": len(cycles),
        "similarity_pct": round(similarity, 1),
        "current_angle_pct_per_month": round(current_angle_pct_per_month, 2),
        "current_angle_pct_per_day": round(current_angle_pct_per_month / 30.0, 4),
        "matched_angle_pct_per_month": round(best["angle_pct_per_month"], 2),
        "matched_rise_pct": round(best["rise_pct"], 1),
        "matched_duration_days": int(best["duration_days"]),
        "summary": (
            f"현재 회귀각 {current_angle_pct_per_month:+.1f}%/월 → "
            f"과거 {best['cycle_number']}번째 상승 사이클({best['angle_pct_per_month']:+.1f}%/월, "
            f"유사도 {similarity:.0f}%)과 가장 유사"
        ),
        "historical_cycles": [
            {
                "cycle_number": c["cycle_number"],
                "angle_pct_per_month": round(c["angle_pct_per_month"], 2),
                "angle_pct_per_day": round(c["angle_pct_per_day"], 4),
                "rise_pct": round(c["rise_pct"], 1),
                "duration_days": c["duration_days"],
            }
            for c in cycles
        ],
    }


class MarketRegimeDetector:
    """
    일봉·주봉·월봉과 코인별 회기(회귀) 앵커를 복합 분석하여 장세를 진단합니다.

    - BEAR: 고점 대비 의미 있는 하락(주봉 하락 추세) — 바닥 횡보 전까지 유지
    - RANGE: 회기 앵커(400/800일 전 가격대) 부근에서 하락 둔화·횡보
    - BULL: 월봉 회복 구간 진입 + 중단기 추세 우상향
    """

    def __init__(self, candle_manager, long_term_ma_period=None, bull_duration_limit_days=100, bear_duration_limit_days=200):
        self.candle_manager = candle_manager
        self.ticker = getattr(candle_manager, "ticker", "BTC")
        profile = TICKER_CYCLE_PROFILE.get(self.ticker, TICKER_CYCLE_PROFILE["BTC"])

        self.regression_days = long_term_ma_period or profile["regression_days"]
        if long_term_ma_period and profile.get("regression_days") == profile.get("cycle_anchor_days"):
            self.cycle_anchor_days = int(long_term_ma_period)
        else:
            self.cycle_anchor_days = profile["cycle_anchor_days"]
        self.recovery_days = profile["recovery_days"]
        self.peak_lookback_days = profile["peak_lookback_days"]
        self.bear_drawdown_pct = profile["bear_drawdown_pct"]
        self.bottoming_anchor_band_pct = profile["bottoming_anchor_band_pct"]

        self.long_term_ma_period = self.regression_days
        self.bull_duration_limit_days = bull_duration_limit_days
        self.bear_duration_limit_days = bear_duration_limit_days

    def sync_regression_period(self, days: int):
        """설정 파일에서 회귀 기간 변경 시 동기화."""
        if days and days > 0:
            self.regression_days = int(days)
            self.long_term_ma_period = int(days)
            profile = TICKER_CYCLE_PROFILE.get(self.ticker, TICKER_CYCLE_PROFILE["BTC"])
            if profile.get("regression_days") == profile.get("cycle_anchor_days"):
                self.cycle_anchor_days = int(days)

    def detect_regime(self) -> str:
        return self.detect_regime_detailed()["regime"]

    def _analyze_monthly(self, m_prices: list) -> dict:
        if len(m_prices) < 6:
            return {
                "phase": "unknown",
                "months_since_peak": 0,
                "monthly_drawdown_pct": 0.0,
                "slope_3m_pct": 0.0,
                "slope_6m_pct": 0.0,
                "recovery_progress_pct": 0.0,
            }

        price_now = float(m_prices[-1])
        lookback = min(len(m_prices), 48)
        window = m_prices[-lookback:]
        peak = max(window)
        peak_idx = window.index(peak)
        months_since_peak = lookback - 1 - peak_idx
        monthly_dd = (price_now - peak) / peak * 100 if peak > 0 else 0.0

        slope_3m = _slope_rate_pct(m_prices, min(3, len(m_prices)), price_now) * 30
        slope_6m = _slope_rate_pct(m_prices, min(6, len(m_prices)), price_now) * 30

        recovery_progress = min(100.0, months_since_peak / max(self.recovery_days / 30.0, 1) * 100)

        if monthly_dd <= -self.bear_drawdown_pct * 0.6 and slope_3m < -1.0:
            phase = "decline"
        elif abs(monthly_dd) <= self.bear_drawdown_pct * 0.5 and abs(slope_3m) <= 2.0 and abs(slope_6m) <= 1.5:
            phase = "bottom"
        elif slope_3m > 2.0 and slope_6m > 0:
            phase = "recovery" if recovery_progress < 80 else "expansion"
        elif slope_3m > 0 and monthly_dd < -10:
            phase = "early_recovery"
        else:
            phase = "transition"

        return {
            "phase": phase,
            "months_since_peak": int(months_since_peak),
            "monthly_drawdown_pct": float(monthly_dd),
            "slope_3m_pct": float(slope_3m),
            "slope_6m_pct": float(slope_6m),
            "recovery_progress_pct": float(recovery_progress),
        }

    def _is_bottoming(self, slope_rate_90: float, slope_rate_30: float, w_prices: list) -> bool:
        daily_flat = slope_rate_90 >= -0.08 and slope_rate_30 >= -0.15
        if len(w_prices) >= 8:
            recent = w_prices[-8:]
            w_high = max(recent)
            w_low = min(recent)
            w_range = (w_high - w_low) / w_low * 100 if w_low > 0 else 999
            weekly_flat = w_range <= 18.0
            return daily_flat and weekly_flat
        return daily_flat and slope_rate_30 >= -0.05

    def detect_regime_detailed(self) -> dict:
        prices = self.candle_manager.get_prices("D")
        w_prices = self.candle_manager.get_prices("W")
        m_prices = self.candle_manager.get_prices("M")
        n = len(prices)

        min_required = min(180, self.regression_days)
        if n < min_required:
            return {
                "regime": "RANGE",
                "reason": f"일봉 데이터 부족 ({n}/{min_required}). 분석 대기 중.",
                "metrics": self._empty_metrics(),
            }

        actual_period = min(self.regression_days, n)
        price_now = float(prices[-1])

        fair_value, _ = _linear_fair_value(prices, actual_period)
        dev_pct = (price_now - fair_value) / fair_value * 100 if fair_value > 0 else 0.0

        anchor_idx = min(self.cycle_anchor_days, n - 1)
        cycle_anchor_price = float(prices[-1 - anchor_idx])
        anchor_dev_pct = (price_now - cycle_anchor_price) / cycle_anchor_price * 100 if cycle_anchor_price > 0 else 0.0

        peak_lb = min(n, self.peak_lookback_days)
        recent = prices[-peak_lb:]
        peak_price = max(recent)
        peak_index = recent.index(peak_price)
        days_since_peak = peak_lb - 1 - peak_index
        drawdown_pct = (price_now - peak_price) / peak_price * 100 if peak_price > 0 else 0.0

        slope_rate_365 = _slope_rate_pct(prices, min(365, n), price_now)
        slope_rate_180 = _slope_rate_pct(prices, min(180, n), price_now)
        slope_rate_90 = _slope_rate_pct(prices, min(90, n), price_now)
        slope_rate_30 = _slope_rate_pct(prices, min(30, n), price_now)

        weekly_dd = 0.0
        if len(w_prices) >= 12:
            w_peak = max(w_prices[-12:])
            weekly_dd = (price_now - w_peak) / w_peak * 100 if w_peak > 0 else 0.0

        monthly = self._analyze_monthly(m_prices)
        bottoming = self._is_bottoming(slope_rate_90, slope_rate_30, w_prices)
        at_anchor_zone = abs(anchor_dev_pct) <= self.bottoming_anchor_band_pct

        final_regime = "BEAR"
        reason = ""

        # 1) BULL — 월봉 회복·확장 + 중단기 상승
        if (
            monthly["phase"] in ("recovery", "expansion", "early_recovery")
            and slope_rate_90 > 0.03
            and slope_rate_30 > 0.0
            and drawdown_pct > -self.bear_drawdown_pct
        ):
            final_regime = "BULL"
            reason = (
                f"월봉 {monthly['phase']} 구간(고점 대비 {monthly['monthly_drawdown_pct']:.1f}%, "
                f"회복진행 {monthly['recovery_progress_pct']:.0f}%) + 90일 추세 우상향으로 BULL."
            )
        elif dev_pct >= -5.0 and slope_rate_90 > 0.05 and slope_rate_30 > 0.05 and drawdown_pct > -15:
            final_regime = "BULL"
            reason = (
                f"{self.regression_days}일 회귀선 대비 안정({dev_pct:+.1f}%) + "
                f"중단기 동반 상승으로 BULL."
            )

        # 2) RANGE — 하락 후 회기 앵커 부근 바닥 횡보 (BEAR에서만 전환)
        elif (
            drawdown_pct <= -self.bear_drawdown_pct * 0.5
            and bottoming
            and (at_anchor_zone or monthly["phase"] == "bottom")
        ):
            final_regime = "RANGE"
            reason = (
                f"고점 대비 {drawdown_pct:.1f}% 하락 후 {self.cycle_anchor_days}일 회기 앵커 "
                f"({cycle_anchor_price:,.0f}원, 괴리 {anchor_dev_pct:+.1f}%) 부근 횡보 - 바닥 RANGE."
            )

        # 3) BEAR — 주봉/월봉 하락장 유지 (바닥 확인 전)
        elif (
            drawdown_pct <= -self.bear_drawdown_pct
            or weekly_dd <= -self.bear_drawdown_pct * 0.8
            or monthly["phase"] == "decline"
            or (anchor_dev_pct < -self.bottoming_anchor_band_pct and slope_rate_90 < -0.03)
            or (price_now < cycle_anchor_price * 0.90 and slope_rate_30 < 0)
        ):
            final_regime = "BEAR"
            reason = (
                f"고점({peak_price:,.0f}) 대비 {drawdown_pct:.1f}% 하락, "
                f"{self.cycle_anchor_days}일 회기가 {cycle_anchor_price:,.0f}원 "
                f"(현재 괴리 {anchor_dev_pct:+.1f}%). "
                f"월봉 {monthly['phase']}, 주봉 {weekly_dd:.1f}% - 하락장(BEAR) 유지."
            )

        # 4) 약한 하락/조정 — 회귀선·기울기 종합
        elif slope_rate_90 < -0.05 or slope_rate_30 < -0.12:
            final_regime = "BEAR"
            reason = (
                f"고점 대비 {drawdown_pct:.1f}% 조정 중, 90일 기울기 {slope_rate_90:+.3f}%/일 - BEAR."
            )

        elif bottoming and abs(slope_rate_90) <= 0.08:
            final_regime = "RANGE"
            reason = (
                f"하락 둔화(90일 {slope_rate_90:+.3f}%/일), "
                f"회기 앵커 괴리 {anchor_dev_pct:+.1f}% - 횡보(RANGE)."
            )

        else:
            final_regime = "RANGE"
            reason = (
                f"뚜렷한 방향성 없음 - 괴리 {dev_pct:+.1f}%, "
                f"고점 대비 {drawdown_pct:.1f}%, 월봉 {monthly['phase']} → RANGE."
            )

        bull_cycles = _detect_historical_bull_cycles(prices)
        current_angle_monthly = _current_cycle_angle_pct_per_month(prices, self.regression_days)
        if final_regime == "BULL":
            alt = slope_rate_90 * 30
            if abs(alt) > abs(current_angle_monthly):
                current_angle_monthly = alt
        cycle_match = _match_current_to_historical_cycles(bull_cycles, current_angle_monthly)

        if cycle_match:
            reason = f"{reason} | {cycle_match['summary']}"

        print(
            f"[RegimeDetector] {self.ticker} | 가격: {price_now:,.0f} | "
            f"회귀{self.regression_days}d: {fair_value:,.0f} ({dev_pct:+.1f}%) | "
            f"앵커{self.cycle_anchor_days}d: {cycle_anchor_price:,.0f} ({anchor_dev_pct:+.1f}%) | "
            f"고점대비: {drawdown_pct:.1f}% | 월봉: {monthly['phase']} | "
            f"판정: {final_regime} - {reason}"
        )

        return {
            "regime": final_regime,
            "reason": reason,
            "cycle_match": cycle_match,
            "metrics": {
                "days_since_peak": int(days_since_peak),
                "deviation_pct": float(dev_pct),
                "anchor_deviation_pct": float(anchor_dev_pct),
                "drawdown_from_peak_pct": float(drawdown_pct),
                "cycle_anchor_price": float(cycle_anchor_price),
                "slope_pct": float(slope_rate_90),
                "slope_rate_365": float(slope_rate_365),
                "slope_rate_180": float(slope_rate_180),
                "slope_rate_90": float(slope_rate_90),
                "slope_rate_30": float(slope_rate_30),
                "fair_value": float(fair_value),
                "weekly_drawdown_pct": float(weekly_dd),
                "monthly_phase": monthly["phase"],
                "monthly_drawdown_pct": monthly["monthly_drawdown_pct"],
                "recovery_progress_pct": monthly["recovery_progress_pct"],
                "regression_days": int(self.regression_days),
                "cycle_anchor_days": int(self.cycle_anchor_days),
                "current_angle_pct_per_month": float(current_angle_monthly),
                "cycle_match": cycle_match,
                "historical_cycles": cycle_match.get("historical_cycles", []) if cycle_match else [],
            },
        }

    @staticmethod
    def _empty_metrics() -> dict:
        return {
            "days_since_peak": 0,
            "deviation_pct": 0.0,
            "anchor_deviation_pct": 0.0,
            "drawdown_from_peak_pct": 0.0,
            "cycle_anchor_price": 0.0,
            "slope_pct": 0.0,
            "slope_rate_365": 0.0,
            "slope_rate_180": 0.0,
            "slope_rate_90": 0.0,
            "slope_rate_30": 0.0,
            "fair_value": 0.0,
            "weekly_drawdown_pct": 0.0,
            "monthly_phase": "unknown",
            "monthly_drawdown_pct": 0.0,
            "recovery_progress_pct": 0.0,
            "regression_days": 0,
            "cycle_anchor_days": 0,
            "current_angle_pct_per_month": 0.0,
            "cycle_match": None,
            "historical_cycles": [],
        }


if __name__ == "__main__":
    class DummyCandleManager:
        def __init__(self, daily, weekly=None, monthly=None, ticker="BTC"):
            self.prices_d = daily
            self.prices_w = weekly or daily[-52:]
            self.prices_m = monthly or daily[-24:]
            self.ticker = ticker

        def get_prices(self, tf):
            if tf == "D":
                return self.prices_d
            if tf == "W":
                return self.prices_w
            if tf == "M":
                return self.prices_m
            return self.prices_d

    n = 1200
    peak_at = 900
    daily = [50_000_000 + i * 20_000 for i in range(peak_at)]
    daily += [daily[-1] * (1 - 0.003 * i) for i in range(1, n - peak_at + 1)]
    det = MarketRegimeDetector(DummyCandleManager(daily, ticker="BTC"), long_term_ma_period=800)
    print("하락 시나리오:", det.detect_regime())

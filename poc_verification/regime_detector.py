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
        "bear_monthly_peak_lookback": 5,
        "bear_monthly_peak_gap": 3,
        "bear_max_episodes": 10,
        "bear_min_drawdown_mult": 0.6,
    },
    "ETH": {
        "regression_days": 400,
        "cycle_anchor_days": 400,
        "recovery_days": 365,
        "peak_lookback_days": 730,
        "bear_drawdown_pct": 25.0,
        "bottoming_anchor_band_pct": 12.0,
        "bear_monthly_peak_lookback": 4,
        "bear_monthly_peak_gap": 3,
        "bear_max_episodes": 8,
        "bear_min_drawdown_mult": 0.55,
    },
    "XRP": {
        "regression_days": 400,
        "cycle_anchor_days": 400,
        "recovery_days": 1095,
        "peak_lookback_days": 1460,
        "bear_drawdown_pct": 30.0,
        "bottoming_anchor_band_pct": 15.0,
        "bear_monthly_peak_lookback": 4,
        "bear_monthly_peak_gap": 4,
        "bear_max_episodes": 8,
        "bear_min_drawdown_mult": 0.5,
    },
    # KRW-USDT: 원화 환율 연동 스테이블코인 (1 USDT ≒ 1 USD) — 400일 회귀·사이클
    "USDT": {
        "regression_days": 400,
        "cycle_anchor_days": 400,
        "recovery_days": 90,
        "peak_lookback_days": 400,
        "bear_drawdown_pct": 3.0,
        "bottoming_anchor_band_pct": 2.0,
    },
}

STABLECOIN_TICKERS = frozenset({"USDT"})


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


def detect_historical_bear_episodes(
    prices: list,
    dates: list = None,
    min_drawdown_pct: float = 20.0,
    min_duration_days: int = 30,
    peak_lookback: int = 120,
    recovery_ratio: float = 0.382,
    min_peak_gap_days: int = 90,
    max_episodes: int = 10,
    min_data_points: int = 365,
) -> list:
    """
    일봉 종가에서 고점→저점 하락 레그를 추출합니다.
    BTC 월봉 차트의 역사적 하락 화살표(약 8회)에 대응하는 구간을 BEAR 학습·비교에 사용합니다.
    """
    n = len(prices)
    if n < min_data_points:
        return []

    forward_window = min(60, max(12, n // 8))
    min_future_len = min(15, max(3, forward_window // 4))
    tail_guard = max(3, min(20, forward_window // 4))

    peaks = []
    for i in range(peak_lookback, n - tail_guard):
        window = prices[i - peak_lookback : i + 1]
        if prices[i] != max(window):
            continue
        future = prices[i : min(i + forward_window, n)]
        if len(future) < min_future_len:
            continue
        trough_future = min(future)
        if trough_future <= 0 or prices[i] <= 0:
            continue
        prelim_dd = (prices[i] - trough_future) / prices[i] * 100
        if prelim_dd < min_drawdown_pct * 0.45:
            continue
        if peaks and i - peaks[-1] < min_peak_gap_days:
            if prices[i] > prices[peaks[-1]]:
                peaks[-1] = i
            continue
        peaks.append(i)

    episodes = []
    for pi, peak_idx in enumerate(peaks):
        seg = prices[peak_idx:]
        if len(seg) < min_duration_days:
            continue
        trough_rel = int(np.argmin(seg))
        trough_idx = peak_idx + trough_rel
        peak_p = float(prices[peak_idx])
        trough_p = float(prices[trough_idx])
        if peak_p <= 0:
            continue
        dd = (peak_p - trough_p) / peak_p * 100
        if dd < min_drawdown_pct:
            continue

        recovery_target = trough_p + (peak_p - trough_p) * recovery_ratio
        end_idx = trough_idx
        next_peak = peaks[pi + 1] if pi + 1 < len(peaks) else n - 1
        limit = min(next_peak, peak_idx + 800, n - 1)
        for j in range(trough_idx + 1, limit + 1):
            end_idx = j
            if prices[j] >= recovery_target:
                break

        if end_idx - peak_idx < min_duration_days:
            continue

        start_date = dates[peak_idx] if dates else str(peak_idx)
        end_date = dates[end_idx] if dates else str(end_idx)
        episodes.append(
            {
                "episode_number": len(episodes) + 1,
                "start_date": start_date,
                "end_date": end_date,
                "peak_date": start_date,
                "trough_date": dates[trough_idx] if dates else str(trough_idx),
                "peak_price": round(peak_p, 2),
                "trough_price": round(trough_p, 2),
                "drawdown_pct": round(dd, 1),
                "duration_days": int(end_idx - peak_idx),
            }
        )

    episodes.sort(key=lambda e: e["start_date"])
    major = [e for e in episodes if e["drawdown_pct"] >= min_drawdown_pct]
    if len(major) >= 3:
        episodes = major

    if len(episodes) > max_episodes:
        by_dd = sorted(episodes, key=lambda e: e["drawdown_pct"], reverse=True)[:max_episodes]
        episodes = sorted(by_dd, key=lambda e: e["start_date"])

    for i, ep in enumerate(episodes):
        ep["episode_number"] = i + 1

    return episodes


def get_bear_pattern_params(ticker: str) -> dict:
    """코인별 월봉 하락 패턴 감지·학습 파라미터."""
    profile = TICKER_CYCLE_PROFILE.get(ticker, TICKER_CYCLE_PROFILE["BTC"])
    mult = float(profile.get("bear_min_drawdown_mult", 0.6))
    return {
        "min_drawdown_pct": max(10.0, float(profile["bear_drawdown_pct"]) * mult),
        "peak_lookback": int(profile.get("bear_monthly_peak_lookback", 5)),
        "min_peak_gap_days": int(profile.get("bear_monthly_peak_gap", 3)),
        "max_episodes": int(profile.get("bear_max_episodes", 10)),
        "bottoming_band_pct": float(profile.get("bottoming_anchor_band_pct", 15.0)),
        "bear_drawdown_pct": float(profile["bear_drawdown_pct"]),
    }


def _monthly_slope_pct(m_prices: list, months: int = 3) -> float:
    """월봉 종가 기준 N개월 변화율(%)."""
    n = len(m_prices)
    lb = min(months, n - 1)
    if lb < 1:
        return 0.0
    old = float(m_prices[-lb - 1])
    now = float(m_prices[-1])
    if old <= 0:
        return 0.0
    return (now - old) / old * 100


def detect_active_monthly_bear_pattern(m_prices: list, ticker: str = "BTC") -> dict:
    """
    현재 월봉이 고점→하락 패턴 중인지 판별.
    과거에 표시한 화살표와 동일 규칙으로 이후에도 유사 패턴이면 BEAR로 본다.
    """
    params = get_bear_pattern_params(ticker)
    min_dd = params["min_drawdown_pct"]
    n = len(m_prices)
    if n < 8:
        return {
            "active": False,
            "months_since_peak": 0,
            "drawdown_from_peak_pct": 0.0,
            "slope_3m_pct": 0.0,
            "peak_price": 0.0,
            "peak_date_idx": -1,
        }

    lookback = min(24, n)
    window = m_prices[-lookback:]
    peak = float(max(window))
    peak_idx_rel = window.index(peak)
    peak_idx = n - lookback + peak_idx_rel
    months_since_peak = lookback - 1 - peak_idx_rel
    price_now = float(m_prices[-1])
    dd = (price_now - peak) / peak * 100 if peak > 0 else 0.0
    slope_3m = _monthly_slope_pct(m_prices, 3)

    still_falling = slope_3m < -3.0 or dd <= -min_dd * 0.45
    active = (
        months_since_peak <= 14
        and dd <= -min_dd * 0.35
        and still_falling
    )

    return {
        "active": active,
        "months_since_peak": int(months_since_peak),
        "drawdown_from_peak_pct": round(dd, 1),
        "slope_3m_pct": round(slope_3m, 2),
        "peak_price": peak,
        "peak_date_idx": int(peak_idx),
    }


def _estimate_rising_bottom_price(episodes: list) -> Optional[float]:
    """역사 하락 에피소드 저점(바닥선) 추세로 현재 예상 지지가 추정."""
    troughs = [float(ep["trough_price"]) for ep in (episodes or []) if ep.get("trough_price")]
    if not troughs:
        return None
    if len(troughs) == 1:
        return troughs[0]
    recent = troughs[-3:]
    x = np.arange(len(recent))
    slope, intercept = np.polyfit(x, recent, 1)
    projected = float(slope * len(recent) + intercept)
    return max(projected, recent[-1] * 0.85)


def _count_recent_episode_peaks(episodes: list, month_keys: list, trailing: int = 12) -> int:
    if not episodes or not month_keys:
        return 0
    cutoff = str(month_keys[-trailing])[:7] if len(month_keys) >= trailing else str(month_keys[0])[:7]
    return sum(1 for e in episodes if str(e.get("peak_date", ""))[:7] >= cutoff)


def assess_bear_entry_timing(
    m_prices: list,
    episodes: list,
    ticker: str = "BTC",
    month_keys: list = None,
) -> dict:
    """
    월봉 하락 패턴 대비 진입 적합도.
    - BTC: 고점 하락 중 → 단타(빠른 익절) 구간
    - ETH: 바닥선 도달 → 스윙 진입 유리
    - XRP: 바닥 접근 중 → 1~2개월 후 진입 유리
    """
    profile = TICKER_CYCLE_PROFILE.get(ticker, TICKER_CYCLE_PROFILE["BTC"])
    band = float(profile.get("bottoming_anchor_band_pct", 15.0))
    active = detect_active_monthly_bear_pattern(m_prices, ticker)
    price_now = float(m_prices[-1]) if m_prices else 0.0

    projected_bottom = _estimate_rising_bottom_price(episodes)
    dist_to_bottom_pct = None
    if projected_bottom and projected_bottom > 0:
        dist_to_bottom_pct = (price_now - projected_bottom) / projected_bottom * 100

    slope_3m = active["slope_3m_pct"]
    dd = active["drawdown_from_peak_pct"]
    bear_dd_thresh = float(profile["bear_drawdown_pct"])
    timeline = month_keys or [str(i) for i in range(len(m_prices))]
    recent_peak_count = _count_recent_episode_peaks(episodes, timeline, trailing=12)
    at_bottom_line = dist_to_bottom_pct is not None and abs(dist_to_bottom_pct) <= band

    phase = "transition"
    entry_style = "cautious_swing"
    entry_score = 40.0
    summary = "월봉 하락 패턴 분석 중"

    # 바닥선 부근 + 최근 복수 고점 연쇄 하락 → 단타 (BTC형 2025 연속 고점)
    if at_bottom_line and recent_peak_count >= 2 and active["months_since_peak"] <= 10:
        phase = "active_decline"
        entry_style = "scalp_fast"
        entry_score = max(18.0, 42.0 - abs(dd) * 0.25)
        summary = (
            f"바닥선 부근이나 최근 고점 {recent_peak_count}회 연쇄 하락 — "
            f"단타·빠른 익절 구간(BTC형)"
        )
    # 바닥선 도달 → 스윙 진입 (ETH형)
    elif at_bottom_line:
        phase = "bottom_zone"
        entry_style = "swing_bottom"
        entry_score = 82.0 if slope_3m >= -5.0 else 68.0
        summary = f"예상 바닥선({projected_bottom:,.0f}원) 부근 — ETH형 바닥 진입 구간"
    elif not active["active"] and (dist_to_bottom_pct is None or dist_to_bottom_pct <= band * 1.5):
        phase = "bottom_zone"
        entry_style = "swing_bottom"
        entry_score = 78.0
        summary = "하락 레그 종료·바닥권 — 스윙 진입 유리"
    # 고점 하락 진행 중 → 단타 우선 (BTC형)
    elif active["active"] and dd <= -bear_dd_thresh * 0.35 and slope_3m < -3.0:
        phase = "active_decline"
        entry_style = "scalp_fast"
        entry_score = max(15.0, 45.0 - abs(dd) * 0.3)
        summary = f"월봉 고점 대비 {dd:.1f}% 하락 중 — 단타·빠른 익절 구간(BTC형)"
    elif dist_to_bottom_pct is not None and dist_to_bottom_pct <= band * 2.5 and slope_3m > -8.0:
        phase = "approaching_bottom"
        entry_style = "cautious_swing"
        eta_months = max(1, int(dist_to_bottom_pct / max(band, 5)))
        entry_score = max(35.0, 70.0 - dist_to_bottom_pct * 1.2)
        summary = f"바닥선 접근 중(괴리 {dist_to_bottom_pct:+.1f}%) — 약 {eta_months}개월 후 진입 유리(XRP형)"
    elif dist_to_bottom_pct is not None and dist_to_bottom_pct > band * 2.5 and slope_3m < -2.0:
        phase = "far_from_bottom"
        entry_style = "wait"
        entry_score = max(8.0, 25.0 - dist_to_bottom_pct * 0.2)
        summary = f"바닥선 대비 {dist_to_bottom_pct:+.1f}% 위 — 하락 여지, 진입 대기"
    elif active["active"]:
        phase = "early_decline"
        entry_style = "scalp_fast"
        entry_score = 35.0
        summary = f"월봉 하락 초기(고점 대비 {dd:.1f}%) — 보수적 단타"

    return {
        "phase": phase,
        "entry_style": entry_style,
        "entry_score": round(entry_score, 1),
        "bottom_proximity_pct": round(max(0.0, 100.0 - abs(dist_to_bottom_pct or 50.0)), 1),
        "projected_bottom_price": projected_bottom,
        "dist_to_bottom_pct": round(dist_to_bottom_pct, 1) if dist_to_bottom_pct is not None else None,
        "pattern_active": active["active"],
        "drawdown_from_peak_pct": dd,
        "summary": summary,
    }


def build_current_bear_episode(
    m_prices: list,
    month_dates: list,
    ticker: str = "BTC",
) -> Optional[dict]:
    """진행 중인 월봉 하락 레그를 학습용 에피소드로 생성."""
    active = detect_active_monthly_bear_pattern(m_prices, ticker)
    if not active["active"] or active["peak_date_idx"] < 0:
        return None

    peak_idx = active["peak_date_idx"]
    seg = m_prices[peak_idx:]
    if len(seg) < 2:
        return None
    trough_rel = int(np.argmin(seg))
    trough_idx = peak_idx + trough_rel
    peak_p = float(m_prices[peak_idx])
    trough_p = float(m_prices[trough_idx])
    price_now = float(m_prices[-1])
    dd = (peak_p - trough_p) / peak_p * 100 if peak_p > 0 else 0.0

    start_date = month_dates[peak_idx] if month_dates else str(peak_idx)
    end_date = month_dates[-1] if month_dates else str(len(m_prices) - 1)

    return {
        "episode_number": 0,
        "start_date": start_date,
        "end_date": end_date,
        "peak_date": start_date,
        "trough_date": month_dates[trough_idx] if month_dates else str(trough_idx),
        "peak_price": round(peak_p, 2),
        "trough_price": round(trough_p, 2),
        "drawdown_pct": round(dd, 1),
        "duration_days": len(seg) - 1,
        "ongoing": True,
        "current_price": round(price_now, 2),
    }


def analyze_bear_pattern_context(
    m_prices: list,
    ticker: str = "BTC",
    month_dates: list = None,
) -> dict:
    """
    역사적 하락 에피소드 + 현재 월봉 패턴 + 진입 적합도 통합 분석.
    백테스트 학습·실시간 BEAR 판정·진입 게이트에 공통 사용.
    """
    params = get_bear_pattern_params(ticker)
    if month_dates is None:
        month_dates = [str(i) for i in range(len(m_prices))]

    episodes = detect_historical_bear_episodes(
        m_prices,
        month_dates,
        min_drawdown_pct=params["min_drawdown_pct"],
        min_duration_days=2,
        peak_lookback=params["peak_lookback"],
        min_peak_gap_days=params["min_peak_gap_days"],
        min_data_points=24,
        max_episodes=params["max_episodes"],
    )

    current_ep = build_current_bear_episode(m_prices, month_dates, ticker)
    if current_ep:
        peak_ym = current_ep["peak_date"][:7]
        if not any(ep.get("peak_date", "")[:7] == peak_ym for ep in episodes):
            current_ep["episode_number"] = len(episodes) + 1
            episodes.append(current_ep)

    active = detect_active_monthly_bear_pattern(m_prices, ticker)
    entry_timing = assess_bear_entry_timing(m_prices, episodes, ticker, month_dates)

    matched = None
    if episodes and active["active"]:
        dd_now = abs(active["drawdown_from_peak_pct"])
        best = min(episodes, key=lambda e: abs(e["drawdown_pct"] - dd_now))
        diff = abs(best["drawdown_pct"] - dd_now)
        similarity = max(0.0, min(100.0, 100.0 - diff * 2.5))
        matched = {
            "matched_episode_number": best["episode_number"],
            "total_episodes": len(episodes),
            "similarity_pct": round(similarity, 1),
            "matched_drawdown_pct": best["drawdown_pct"],
            "summary": (
                f"현재 월봉 하락 {dd_now:.1f}% → 과거 #{best['episode_number']}번 패턴"
                f"({best['drawdown_pct']:.1f}%, 유사도 {similarity:.0f}%)"
            ),
        }

    return {
        "pattern_active": active["active"] or entry_timing["phase"] in (
            "active_decline", "early_decline", "approaching_bottom"
        ),
        "historical_episodes": episodes,
        "episode_count": len(episodes),
        "current_episode": current_ep,
        "active_pattern": active,
        "entry_timing": entry_timing,
        "pattern_match": matched,
        "pattern_summary": entry_timing["summary"],
    }


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

    def _detect_stablecoin_regime(self, prices: list, n: int) -> dict:
        """USDT 등 스테이블코인: 페그(원화 환율) 대비 미세 괴리로 장세 판정."""
        min_required = min(30, self.regression_days)
        if n < min_required:
            return {
                "regime": "RANGE",
                "reason": f"스테이블코인 데이터 부족 ({n}/{min_required}). 페그 주변 횡보(RANGE) 대기.",
                "cycle_match": None,
                "metrics": self._empty_metrics(),
            }

        actual_period = min(self.regression_days, n)
        price_now = float(prices[-1])
        fair_value, _ = _linear_fair_value(prices, actual_period)
        dev_pct = (price_now - fair_value) / fair_value * 100 if fair_value > 0 else 0.0

        peak_lb = min(n, self.peak_lookback_days)
        peak_price = max(prices[-peak_lb:])
        drawdown_pct = (price_now - peak_price) / peak_price * 100 if peak_price > 0 else 0.0

        if dev_pct <= -self.bear_drawdown_pct or drawdown_pct <= -self.bear_drawdown_pct:
            regime = "BEAR"
            reason = (
                f"스테이블코인 페그 이탈(회귀 {dev_pct:+.2f}%, 고점대비 {drawdown_pct:.2f}%) — "
                f"할인 구간, 단기 반등 스캘핑(BEAR)."
            )
        elif dev_pct >= self.bottoming_anchor_band_pct:
            regime = "BULL"
            reason = (
                f"스테이블코인 프리미엄(회귀 {dev_pct:+.2f}%) — "
                f"환율 상단, 보유·미세 익절(BULL)."
            )
        else:
            regime = "RANGE"
            reason = (
                f"스테이블코인 페그 안정(회귀 {dev_pct:+.2f}%, 고점대비 {drawdown_pct:.2f}%) — "
                f"1 USDT ≈ 1 USD 대피·횡보(RANGE)."
            )

        metrics = self._empty_metrics()
        metrics.update({
            "deviation_pct": float(dev_pct),
            "drawdown_from_peak_pct": float(drawdown_pct),
            "fair_value": float(fair_value),
            "regression_days": int(self.regression_days),
            "cycle_anchor_days": int(self.cycle_anchor_days),
        })

        lookback = min(400, n)
        current_angle_monthly = _current_cycle_angle_pct_per_month(prices, lookback)
        bull_cycles = _detect_historical_bull_cycles(prices)
        cycle_match = _match_current_to_historical_cycles(bull_cycles, current_angle_monthly)
        if cycle_match:
            reason = f"{reason} | {cycle_match['summary']}"
        metrics["current_angle_pct_per_month"] = float(current_angle_monthly)
        if cycle_match:
            metrics["cycle_match"] = cycle_match
            metrics["historical_cycles"] = cycle_match.get("historical_cycles", [])

        print(
            f"[RegimeDetector] {self.ticker} | 가격: {price_now:,.2f} | "
            f"페그회귀{self.regression_days}d: {fair_value:,.2f} ({dev_pct:+.2f}%) | "
            f"400d각도: {current_angle_monthly:+.2f}%/월 | "
            f"판정: {regime} - {reason}"
        )

        return {
            "regime": regime,
            "reason": reason,
            "cycle_match": cycle_match,
            "metrics": metrics,
        }

    def detect_regime_detailed(self) -> dict:
        regime_series = {}
        if hasattr(self.candle_manager, "get_regime_series"):
            regime_series = self.candle_manager.get_regime_series() or {}

        prices = regime_series.get("daily_prices") or self.candle_manager.get_prices("D")
        w_prices = regime_series.get("weekly_prices") or self.candle_manager.get_prices("W")
        m_prices = regime_series.get("monthly_prices") or self.candle_manager.get_prices("M")
        m_month_dates = list(regime_series.get("month_dates") or [])
        if not m_month_dates and m_prices:
            m_month_dates = [str(i) for i in range(len(m_prices))]
        n = len(prices)

        if self.ticker in STABLECOIN_TICKERS:
            return self._detect_stablecoin_regime(prices, n)

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

        bear_ctx = analyze_bear_pattern_context(m_prices, self.ticker, m_month_dates)

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
            or bear_ctx["pattern_active"]
            or (anchor_dev_pct < -self.bottoming_anchor_band_pct and slope_rate_90 < -0.03)
            or (price_now < cycle_anchor_price * 0.90 and slope_rate_30 < 0)
        ):
            final_regime = "BEAR"
            timing = bear_ctx.get("entry_timing", {})
            reason = (
                f"고점({peak_price:,.0f}) 대비 {drawdown_pct:.1f}% 하락, "
                f"{self.cycle_anchor_days}일 회기가 {cycle_anchor_price:,.0f}원 "
                f"(현재 괴리 {anchor_dev_pct:+.1f}%). "
                f"월봉 {monthly['phase']}, 역사 하락패턴 {bear_ctx['episode_count']}회 — "
                f"{timing.get('summary', '하락장(BEAR)')}"
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
        if bear_ctx.get("pattern_match"):
            reason = f"{reason} | {bear_ctx['pattern_match']['summary']}"

        print(
            f"[RegimeDetector] {self.ticker} | 가격: {price_now:,.0f} | "
            f"회귀{self.regression_days}d: {fair_value:,.0f} ({dev_pct:+.1f}%) | "
            f"앵커{self.cycle_anchor_days}d: {cycle_anchor_price:,.0f} ({anchor_dev_pct:+.1f}%) | "
            f"고점대비: {drawdown_pct:.1f}% | 월봉: {monthly['phase']} | "
            f"하락패턴: {bear_ctx['episode_count']}회, 진입점수 {bear_ctx['entry_timing']['entry_score']:.0f} | "
            f"판정: {final_regime} - {reason}"
        )

        return {
            "regime": final_regime,
            "reason": reason,
            "cycle_match": cycle_match,
            "bear_pattern": bear_ctx,
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
                "bear_episode_count": int(bear_ctx["episode_count"]),
                "bear_entry_score": float(bear_ctx["entry_timing"]["entry_score"]),
                "bear_entry_style": bear_ctx["entry_timing"]["entry_style"],
                "bear_entry_phase": bear_ctx["entry_timing"]["phase"],
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

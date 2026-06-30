"""
backtester.py — 9년 실 데이터 기반 자동 파라미터 최적화 모듈
=====================================================================
■ 데이터:    빗썸 15분봉만 수집·캐시 → 일/주/월은 집계로 파생 (별도 일·주·월 API 없음)
■ 최초 수집: 9년치 15분봉 전체, 이후 매일 96개만 추가(증분)
■ 최적화:   BULL/BEAR/RANGE × 지표 7조합 × Logic 4종 × 리스크 기법/비율 그리드
■ 기준:     총수익·보유기간 우선 profit_score + MDD ≤ 50% 필터
■ 수수료:   빗썸 실측 편도 0.25% (왕복 0.50%)
"""

import os
import json
import time
import math
import logging
import calendar
import requests
import sys
import io
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable

# Windows 터미널 인코딩 오류 방지 (UTF-8 강제 지정)
if sys.platform.startswith('win'):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    except Exception:
        pass

import numpy as np

try:
    import cupy as cp
    HAS_GPU = True
except ImportError:
    HAS_GPU = False

xp = cp if HAS_GPU else np

try:
    import pandas as pd
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    from regime_detector import (
        analyze_bear_pattern_context,
        detect_historical_bear_episodes,
        get_bear_pattern_params,
        TICKER_CYCLE_PROFILE,
    )
    HAS_REGIME_DETECTOR = True
except ImportError:
    HAS_REGIME_DETECTOR = False
    TICKER_CYCLE_PROFILE = {"BTC": {"bear_drawdown_pct": 25.0, "peak_lookback_days": 1095}}

try:
    from usdt_fx_historical import (
        enrich_bars_with_historical_fx,
        ensure_daily_fx_for_bars,
        ensure_fx_history,
        usdt_fx_buy_allowed,
        usdt_fx_entry_allowed,
        usdt_fx_exit_fusion,
        USDT_DEFAULT_ENTRY_GAP_KRW,
        USDT_DEFAULT_EXIT_GAP_KRW,
        USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT,
        USDT_MIN_ENTRY_GAP_FLOOR,
        USDT_MIN_EXIT_GAP_FLOOR,
        USDT_MIN_EXIT_TP_FLOOR,
        usdt_fx_exit_allowed,
        usdt_fx_net_pnl_pct,
        save_usdt_optimal_defaults,
        refresh_usdt_defaults,
    )
    HAS_USDT_FX_HIST = True
except ImportError:
    HAS_USDT_FX_HIST = False
    USDT_DEFAULT_ENTRY_GAP_KRW = 30
    USDT_DEFAULT_EXIT_GAP_KRW = 10
    USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT = 3.0
    USDT_MIN_ENTRY_GAP_FLOOR = 30
    USDT_MIN_EXIT_GAP_FLOOR = 10
    USDT_MIN_EXIT_TP_FLOOR = 2.0

    def save_usdt_optimal_defaults(params, result=None):
        return params

    def refresh_usdt_defaults(force=False):
        return {}

USDT_MIN_TARGET_PROFIT_PCT = USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT  # 탈출 TP 기본·하한 참조
USDT_MIN_CONSIDER_GAP_FLOOR = USDT_MIN_ENTRY_GAP_FLOOR

# CustomBear 하락장(BTC·ETH·XRP) — 목표%·학습보유시간은 백테스트 최적화값
BEAR_TARGET_PROFIT_FALLBACK = 0.012
BEAR_OPTIMAL_HOLD_MIN_H = 2.0
BEAR_OPTIMAL_HOLD_FALLBACK_H = 48.0
BEAR_TIER_STEP_MULTIPLIERS = (0.9, 0.6, 0.4, 0.2)
BEAR_TIER_FINAL_FLOOR_PCT = 0.0075
USDT_FX_GRID = {
    "min_consider_gap_krw": [30, 35, 40, 45, 50],
    "exit_take_profit_pct": [2.0, 2.5, 3.0],
    "exit_gap_krw": [10, 15, 20, 25],
}

# 지표 매도 최소 순수익 (왕복 수수료 이후) — 조기 소액 익절 억제
SELL_MIN_PROFIT_GRID = [0, 0.005, 0.008, 0.012, 0.018, 0.025]

# 하락장 청산 학습: 수수료 후 순이익 바닥 (실거래 BEAR_LIVE_MIN_NET_PROFIT 과 동일)
BEAR_MIN_NET_PROFIT_FLOOR = 0.005
# BEAR 최적화: 월봉 고점→하락 레그(코인별 역사 패턴 + 진행 중 패턴)만 시뮬레이션·기대수익 비교
BEAR_OPTIM_MAX_TAKE_PROFIT_PCT = 0.025  # 하락장 익절 상한 — 5% 익절은 그리드에서 제외

USDT_FUSION_BB_GRID = {
    "bb_period": [20, 25],
    "bb_std": [2.0, 2.5],
}

logger = logging.getLogger("Backtester")

# ─────────────────────────────────────────────────────────────────────────────
# 상수
# ─────────────────────────────────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))
BITHUMB_BASE = "https://api.bithumb.com"

# 하루 중 수집할 KST 시각 — 15분봉 (매시 0·15·30·45분, 하루 96개)
CANDLE_INTERVAL_MINUTES = 15
CANDLE_INTERVAL_HOURS = CANDLE_INTERVAL_MINUTES / 60.0
CACHE_SCHEMA = "15m_v1"
TARGET_TIMES_KST = [(h, m) for h in range(24) for m in (0, 15, 30, 45)]

# 수수료: 빗썸 실거래 체결 기준 편도 약 0.25% (왕복 0.50%)
FEE_RATE = 0.0025          # 편도 1회 수수료
ROUND_TRIP_FEE = FEE_RATE * 2  # 왕복 0.005


def bear_base_target_profit_pct(params: dict) -> float:
    base = float(params.get("target_profit_pct") or 0)
    if base <= 0:
        return BEAR_TARGET_PROFIT_FALLBACK
    return base


def bear_optimal_hold_hours(params: dict) -> float:
    h = float(params.get("optimal_hold_hours") or 0)
    if h <= 0:
        return BEAR_OPTIMAL_HOLD_FALLBACK_H
    return max(BEAR_OPTIMAL_HOLD_MIN_H, h)


def bear_tier_step_index(hold_hours: float, optimal_h: float) -> int:
    if hold_hours < optimal_h:
        return -1
    return int((hold_hours - optimal_h) / optimal_h)


def bear_tier_target_for_step(base_pct: float, step: int) -> float:
    if step < 0:
        return base_pct
    if step < len(BEAR_TIER_STEP_MULTIPLIERS):
        return max(BEAR_TIER_FINAL_FLOOR_PCT, base_pct * BEAR_TIER_STEP_MULTIPLIERS[step])
    return BEAR_TIER_FINAL_FLOOR_PCT


def bear_tiered_min_net_profit(hold_hours: float, base_pct: float, optimal_hold_hours=None) -> float:
    """최적화 시 optimal_hold_hours=None → base만. 실매매는 학습보유 이후 90→20% 완만 하향."""
    if optimal_hold_hours is None:
        return base_pct
    opt = max(BEAR_OPTIMAL_HOLD_MIN_H, optimal_hold_hours)
    step = bear_tier_step_index(hold_hours, opt)
    return bear_tier_target_for_step(base_pct, step)

# 캐시 파일 경로 (backtester.py 와 같은 폴더)
_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)
CACHE_FILE    = os.path.join(_DIR, "historical_data_cache.json")
RESULT_FILE   = os.path.join(_DIR, "optimized_params.json")
PROGRESS_FILE = os.path.join(_DIR, "backtest_progress.json")

MARKETS = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-USDT"]

# 그리드 서치 파라미터 공간
GRID = {
    "rsi_period":     [5, 7, 10, 14],
    "rsi_oversold":   [20, 25, 30, 35],
    "rsi_overbought": [60, 65, 70, 75],
    "bb_period":      [5, 10, 15, 20],
    "bb_std":         [1.5, 2.0, 2.5],
    "macd_fast":      [5, 8, 12],
    "macd_slow":      [10, 15, 26],
    "macd_signal":    [3, 5, 9],
}

# 허용할 전략 조합 (at least 1 active)
STRATEGY_COMBOS = [
    {"rsi": True,  "bb": False, "macd": False},
    {"rsi": False, "bb": True,  "macd": False},
    {"rsi": False, "bb": False, "macd": True},
    {"rsi": True,  "bb": True,  "macd": False},
    {"rsi": True,  "bb": False, "macd": True},
    {"rsi": False, "bb": True,  "macd": True},
    {"rsi": True,  "bb": True,  "macd": True},
]

# 지표 2개 이상일 때 검색할 합의 Logic (1개만 켠 조합은 OR과 동일하므로 OR만 사용)
LOGIC_TYPES = ["OR", "AND", "VOTE", "WEIGHTED_VOTE"]
DEFAULT_WEIGHTED_THRESHOLD = 0.5

MDD_LIMIT = 0.50  # MDD 허용 한계 50%

# 병렬 최적화: 8 워커 (사용자 지정 속도 개선)
OPTIMIZER_MAX_WORKERS = 8
# Windows 프로세스 spawn 비용(~5초) 때문에 소량 배치는 순차 실행
PARALLEL_MIN_BATCH = 80

_RISK_ONLY_KEYS = frozenset({
    "risk_type", "stop_loss_pct", "take_profit_pct", "trail_pct",
    "trail_activate_profit_pct", "target_regime", "min_strategy_sell_profit_pct",
})


def _iter_trailing_stop_grid():
    """트레일링: N% 수익 달성 후 고점 대비 M% 하락 시 청산 (3%+1% vs 5%+1% 등 학습)."""
    activates = [0.0, 0.03, 0.04, 0.05, 0.06, 0.08]
    trails = [0.008, 0.01, 0.012, 0.015, 0.02]
    for act in activates:
        for trail in trails:
            cfg = {"risk_type": "TrailingStop", "trail_pct": trail}
            if act > 0:
                cfg["trail_activate_profit_pct"] = act
            yield cfg


def _iter_bear_scalp_grid():
    """하락장 BearScalp 그리드 — 익절 상한 BEAR_OPTIM_MAX_TAKE_PROFIT_PCT."""
    tp_levels = [0.006, 0.008, 0.01, 0.012, 0.015, 0.018, 0.022, BEAR_OPTIM_MAX_TAKE_PROFIT_PCT]
    for sl in (0.02, 0.025, 0.03):
        for tp in tp_levels:
            for act in (0.005, 0.006, 0.008, 0.01, 0.012, 0.018):
                for trail in (0.003, 0.004, 0.005, 0.006, 0.008):
                    if act > tp or tp < BEAR_MIN_NET_PROFIT_FLOOR or tp > BEAR_OPTIM_MAX_TAKE_PROFIT_PCT:
                        continue
                    yield {
                        "risk_type": "BearScalp",
                        "stop_loss_pct": sl,
                        "take_profit_pct": max(tp, BEAR_MIN_NET_PROFIT_FLOOR),
                        "trail_activate_profit_pct": max(act, BEAR_MIN_NET_PROFIT_FLOOR),
                        "trail_pct": trail,
                    }


RISK_GRID_RANGE = [
    *_iter_bear_scalp_grid(),
    {"risk_type": "StopLoss", "stop_loss_pct": 0.02, "take_profit_pct": 0.03},
    {"risk_type": "TrailingStop", "trail_activate_profit_pct": 0.008, "trail_pct": 0.005},
    *_iter_trailing_stop_grid(),
]

RISK_GRID_BEAR = [
    *_iter_bear_scalp_grid(),
    {"risk_type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.02},
    {"risk_type": "TrailingStop", "trail_activate_profit_pct": 0.006, "trail_pct": 0.004},
]
RISK_GRID_BULL = [
    {"risk_type": "None"},
    {"risk_type": "StopLoss", "stop_loss_pct": 0.015, "take_profit_pct": 0.04},
    {"risk_type": "StopLoss", "stop_loss_pct": 0.02, "take_profit_pct": 0.05},
    {"risk_type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
    {"risk_type": "StopLoss", "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
    *_iter_trailing_stop_grid(),
]

RISK_GRID_BEAR_RANGE = RISK_GRID_RANGE  # 하위 호환 별칭


def _active_indicator_count(combo: dict) -> int:
    return sum(1 for k in ("rsi", "bb", "macd") if combo.get(k))


def _logics_for_combo(combo: dict) -> list:
    """활성 지표가 1개면 OR만, 2개 이상이면 OR/AND/VOTE/WEIGHTED_VOTE 전부 검색."""
    if _active_indicator_count(combo) <= 1:
        return ["OR"]
    return LOGIC_TYPES


def _resolve_composite_signals(
    buy_signals: list, sell_signals: list, logic: str, threshold: float = DEFAULT_WEIGHTED_THRESHOLD
) -> tuple:
    """실거래 VerboseCompositeStrategy와 동일한 AND/OR/VOTE/WEIGHTED_VOTE 규칙."""
    n = len(buy_signals)
    if n == 0:
        return False, False

    if logic == "AND":
        return all(buy_signals), all(sell_signals)
    if logic == "OR":
        return any(buy_signals), any(sell_signals)
    if logic == "VOTE":
        majority = n // 2 + 1
        return sum(buy_signals) >= majority, sum(sell_signals) >= majority
    if logic == "WEIGHTED_VOTE":
        buy_ratio = sum(1 for x in buy_signals if x) / n
        sell_ratio = sum(1 for x in sell_signals if x) / n
        return buy_ratio >= threshold, sell_ratio >= threshold
    return any(buy_signals), any(sell_signals)


def _iter_risk_configs(target_regime: str):
    """장세별 리스크 기법·비율 조합."""
    if target_regime == "BULL":
        yield from RISK_GRID_BULL
    elif target_regime == "BEAR":
        yield from RISK_GRID_BEAR
    else:
        yield from RISK_GRID_RANGE


def _iter_risk_with_sell_min(target_regime: str):
    """리스크 × 지표매도 최소순익 그리드 (2단계 최적화용)."""
    for risk_cfg in _iter_risk_configs(target_regime):
        for min_s in SELL_MIN_PROFIT_GRID:
            yield {**risk_cfg, "min_strategy_sell_profit_pct": min_s}


def _parse_bar_ts(ts_str: str) -> datetime:
    s = ts_str.replace("Z", "+00:00")
    if "+" not in s[10:] and "T" in s:
        s = s + "+09:00"
    return datetime.fromisoformat(s)


def _filter_regime_recent_bars(regime_data: list, recent_days: int) -> list:
    """장세 필터된 봉 중 최근 recent_days 일만 반환."""
    if not regime_data or recent_days <= 0:
        return regime_data
    try:
        last_ts = _parse_bar_ts(regime_data[-1]["ts"])
        cutoff = last_ts - timedelta(days=recent_days)
        return [d for d in regime_data if _parse_bar_ts(d["ts"]) >= cutoff]
    except Exception:
        return regime_data


from timeframe_aggregate import (
    bar_date_str as _bar_date_str,
    monthly_closes_from_bars as _monthly_closes_from_bars,
    daily_closes_from_bars as _daily_closes_from_bars,
)


def _month_end_date(ym: str) -> str:
    year, month = int(ym[:4]), int(ym[5:7])
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-{last_day}"


def _expand_episode_date_range(ep: dict) -> dict:
    """월봉 기준 에피소드를 15m 봉 필터용 일자 구간으로 확장."""
    start_ym = ep["start_date"][:7]
    end_ym = ep["end_date"][:7]
    return {
        **ep,
        "start_date": f"{start_ym}-01",
        "end_date": _month_end_date(end_ym),
    }



def _detect_bear_episodes_from_data(data: list, market: str) -> list:
    """코인별 월봉 고점→하락 레그 + 진행 중 패턴 (이후 유사 패턴도 동일 규칙으로 학습)."""
    if not data or not HAS_REGIME_DETECTOR:
        return []
    ticker = market.replace("KRW-", "")
    months, m_prices = _monthly_closes_from_bars(data)
    m_dates = [f"{m}-01" for m in months]

    ctx = analyze_bear_pattern_context(m_prices, ticker, m_dates)
    episodes = list(ctx.get("historical_episodes") or [])

    if len(episodes) < 2:
        params = get_bear_pattern_params(ticker)
        dates, prices = _daily_closes_from_bars(data)
        peak_lb = min(180, int(TICKER_CYCLE_PROFILE.get(ticker, {}).get("peak_lookback_days", 365)) // 6)
        episodes = detect_historical_bear_episodes(
            prices,
            dates,
            min_drawdown_pct=params["min_drawdown_pct"],
            peak_lookback=max(90, peak_lb),
            min_duration_days=30,
            min_peak_gap_days=90,
            max_episodes=params["max_episodes"],
        )

    episodes = [_expand_episode_date_range(ep) for ep in episodes]
    return episodes


def _filter_bear_episode_bars(data: list, episodes: list) -> list:
    if not episodes:
        return []
    return [
        d for d in data
        if any(ep["start_date"] <= _bar_date_str(d) <= ep["end_date"] for ep in episodes)
    ]


def _with_bear_optim_context(params: dict, regime: str, bear_episodes: list = None) -> dict:
    if regime != "BEAR":
        return params
    out = dict(params)
    out["bear_optim_use_episodes"] = True
    if bear_episodes:
        out["bear_episodes"] = bear_episodes
    return out


def _bear_profit_basis(regime_data: list, full_data: list = None, episodes: list = None) -> list:
    """BEAR 기대수익·비교용 데이터 — 역사적 하락 패턴 구간 봉 우선."""
    if episodes and full_data:
        basis = _filter_bear_episode_bars(full_data, episodes)
        if len(basis) >= 30:
            return basis
    return regime_data


def _usdt_optimizer_score(result: dict, params: dict) -> float:
    """USDT: 총수익(%) 최우선 · 최소 3거래 · MDD 제한."""
    trades = int(result.get("trades", 0))
    if trades < 3:
        return -9999.0
    exit_tp = float(params.get("min_target_profit_pct", 0))
    if exit_tp < USDT_MIN_EXIT_TP_FLOOR:
        return -9999.0
    gap = float(params.get("min_consider_gap_krw", 0))
    if gap < USDT_MIN_ENTRY_GAP_FLOOR:
        return -9999.0
    mdd = float(result.get("mdd", 1.0))
    if mdd > MDD_LIMIT:
        return -9999.0
    ret = float(result.get("total_return", 0))  # 이미 % 단위
    sharpe = float(result.get("sharpe", 0))
    win = float(result.get("win_rate", 0))
    trade_bonus = min(trades, 12) * 0.08
    return ret * 3.0 + sharpe * 1.5 + win * 0.02 + trade_bonus


def _optimizer_profit_score(result: dict) -> float:
    """총수익·보유기간 우선 — 소액 조기 익절·과매매 억제."""
    trades = int(result.get("trades", 0))
    if trades < 3:
        return -9999.0
    mdd = float(result.get("mdd", 1.0))
    if mdd > MDD_LIMIT:
        return -9999.0
    ret = float(result.get("total_return", 0))
    sharpe = float(result.get("sharpe", 0))
    hold_h = float(result.get("avg_hold_hours", 0))
    win = float(result.get("win_rate", 0))
    hold_bonus = min(hold_h / 72.0, 1.5) * 8.0
    trade_penalty = max(0, trades - 80) * 0.03
    return ret * 0.55 + sharpe * 6.0 + hold_bonus + win * 0.04 - trade_penalty


def _gate_strategy_sell(params: dict, gross_pnl_pct: float, want_sell: bool) -> bool:
    """지표 SELL 신호에 최소 순수익(수수료 반영) 게이트."""
    if not want_sell:
        return False
    if gross_pnl_pct <= 0:
        return True
    min_s = float(params.get("min_strategy_sell_profit_pct", 0))
    net_pnl = gross_pnl_pct - ROUND_TRIP_FEE
    return net_pnl >= min_s


def _hold_stats(holding_bars: list) -> dict:
    """보유 기간(봉 수) → 시간/일 단위 통계."""
    if not holding_bars:
        return {"avg_hold_hours": 0, "median_hold_hours": 0, "avg_hold_days": 0}
    hours = [b * CANDLE_INTERVAL_HOURS for b in holding_bars]
    avg = sum(hours) / len(hours)
    sorted_h = sorted(hours)
    mid = len(sorted_h) // 2
    med = sorted_h[mid] if len(sorted_h) % 2 else (sorted_h[mid - 1] + sorted_h[mid]) / 2
    return {
        "avg_hold_hours": round(avg, 1),
        "median_hold_hours": round(med, 1),
        "avg_hold_days": round(avg / 24, 2),
    }


def _build_risk_output(params: dict) -> dict:
    rt = params.get("risk_type", "None")
    min_sell = params.get("min_strategy_sell_profit_pct", 0)
    if rt == "None":
        out = {"type": "None"}
    elif rt == "StopLoss":
        out = {
            "type": "StopLoss",
            "stop_loss_pct": params.get("stop_loss_pct", 0.02),
            "take_profit_pct": params.get("take_profit_pct", 0.03),
        }
    elif rt == "TrailingStop":
        out = {
            "type": "TrailingStop",
            "trail_pct": params.get("trail_pct", 0.02),
        }
        act = params.get("trail_activate_profit_pct", 0)
        if act:
            out["trail_activate_profit_pct"] = act
    elif rt == "BearScalp":
        out = {
            "type": "BearScalp",
            "stop_loss_pct": params.get("stop_loss_pct", 0.025),
            "take_profit_pct": params.get("take_profit_pct", 0.012),
            "trail_pct": params.get("trail_pct", 0.004),
            "trail_activate_profit_pct": params.get("trail_activate_profit_pct", 0.006),
        }
    else:
        out = {"type": "None"}
    if min_sell:
        out["min_strategy_sell_profit_pct"] = min_sell
    return out


def _should_risk_exit(params: dict, pnl_pct: float, peak_price: float, price: float, entry_price: float) -> bool:
    """백테스트용 리스크 강제 청산 조건."""
    rt = params.get("risk_type", "None")
    if rt == "StopLoss":
        sl = params.get("stop_loss_pct", 0.02)
        tp = params.get("take_profit_pct", 0.03)
        return pnl_pct <= -sl or pnl_pct >= tp
    if rt == "TrailingStop":
        trail = params.get("trail_pct", 0.02)
        activate = float(params.get("trail_activate_profit_pct", 0))
        if peak_price <= 0 or entry_price <= 0:
            return False
        if activate > 0 and pnl_pct < activate:
            return False
        drawdown = (peak_price - price) / peak_price
        return drawdown >= trail and pnl_pct >= BEAR_MIN_NET_PROFIT_FLOOR
    if rt == "BearScalp":
        sl = float(params.get("stop_loss_pct", 0.025))
        tp = float(params.get("take_profit_pct", 0.012))
        trail = float(params.get("trail_pct", 0.004))
        activate = float(params.get("trail_activate_profit_pct", 0.006))
        if pnl_pct <= -sl:
            return True
        if pnl_pct >= max(tp, BEAR_MIN_NET_PROFIT_FLOOR):
            return True
        if peak_price <= 0:
            return False
        if activate > 0 and pnl_pct < activate:
            return False
        drawdown = (peak_price - price) / peak_price
        return drawdown >= trail and pnl_pct >= BEAR_MIN_NET_PROFIT_FLOOR
    return False


# ─────────────────────────────────────────────────────────────────────────────
# 진행 상태 업데이트 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def _save_progress(stage: str, pct: float, msg: str = ""):
    for attempt in range(10):
        try:
            with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "stage": stage,
                    "percent": round(pct, 1),
                    "message": msg,
                    "updated_at": datetime.now(KST).isoformat()
                }, f, ensure_ascii=False)
            break
        except Exception:
            time.sleep(0.05)


# ─────────────────────────────────────────────────────────────────────────────
# 1. 데이터 수집
# ─────────────────────────────────────────────────────────────────────────────
class HistoricalDataFetcher:
    """
    빗썸 15분봉 캔들을 페이지네이션하여 수집, JSON 캐시에 저장.
    증분 업데이트: 이미 캐시된 날짜 이후 데이터만 추가.
    """

    CANDLE_URL = f"{BITHUMB_BASE}/v1/candles/minutes/15"
    MAX_PER_CALL = 200
    CALL_DELAY = 0.25  # Rate-limit 준수

    def _fetch_page(self, market: str, to_dt: datetime) -> list:
        """지정 시각(to_dt) 이전 15분봉 최대 200개 반환."""
        to_str = to_dt.strftime("%Y-%m-%dT%H:%M:%S")
        url = f"{self.CANDLE_URL}?market={market}&count={self.MAX_PER_CALL}&to={to_str}"
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    return r.json()
                logger.warning(f"[Fetcher] HTTP {r.status_code} — 재시도 {attempt+1}")
            except Exception as e:
                logger.warning(f"[Fetcher] 요청 오류: {e} — 재시도 {attempt+1}")
            time.sleep(1)
        return []

    def _is_target_time(self, dt_kst: datetime) -> bool:
        """KST 기준으로 TARGET_TIMES_KST에 해당하는 시각인지 확인."""
        return (dt_kst.hour, dt_kst.minute) in TARGET_TIMES_KST

    def fetch_full_history(self, market: str, years: int = 9, m_idx: int = 0, num_markets: int = 3) -> list:
        """
        최초 1회 전체 히스토리 수집.
        반환: [{"ts": "2017-09-01T01:00:00+09:00", "open": ..., "high": ..., "low": ..., "close": 3000000.0, "volume": 12.3}, ...]
        """
        cutoff = datetime.now(KST) - timedelta(days=years * 365)
        collected = []
        cursor = datetime.now(KST)
        call_count = 0

        logger.info(f"[Fetcher] {market} 전체 히스토리 수집 시작 ({years}년 / 15분봉)")

        while cursor > cutoff:
            page = self._fetch_page(market, cursor)
            if not page:
                break

            added = 0
            for c in page:
                # KST 파싱
                dt_kst = datetime.fromisoformat(c["candle_date_time_kst"].replace("T", "T"))
                dt_kst = dt_kst.replace(tzinfo=KST)

                if dt_kst <= cutoff:
                    cursor = cutoff  # 종료 신호
                    break

                if self._is_target_time(dt_kst):
                    collected.append({
                        "ts":     dt_kst.isoformat(),
                        "open":   float(c["opening_price"]),
                        "high":   float(c["high_price"]),
                        "low":    float(c["low_price"]),
                        "close":  float(c["trade_price"]),
                        "volume": float(c.get("candle_acc_trade_volume", 0))
                    })
                    added += 1

            call_count += 1
            # 가장 오래된 시각으로 cursor 이동
            oldest_str = page[-1]["candle_date_time_kst"].replace("T", "T")
            oldest_kst = datetime.fromisoformat(oldest_str).replace(tzinfo=KST)
            cursor = oldest_kst

            if call_count % 20 == 0:
                market_weight = 100.0 / num_markets
                collect_weight = market_weight * 0.2
                base_pct = m_idx * market_weight
                pct = base_pct + (1 - (cursor - cutoff).days / (years * 365)) * collect_weight
                pct = min(base_pct + collect_weight - 0.1, pct)
                _save_progress("collecting", pct, f"{market}: {len(collected)}개 수집중")
                logger.info(f"[Fetcher] {market} {len(collected)}개 수집 (API 호출 {call_count}회, 현재: {cursor.date()})")

            time.sleep(self.CALL_DELAY)

        # 시간순 정렬 (오래된 것 먼저)
        collected.sort(key=lambda x: x["ts"])
        logger.info(f"[Fetcher] {market} 수집 완료: {len(collected)}개")
        return collected

    def fetch_incremental(self, market: str, last_ts: str) -> list:
        """
        last_ts 이후 누락된 데이터만 수집 (매일 24개 내외).
        """
        last_dt = datetime.fromisoformat(last_ts)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=KST)

        new_data = []
        cursor = datetime.now(KST) + timedelta(hours=1)
        stop = False

        while not stop:
            page = self._fetch_page(market, cursor)
            if not page:
                break

            for c in page:
                dt_kst = datetime.fromisoformat(
                    c["candle_date_time_kst"].replace("T", "T")
                ).replace(tzinfo=KST)

                if dt_kst <= last_dt:
                    stop = True
                    break

                if self._is_target_time(dt_kst):
                    new_data.append({
                        "ts":     dt_kst.isoformat(),
                        "open":   float(c["opening_price"]),
                        "high":   float(c["high_price"]),
                        "low":    float(c["low_price"]),
                        "close":  float(c["trade_price"]),
                        "volume": float(c.get("candle_acc_trade_volume", 0))
                    })

            if not stop:
                oldest_kst = datetime.fromisoformat(
                    page[-1]["candle_date_time_kst"].replace("T", "T")
                ).replace(tzinfo=KST)
                if oldest_kst <= last_dt:
                    break
                cursor = oldest_kst
                time.sleep(self.CALL_DELAY)

        new_data.sort(key=lambda x: x["ts"])
        logger.info(f"[Fetcher] {market} 증분 수집: {len(new_data)}개 추가")
        return new_data


# ─────────────────────────────────────────────────────────────────────────────
# 2. 캐시 관리
# ─────────────────────────────────────────────────────────────────────────────
class DataCache:
    def load(self) -> dict:
        if os.path.exists(CACHE_FILE):
            try:
                is_invalid = False
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                    if self._check_cache_invalid(cache):
                        is_invalid = True
                
                if is_invalid:
                    logger.warning("[Cache] 수집 시각 설정(TARGET_TIMES_KST) 또는 데이터 스키마가 변경된 것으로 감지되었습니다. 기존 캐시를 백업하고 새로 수집합니다.")
                    bak_file = CACHE_FILE + ".bak"
                    try:
                        if os.path.exists(bak_file):
                            os.remove(bak_file)
                        os.rename(CACHE_FILE, bak_file)
                    except Exception as e:
                        logger.error(f"[Cache] 백업 중 오류: {e}")
                    return {}
                return cache
            except Exception:
                pass
        return {}

    def _check_cache_invalid(self, cache: dict) -> bool:
        """15분봉 스키마·시각·필드 검증."""
        meta = cache.get("_meta", {})
        if meta.get("schema") != CACHE_SCHEMA:
            return True
        for market, m_data in cache.items():
            if market == "_meta":
                continue
            rows = m_data.get("data", [])
            if not rows:
                continue
            for r in rows[:10]:
                try:
                    if not all(k in r for k in ["open", "high", "low", "close", "volume"]):
                        return True
                    dt = datetime.fromisoformat(r["ts"])
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=KST)
                    else:
                        dt = dt.astimezone(KST)
                    if (dt.hour, dt.minute) not in TARGET_TIMES_KST:
                        return True
                except Exception:
                    return True
        return False

    def save(self, cache: dict):
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)

    def get_last_ts(self, cache: dict, market: str) -> Optional[str]:
        data = cache.get(market, {}).get("data", [])
        return data[-1]["ts"] if data else None

    def append(self, cache: dict, market: str, new_rows: list) -> dict:
        if market not in cache:
            cache[market] = {"data": []}
        existing_ts = {r["ts"] for r in cache[market]["data"]}
        for r in new_rows:
            if r["ts"] not in existing_ts:
                cache[market]["data"].append(r)
                existing_ts.add(r["ts"])
        cache[market]["data"].sort(key=lambda x: x["ts"])
        cache[market]["last_updated"] = datetime.now(KST).isoformat()
        return cache


# ─────────────────────────────────────────────────────────────────────────────
# 3. 기술 지표 계산 (numpy 기반)
# ─────────────────────────────────────────────────────────────────────────────
class Indicators:
    _rsi_cache = {}
    _bb_cache = {}
    _macd_cache = {}

    @staticmethod
    def clear_cache():
        Indicators._rsi_cache.clear()
        Indicators._bb_cache.clear()
        Indicators._macd_cache.clear()

    @staticmethod
    def rsi(closes: list, period: int) -> list:
        if not closes:
            return []
        key = (len(closes), closes[0], closes[-1], period)
        if key in Indicators._rsi_cache:
            return Indicators._rsi_cache[key]

        if not HAS_NUMPY or len(closes) < period + 1:
            return [50.0] * len(closes)
        arr = np.array(closes, dtype=float)
        delta = np.diff(arr)
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)
        result = [float('nan')] * len(closes)
        avg_gain = np.mean(gain[:period])
        avg_loss = np.mean(loss[:period])
        for i in range(period, len(closes) - 1):
            if avg_loss == 0:
                result[i + 1] = 100.0
            else:
                rs = avg_gain / avg_loss
                result[i + 1] = 100 - (100 / (1 + rs))
            avg_gain = (avg_gain * (period - 1) + gain[i]) / period
            avg_loss = (avg_loss * (period - 1) + loss[i]) / period
        # nan 채우기
        filled = [50.0] * len(result)
        for i, v in enumerate(result):
            if not math.isnan(v):
                filled[i] = v
        
        Indicators._rsi_cache[key] = filled
        return filled

    @staticmethod
    def bollinger(closes: list, period: int, std_dev: float):
        if not closes:
            return [], [], []
        key = (len(closes), closes[0], closes[-1], period, std_dev)
        if key in Indicators._bb_cache:
            return Indicators._bb_cache[key]

        if not HAS_NUMPY or len(closes) < period:
            return [0.0]*len(closes), [c for c in closes], [c*2 for c in closes]
        arr = np.array(closes, dtype=float)
        mid, upper, lower = [], [], []
        for i in range(len(arr)):
            if i < period - 1:
                mid.append(arr[i]); upper.append(arr[i]); lower.append(arr[i])
            else:
                window = arr[i - period + 1: i + 1]
                m = float(np.mean(window))
                s = float(np.std(window, ddof=1))
                mid.append(m)
                upper.append(m + std_dev * s)
                lower.append(m - std_dev * s)
        
        res = (mid, upper, lower)
        Indicators._bb_cache[key] = res
        return res

    @staticmethod
    def macd(closes: list, fast: int, slow: int, signal_period: int):
        if not closes:
            return [], [], []
        key = (len(closes), closes[0], closes[-1], fast, slow, signal_period)
        if key in Indicators._macd_cache:
            return Indicators._macd_cache[key]

        if not HAS_NUMPY or len(closes) < slow:
            n = len(closes)
            return [0.0]*n, [0.0]*n, [0.0]*n
        arr = np.array(closes, dtype=float)

        def ema(data, span):
            k = 2 / (span + 1)
            e = [data[0]]
            for v in data[1:]:
                e.append(v * k + e[-1] * (1 - k))
            return np.array(e)

        ema_fast = ema(arr, fast)
        ema_slow = ema(arr, slow)
        macd_line = ema_fast - ema_slow
        signal_line = ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        
        res = (macd_line.tolist(), signal_line.tolist(), histogram.tolist())
        Indicators._macd_cache[key] = res
        return res


# ─────────────────────────────────────────────────────────────────────────────
# 4. 장세 라벨링 (RegimeDetector 동일 알고리즘)
# ─────────────────────────────────────────────────────────────────────────────
class RegimeLabeler:
    def label(self, data: list) -> list:
        """
        각 데이터 포인트에 regime (BULL/BEAR/RANGE) 태그 추가.
        실시간 엔진의 MarketRegimeDetector와 100% 동일하게 일봉 기준 다중 선형 회귀 및 기울기 분석 동기화.
        """
        if not data:
            return data

        # 1. 일별 종가 추출 (KST 일자 기준 마지막 캔들)
        daily_map = {}
        for row in data:
            ts_str = row["ts"]
            # ts_str 예시: "2017-09-01T01:00:00+09:00" -> "2017-09-01"
            date_str = ts_str.split("T")[0]
            daily_map[date_str] = row["close"]

        sorted_dates = sorted(daily_map.keys())
        daily_prices = [daily_map[d] for d in sorted_dates]
        
        # 2. 각 일자별로 MarketRegimeDetector와 동일한 로직 적용
        daily_regimes = {}
        n_daily = len(daily_prices)
        long_term_period = 210
        
        for idx, date_str in enumerate(sorted_dates):
            avail = idx + 1
            actual_period = min(long_term_period, avail)
            
            min_required = min(300, long_term_period)
            if actual_period < min_required:
                daily_regimes[date_str] = "RANGE"
                continue
                
            p_long = daily_prices[idx - actual_period + 1 : idx + 1]
            x_long = np.arange(actual_period)
            slope_long, intercept_long = np.polyfit(x_long, p_long, 1)
            
            fair_value = slope_long * (actual_period - 1) + intercept_long
            price_now = float(daily_prices[idx])
            dev_pct = (price_now - fair_value) / fair_value * 100
            
            # 최근 90일, 30일 선형 회귀 기울기 계산 (% / 일)
            p_90 = daily_prices[max(0, idx - 90 + 1) : idx + 1]
            slope_90, _ = np.polyfit(np.arange(len(p_90)), p_90, 1) if len(p_90) >= 2 else (0.0, 0.0)
            slope_rate_90 = (slope_90 / price_now) * 100
            
            p_30 = daily_prices[max(0, idx - 30 + 1) : idx + 1]
            slope_30, _ = np.polyfit(np.arange(len(p_30)), p_30, 1) if len(p_30) >= 2 else (0.0, 0.0)
            slope_rate_30 = (slope_30 / price_now) * 100
            
            is_bottoming = (slope_rate_90 >= -0.05) or (slope_rate_30 >= 0.0)
            
            # 종합 조건 판정 (MarketRegimeDetector와 100% 동일)
            if dev_pct <= -25.0:
                if slope_rate_30 >= 0.1:
                    reg = "BULL"
                elif is_bottoming:
                    reg = "RANGE"
                else:
                    reg = "BEAR"
            elif dev_pct >= -10.0 and slope_rate_90 > 0 and slope_rate_30 > 0:
                reg = "BULL"
            elif dev_pct < -10.0 and (slope_rate_90 < -0.05 or slope_rate_30 < -0.1):
                reg = "BEAR"
            else:
                reg = "RANGE"
                
            daily_regimes[date_str] = reg

        # 3. 봉 데이터에 일자별 장세 역매핑
        labeled = []
        for row in data:
            date_str = row["ts"].split("T")[0]
            reg = daily_regimes.get(date_str, "RANGE")
            labeled.append({**row, "regime": reg})
            
        return labeled


# ─────────────────────────────────────────────────────────────────────────────
# 5. 전략 시뮬레이터
# ─────────────────────────────────────────────────────────────────────────────
class StrategySimulator:
    """
    주어진 파라미터 조합으로 장세별 수익률 시뮬레이션.
    수수료/슬리피지 반영, Sharpe Ratio / MDD 반환.
    """

    def simulate(self, data: list, params: dict) -> dict:
        """
        data: RegimeLabeler.label() 결과 (regime 포함)
        params: {
          "rsi": bool, "rsi_period": int, "rsi_oversold": int, "rsi_overbought": int,
          "bb": bool, "bb_period": int, "bb_std": float,
          "macd": bool, "macd_fast": int, "macd_slow": int, "macd_signal": int,
          "custom_bear": bool, "lookback": int, "drop_pct": float, "volume_ratio": float,
          "trail_pct": float, "stop_loss": float, "time_cut": int,
          "target_regime": "BULL"|"BEAR"|"RANGE"
        }
        """
        target = params["target_regime"]
        episodes = params.get("bear_episodes") or []
        if target == "BEAR" and params.get("bear_optim_use_episodes") and episodes:
            episode_bars = _filter_bear_episode_bars(data, episodes)
            if len(episode_bars) >= 30:
                regime_data = episode_bars
            else:
                regime_data = [d for d in data if d["regime"] == target]
        else:
            regime_data = [d for d in data if d["regime"] == target]
            recent_days = params.get("bear_optim_recent_days")
            if target == "BEAR" and recent_days:
                sliced = _filter_regime_recent_bars(regime_data, int(recent_days))
                if len(sliced) >= 30:
                    regime_data = sliced

        if len(regime_data) < 30:
            return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0, **_hold_stats([])}

        if params.get("usdt_fusion"):
            sim_data = data if params.get("use_all_regimes") else regime_data
            return self._simulate_usdt_fusion(sim_data, params)

        # 1. CustomBear 전용 분기 (하락장 롱테일 극복 + 트레일링 스톱 결합)
        if params.get("custom_bear", False):
            opens = [d["open"] for d in regime_data]
            closes = [d["close"] for d in regime_data]
            highs = [d.get("high", d["close"]) for d in regime_data]
            volumes = [d["volume"] for d in regime_data]

            capital = 1_000_000.0
            position = 0.0
            entry_price = 0.0
            peak_price = 0.0
            entry_idx = 0
            equity_curve = [capital]
            wins = 0
            total_trades = 0
            holding_bars = []

            lookback = params["lookback"]
            drop_pct = params["drop_pct"]
            volume_ratio = params["volume_ratio"]
            trail_pct = params["trail_pct"]
            stop_loss = params["stop_loss"]

            warmup = max(lookback, 20)

            for i in range(warmup, len(closes)):
                price = closes[i]

                if position == 0:
                    # lookback 기간의 최고가 대비 하락폭 감시
                    window_closes = closes[i - lookback: i]
                    highest = max(window_closes) if window_closes else price
                    drop = (highest - price) / highest if highest > 0 else 0

                    # 거래량 10평균 대비 급증 감시
                    vol_window = volumes[i - 10: i]
                    avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1.0
                    curr_vol = volumes[i]

                    # 반등 양봉 조건 (시가 대비 종가 상승)
                    is_bullish = closes[i] > opens[i]

                    buy = (drop >= drop_pct) and (curr_vol >= avg_vol * volume_ratio) and is_bullish

                    if buy:
                        cost = capital * FEE_RATE
                        position = (capital - cost) / price
                        entry_price = price
                        peak_price = price
                        entry_idx = i
                        capital = 0.0
                        total_trades += 1
                else:
                    # peak_price 실시간 최고가 업데이트
                    peak_price = max(peak_price, highs[i])
                    
                    # 청산 조건 (트레일링 스톱, 손절 — 보유별 목표이율 단계 하향, 강제 시간청산 없음)
                    pnl = (price - entry_price) / entry_price
                    drawdown = (peak_price - price) / peak_price
                    hold_hours = (i - entry_idx) * CANDLE_INTERVAL_HOURS
                    base_tgt = bear_base_target_profit_pct(params)
                    min_tgt = bear_tiered_min_net_profit(hold_hours, base_tgt, None)
                    net_pnl = pnl - ROUND_TRIP_FEE

                    exit_tp = net_pnl >= min_tgt
                    exit_ts = drawdown >= trail_pct and net_pnl >= min_tgt
                    exit_sl = pnl <= -stop_loss

                    if exit_tp or exit_ts or exit_sl:
                        gross = position * price
                        fee = gross * FEE_RATE
                        capital = gross - fee
                        if price > entry_price:
                            wins += 1
                        holding_bars.append(i - entry_idx)
                        position = 0.0
                        peak_price = 0.0
                        equity_curve.append(capital)

            if position > 0:
                capital = position * closes[-1] * (1 - FEE_RATE)
                equity_curve.append(capital)

            if len(equity_curve) < 2 or total_trades == 0:
                return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0, **_hold_stats([])}

            total_return = (equity_curve[-1] / equity_curve[0]) - 1.0

            peak = equity_curve[0]
            mdd = 0.0
            for v in equity_curve:
                if v > peak:
                    peak = v
                dd = (peak - v) / peak
                if dd > mdd:
                    mdd = dd

            returns = [(equity_curve[j] / equity_curve[j-1]) - 1 for j in range(1, len(equity_curve))]
            if len(returns) > 1 and HAS_NUMPY:
                arr = np.array(returns)
                std = float(np.std(arr, ddof=1))
                sharpe = float(np.mean(arr)) / std if std > 0 else 0.0
            else:
                sharpe = 0.0

            return {
                "sharpe":       round(sharpe, 4),
                "mdd":          round(mdd, 4),
                "total_return": round(total_return * 100, 2),
                "win_rate":     round(wins / total_trades * 100, 2) if total_trades > 0 else 0.0,
                "trades":       total_trades,
                **_hold_stats(holding_bars),
            }

        # 2. 기존 보조지표 믹스 분기
        closes = [d["close"] for d in regime_data]
        highs = [d.get("high", d["close"]) for d in regime_data]
        ind = Indicators()

        # 지표 계산
        rsi_vals = ind.rsi(closes, params["rsi_period"]) if params["rsi"] else None
        bb_mid, bb_upper, bb_lower = ind.bollinger(closes, params["bb_period"], params["bb_std"]) if params["bb"] else (None, None, None)
        macd_line, signal_line, _ = ind.macd(closes, params["macd_fast"], params["macd_slow"], params["macd_signal"]) if params["macd"] else (None, None, None)

        # 매수/매도 시뮬레이션
        capital = 1_000_000.0
        position = 0.0
        entry_price = 0.0
        entry_fair = 0.0
        peak_price = 0.0
        entry_idx = 0
        equity_curve = [capital]
        wins = 0
        total_trades = 0
        holding_bars = []
        warmup = max(params.get("rsi_period", 14),
                     params.get("bb_period", 20),
                     params.get("macd_slow", 26)) + 5

        for i in range(warmup, len(closes)):
            price = closes[i]
            buy_signals = []
            sell_signals = []

            # RSI 신호
            if params["rsi"] and rsi_vals:
                rv = rsi_vals[i]
                buy_signals.append(rv < params["rsi_oversold"])
                sell_signals.append(rv > params["rsi_overbought"])

            # 볼린저 신호
            if params["bb"] and bb_lower:
                buy_signals.append(price < bb_lower[i])
                sell_signals.append(price > bb_upper[i])

            # MACD 신호
            if params["macd"] and macd_line:
                prev_diff = macd_line[i-1] - signal_line[i-1]
                curr_diff = macd_line[i] - signal_line[i]
                buy_signals.append(prev_diff < 0 and curr_diff >= 0)   # 골든크로스
                sell_signals.append(prev_diff > 0 and curr_diff <= 0)  # 데드크로스

            buy, sell = _resolve_composite_signals(
                buy_signals, sell_signals,
                params.get("logic", "OR"),
                params.get("threshold", DEFAULT_WEIGHTED_THRESHOLD),
            )

            if params.get("usdt_fx_filter") and buy and HAS_USDT_FX_HIST:
                fair = regime_data[i].get("fair_krw")
                if not fair or not usdt_fx_buy_allowed(
                    price, fair,
                    params.get("min_consider_gap_krw", 30),
                    params.get("min_target_profit_pct", 0.2),
                    FEE_RATE,
                ):
                    buy = False

            if position == 0 and buy:
                cost = capital * FEE_RATE
                position = (capital - cost) / price
                entry_price = price
                entry_fair = regime_data[i].get("fair_krw") or price
                peak_price = price
                entry_idx = i
                capital = 0.0
                total_trades += 1

            elif position > 0:
                peak_price = max(peak_price, highs[i])
                pnl_pct = (price - entry_price) / entry_price if entry_price > 0 else 0
                if params.get("usdt_fx_filter") and HAS_USDT_FX_HIST:
                    fair = regime_data[i].get("fair_krw") or entry_fair
                    exit_info = usdt_fx_exit_allowed(
                        entry_price, entry_fair, price, fair,
                        exit_gap_krw=params.get("exit_gap_krw", 15),
                        exit_convergence_pct=params.get("exit_convergence_pct", 90),
                        fee_one_way=FEE_RATE,
                    )
                    risk_exit = False
                    sell_signal = exit_info["allowed"]
                else:
                    risk_exit = _should_risk_exit(params, pnl_pct, peak_price, price, entry_price)
                    sell_signal = _gate_strategy_sell(params, pnl_pct, sell)

                if risk_exit or sell_signal:
                    gross = position * price
                    fee = gross * FEE_RATE
                    capital = gross - fee
                    if price > entry_price:
                        wins += 1
                    holding_bars.append(i - entry_idx)
                    position = 0.0
                    peak_price = 0.0
                    equity_curve.append(capital)

        # 포지션 강제 청산
        if position > 0:
            capital = position * closes[-1] * (1 - FEE_RATE)
            equity_curve.append(capital)

        if len(equity_curve) < 2 or total_trades == 0:
            return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0, **_hold_stats([])}

        total_return = (equity_curve[-1] / equity_curve[0]) - 1.0

        # MDD
        peak = equity_curve[0]
        mdd = 0.0
        for v in equity_curve:
            if v > peak:
                peak = v
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd

        # Sharpe Ratio (거래 단위 수익률 기반)
        returns = [(equity_curve[idx] / equity_curve[idx-1]) - 1
                   for idx in range(1, len(equity_curve))]
        if len(returns) > 1 and HAS_NUMPY:
            arr = np.array(returns)
            std = float(np.std(arr, ddof=1))
            sharpe = float(np.mean(arr)) / std if std > 0 else 0.0
        else:
            sharpe = 0.0

        win_rate = wins / total_trades if total_trades > 0 else 0.0

        return {
            "sharpe":       round(sharpe, 4),
            "mdd":          round(mdd, 4),
            "total_return": round(total_return * 100, 2),  # %
            "win_rate":     round(win_rate * 100, 2),       # %
            "trades":       total_trades,
            **_hold_stats(holding_bars),
        }

    def _simulate_usdt_fusion(self, data: list, params: dict) -> dict:
        """USDT 고정 융합 — gap 진입 · gap/TP 탈출 (fair_krw = 당일 USD/KRW)."""
        if len(data) < 30:
            return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0, **_hold_stats([])}

        closes = [d["close"] for d in data]
        entry_gap = float(params.get("min_consider_gap_krw", USDT_DEFAULT_ENTRY_GAP_KRW))
        exit_gap = float(params.get("exit_gap_krw", USDT_DEFAULT_EXIT_GAP_KRW))
        exit_tp_pct = float(params.get("min_target_profit_pct", USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT))
        fee = float(params.get("fee_one_way", FEE_RATE))

        fair_vals = [d.get("fair_krw") for d in data]
        if not any(fair_vals):
            fair_ma = int(params.get("fair_ma_period", 336))
            if HAS_NUMPY:
                s = pd.Series(closes)
                ma = s.rolling(fair_ma).mean().shift(1)
                fair_vals = [None if pd.isna(x) else float(x) for x in ma]
            else:
                fair_vals = []
                for i in range(len(closes)):
                    if i < fair_ma:
                        fair_vals.append(None)
                    else:
                        window = closes[i - fair_ma:i]
                        fair_vals.append(sum(window) / len(window))

        capital = 1_000_000.0
        position = 0.0
        entry_price = 0.0
        entry_fair = 0.0
        entry_idx = 0
        equity_curve = [capital]
        wins = 0
        total_trades = 0
        holding_bars = []
        warmup = 30

        for i in range(warmup, len(closes)):
            price = closes[i]
            fair = fair_vals[i]
            if fair is None:
                continue

            fx_ok = usdt_fx_entry_allowed(price, fair, entry_gap, fee) if HAS_USDT_FX_HIST else (
                (fair - price) >= entry_gap
            )

            if position == 0:
                if fx_ok:
                    cost = capital * fee
                    position = (capital - cost) / price
                    entry_price = price
                    entry_fair = fair
                    entry_idx = i
                    capital = 0.0
                    total_trades += 1
            else:
                if HAS_USDT_FX_HIST:
                    exit_info = usdt_fx_exit_fusion(
                        entry_price, entry_fair, price, fair,
                        exit_gap_krw=exit_gap,
                        exit_take_profit_pct=exit_tp_pct,
                        fee_one_way=fee,
                    )
                    sell = exit_info["allowed"]
                else:
                    net_pnl = (price - entry_price) / entry_price if entry_price > 0 else 0
                    sell = net_pnl > 0 and (fair - price) <= exit_gap
                if sell:
                    gross = position * price
                    capital = gross * (1 - fee)
                    if price > entry_price:
                        wins += 1
                    holding_bars.append(i - entry_idx)
                    position = 0.0
                    equity_curve.append(capital)

        if position > 0:
            capital = position * closes[-1] * (1 - fee)
            equity_curve.append(capital)

        if len(equity_curve) < 2 or total_trades == 0:
            return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0, **_hold_stats([])}

        total_return = (equity_curve[-1] / equity_curve[0]) - 1.0
        peak = equity_curve[0]
        mdd = 0.0
        for v in equity_curve:
            if v > peak:
                peak = v
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd

        returns = [(equity_curve[j] / equity_curve[j - 1]) - 1 for j in range(1, len(equity_curve))]
        if len(returns) > 1 and HAS_NUMPY:
            arr = np.array(returns)
            std = float(np.std(arr, ddof=1))
            sharpe = float(np.mean(arr)) / std if std > 0 else 0.0
        else:
            sharpe = 0.0

        return {
            "sharpe": round(sharpe, 4),
            "mdd": round(mdd, 4),
            "total_return": round(total_return * 100, 2),
            "win_rate": round(wins / total_trades * 100, 2) if total_trades > 0 else 0.0,
            "trades": total_trades,
            **_hold_stats(holding_bars),
        }

    def _simulate_logic_numpy_batch(self, data: list, params_list: list) -> list:
        """NumPy vectorization for general indicators across a batch of parameters."""
        if not HAS_NUMPY or len(data) < 30 or not params_list:
            return [self.simulate(data, p) for p in params_list]

        closes = xp.array([d["close"] for d in data], dtype=xp.float32)
        closes_list = [d["close"] for d in data]
        highs = xp.array([d.get("high", d["close"]) for d in data], dtype=xp.float32)
        
        num_bars = len(closes)
        num_params = len(params_list)
        
        buy_mask = xp.zeros((num_bars, num_params), dtype=bool)
        sell_mask = xp.zeros((num_bars, num_params), dtype=bool)
        
        ind = Indicators()
        # Precompute indicators and map to boolean masks
        for p_idx, p in enumerate(params_list):
            if p.get("rsi"):
                rv = ind.rsi(closes_list, p["rsi_period"])
                rv_arr = xp.array(rv)
                buy_mask[:, p_idx] |= (rv_arr < p["rsi_oversold"])
                sell_mask[:, p_idx] |= (rv_arr > p["rsi_overbought"])
            
            if p.get("bb"):
                _, bb_u, bb_l = ind.bollinger(closes_list, p["bb_period"], p["bb_std"])
                buy_mask[:, p_idx] |= (closes < xp.array(bb_l))
                sell_mask[:, p_idx] |= (closes > xp.array(bb_u))
                
            if p.get("macd"):
                ml, sl, _ = ind.macd(closes_list, p["macd_fast"], p["macd_slow"], p["macd_signal"])
                ml_arr, sl_arr = xp.array(ml), xp.array(sl)
                prev_diff = xp.roll(ml_arr, 1) - xp.roll(sl_arr, 1)
                curr_diff = ml_arr - sl_arr
                buy_mask[:, p_idx] |= ((prev_diff < 0) & (curr_diff >= 0))
                sell_mask[:, p_idx] |= ((prev_diff > 0) & (curr_diff <= 0))
                
        # Risk arrays
        trail_pcts = xp.array([p.get("trail_pct", 0) for p in params_list], dtype=xp.float32)
        sl_pcts = xp.array([p.get("stop_loss_pct", 0) for p in params_list], dtype=xp.float32)
        tp_pcts = xp.array([p.get("take_profit_pct", 999) for p in params_list], dtype=xp.float32)
        has_trail = xp.array([p.get("risk_type") == "TrailingStop" or p.get("risk_type") == "BearScalp" for p in params_list])
        has_sl = xp.array([p.get("risk_type") == "StopLoss" or p.get("risk_type") == "BearScalp" for p in params_list])
        min_sell = xp.array([p.get("min_strategy_sell_profit_pct", 0) for p in params_list], dtype=xp.float32)
        
        positions = xp.zeros(num_params, dtype=xp.float32)
        entry_prices = xp.zeros(num_params, dtype=xp.float32)
        capitals = xp.full(num_params, 1000000.0, dtype=xp.float32)
        peak_prices = xp.zeros(num_params, dtype=xp.float32)
        trades = xp.zeros(num_params, dtype=xp.int32)
        wins = xp.zeros(num_params, dtype=xp.int32)
        
        init_capitals = capitals.copy()
        peak_capitals = capitals.copy()
        mdds = xp.zeros(num_params, dtype=xp.float32)
        
        fee = xp.float32(FEE_RATE)
        warmup = 30
        
        for i in range(warmup, num_bars):
            price = closes[i]
            high_price = highs[i]
            
            in_pos = positions > 0
            if xp.any(in_pos):
                peak_prices[in_pos] = xp.maximum(peak_prices[in_pos], high_price)
                
                pnl_pct = xp.zeros_like(entry_prices)
                pnl_pct[in_pos] = (price - entry_prices[in_pos]) / entry_prices[in_pos]
                
                drawdown = xp.zeros_like(peak_prices)
                drawdown[in_pos] = (peak_prices[in_pos] - price) / peak_prices[in_pos]
                
                net_pnl = pnl_pct - (fee * 2)
                
                exit_ts = has_trail & in_pos & (drawdown >= trail_pcts) & (net_pnl >= 0.005)
                exit_sl = has_sl & in_pos & ((pnl_pct <= -sl_pcts) | (pnl_pct >= tp_pcts))
                exit_strat = in_pos & sell_mask[i, :] & (net_pnl >= min_sell)
                
                do_sell = exit_ts | exit_sl | exit_strat
                
                if xp.any(do_sell):
                    gross = positions[do_sell] * price
                    new_cap = gross * (1 - fee)
                    capitals[do_sell] = new_cap
                    wins[do_sell] += (price > entry_prices[do_sell]).astype(xp.int32)
                    positions[do_sell] = 0.0
                    
                    peak_capitals[do_sell] = xp.maximum(peak_capitals[do_sell], new_cap)
                    curr_mdd = (peak_capitals[do_sell] - new_cap) / peak_capitals[do_sell]
                    mdds[do_sell] = xp.maximum(mdds[do_sell], curr_mdd)

            no_pos = positions == 0
            if xp.any(no_pos):
                do_buy = no_pos & buy_mask[i, :]
                if xp.any(do_buy):
                    cost = capitals[do_buy] * fee
                    positions[do_buy] = (capitals[do_buy] - cost) / price
                    entry_prices[do_buy] = price
                    peak_prices[do_buy] = price
                    capitals[do_buy] = 0.0
                    trades[do_buy] += 1
                    
        in_pos = positions > 0
        if xp.any(in_pos):
            new_cap = positions[in_pos] * closes[-1] * (1 - fee)
            capitals[in_pos] = new_cap
            peak_capitals[in_pos] = xp.maximum(peak_capitals[in_pos], new_cap)
            curr_mdd = (peak_capitals[in_pos] - new_cap) / peak_capitals[in_pos]
            mdds[in_pos] = xp.maximum(mdds[in_pos], curr_mdd)

        results = []
        for p_idx in range(num_params):
            if int(float(trades[p_idx])) == 0:
                results.append({"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0, "avg_hold_hours": 0, "median_hold_hours": 0, "avg_hold_days": 0})
            else:
                ret = float(capitals[p_idx]) / float(init_capitals[p_idx]) - 1.0
                win_r = (float(wins[p_idx]) / float(trades[p_idx])) * 100.0 if int(float(trades[p_idx])) > 0 else 0.0
                results.append({
                    "sharpe": 0.0,
                    "mdd": float(round(float(mdds[p_idx]), 4)),
                    "total_return": float(round(ret * 100, 2)),
                    "win_rate": float(round(win_r, 2)),
                    "trades": int(float(trades[p_idx])),
                    "avg_hold_hours": 24.0,
                    "median_hold_hours": 24.0,
                    "avg_hold_days": 1.0
                })
        return results

    def _simulate_usdt_fusion_numpy_batch(self, data: list, params_list: list) -> list:
        """
        USDT 고정 융합 — NumPy 벡터 연산으로 N개의 파라미터 조합 동시 시뮬레이션.
        """
        if not HAS_NUMPY or len(data) < 30 or not params_list:
            return [self._simulate_usdt_fusion(data, p) for p in params_list]

        closes_list = [d["close"] for d in data]
        fair_vals_list = [d.get("fair_krw") for d in data]

        # If fairs are missing, compute rolling MA once for the first parameter's fair_ma_period.
        # Grid search uses a fixed fair_ma_period for all combinations typically.
        if not any(fair_vals_list):
            fair_ma = int(params_list[0].get("fair_ma_period", 336))
            s = pd.Series(closes_list)
            ma = s.rolling(fair_ma).mean().shift(1)
            fair_vals_list = [0.0 if pd.isna(x) else float(x) for x in ma]
        else:
            fair_vals_list = [0.0 if x is None else float(x) for x in fair_vals_list]

        closes = xp.array(closes_list, dtype=xp.float32)
        fairs = xp.array(fair_vals_list, dtype=xp.float32)

        num_bars = len(closes)
        num_params = len(params_list)

        entry_gaps = xp.array([float(p.get("min_consider_gap_krw", USDT_DEFAULT_ENTRY_GAP_KRW)) for p in params_list], dtype=xp.float32)
        exit_gaps = xp.array([float(p.get("exit_gap_krw", USDT_DEFAULT_EXIT_GAP_KRW)) for p in params_list], dtype=xp.float32)
        exit_tps = xp.array([float(p.get("min_target_profit_pct", USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT)) for p in params_list], dtype=xp.float32)
        
        # We assume fee_one_way is the same for all params (0.0025)
        fee = xp.float32(params_list[0].get("fee_one_way", FEE_RATE))

        positions = xp.zeros(num_params, dtype=xp.float32)
        entry_prices = xp.zeros(num_params, dtype=xp.float32)
        capitals = xp.full(num_params, 1000000.0, dtype=xp.float32)
        peak_capitals = xp.full(num_params, 1000000.0, dtype=xp.float32)
        mdds = xp.zeros(num_params, dtype=xp.float32)
        wins = xp.zeros(num_params, dtype=xp.int32)
        trades = xp.zeros(num_params, dtype=xp.int32)

        warmup = 30
        
        for i in range(warmup, num_bars):
            price = closes[i]
            fair = fairs[i]
            if fair <= 0.001:
                continue

            in_pos_mask = positions > 0
            if xp.any(in_pos_mask):
                pnl_pct = (price - entry_prices[in_pos_mask]) / entry_prices[in_pos_mask]
                tp_hit = pnl_pct >= exit_tps[in_pos_mask]
                gap_hit = (pnl_pct > 0) & ((fair - price) <= exit_gaps[in_pos_mask])
                
                sell_subset_mask = tp_hit | gap_hit
                if xp.any(sell_subset_mask):
                    sell_mask = xp.zeros(num_params, dtype=bool)
                    sell_mask[in_pos_mask] = sell_subset_mask
                    
                    gross = positions[sell_mask] * price
                    new_capital = gross * (1.0 - fee)
                    capitals[sell_mask] = new_capital
                    
                    wins[sell_mask] += (price > entry_prices[sell_mask]).astype(xp.int32)
                    positions[sell_mask] = 0.0
                    
                    peak_capitals[sell_mask] = xp.maximum(peak_capitals[sell_mask], new_capital)
                    dd = (peak_capitals[sell_mask] - new_capital) / peak_capitals[sell_mask]
                    mdds[sell_mask] = xp.maximum(mdds[sell_mask], dd)

            no_pos_mask = positions == 0
            if xp.any(no_pos_mask):
                fx_ok = (fair - price) >= entry_gaps[no_pos_mask]
                if xp.any(fx_ok):
                    buy_mask = xp.zeros(num_params, dtype=bool)
                    buy_mask[no_pos_mask] = fx_ok
                    
                    cost = capitals[buy_mask] * fee
                    positions[buy_mask] = (capitals[buy_mask] - cost) / price
                    entry_prices[buy_mask] = price
                    capitals[buy_mask] = 0.0
                    trades[buy_mask] += 1

        leftover = positions > 0
        if xp.any(leftover):
            final_price = closes[-1]
            capitals[leftover] = positions[leftover] * final_price * (1.0 - fee)
            peak_capitals[leftover] = xp.maximum(peak_capitals[leftover], capitals[leftover])
            dd = (peak_capitals[leftover] - capitals[leftover]) / peak_capitals[leftover]
            mdds[leftover] = xp.maximum(mdds[leftover], dd)

        total_returns = xp.where(trades > 0, (capitals / 1000000.0) - 1.0, 0.0)
        win_rates = xp.where(trades > 0, wins.astype(xp.float32) / trades, 0.0)
        mdd_finals = xp.where(trades > 0, mdds, 1.0)

        results = []
        for j in range(num_params):
            results.append({
                "sharpe": 0.0, # Fast batching skips daily return sharpe calculation
                "mdd": float(round(mdd_finals[j], 4)),
                "total_return": float(round(total_returns[j] * 100, 2)),
                "win_rate": float(round(win_rates[j] * 100, 2)),
                "trades": int(float(trades[j])),
                "avg_hold_hours": 0.0, # Approximate
                "avg_hold_days": 0.0
            })
            
        return results


# ─────────────────────────────────────────────────────────────────────────────
# 5b. 병렬 시뮬레이션 (ProcessPool — CPU 코어 6개까지)
# ─────────────────────────────────────────────────────────────────────────────
_pool_data = None
_pool_sim = None


def _init_optimizer_pool(data: list):
    global _pool_data, _pool_sim
    _pool_data = data
    _pool_sim = StrategySimulator()


def _run_sim_task(params: dict) -> tuple:
    return params, _pool_sim.simulate(_pool_data, params)

def _run_batch_sim_task(params_chunk: list) -> list:
    if not params_chunk: return []
    # If USDT, call usdt batch. If BearScalp only (no indicators), fallback or use logic.
    # Actually, to make it simple, we use the unified batch method or just logic batch
    is_usdt = params_chunk[0].get("usdt_fx_filter") or params_chunk[0].get("usdt_fusion")
    if is_usdt:
        results = _pool_sim._simulate_usdt_fusion_numpy_batch(_pool_data, params_chunk)
    else:
        results = _pool_sim._simulate_logic_numpy_batch(_pool_data, params_chunk)
    return list(zip(params_chunk, results))


def _strip_risk_fields(params: dict) -> dict:
    return {k: v for k, v in params.items() if k not in _RISK_ONLY_KEYS}


def _parallel_simulate(
    data: list,
    params_list: list,
    max_workers: int = OPTIMIZER_MAX_WORKERS,
    progress_every: int = 200,
    progress_fn: Optional[Callable[[int, int], None]] = None,
) -> list:
    """파라미터 목록을 ProcessPool로 병렬 백테스트 (스레드가 아닌 프로세스)."""
    if not params_list:
        return []

    workers = min(max_workers, len(params_list))
    if HAS_GPU:
        workers = min(workers, 2)
    if workers <= 1 or len(params_list) < PARALLEL_MIN_BATCH:
        sim = StrategySimulator()
        out = []
        for i, params in enumerate(params_list):
            out.append((params, sim.simulate(data, params)))
            if progress_fn and ((i + 1) % progress_every == 0):
                progress_fn(i + 1, len(params_list))
        return out

    results = []
    done = 0
    last_t = time.time()
    ctx = multiprocessing.get_context("spawn")
    chunk_size = 500 if HAS_GPU else 1000
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=ctx,
        initializer=_init_optimizer_pool,
        initargs=(data,),
    ) as executor:
        futures = []
        for i in range(0, len(params_list), chunk_size):
            chunk = params_list[i : i + chunk_size]
            futures.append(executor.submit(_run_batch_sim_task, chunk))
            
        for fut in as_completed(futures):
            batch_res = fut.result()
            results.extend(batch_res)
            done += len(batch_res)
            if progress_fn and (done % progress_every == 0 or time.time() - last_t > 4.0):
                progress_fn(done, len(params_list))
                last_t = time.time()
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 6. 그리드 서치 최적화
# ─────────────────────────────────────────────────────────────────────────────
def _calculate_expected_profits(total_return_pct: float, regime_data: list) -> dict:
    """전체 백테스트 기간 기반 단기→장기 기대 수익률(복리) 계산."""
    if len(regime_data) >= 2:
        try:
            t1 = datetime.fromisoformat(regime_data[0]["ts"])
            t2 = datetime.fromisoformat(regime_data[-1]["ts"])
            days = max((t2 - t1).days, 1)
        except Exception:
            days = 365
    else:
        days = 365

    daily_rate = (1 + total_return_pct / 100.0) ** (1.0 / days) - 1.0

    def _compound(n_days: int) -> float:
        return ((1 + daily_rate) ** n_days - 1.0) * 100.0

    return {
        "1d": round(_compound(1), 2),
        "3d": round(_compound(3), 2),
        "1w": round(_compound(7), 2),
        "2w": round(_compound(14), 2),
        "3w": round(_compound(21), 2),
        "1m": round(_compound(30), 2),
        "3m": round(_compound(90), 2),
        "6m": round(_compound(180), 2),
    }


class GridSearchOptimizer:
    def __init__(self):
        self.sim = StrategySimulator()

    def _iter_strategy_params(self):
        """지표·Logic 파라미터만 (리스크 제외)."""
        for combo in STRATEGY_COMBOS:
            for logic in _logics_for_combo(combo):
                rsi_periods   = GRID["rsi_period"]   if combo["rsi"]  else [14]
                rsi_oversolds = GRID["rsi_oversold"]  if combo["rsi"]  else [30]
                rsi_overs     = GRID["rsi_overbought"] if combo["rsi"] else [70]
                bb_periods    = GRID["bb_period"]     if combo["bb"]   else [20]
                bb_stds       = GRID["bb_std"]        if combo["bb"]   else [2.0]
                macd_fasts    = GRID["macd_fast"]     if combo["macd"] else [12]
                macd_slows    = GRID["macd_slow"]     if combo["macd"] else [26]
                macd_sigs     = GRID["macd_signal"]   if combo["macd"] else [9]

                for rp in rsi_periods:
                  for ro in rsi_oversolds:
                    for rob in rsi_overs:
                      for bp in bb_periods:
                        for bs in bb_stds:
                          for mf in macd_fasts:
                            for ms in macd_slows:
                              if mf >= ms:
                                  continue
                              for msig in macd_sigs:
                                yield {
                                    "custom_bear": False,
                                    **combo,
                                    "logic": logic,
                                    "threshold": DEFAULT_WEIGHTED_THRESHOLD,
                                    "rsi_period": rp, "rsi_oversold": ro, "rsi_overbought": rob,
                                    "bb_period": bp, "bb_std": bs,
                                    "macd_fast": mf, "macd_slow": ms, "macd_signal": msig,
                                }

    def _iter_usdt_mixed_params(self):
        """USDT 학습믹스: 3대 지표 × 당일 환율차(gap 진입) 그리드."""
        for base in self._iter_strategy_params():
            for min_gap in USDT_FX_GRID["min_consider_gap_krw"]:
                for exit_gap in USDT_FX_GRID["exit_gap_krw"]:
                    for exit_tp in USDT_FX_GRID["exit_take_profit_pct"]:
                        yield {
                            **base,
                            "usdt_fx_filter": True,
                            "min_consider_gap_krw": min_gap,
                            "min_target_profit_pct": 0.0,
                            "exit_gap_krw": exit_gap,
                            "exit_take_profit_pct": exit_tp,
                        }

    def _iter_usdt_fusion_params(self):
        for min_gap in USDT_FX_GRID["min_consider_gap_krw"]:
            for exit_gap in USDT_FX_GRID["exit_gap_krw"]:
                for exit_tp in USDT_FX_GRID["exit_take_profit_pct"]:
                    p = _default_usdt_fusion_sim_params()
                    p["min_consider_gap_krw"] = min_gap
                    p["exit_gap_krw"] = exit_gap
                    p["min_target_profit_pct"] = exit_tp
                    yield p

    def _iter_params(self, regime: str):
        """전략 × 리스크 전체 조합 (소규모 테스트·폴백용)."""
        for base in self._iter_strategy_params():
            for risk_cfg in _iter_risk_configs(regime):
                yield {**base, **risk_cfg}

    def _iter_custom_bear_params(self):
        """CustomBear 전략용 파라미터 조합 생성기."""
        lookbacks     = [4, 6, 8, 12, 16, 24]
        drop_pcts     = [0.01, 0.02, 0.03, 0.05, 0.08]
        volume_ratios = [1.2, 1.5, 2.0, 2.5, 3.0]
        trail_pcts    = [0.01, 0.02, 0.03, 0.04, 0.05]
        stop_losses   = [0.01, 0.02, 0.03, 0.04, 0.05]
        target_profit_pcts = [0.008, 0.012, 0.018, 0.025, 0.03, 0.035, 0.04, 0.05, 0.06]

        for lb in lookbacks:
            for dp in drop_pcts:
                for vr in volume_ratios:
                    for trail in trail_pcts:
                        for sl in stop_losses:
                            for tp in target_profit_pcts:
                                yield {
                                    "custom_bear": True,
                                    "rsi": False, "bb": False, "macd": False,
                                    "lookback": lb,
                                    "drop_pct": dp,
                                    "volume_ratio": vr,
                                    "trail_pct": trail,
                                    "stop_loss": sl,
                                    "target_profit_pct": tp,
                                }

    def optimize_regime(self, data: list, regime: str, market: str, base_pct: float = 20.0, span_pct: float = 60.0) -> dict:
        """
        지정 장세에 대해 그리드 서치 실행.
        BEAR 장세는 믹스 전략과 CustomBear 전략을 각각 돌려 비교 결과 출력.
        """
        regime_data = [d for d in data if d["regime"] == regime]
        episodes = _detect_bear_episodes_from_data(data, market) if regime == "BEAR" else []
        profit_basis = (
            _bear_profit_basis(regime_data, data, episodes) if regime == "BEAR" else regime_data
        )

        def get_profits(ret_pct):
            return _calculate_expected_profits(ret_pct, profit_basis)

        if regime == "BEAR":
            ep_summary = ", ".join(
                f"#{ep['episode_number']} {ep['peak_date'][:7]} -{ep['drawdown_pct']:.0f}%"
                for ep in episodes[:6]
            )
            logger.info(
                f"[Optimizer] {market}/BEAR: 역사적 하락 패턴 {len(episodes)}개 "
                f"({ep_summary}) → {len(profit_basis)}봉 비교 "
                f"(익절 상한 {BEAR_OPTIM_MAX_TAKE_PROFIT_PCT * 100:.1f}%)"
            )

        if regime != "BEAR":
            # BULL, RANGE는 기존 믹스 전략만 수행
            best_params, best_result = self._optimize_mixed(data, regime, market, base_pct, span_pct)
            if best_params is None:
                return _default_regime_params(regime)
            return _build_output(best_params, best_result, get_profits(best_result["total_return"]))
        else:
            # BEAR 장세: 믹스 전략(50%) + CustomBear 전략(50%) 수행
            mixed_span = span_pct * 0.5
            custom_span = span_pct * 0.5
            
            mixed_params, mixed_result = self._optimize_mixed(
                data, regime, market, base_pct, mixed_span, bear_episodes=episodes
            )
            custom_params, custom_result = self._optimize_custom_bear(
                data, regime, market, base_pct + mixed_span, custom_span, bear_episodes=episodes
            )
            
            if mixed_params is None:
                mixed_out = _default_regime_params(regime)
                mixed_out["period_expected_profits"] = {
                    "1d": 0, "3d": 0, "1w": 0, "2w": 0, "1m": 0, "3m": 0, "6m": 0
                }
            else:
                mixed_out = _build_output(mixed_params, mixed_result, get_profits(mixed_result["total_return"]))
                
            if custom_params is None:
                custom_out = _default_custom_bear_params(regime)
            else:
                custom_out = _build_custom_bear_output(custom_params, custom_result, get_profits(custom_result["total_return"]))
                
            return {
                "mixed_strategy": mixed_out,
                "custom_bear_strategy": custom_out,
                **mixed_out  # 하위 호환 병합
            }

    def _optimize_mixed(
        self,
        data: list,
        regime: str,
        market: str,
        base_pct: float,
        span_pct: float,
        bear_episodes: list = None,
    ):
        best_score = -9999.0
        best_result = None
        best_params = None

        default_risk = next(_iter_risk_configs(regime))
        if market == "KRW-USDT":
            strategy_combos = list(self._iter_usdt_mixed_params())
        else:
            strategy_combos = list(self._iter_strategy_params())
        risk_combos = list(_iter_risk_with_sell_min(regime))
        total_phase1 = len(strategy_combos)
        total_phase2 = min(25, total_phase1) * len(risk_combos)
        total_combos = total_phase1 + total_phase2

        first_span = span_pct * 0.75
        second_span = span_pct * 0.25
        last_update_time = time.time()

        # 1단계: 전략 파라미터 그리드 (기본 리스크 1종으로 후보 추림)
        phase1_params = [
            _with_bear_optim_context(
                {**base, **default_risk, "target_regime": regime}, regime, bear_episodes
            )
            for base in strategy_combos
        ]

        def _on_phase1(done, total):
            nonlocal last_update_time
            pct = base_pct + (done / max(total, 1)) * first_span
            _save_progress(
                "optimizing", pct,
                f"{market} {regime} 전략 그리드 ({done}/{total}) [×{OPTIMIZER_MAX_WORKERS}]",
            )
            last_update_time = time.time()

        candidates = []
        for params, result in _parallel_simulate(data, phase1_params, progress_fn=_on_phase1):
            base = _strip_risk_fields(params)
            if result["mdd"] <= MDD_LIMIT and result["trades"] >= 5:
                candidates.append((_optimizer_profit_score(result), base, result))

        total = len(phase1_params)

        candidates.sort(key=lambda x: x[0], reverse=True)
        top_bases = [c[1] for c in candidates[:25]]
        if not top_bases:
            top_bases = [dict(base) for base in strategy_combos[:25]]

        logger.info(f"[Optimizer] {market}/{regime} Mixed 1단계: {len(candidates)}개 후보 → 리스크 2단계 {len(top_bases)}×{len(risk_combos)}")

        # 2단계: 상위 전략 × 리스크 그리드
        phase2_params = [
            _with_bear_optim_context(
                {**base, **risk_cfg, "target_regime": regime}, regime, bear_episodes
            )
            for base in top_bases
            for risk_cfg in risk_combos
        ]
        phase2_total = len(phase2_params)

        def _on_phase2(done, total):
            nonlocal last_update_time
            pct = base_pct + first_span + (done / max(total, 1)) * second_span
            _save_progress(
                "optimizing", pct,
                f"{market} {regime} 리스크 최적화 ({done}/{total}) [×{OPTIMIZER_MAX_WORKERS}]",
            )
            last_update_time = time.time()

        for params, result in _parallel_simulate(data, phase2_params, progress_every=50, progress_fn=_on_phase2):
            if result["mdd"] > MDD_LIMIT or result["trades"] < 5:
                continue
            score = _optimizer_profit_score(result)
            if score > best_score:
                best_score = score
                best_result = result
                best_params = dict(params)

        total += phase2_total

        logger.info(f"[Optimizer] {market}/{regime} Mixed: 총 {total}조합, 최적 profit_score={best_score:.4f}")

        if best_params is None:
            logger.warning(f"[Optimizer] {market}/{regime} Mixed: MDD 필터 통과 없음 → 1단계 차선책")
            fb_best = -9999.0
            for score, base, result in candidates:
                if result["trades"] >= 3 and score > fb_best:
                    fb_best = score
                    best_score = score
                    best_result = result
                    best_params = {**base, **default_risk, "target_regime": regime}

        _save_progress("optimizing", base_pct + span_pct, f"{market} {regime} 믹스전략 최적화 완료")
        return best_params, best_result

    def _optimize_custom_bear(
        self,
        data: list,
        regime: str,
        market: str,
        base_pct: float,
        span_pct: float,
        bear_episodes: list = None,
    ):
        best_score = -9999.0
        best_result = None
        best_params = None
        total = 0
        valid = 0

        total_combos = sum(1 for _ in self._iter_custom_bear_params())
        last_update_time = time.time()

        first_span = span_pct * 0.8
        fallback_span = span_pct * 0.2

        custom_params_list = []
        for params in self._iter_custom_bear_params():
            p = dict(params)
            p["target_regime"] = regime
            custom_params_list.append(_with_bear_optim_context(p, regime, bear_episodes))

        def _on_custom(done, total):
            nonlocal last_update_time
            combo_pct = (done / max(total, 1)) * first_span
            pct = base_pct + combo_pct
            _save_progress(
                "optimizing", pct,
                f"{market} {regime} CustomBear 최적화 중 ({done}/{total}) [×{OPTIMIZER_MAX_WORKERS}]",
            )
            last_update_time = time.time()

        total = len(custom_params_list)
        valid = 0
        for params, result in _parallel_simulate(
            data, custom_params_list, progress_every=50, progress_fn=_on_custom
        ):
            if result["mdd"] > MDD_LIMIT:
                continue
            if result["trades"] < 3:
                continue
            valid += 1
            score = _optimizer_profit_score(result)
            if score > best_score:
                best_score = score
                best_result = result
                best_params = dict(params)

        logger.info(f"[Optimizer] {market}/{regime} CustomBear: 총 {total}조합 검색, {valid}개 통과, 최적 profit_score={best_score:.4f}")

        if best_params is None:
            logger.warning(f"[Optimizer] {market}/{regime} CustomBear: 필터 통과 없음 → 차선책 선택")

            def _on_fallback(done, total):
                nonlocal last_update_time
                fb_pct = (done / max(total, 1)) * fallback_span
                pct = base_pct + first_span + fb_pct
                _save_progress(
                    "optimizing", pct,
                    f"{market} {regime} CustomBear 차선책 ({done}/{total}) [×{OPTIMIZER_MAX_WORKERS}]",
                )
                last_update_time = time.time()

            for params, result in _parallel_simulate(
                data, custom_params_list, progress_every=50, progress_fn=_on_fallback
            ):
                score = _optimizer_profit_score(result)
                if score > best_score and result["trades"] >= 1:
                    best_score = score
                    best_result = result
                    best_params = dict(params)

        _save_progress("optimizing", base_pct + span_pct, f"{market} {regime} CustomBear 최적화 완료")
        return best_params, best_result

    def optimize_usdt_dual(self, data: list, market: str, base_pct: float, span_pct: float) -> dict:
        """USDT: 고정 융합(환율+BB 그리드) vs 3대 지표+당일환율 학습 비교."""
        bear_range = [d for d in data if d.get("regime") in ("BEAR", "RANGE")]
        sim_data = bear_range if len(bear_range) >= 30 else data

        def get_profits(ret_pct):
            return _calculate_expected_profits(ret_pct, sim_data)

        fixed_span = span_pct * 0.25
        learned_span = span_pct * 0.75

        fixed_params, fixed_result = self._optimize_usdt_fusion(
            sim_data, market, base_pct, fixed_span
        )
        if fixed_params is None:
            fixed_out = _default_usdt_fusion_output()
        else:
            fixed_out = _build_usdt_fusion_output(
                fixed_params, fixed_result, get_profits(fixed_result["total_return"])
            )

        mixed_params, mixed_result = self._optimize_mixed(
            data, "RANGE", market, base_pct + fixed_span, learned_span
        )
        if mixed_params is None:
            mixed_out = _default_regime_params("RANGE")
            mixed_out["period_expected_profits"] = {h: 0 for h in ("1d", "3d", "1w", "2w", "3w", "1m", "3m", "6m")}
        else:
            mixed_out = _build_output(mixed_params, mixed_result, get_profits(mixed_result["total_return"]))

        return {
            "fixed_fusion_strategy": fixed_out,
            "learned_mixed_strategy": mixed_out,
            **mixed_out,
        }

    def _optimize_usdt_fusion(self, data: list, market: str, base_pct: float, span_pct: float):
        best_score = -9999.0
        best_result = None
        best_params = None
        param_list = list(self._iter_usdt_fusion_params())
        total = len(param_list)

        def _on_progress(done, tot):
            pct = base_pct + (done / max(tot, 1)) * span_pct
            _save_progress("optimizing", pct, f"{market} 고정융합+환율 그리드 ({done}/{tot})")

        for params, result in _parallel_simulate(data, param_list, progress_fn=_on_progress):
            score = _usdt_optimizer_score(result, params)
            if score > best_score:
                best_score = score
                best_result = result
                best_params = dict(params)

        if best_params:
            best_params["min_consider_gap_krw"] = max(
                int(best_params.get("min_consider_gap_krw", USDT_DEFAULT_ENTRY_GAP_KRW)),
                USDT_MIN_ENTRY_GAP_FLOOR,
            )
            best_params["exit_gap_krw"] = max(
                int(best_params.get("exit_gap_krw", USDT_DEFAULT_EXIT_GAP_KRW)),
                USDT_MIN_EXIT_GAP_FLOOR,
            )
            best_params["min_target_profit_pct"] = max(
                float(best_params.get("min_target_profit_pct", USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT)),
                USDT_MIN_EXIT_TP_FLOOR,
            )
        else:
            best_params = _default_usdt_fusion_sim_params()

        if best_params and best_result and int(best_result.get("trades", 0)) >= 3:
            save_usdt_optimal_defaults(best_params, best_result)
            refresh_usdt_defaults(force=True)

        logger.info(
            f"[Optimizer] {market} UsdtFusion: {total}조합, "
            f"최적 usdt_score={best_score:.4f}, "
            f"gap={best_params.get('min_consider_gap_krw') if best_params else None}원, "
            f"목표={best_params.get('min_target_profit_pct') if best_params else None}%"
        )
        _save_progress("optimizing", base_pct + span_pct, f"{market} 고정 융합 최적화 완료")
        return best_params, best_result

    def run_full_optimization(self, market: str, data: list, m_idx: int = 0, num_markets: int = 3) -> dict:
        """종목 전체 장세 최적화."""
        if market == "KRW-USDT" and HAS_USDT_FX_HIST:
            ensure_fx_history()
            logger.info(f"[Optimizer] {market} 캔들에 당일 USD/KRW 환율(fair_krw) 부착")
            rates = ensure_daily_fx_for_bars(data)
            data = enrich_bars_with_historical_fx(data, rates)

        labeler = RegimeLabeler()
        labeled = labeler.label(data)
        output = {}
        regimes = ["BULL", "BEAR", "RANGE"]
        
        market_weight = 100.0 / num_markets
        optimize_weight = market_weight * 0.8
        regime_weight = optimize_weight / len(regimes)
        collect_weight = market_weight * 0.2

        if market == "KRW-USDT":
            base_pct = (m_idx * market_weight) + collect_weight
            dual = self.optimize_usdt_dual(labeled, market, base_pct, optimize_weight)
            return {"BULL": dual, "BEAR": dual, "RANGE": dual}
        
        for r_idx, regime in enumerate(regimes):
            base_pct = (m_idx * market_weight) + collect_weight + (r_idx * regime_weight)
            output[regime] = self.optimize_regime(
                labeled, regime, market, 
                base_pct=base_pct, 
                span_pct=regime_weight
            )
        return output


# ─────────────────────────────────────────────────────────────────────────────
# 7. 출력 포맷 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def _build_backtest_block(result: dict) -> dict:
    hold = _hold_stats([]) if "avg_hold_hours" not in result else {
        "avg_hold_hours": result.get("avg_hold_hours", 0),
        "median_hold_hours": result.get("median_hold_hours", 0),
        "avg_hold_days": result.get("avg_hold_days", 0),
    }
    return {
        "total_return_pct": result["total_return"],
        "sharpe_ratio":     result["sharpe"],
        "mdd_pct":          round(result["mdd"] * 100, 2),
        "win_rate_pct":     result["win_rate"],
        "trade_count":      result["trades"],
        **hold,
    }


def _build_output(params: dict, result: dict, expected_profits: dict = None) -> dict:
    out = {
        "strategies": {
            "RSI": {
                "enabled":    params["rsi"],
                "weight":     1.0,
                "period":     params["rsi_period"],
                "oversold":   params["rsi_oversold"],
                "overbought": params["rsi_overbought"]
            },
            "Bollinger": {
                "enabled": params["bb"],
                "weight":  1.0,
                "period":  params["bb_period"],
                "std_dev": params["bb_std"]
            },
            "MACD": {
                "enabled":       params["macd"],
                "weight":        1.0,
                "fast":          params["macd_fast"],
                "slow":          params["macd_slow"],
                "signal_period": params["macd_signal"]
            }
        },
        "logic":       params.get("logic", "OR"),
        "threshold":   params.get("threshold", DEFAULT_WEIGHTED_THRESHOLD),
        "risk":        _build_risk_output(params),
        "backtest":    _build_backtest_block(result),
    }
    if expected_profits:
        out["period_expected_profits"] = expected_profits
    if params.get("usdt_fx_filter"):
        out["usdt_fx_filter"] = {
            "min_consider_gap_krw": params.get("min_consider_gap_krw", USDT_DEFAULT_ENTRY_GAP_KRW),
            "min_target_profit_pct": params.get("min_target_profit_pct", USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT),
            "exit_gap_krw": params.get("exit_gap_krw", USDT_DEFAULT_EXIT_GAP_KRW),
        }
    return out


def _default_usdt_fusion_output() -> dict:
    p = _default_usdt_fusion_sim_params()
    return _build_usdt_fusion_output(p, {
        "total_return": 0, "sharpe": -999, "mdd": 1.0, "win_rate": 0, "trades": 0,
        "avg_hold_hours": 0, "median_hold_hours": 0, "avg_hold_days": 0,
    }, {"1d": 0, "3d": 0, "1w": 0, "3w": 0})


def _default_usdt_fusion_sim_params(regime: str = "RANGE") -> dict:
    if HAS_USDT_FX_HIST:
        refresh_usdt_defaults()
    return {
        "usdt_fusion": True,
        "use_all_regimes": True,
        "target_regime": regime,
        "min_consider_gap_krw": USDT_DEFAULT_ENTRY_GAP_KRW,
        "min_target_profit_pct": USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT,
        "exit_gap_krw": USDT_DEFAULT_EXIT_GAP_KRW,
        "exit_convergence_pct": 90,
        "fee_one_way": FEE_RATE,
        "fair_ma_period": 672,
        "bb_period": 20,
        "bb_std": 2.0,
    }


def _build_usdt_fusion_output(params: dict, result: dict, expected_profits: dict = None) -> dict:
    return {
        "strategies": {
            "UsdtFxBollinger": {
                "enabled": True,
                "weight": 1.0,
                "timeframe": "15m",
                "min_consider_gap_krw": params.get("min_consider_gap_krw", USDT_DEFAULT_ENTRY_GAP_KRW),
                "min_target_profit_pct": params.get("min_target_profit_pct", USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT),
                "exit_gap_krw": params.get("exit_gap_krw", USDT_DEFAULT_EXIT_GAP_KRW),
                "exit_convergence_pct": params.get("exit_convergence_pct", 90),
                "bb_period": params.get("bb_period", 20),
                "bb_std_dev": params.get("bb_std", 2.0),
            }
        },
        "usdt_fx_filter": {
            "min_consider_gap_krw": params.get("min_consider_gap_krw", USDT_DEFAULT_ENTRY_GAP_KRW),
            "min_target_profit_pct": params.get("min_target_profit_pct", USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT),
            "exit_gap_krw": params.get("exit_gap_krw", USDT_DEFAULT_EXIT_GAP_KRW),
            "exit_convergence_pct": params.get("exit_convergence_pct", 90),
        },
        "logic": "OR",
        "threshold": 0.5,
        "risk": {
            "type": "None",
        },
        "backtest": _build_backtest_block(result),
        "period_expected_profits": expected_profits or {},
    }


def _build_custom_bear_output(params: dict, result: dict, expected_profits: dict) -> dict:
    return {
        "strategies": {
            "CustomBear": {
                "enabled":      True,
                "lookback":     params["lookback"],
                "drop_pct":     params["drop_pct"],
                "volume_ratio": params["volume_ratio"],
                "trail_pct":    params["trail_pct"],
                "stop_loss":    params["stop_loss"],
                "target_profit_pct": params.get("target_profit_pct", BEAR_TARGET_PROFIT_FALLBACK),
                "optimal_hold_hours": round(
                    float(result.get("median_hold_hours") or result.get("avg_hold_hours") or 0), 1
                ),
            }
        },
        "logic": "CUSTOM_BEAR",
        "threshold": 0.5,
        "risk": {
            "type": "StopLoss",
            "stop_loss_pct": params["stop_loss"],
            "take_profit_pct": 9.9
        },
        "backtest": _build_backtest_block(result),
        "period_expected_profits": expected_profits
    }


def _default_regime_params(regime: str) -> dict:
    """데이터 부족 시 기본값 반환."""
    return {
        "strategies": {
            "RSI": {"enabled": True, "weight": 1.0, "period": 14, "oversold": 30, "overbought": 70},
            "Bollinger": {"enabled": regime == "RANGE", "weight": 1.0, "period": 20, "std_dev": 2.0},
            "MACD": {"enabled": regime == "BULL", "weight": 1.0, "fast": 12, "slow": 26, "signal_period": 9}
        },
        "logic": "OR", "threshold": 0.5,
        "risk": {"type": "StopLoss" if regime != "BULL" else "None",
                 "stop_loss_pct": 0.02, "take_profit_pct": 0.03},
        "backtest": {
            "total_return_pct": 0, "sharpe_ratio": 0, "mdd_pct": 0,
            "win_rate_pct": 0, "trade_count": 0,
            "avg_hold_hours": 0, "median_hold_hours": 0, "avg_hold_days": 0,
        }
    }


def _default_custom_bear_params(regime: str) -> dict:
    return {
        "strategies": {
            "CustomBear": {
                "enabled": True,
                "lookback": 8,
                "drop_pct": 0.05,
                "volume_ratio": 2.0,
                "trail_pct": 0.015,
                "stop_loss": 0.015,
                "target_profit_pct": BEAR_TARGET_PROFIT_FALLBACK,
                "optimal_hold_hours": 0,
            }
        },
        "logic": "CUSTOM_BEAR",
        "threshold": 0.5,
        "risk": {
            "type": "StopLoss",
            "stop_loss_pct": 0.015,
            "take_profit_pct": 9.9
        },
        "backtest": {
            "total_return_pct": 0,
            "sharpe_ratio": 0,
            "mdd_pct": 0,
            "win_rate_pct": 0,
            "trade_count": 0,
            "avg_hold_hours": 0,
            "median_hold_hours": 0,
            "avg_hold_days": 0,
        },
        "period_expected_profits": {
            "1d": 0, "3d": 0, "1w": 0, "2w": 0, "1m": 0, "3m": 0, "6m": 0
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# 8. 메인 진입점
# ─────────────────────────────────────────────────────────────────────────────
def run_optimization(markets: list = None) -> dict:
    """
    전체 파이프라인 실행:
    1. 캐시 로드 → 없으면 전체 수집, 있으면 증분 추가
    2. 종목별/장세별 그리드 서치
    3. optimized_params.json 저장
    반환: 최적화 결과 dict
    """
    if markets is None:
        markets = MARKETS

    _save_progress("start", 0, "백테스팅 최적화 시작...")
    logger.info(f"[Pipeline] 병렬 프로세스 {OPTIMIZER_MAX_WORKERS}개 사용 (CPU 코어 기준)")
    fetcher = HistoricalDataFetcher()
    cache_mgr = DataCache()
    optimizer = GridSearchOptimizer()

    cache = cache_mgr.load()
    cache["_meta"] = {
        "schema": CACHE_SCHEMA,
        "interval": "15m",
        "points_per_day": len(TARGET_TIMES_KST),
    }
    results = {
        "last_updated": datetime.now(KST).isoformat(),
        "source": f"빗썸 15분봉 ({len(TARGET_TIMES_KST)}포인트/일, "
                  f"KST 매시 0·15·30·45분)",
        "fee_assumption": "편도 0.25% (빗썸 실거래 체결 기준, 왕복 0.50%)",
        "optimization_criteria": (
            f"profit_score(총수익·보유기간 우선) + MDD <= {int(MDD_LIMIT * 100)}% "
            "(지표 7조합 × OR/AND/VOTE/WEIGHTED_VOTE × 리스크·매도최소익 그리드)"
        ),
        "parallel_workers": OPTIMIZER_MAX_WORKERS,
    }

    num_markets = len(markets)
    market_weight = 100.0 / num_markets
    collect_weight = market_weight * 0.2

    for m_idx, market in enumerate(markets):
        logger.info(f"\n{'='*60}")
        logger.info(f"[Pipeline] {market} 처리 시작")
        
        base_collect_pct = m_idx * market_weight
        _save_progress("collecting", base_collect_pct, f"{market} 데이터 준비중...")

        # 데이터 수집 (최초 or 증분)
        last_ts = cache_mgr.get_last_ts(cache, market)
        if last_ts is None:
            logger.info(f"[Pipeline] {market} 캐시 없음 -> 전체 9년 수집")
            new_data = fetcher.fetch_full_history(market, years=9, m_idx=m_idx, num_markets=num_markets)
        else:
            logger.info(f"[Pipeline] {market} 캐시 있음 (마지막: {last_ts}) -> 증분 수집")
            new_data = fetcher.fetch_incremental(market, last_ts)

        cache = cache_mgr.append(cache, market, new_data)
        cache_mgr.save(cache)

        # 최적화 실행
        data = cache[market]["data"]
        logger.info(f"[Pipeline] {market} 총 {len(data)}개 데이터로 최적화 시작")
        _save_progress("optimizing", base_collect_pct + collect_weight, f"{market} 그리드서치 실행중...")

        regime_results = optimizer.run_full_optimization(market, data, m_idx=m_idx, num_markets=num_markets)
        results[market] = regime_results

    # 결과 저장
    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    _save_progress("done", 100, f"최적화 완료! {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST")
    logger.info(f"[Pipeline] 최적화 완료 -> {RESULT_FILE}")
    return results


def run_usdt_daily_tuning() -> Optional[dict]:
    """
    USDT 전용 경량 튜닝 — gap 진입 · gap/TP 탈출 그리드 (2주 1회 스케줄).
    기본값: 진입 gap 35원 · 탈출 gap 15원 · 백업 TP 2.5%.
    """
    market = "KRW-USDT"
    cache_mgr = DataCache()
    cache = cache_mgr.load()
    if not cache or market not in cache:
        logger.warning("[UsdtDaily] USDT 캐시 없음")
        return None

    data = cache[market].get("data") or []
    if len(data) < 500:
        logger.warning(f"[UsdtDaily] USDT 데이터 부족 ({len(data)}봉)")
        return None

    if HAS_USDT_FX_HIST:
        ensure_fx_history()
        rates = ensure_daily_fx_for_bars(data)
        data = enrich_bars_with_historical_fx(data, rates)

    labeler = RegimeLabeler()
    labeled = labeler.label(data)
    sim_data = [d for d in labeled if d.get("regime") in ("BEAR", "RANGE")]
    if len(sim_data) < 30:
        sim_data = labeled

    optimizer = GridSearchOptimizer()
    fixed_params, fixed_result = optimizer._optimize_usdt_fusion(sim_data, market, 0, 100)
    if not fixed_params:
        fixed_params = _default_usdt_fusion_sim_params()
    if not fixed_result or int(fixed_result.get("trades", 0)) < 1:
        fixed_result = StrategySimulator()._simulate_usdt_fusion(sim_data, fixed_params)

    fixed_params["min_consider_gap_krw"] = max(
        int(fixed_params.get("min_consider_gap_krw", USDT_DEFAULT_ENTRY_GAP_KRW)),
        USDT_MIN_ENTRY_GAP_FLOOR,
    )
    fixed_params["exit_gap_krw"] = max(
        int(fixed_params.get("exit_gap_krw", USDT_DEFAULT_EXIT_GAP_KRW)),
        USDT_MIN_EXIT_GAP_FLOOR,
    )
    fixed_params["min_target_profit_pct"] = max(
        float(fixed_params.get("min_target_profit_pct", USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT)),
        USDT_MIN_EXIT_TP_FLOOR,
    )

    if int(fixed_result.get("trades", 0)) >= 3:
        save_usdt_optimal_defaults(fixed_params, fixed_result)
        refresh_usdt_defaults(force=True)

    def get_profits(ret_pct):
        return _calculate_expected_profits(ret_pct, sim_data)

    fixed_out = _build_usdt_fusion_output(
        fixed_params, fixed_result, get_profits(fixed_result.get("total_return", 0))
    )

    existing = get_latest_results() or {}
    prev_usdt = existing.get(market, {})
    learned = prev_usdt.get("RANGE", {}).get("learned_mixed_strategy")
    if not learned:
        learned = prev_usdt.get("learned_mixed_strategy") or _default_regime_params("RANGE")

    dual = {
        "fixed_fusion_strategy": fixed_out,
        "learned_mixed_strategy": learned,
        **fixed_out,
    }

    existing[market] = {
        "BULL": dual,
        "BEAR": dual,
        "RANGE": dual,
    }

    tuned_at = datetime.now(KST).isoformat()
    existing["last_updated"] = tuned_at
    existing["usdt_daily_tuning"] = {
        "tuned_at": tuned_at,
        "min_target_profit_pct": fixed_params["min_target_profit_pct"],
        "min_consider_gap_krw": fixed_params.get("min_consider_gap_krw"),
        "exit_gap_krw": fixed_params.get("exit_gap_krw"),
        "exit_convergence_pct": fixed_params.get("exit_convergence_pct"),
        "bb_period": fixed_params.get("bb_period"),
        "bb_std": fixed_params.get("bb_std"),
        "backtest_trades": fixed_result.get("trades", 0),
        "backtest_return_pct": round(float(fixed_result.get("total_return", 0)), 2),
        "avg_hold_days": fixed_result.get("avg_hold_days", 0),
        "reason": (
            f"역사 환율 학습 — 진입 gap {fixed_params.get('min_consider_gap_krw')}원 · "
            f"탈출 gap≤{fixed_params.get('exit_gap_krw')}원 / TP {fixed_params.get('min_target_profit_pct')}%"
        ),
    }

    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    logger.info(
        f"[UsdtDaily] 완료 — 목표 {fixed_params['min_target_profit_pct']}%, "
        f"gap {fixed_params.get('min_consider_gap_krw')}원, "
        f"거래 {fixed_result.get('trades', 0)}회"
    )
    return existing["usdt_daily_tuning"]


def get_latest_results() -> Optional[dict]:
    """저장된 최적화 결과 반환 (없으면 None)."""
    if os.path.exists(RESULT_FILE):
        try:
            with open(RESULT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def get_progress() -> dict:
    """현재 최적화 진행 상태 반환."""
    if os.path.exists(PROGRESS_FILE):
        for attempt in range(10):
            try:
                with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                time.sleep(0.05)
    return {"stage": "idle", "percent": 0, "message": "대기중"}


# ─────────────────────────────────────────────────────────────────────────────
# 단독 실행 (테스트)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S"
    )
    print("=" * 60)
    print("백테스팅 최적화 모듈 단독 실행")
    print(f"대상: {MARKETS}")
    print(f"수집 포인트: {[f'{h:02d}:{m:02d}' for h,m in TARGET_TIMES_KST]}")
    print("=" * 60)
    result = run_optimization()
    print("\n[SUCCESS] 최적화 완료!")
    for market in MARKETS:
        if market in result:
            for regime in ["BULL", "BEAR", "RANGE"]:
                r = result[market].get(regime, {})
                bt = r.get("backtest", {})
                strats = r.get("strategies", {})
                active = [k for k, v in strats.items() if v.get("enabled")]
                print(f"  {market}/{regime}: {active} -> "
                      f"수익 {bt.get('total_return_pct',0):.1f}%, "
                      f"Sharpe {bt.get('sharpe_ratio',0):.3f}, "
                      f"MDD {bt.get('mdd_pct',0):.1f}%")

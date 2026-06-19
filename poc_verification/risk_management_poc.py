import os
import sys
import json
import copy
import uuid
import asyncio
import queue
import threading
import time
import websocket
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import hashlib
import jwt
from urllib.parse import urlencode, quote

KST = timezone(timedelta(hours=9))

# ── 백테스팅 모듈 임포트 (표준 import — ProcessPool pickle 호환) ──
_bt_dir = os.path.dirname(os.path.abspath(__file__))
if _bt_dir not in sys.path:
    sys.path.insert(0, _bt_dir)
try:
    import backtester as _bt_mod
    run_optimization    = _bt_mod.run_optimization
    get_latest_results  = _bt_mod.get_latest_results
    get_progress        = _bt_mod.get_progress
    BACKTEST_AVAILABLE  = True
    print("[Backtester] 모듈 로드 성공")
except Exception as _bt_err:
    BACKTEST_AVAILABLE = False
    _bt_mod = None
    print(f"[Backtester] 모듈 로드 실패 (무시): {_bt_err}")
    def run_optimization(markets=None): return {}
    def get_latest_results(): return None
    def get_progress(): return {"stage": "unavailable", "percent": 0, "message": "모듈 없음"}

# .env 환경 변수 파일 로더 구현
def load_env_file():
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.env"))
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip()
        print(f"[System] .env 설정을 로드했습니다.")
    else:
        print(f"[System] Warning: .env 파일을 찾을 수 없습니다.")

load_env_file()

BITHUMB_ACCESS_KEY = os.getenv("BITHUMB_ACCESS_KEY", "")
BITHUMB_SECRET_KEY = os.getenv("BITHUMB_SECRET_KEY", "")
BITHUMB_ORDER_MODE = os.getenv("BITHUMB_ORDER_MODE", "simulation").lower()

print(f"[System] Bithumb Access Key 존재 여부: {bool(BITHUMB_ACCESS_KEY)}")
print(f"[System] Bithumb Secret Key 존재 여부: {bool(BITHUMB_SECRET_KEY)}")
print(f"[System] Bithumb Order Mode: {BITHUMB_ORDER_MODE.upper()}")

# 빗썸 실거래 수수료 (체결 내역 기준 편도 약 0.25%)
BITHUMB_FEE_RATE_ONE_WAY = 0.0025


def calc_net_pnl_pct(avg_price: float, current_price: float) -> float:
    """매수·매도 수수료를 반영한 순수익률 (왕복 약 0.50%)."""
    if avg_price <= 0 or current_price <= 0:
        return 0.0
    entry_cost = avg_price * (1 + BITHUMB_FEE_RATE_ONE_WAY)
    exit_value = current_price * (1 - BITHUMB_FEE_RATE_ONE_WAY)
    return (exit_value - entry_cost) / entry_cost


# 빗썸 JWT 서명 및 헤더 생성 로직
def create_bithumb_headers(params: dict = None) -> dict:
    if not BITHUMB_ACCESS_KEY or not BITHUMB_SECRET_KEY:
        return {}
        
    payload = {
        "access_key": BITHUMB_ACCESS_KEY,
        "nonce": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000)
    }

    if params:
        # 빗썸 API v2 POST의 검증 방식에 맞춰 정렬 없이 쿼리 스트링 변환
        query_string = urlencode(params).encode("utf-8")
        query_hash = hashlib.sha512(query_string).hexdigest()
        payload["query_hash"] = query_hash
        payload["query_hash_alg"] = "SHA512"

    jwt_token = jwt.encode(payload, BITHUMB_SECRET_KEY, algorithm="HS256")
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

# 템플릿 경로 로딩
TEMPLATE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 
    "../.agents/skills/auto-trading-builder/templates"
))
sys.path.append(TEMPLATE_PATH)

from core_engine import TradingEngine, AccountManager, BaseStrategy, BaseRiskManager, TickerWorker, CompositeStrategy

# 추가 모듈 임포트
from candle_manager import CandleManager
from regime_detector import MarketRegimeDetector, TICKER_CYCLE_PROFILE, STABLECOIN_TICKERS
from usdt_fx_reference import (
    get_usd_krw_rate,
    calc_reverse_premium_pct,
    calc_usdt_fee_aware_edge,
    calc_usdt_consideration_phase,
)
try:
    from usdt_fx_historical import usdt_fx_buy_allowed
except ImportError:
    usdt_fx_buy_allowed = None

# 글로벌 설정 및 엔진 인스턴스 홀더
USDT_DEFAULT_MIN_CONSIDER_GAP_KRW = 40
USDT_FX_REFRESH_MINUTES = 10  # USD/KRW 기준환율 API 갱신 주기 — USDT 체결가는 WebSocket 실시간

def _usdt_fx_bollinger_params() -> dict:
    return {
        "min_consider_gap_krw": USDT_DEFAULT_MIN_CONSIDER_GAP_KRW,
        "min_target_profit_pct": 0.2,
        "sell_premium_pct": 0.15,
        "reference_krw": 0,
        "fx_refresh_minutes": USDT_FX_REFRESH_MINUTES,
        "fee_one_way_pct": 0.25,
        "bb_period": 20,
        "bb_std_dev": 2.0,
    }


def _build_default_usdt_config() -> dict:
    """USDT: 환율 필터(관심구역) + 볼린저 하단 타점 매수 · 밴드/환율 청산."""
    usdt_tactic = {
        "logic": "OR",
        "threshold": 0.5,
        "strategies": [
            {"name": "UsdtFxBollinger", "enabled": True, "weight": 1.0, "timeframe": "15m",
             "params": _usdt_fx_bollinger_params()},
        ],
        "risk": {"type": "StopLoss", "stop_loss_pct": 0.01, "take_profit_pct": 0.025},
    }
    return {
        "active": True,
        "current_regime": "RANGE",
        "regime_override": "AUTO",
        "selected_bear_strategy": "mixed",
        "long_term_ma_period": 400,
        "usdt_fx": {
            "reference_krw": 0,
            "min_consider_gap_krw": USDT_DEFAULT_MIN_CONSIDER_GAP_KRW,
            "min_target_profit_pct": 0.2,
            "sell_premium_pct": 0.15,
            "fx_refresh_minutes": USDT_FX_REFRESH_MINUTES,
            "fee_one_way_pct": 0.25,
            "bb_period": 20,
            "bb_std_dev": 2.0,
        },
        "tactics": {
            "BULL": usdt_tactic,
            "BEAR": usdt_tactic,
            "RANGE": usdt_tactic,
        },
        "selected_usdt_strategy": "auto",
        "usdt_auto_pick": "fixed_fusion",
        "usdt_auto_pick_reason": "초기값 — 고정 융합 전략",
        "resolved_usdt_strategy": "fixed_fusion",
    }


active_ticker_configs = {
    "BTC": {
        "active": True,
        "current_regime": "BEAR",
        "regime_override": "AUTO",
        "selected_bear_strategy": "auto",
        "long_term_ma_period": 800,
        "tactics": {
            "BULL": {
                "logic": "OR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "RSI", "enabled": True, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 38, "overbought": 65}},
                    {"name": "Bollinger", "enabled": False, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                    {"name": "MACD", "enabled": True, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                ],
                "risk": {"type": "None"}
            },
            "BEAR": {
                "logic": "CUSTOM_BEAR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "CustomBear", "enabled": True, "weight": 1.0, "timeframe": "15m", "params": {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "take_profit": 0.02, "stop_loss": 0.015, "time_cut": 48}}
                ],
                "risk": {"type": "StopLoss", "stop_loss_pct": 0.015, "take_profit_pct": 0.02}
            },
            "RANGE": {
                "logic": "OR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "RSI", "enabled": False, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 35, "overbought": 65}},
                    {"name": "Bollinger", "enabled": True, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                    {"name": "MACD", "enabled": False, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                ],
                "risk": {"type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.03}
            }
        }
    },
    "ETH": {
        "active": True,
        "current_regime": "BEAR",
        "regime_override": "AUTO",
        "selected_bear_strategy": "auto",
        "long_term_ma_period": 400,
        "tactics": {
            "BULL": {
                "logic": "OR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "RSI", "enabled": True, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 38, "overbought": 65}},
                    {"name": "Bollinger", "enabled": False, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                    {"name": "MACD", "enabled": True, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                ],
                "risk": {"type": "None"}
            },
            "BEAR": {
                "logic": "CUSTOM_BEAR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "CustomBear", "enabled": True, "weight": 1.0, "timeframe": "15m", "params": {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "take_profit": 0.02, "stop_loss": 0.015, "time_cut": 48}}
                ],
                "risk": {"type": "StopLoss", "stop_loss_pct": 0.015, "take_profit_pct": 0.02}
            },
            "RANGE": {
                "logic": "OR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "RSI", "enabled": False, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 35, "overbought": 65}},
                    {"name": "Bollinger", "enabled": True, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                    {"name": "MACD", "enabled": False, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                ],
                "risk": {"type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.03}
            }
        }
    },
    "XRP": {
        "active": True,
        "current_regime": "BEAR",
        "regime_override": "AUTO",
        "selected_bear_strategy": "auto",
        "long_term_ma_period": 400,
        "tactics": {
            "BULL": {
                "logic": "OR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "RSI", "enabled": True, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 38, "overbought": 65}},
                    {"name": "Bollinger", "enabled": False, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                    {"name": "MACD", "enabled": True, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                ],
                "risk": {"type": "None"}
            },
            "BEAR": {
                "logic": "CUSTOM_BEAR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "CustomBear", "enabled": True, "weight": 1.0, "timeframe": "15m", "params": {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "take_profit": 0.02, "stop_loss": 0.015, "time_cut": 48}}
                ],
                "risk": {"type": "StopLoss", "stop_loss_pct": 0.015, "take_profit_pct": 0.02}
            },
            "RANGE": {
                "logic": "OR",
                "threshold": 0.5,
                "strategies": [
                    {"name": "RSI", "enabled": False, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 35, "overbought": 65}},
                    {"name": "Bollinger", "enabled": True, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                    {"name": "MACD", "enabled": False, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                ],
                "risk": {"type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.03}
            }
        }
    },
    "USDT": _build_default_usdt_config(),
}
engine_instance = None

CONFIG_FILE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 
    "strategy_config.json"
))

DEFAULT_REGIME_OVERRIDE = "AUTO"
DEFAULT_BEAR_STRATEGY = "auto"  # 하락장 기본: 백테스트 우수 전략 자동 선택 (UI에서 custom_bear/mixed 고정 가능)
SUPPORTED_TICKERS = ["BTC", "ETH", "XRP", "USDT"]
# 백테스트·실매매 지표 정렬 타임프레임 (15분봉 96포인트/일)
OPTIM_BACKTEST_TIMEFRAME = "15m"
STRATEGY_BAR_SECONDS = 900
ORDER_SAME_DIR_COOLDOWN_SEC = 3  # 동일 틱 중복 주문만 방지 (분석 타이밍은 차단하지 않음)
TIMEFRAME_BAR_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400}
PORTFOLIO_MAX_SINGLE_BUY_OF_SHARE = 0.5  # 종목 할당분(1/N)의 절반까지 1회 매수
MIN_BITHUMB_ORDER_KRW = 5000.0

# BEAR auto: 단기→장기 순으로 예상 수익 비교 (동률 시 다음 기간)
BEAR_COMPARE_HORIZONS = ["1d", "3d", "1w", "2w", "1m", "3m", "6m"]
BEAR_HORIZON_LABELS = {
    "1d": "1일", "3d": "3일", "1w": "1주", "2w": "2주",
    "1m": "1개월", "3m": "3개월", "6m": "6개월",
}

# 하락장 실거래 프로필 — 칼날 잡기 차단 + 수수료 후 순이익 청산
BEAR_LIVE_RSI_OVERSOLD = 18          # RSI 매수: 더 깊은 과매도
BEAR_LIVE_RSI_OVERBOUGHT = 62        # RSI 매도: 반등 초기 청산
BEAR_LIVE_BB_STD_DEV = 3.5           # BB: 하단 이탈 폭 확대
BEAR_LIVE_STOP_LOSS = 0.03
BEAR_LIVE_MIN_NET_PROFIT = 0.005     # 수수료(0.5%) 후 최소 순이익 바닥 — 학습값이 이보다 낮으면만 상향
BEAR_RISK_FALLBACK = {
    "take_profit_pct": 0.012,
    "trail_pct": 0.004,
    "trail_activate_profit_pct": 0.006,
    "min_strategy_sell_profit_pct": 0.005,
}
BEAR_LIVE_BUY_DROP_PCT = 0.045
BEAR_LIVE_BUY_VOL_RATIO = 2.8

# USDT: 고정 융합 vs 학습 3대지표 — 단기(최대 3주) 예상 수익 비교
USDT_COMPARE_HORIZONS = ["1d", "3d", "1w", "3w"]
USDT_HORIZON_LABELS = {"1d": "1일", "3d": "3일", "1w": "1주", "3w": "3주"}
DEFAULT_USDT_STRATEGY = "auto"


def _expand_usdt_profit_horizons(strategy_reg: dict) -> dict:
    profits = dict(strategy_reg.get("period_expected_profits") or {})
    if all(h in profits for h in USDT_COMPARE_HORIZONS):
        return profits
    ret = strategy_reg.get("backtest", {}).get("total_return_pct", 0)
    if BACKTEST_AVAILABLE and _bt_mod:
        computed = _bt_mod._calculate_expected_profits(ret, [])
        for h in USDT_COMPARE_HORIZONS:
            profits.setdefault(h, computed.get(h, 0))
    return profits


def _compute_usdt_auto_pick(usdt_opt_reg: dict) -> tuple:
    """USDT: 1일→3일→1주→3주 예상 수익 비교, 동률 시 Sharpe → 고정 융합 우선."""
    fixed = usdt_opt_reg.get("fixed_fusion_strategy", {})
    learned = usdt_opt_reg.get("learned_mixed_strategy", {})
    f_profits = _expand_usdt_profit_horizons(fixed)
    l_profits = _expand_usdt_profit_horizons(learned)

    for horizon in USDT_COMPARE_HORIZONS:
        fv = f_profits.get(horizon, 0)
        lv = l_profits.get(horizon, 0)
        label = USDT_HORIZON_LABELS[horizon]
        if fv > lv:
            return "fixed_fusion", f"{label} 예상 수익 우세 (고정융합 {fv:+.2f}% > 학습믹스 {lv:+.2f}%)"
        if lv > fv:
            return "learned_mixed", f"{label} 예상 수익 우세 (학습믹스 {lv:+.2f}% > 고정융합 {fv:+.2f}%)"

    f_sh = fixed.get("backtest", {}).get("sharpe_ratio", -999)
    l_sh = learned.get("backtest", {}).get("sharpe_ratio", -999)
    if l_sh > f_sh:
        return "learned_mixed", "전 기간 동률 → Sharpe 우세 (학습믹스)"
    if f_sh > l_sh:
        return "fixed_fusion", "전 기간 동률 → Sharpe 우세 (고정융합)"
    return "fixed_fusion", "전 기간 동률 → 고정 융합 전략 우선 (기본값)"


def _build_usdt_fixed_fusion_tactics() -> dict:
    return copy.deepcopy(_build_default_usdt_config()["tactics"]["RANGE"])


def _summarize_usdt_variant(opt_reg: dict) -> dict:
    profits = _expand_usdt_profit_horizons(opt_reg)
    bt = opt_reg.get("backtest", {})
    strats = [k for k, v in opt_reg.get("strategies", {}).items() if v.get("enabled")]
    return {
        "strategies": strats,
        "logic": opt_reg.get("logic"),
        "return_pct": round(bt.get("total_return_pct", 0), 2),
        "sharpe": round(bt.get("sharpe_ratio", 0), 4),
        "avg_hold_days": round(bt.get("avg_hold_days", 0), 2),
        "min_target_profit_pct": (opt_reg.get("usdt_fx_filter") or {}).get("min_target_profit_pct"),
        "expected_profit_1d": round(profits.get("1d", 0), 2),
        "expected_profit_3d": round(profits.get("3d", 0), 2),
        "expected_profit_1w": round(profits.get("1w", 0), 2),
        "expected_profit_3w": round(profits.get("3w", 0), 2),
        "best_gap_krw": (opt_reg.get("usdt_fx_filter") or {}).get("min_consider_gap_krw"),
    }


def _resolve_usdt_strategy(ticker: str) -> str:
    if ticker not in active_ticker_configs:
        return "fixed_fusion"
    selected = active_ticker_configs[ticker].get("selected_usdt_strategy", DEFAULT_USDT_STRATEGY)
    if selected in ("fixed_fusion", "learned_mixed"):
        return selected
    return active_ticker_configs[ticker].get("usdt_auto_pick", "fixed_fusion")


def _apply_usdt_strategy_selection(ticker: str) -> bool:
    if ticker not in active_ticker_configs:
        return False
    cfg = active_ticker_configs[ticker]
    resolved = _resolve_usdt_strategy(ticker)
    cfg["resolved_usdt_strategy"] = resolved
    cache = cfg.get("usdt_tactics_cache", {})
    if resolved == "fixed_fusion":
        tactics = cache.get("fixed_fusion") or _build_usdt_fixed_fusion_tactics()
    else:
        tactics = cache.get("learned_mixed")
        if not tactics:
            tactics = cache.get("fixed_fusion") or _build_usdt_fixed_fusion_tactics()
            cfg["resolved_usdt_strategy"] = "fixed_fusion"
    for reg in ("BULL", "BEAR", "RANGE"):
        cfg["tactics"][reg] = copy.deepcopy(tactics)
    return True


def _apply_usdt_optimization_results(market_results: dict, ticker: str) -> bool:
    opt_reg = (market_results or {}).get("RANGE")
    if not opt_reg or "fixed_fusion_strategy" not in opt_reg:
        return False
    cfg = active_ticker_configs[ticker]
    fixed = opt_reg["fixed_fusion_strategy"]
    learned = opt_reg["learned_mixed_strategy"]
    cfg["usdt_tactics_cache"] = {
        "fixed_fusion": _build_usdt_fixed_fusion_tactics(),
        "learned_mixed": _build_tactics_from_opt_reg(learned),
    }
    pick, reason = _compute_usdt_auto_pick(opt_reg)
    cfg["usdt_auto_pick"] = pick
    cfg["usdt_auto_pick_reason"] = reason
    cfg["usdt_strategy_compare"] = {
        "fixed_fusion": _summarize_usdt_variant(fixed),
        "learned_mixed": _summarize_usdt_variant(learned),
    }
    winner = fixed if pick == "fixed_fusion" else learned
    fx_filter = winner.get("usdt_fx_filter") or fixed.get("usdt_fx_filter") or {}
    if fx_filter:
        usdt_fx = cfg.setdefault("usdt_fx", {})
        for k in ("min_consider_gap_krw", "min_target_profit_pct"):
            if k in fx_filter:
                usdt_fx[k] = fx_filter[k]
        ufb = winner.get("strategies", {}).get("UsdtFxBollinger", {})
        for k in ("bb_period", "bb_std_dev"):
            if k in ufb:
                usdt_fx["bb_period" if k == "bb_period" else "bb_std_dev"] = ufb[k]
        _sync_usdt_strategy_params(usdt_fx)
    return _apply_usdt_strategy_selection(ticker)


def _usdt_live_fx_allows_buy(config: dict, price: float) -> bool:
    """실매매 USDT 매수 전 환율차·수수료 검증 (학습믹스 포함)."""
    if not usdt_fx_buy_allowed or price <= 0:
        return True
    fx_cfg = config.get("usdt_fx", {})
    manual = float(fx_cfg.get("reference_krw") or 0)
    ttl = int(fx_cfg.get("fx_refresh_minutes", USDT_FX_REFRESH_MINUTES)) * 60
    fair, _ = get_usd_krw_rate(manual_krw=manual, cache_ttl_sec=ttl)
    fee = float(fx_cfg.get("fee_one_way_pct", BITHUMB_FEE_RATE_ONE_WAY * 100)) / 100.0
    return usdt_fx_buy_allowed(
        price, fair,
        float(fx_cfg.get("min_consider_gap_krw", USDT_DEFAULT_MIN_CONSIDER_GAP_KRW)),
        float(fx_cfg.get("min_target_profit_pct", 0.2)),
        fee,
    )


def _expand_profit_horizons(strategy_reg: dict) -> dict:
    """구버전 optimized_params에 1d/3d/2w가 없으면 백테스트 수익률로 보간."""
    profits = dict(strategy_reg.get("period_expected_profits") or {})
    if all(h in profits for h in BEAR_COMPARE_HORIZONS):
        return profits
    ret = strategy_reg.get("backtest", {}).get("total_return_pct", 0)
    if BACKTEST_AVAILABLE and _bt_mod:
        computed = _bt_mod._calculate_expected_profits(ret, [])
        for h in BEAR_COMPARE_HORIZONS:
            profits.setdefault(h, computed.get(h, 0))
    return profits


def _compute_bear_auto_pick(bear_opt_reg: dict, cycle_match: dict = None) -> tuple:
    """BEAR: 1일→3일→1주→2주→1달→3달→6달 예상 수익 순 비교, 전부 동률이면 Sharpe·MDD·사이클."""
    custom = bear_opt_reg.get("custom_bear_strategy", {})
    mixed = bear_opt_reg.get("mixed_strategy", {})
    c_profits = _expand_profit_horizons(custom)
    m_profits = _expand_profit_horizons(mixed)

    for horizon in BEAR_COMPARE_HORIZONS:
        c_val = c_profits.get(horizon, 0)
        m_val = m_profits.get(horizon, 0)
        label = BEAR_HORIZON_LABELS[horizon]
        if m_val > c_val:
            return "mixed", f"{label} 예상 수익 우세 (Mixed {m_val:+.2f}% > Custom {c_val:+.2f}%)"
        if c_val > m_val:
            return "custom_bear", f"{label} 예상 수익 우세 (Custom {c_val:+.2f}% > Mixed {m_val:+.2f}%)"

    custom_bt = custom.get("backtest", {})
    mixed_bt = mixed.get("backtest", {})
    c_sharpe = custom_bt.get("sharpe_ratio", -999)
    m_sharpe = mixed_bt.get("sharpe_ratio", -999)
    if m_sharpe > c_sharpe:
        return "mixed", "전 기간 동률 → Sharpe 우세 (Mixed)"
    if m_sharpe < c_sharpe:
        return "custom_bear", "전 기간 동률 → Sharpe 우세 (CustomBear)"
    c_mdd = custom_bt.get("mdd_pct", 999)
    m_mdd = mixed_bt.get("mdd_pct", 999)
    if m_mdd != c_mdd:
        pick = "mixed" if m_mdd < c_mdd else "custom_bear"
        return pick, f"전 기간 동률 → MDD 우세 ({pick})"
    if cycle_match and cycle_match.get("similarity_pct", 0) >= 55:
        matched_angle = cycle_match.get("matched_angle_pct_per_month", 0)
        if matched_angle >= 8:
            return "custom_bear", "전 기간·Sharpe·MDD 동률 → 사이클 각도 급등 구간 (CustomBear)"
        if matched_angle <= 3:
            return "mixed", "전 기간·Sharpe·MDD 동률 → 사이클 각도 완만 구간 (Mixed)"
    return "mixed", "전 기간 동률 → 기본 Mixed"


def _bear_floor_profit(val: float) -> float:
    """학습 청산 비율의 최소 바닥(수수료 후 순이익)만 적용."""
    v = float(val or 0)
    return max(v, BEAR_LIVE_MIN_NET_PROFIT) if v > 0 else BEAR_LIVE_MIN_NET_PROFIT


def _bear_risk_live_from_learned(risk: dict) -> dict:
    """백테스트가 찾은 BEAR 청산 비율을 실거래 BearScalp로 변환 (0.5%는 바닥만)."""
    learned = dict(risk or {})
    r_type = learned.get("type", "StopLoss")

    stop_loss = float(learned.get("stop_loss_pct", BEAR_LIVE_STOP_LOSS))
    take_profit = float(learned.get("take_profit_pct", 0) or BEAR_RISK_FALLBACK["take_profit_pct"])
    trail_pct = float(learned.get("trail_pct", 0) or BEAR_RISK_FALLBACK["trail_pct"])
    trail_activate = float(learned.get("trail_activate_profit_pct", 0))
    min_sell = float(learned.get("min_strategy_sell_profit_pct", 0) or BEAR_RISK_FALLBACK["min_strategy_sell_profit_pct"])

    if r_type == "TrailingStop":
        if take_profit <= 0 or take_profit >= 0.2:
            take_profit = BEAR_RISK_FALLBACK["take_profit_pct"]
        if trail_activate <= 0:
            trail_activate = BEAR_RISK_FALLBACK["trail_activate_profit_pct"]
    elif r_type == "StopLoss":
        if trail_pct <= 0:
            trail_pct = BEAR_RISK_FALLBACK["trail_pct"]
        if trail_activate <= 0:
            trail_activate = BEAR_RISK_FALLBACK["trail_activate_profit_pct"]
    # BearScalp: 학습된 4종 수치 그대로 사용

    return {
        "type": "BearScalp",
        "stop_loss_pct": stop_loss,
        "take_profit_pct": _bear_floor_profit(take_profit),
        "trail_pct": trail_pct,
        "trail_activate_profit_pct": _bear_floor_profit(trail_activate),
        "min_strategy_sell_profit_pct": _bear_floor_profit(min_sell),
        "min_net_profit_pct": BEAR_LIVE_MIN_NET_PROFIT,
        "learned_risk_type": r_type,
    }


def _apply_bear_live_profile(t_cfg: dict) -> dict:
    """하락장 실거래: 칼날 잡기 차단 + 수수료 후 순이익만 청산."""
    import copy
    cfg = copy.deepcopy(t_cfg)

    if cfg.get("logic") == "CUSTOM_BEAR":
        for s in cfg.get("strategies", []):
            if s.get("name") == "CustomBear":
                p = s.setdefault("params", {})
                if p.get("take_profit"):
                    p["take_profit"] = _bear_floor_profit(p["take_profit"])
                p["drop_pct"] = max(p.get("drop_pct", 0.03), BEAR_LIVE_BUY_DROP_PCT)
                p["volume_ratio"] = max(p.get("volume_ratio", 2.0), BEAR_LIVE_BUY_VOL_RATIO)
                p["require_bounce"] = True
    else:
        cfg["logic"] = cfg.get("logic", "AND")
        cfg["sell_logic"] = "OR"
        for s in cfg.get("strategies", []):
            if s.get("name") == "RSI":
                p = s.setdefault("params", {})
                p["oversold"] = min(p.get("oversold", 25), BEAR_LIVE_RSI_OVERSOLD)
                p["overbought"] = min(p.get("overbought", 75), BEAR_LIVE_RSI_OVERBOUGHT)
                p["require_reversal"] = True
            elif s.get("name") == "Bollinger":
                p = s.setdefault("params", {})
                p["std_dev"] = max(p.get("std_dev", 2.5), BEAR_LIVE_BB_STD_DEV)
                p["bounce_buy"] = True

    cfg["risk"] = _bear_risk_live_from_learned(cfg.get("risk", {}))
    return cfg


def _bear_buy_entry_allowed(
    candle_manager,
    timeframe: str,
    price: float,
    tactic: dict,
    bear_timing: dict = None,
) -> tuple:
    """하락장 매수 최종 게이트 — 월봉 패턴별 진입 적합도 반영."""
    timing = bear_timing or {}
    entry_style = timing.get("entry_style", "cautious_swing")

    if entry_style == "wait":
        return False, timing.get("summary", "바닥선 미도달 — 진입 대기")

    candles = candle_manager.get_candles(timeframe)
    if not candles or len(candles) < 8:
        return False, "캔들 부족 — 진입 보류"

    enabled = {s["name"]: s for s in tactic.get("strategies", []) if s.get("enabled")}
    closes = [c["close"] for c in candles]
    c_prev2, c_prev = candles[-3], candles[-2]

    # 연속 음봉 하락 중 + 저점 미회복 → 진입 거부 (칼날 잡기)
    if entry_style in ("scalp_fast", "cautious_swing") and (
        c_prev2["close"] < c_prev2["open"]
        and c_prev["close"] < c_prev["open"]
        and price <= c_prev["close"] * 1.001
    ):
        return False, "연속 하락 중 — 바닥 미확인"

    if entry_style != "swing_bottom" and price < c_prev["low"] * 1.002:
        return False, "직전 봉 저점 미회복"

    rsi_relax = 4.0 if entry_style == "swing_bottom" else (2.0 if entry_style == "cautious_swing" else 0.0)

    rsi_cfg = enabled.get("RSI", {}).get("params", {})
    if "RSI" in enabled:
        period = int(rsi_cfg.get("period", 10))
        if len(closes) <= period + 1:
            return False, "RSI 데이터 부족"
        delta = pd.Series(closes).diff()
        gain = delta.clip(lower=0).rolling(window=period).mean()
        loss = (-delta.clip(upper=0)).rolling(window=period).mean()
        rsi = 100 - (100 / (1 + gain / (loss + 1e-10)))
        curr_rsi, prev_rsi = float(rsi.iloc[-1]), float(rsi.iloc[-2])
        oversold = float(rsi_cfg.get("oversold", BEAR_LIVE_RSI_OVERSOLD)) + rsi_relax
        if entry_style == "swing_bottom":
            if not (prev_rsi < oversold + 4 and curr_rsi >= prev_rsi - 1.5):
                return False, f"바닥권 RSI 확인 미흡 ({prev_rsi:.1f}→{curr_rsi:.1f})"
        elif not (prev_rsi < oversold + 2 and curr_rsi > prev_rsi):
            return False, f"RSI 반등 미확인 ({prev_rsi:.1f}→{curr_rsi:.1f})"

    bb_cfg = enabled.get("Bollinger", {}).get("params", {})
    if "Bollinger" in enabled:
        bb_period = int(bb_cfg.get("period", 15))
        std_dev = float(bb_cfg.get("std_dev", BEAR_LIVE_BB_STD_DEV))
        if entry_style == "swing_bottom":
            std_dev = max(2.5, std_dev - 0.3)
        if len(closes) <= bb_period + 1:
            return False, "BB 데이터 부족"
        df = pd.Series(closes)
        ma = df.rolling(window=bb_period).mean()
        std = df.rolling(window=bb_period).std()
        lower = ma - std_dev * std
        if entry_style == "swing_bottom":
            if not (closes[-2] <= lower.iloc[-2] * 1.01 and price >= lower.iloc[-1] * 0.995):
                return False, "BB 하단권 미확인"
        elif not (closes[-2] < lower.iloc[-2] and price >= lower.iloc[-1]):
            return False, "BB 하단 재진입(바운스) 미확인"

    if "MACD" in enabled and "RSI" not in enabled:
        macd_cfg = enabled["MACD"].get("params", {})
        slow = int(macd_cfg.get("slow", 26))
        if len(closes) <= slow:
            return False, "MACD 데이터 부족"
        df = pd.Series(closes)
        ema_fast = df.ewm(span=macd_cfg.get("fast", 12), adjust=False).mean()
        ema_slow = df.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        macd_sig = macd.ewm(span=macd_cfg.get("signal_period", 9), adjust=False).mean()
        diff = float(macd.iloc[-1] - macd_sig.iloc[-1])
        prev_diff = float(macd.iloc[-2] - macd_sig.iloc[-2])
        if not (prev_diff <= 0 and diff > 0):
            return False, "MACD 상향 전환 미확인"

    style_note = {
        "scalp_fast": "단타(빠른 익절)",
        "swing_bottom": "바닥권 스윙",
        "cautious_swing": "바닥 접근 보수 진입",
    }.get(entry_style, "하락장 진입")
    return True, f"{style_note} — {timing.get('summary', '반등·바닥 확인')}"


def _bear_min_net_profit(tactic: dict) -> float:
    risk = tactic.get("risk", {})
    return float(risk.get("min_net_profit_pct", BEAR_LIVE_MIN_NET_PROFIT))


bear_entry_ranking = {}


def _update_bear_entry_ranking():
    """하락장 코인 간 진입 유리 순위 (ETH 바닥 > XRP 접근 > BTC 단타)."""
    global bear_entry_ranking
    scores = {}
    details = {}
    for ticker in ("BTC", "ETH", "XRP"):
        cfg = active_ticker_configs.get(ticker, {})
        if cfg.get("current_regime") != "BEAR":
            continue
        bp = cfg.get("bear_pattern_analysis", {})
        timing = bp.get("entry_timing", {})
        scores[ticker] = float(timing.get("entry_score", 0))
        details[ticker] = {
            "entry_score": timing.get("entry_score", 0),
            "entry_style": timing.get("entry_style"),
            "phase": timing.get("phase"),
            "summary": timing.get("summary"),
            "episode_count": bp.get("episode_count", 0),
        }
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    bear_entry_ranking = {
        "ranked": [t for t, _ in ranked],
        "scores": scores,
        "details": details,
        "top_pick": ranked[0][0] if ranked else None,
        "summary": (
            "하락장 진입 유리 순위: " + " > ".join(f"{t}({s:.0f})" for t, s in ranked)
            if ranked else ""
        ),
    }
    return bear_entry_ranking


def _resolve_bear_strategy(ticker: str) -> str:
    """selected_bear_strategy(auto/custom_bear/mixed)를 실제 적용 키로 해석."""
    if ticker not in active_ticker_configs:
        return DEFAULT_BEAR_STRATEGY
    selected = active_ticker_configs[ticker].get("selected_bear_strategy", DEFAULT_BEAR_STRATEGY)
    if selected in ("custom_bear", "mixed"):
        return selected
    return active_ticker_configs[ticker].get("bear_auto_pick", "custom_bear")

def _build_tactics_from_opt_reg(opt_reg: dict) -> dict:
    """백테스트 최적화 결과 한 벌을 tactics 블록으로 변환."""
    opt_strats = opt_reg.get("strategies", {})
    new_strategies = []
    for s_name, s_data in opt_strats.items():
        if s_name == "RSI":
            tf = OPTIM_BACKTEST_TIMEFRAME
            params = {"period": s_data.get("period", 14), "oversold": s_data.get("oversold", 30), "overbought": s_data.get("overbought", 70)}
        elif s_name == "Bollinger":
            tf = OPTIM_BACKTEST_TIMEFRAME
            params = {"period": s_data.get("period", 20), "std_dev": s_data.get("std_dev", 2.0)}
        elif s_name == "MACD":
            tf = OPTIM_BACKTEST_TIMEFRAME
            params = {"fast": s_data.get("fast", 12), "slow": s_data.get("slow", 26), "signal_period": s_data.get("signal_period", 9)}
        elif s_name == "CustomBear":
            tf = OPTIM_BACKTEST_TIMEFRAME
            params = {
                "lookback": s_data.get("lookback", 8),
                "drop_pct": s_data.get("drop_pct", 0.05),
                "volume_ratio": s_data.get("volume_ratio", 2.0),
                "trail_pct": s_data.get("trail_pct", 0.015),
                "stop_loss": s_data.get("stop_loss", 0.015),
                "time_cut": s_data.get("time_cut", 24)
            }
        else:
            continue

        new_strategies.append({
            "name": s_name,
            "enabled": s_data.get("enabled", False),
            "weight": s_data.get("weight", 1.0),
            "timeframe": tf,
            "params": params
        })

    return {
        "logic": opt_reg.get("logic", "OR"),
        "threshold": opt_reg.get("threshold", 0.5),
        "strategies": new_strategies,
        "risk": opt_reg.get("risk", {"type": "None"}),
        "backtest_meta": {
            "avg_hold_hours": opt_reg.get("backtest", {}).get("avg_hold_hours", 0),
            "median_hold_hours": opt_reg.get("backtest", {}).get("median_hold_hours", 0),
            "avg_hold_days": opt_reg.get("backtest", {}).get("avg_hold_days", 0),
        },
    }

def _apply_bear_strategy_selection(ticker: str) -> bool:
    """selected_bear_strategy에 맞춰 BEAR tactics 활성화 (캐시 우선)."""
    if ticker not in active_ticker_configs:
        return False

    if ticker in STABLECOIN_TICKERS:
        return _apply_usdt_strategy_selection(ticker)

    resolved = _resolve_bear_strategy(ticker)
    active_ticker_configs[ticker]["resolved_bear_strategy"] = resolved
    cache = active_ticker_configs[ticker].get("bear_tactics_cache", {})
    if resolved in cache:
        active_ticker_configs[ticker]["tactics"]["BEAR"] = cache[resolved]
        return True
    return False

def load_persisted_configs():
    global active_ticker_configs
    if os.path.exists(CONFIG_FILE_PATH):
        try:
            with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
                saved_configs = json.load(f)
                # Merge saved configs into default config to prevent missing new tickers (like XRP) or parameters
                for k, v in saved_configs.items():
                    if k not in active_ticker_configs:
                        active_ticker_configs[k] = v
                    else:
                        if isinstance(v, dict) and isinstance(active_ticker_configs[k], dict):
                            for sub_k, sub_v in v.items():
                                active_ticker_configs[k][sub_k] = sub_v
                        else:
                            active_ticker_configs[k] = v
                print(f"[Storage] 설정을 {CONFIG_FILE_PATH}에서 불러와 기본 설정과 병합했습니다.")
        except Exception as e:
            print(f"[Storage] 설정 파일 읽기 및 병합 실패: {e}")
    else:
        save_persisted_configs()

    for ticker in SUPPORTED_TICKERS:
        cfg = active_ticker_configs.get(ticker)
        if not isinstance(cfg, dict):
            if ticker == "USDT":
                active_ticker_configs[ticker] = _build_default_usdt_config()
            continue
        _migrate_config_to_15m(cfg)
        cfg.setdefault("regime_override", DEFAULT_REGIME_OVERRIDE)
        cfg.setdefault("selected_bear_strategy", DEFAULT_BEAR_STRATEGY)

    usdt_cfg = active_ticker_configs.get("USDT")
    if isinstance(usdt_cfg, dict):
        usdt_cfg.setdefault("selected_usdt_strategy", DEFAULT_USDT_STRATEGY)
        usdt_cfg.setdefault("usdt_auto_pick", "fixed_fusion")
        usdt_cfg.setdefault("resolved_usdt_strategy", "fixed_fusion")
        usdt_cfg.setdefault("usdt_tactics_cache", {})
        usdt_cfg.setdefault("usdt_fx", {})
        usdt_fx = usdt_cfg["usdt_fx"]
        defaults = _build_default_usdt_config()["usdt_fx"]
        for k, v in defaults.items():
            usdt_fx.setdefault(k, v)
        if usdt_cfg.get("long_term_ma_period", 90) != 400:
            usdt_cfg["long_term_ma_period"] = 400
        if usdt_fx.get("min_consider_gap_krw", 30) < USDT_DEFAULT_MIN_CONSIDER_GAP_KRW:
            usdt_fx["min_consider_gap_krw"] = USDT_DEFAULT_MIN_CONSIDER_GAP_KRW
            _sync_usdt_strategy_params(usdt_fx)
        if usdt_fx.get("fx_refresh_minutes", 60) != USDT_FX_REFRESH_MINUTES:
            usdt_fx["fx_refresh_minutes"] = USDT_FX_REFRESH_MINUTES
            _sync_usdt_strategy_params(usdt_fx)
        tactics = usdt_cfg.get("tactics", {})
        uses_legacy = all(
            any(s.get("name") == "ReversePremium" for s in tactics.get(reg, {}).get("strategies", []))
            and not any(s.get("name") == "UsdtFxBollinger" for s in tactics.get(reg, {}).get("strategies", []))
            for reg in ("BULL", "BEAR", "RANGE")
            if reg in tactics
        )
        if uses_legacy:
            preserved_fx = dict(usdt_fx)
            active_ticker_configs["USDT"] = _build_default_usdt_config()
            active_ticker_configs["USDT"]["usdt_fx"].update(preserved_fx)
            fx = active_ticker_configs["USDT"]["usdt_fx"]
            sync_keys = (
                "reference_krw", "min_consider_gap_krw", "min_target_profit_pct",
                "sell_premium_pct", "fx_refresh_minutes", "fee_one_way_pct",
                "bb_period", "bb_std_dev",
            )
            synced = {k: fx[k] for k in sync_keys if k in fx}
            for regime in ("BULL", "BEAR", "RANGE"):
                for s in active_ticker_configs["USDT"]["tactics"][regime]["strategies"]:
                    if s.get("name") == "UsdtFxBollinger":
                        s["params"].update(synced)
            save_persisted_configs()
            print("[Storage] USDT 전략을 환율+볼린저 융합(UsdtFxBollinger)으로 업그레이드했습니다.")

def save_persisted_configs():
    try:
        with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(active_ticker_configs, f, indent=4, ensure_ascii=False)
            print(f"[Storage] 설정을 {CONFIG_FILE_PATH}에 백업 저장했습니다.")
    except Exception as e:
        print(f"[Storage] 설정 파일 저장 실패: {e}")

def _summarize_opt_reg_for_ui(opt_reg: dict, bear_variant: str = None) -> dict:
    """프론트 표시용 최적화 요약."""
    bt = opt_reg.get("backtest", {})
    strats = [k for k, v in opt_reg.get("strategies", {}).items() if v.get("enabled")]
    risk = opt_reg.get("risk", {})
    min_sell = risk.get("min_strategy_sell_profit_pct", 0)
    sell_note = ""
    if risk.get("type") == "TrailingStop":
        act = risk.get("trail_activate_profit_pct", 0)
        if act:
            sell_note = f"트레일 +{round(act * 100, 1)}% 후 -{round(risk.get('trail_pct', 0) * 100, 1)}%"
        else:
            sell_note = f"트레일 -{round(risk.get('trail_pct', 0) * 100, 1)}%"
    elif risk.get("type") == "BearScalp":
        tp = risk.get("take_profit_pct", 0)
        act = risk.get("trail_activate_profit_pct", 0)
        trail = risk.get("trail_pct", 0)
        sell_note = f"BEAR학습 TP {_bear_floor_profit(tp)*100:.2f}%·트레일 +{act*100:.2f}%/-{trail*100:.1f}%"
    elif risk.get("type") == "StopLoss":
        sell_note = f"TP {round(risk.get('take_profit_pct', 0) * 100, 1)}%"
    if min_sell:
        sell_note = (sell_note + " · " if sell_note else "") + f"지표매도≥{round(min_sell * 100, 2)}%"
    summary = {
        "logic": opt_reg.get("logic", "OR"),
        "strategies": strats,
        "risk_type": risk.get("type", "None"),
        "sell_strategy": sell_note or risk.get("type", "None"),
        "min_strategy_sell_profit_pct": min_sell,
        "return_pct": round(bt.get("total_return_pct", 0), 2),
        "sharpe": round(bt.get("sharpe_ratio", 0), 4),
        "avg_hold_days": round(bt.get("avg_hold_days", 0), 2),
        "expected_profit_1d": round(_expand_profit_horizons(opt_reg).get("1d", 0), 2),
        "expected_profit_1m": round(_expand_profit_horizons(opt_reg).get("1m", 0), 2),
    }
    if bear_variant:
        summary["bear_variant"] = bear_variant
    return summary


def _build_optimized_default_snapshot(ticker: str, results: dict) -> dict:
    """코인별 최적화 디폴트 스냅샷 (프론트·설정 저장용)."""
    market = f"KRW-{ticker}"
    cfg = active_ticker_configs.get(ticker, {})
    current_regime = cfg.get("current_regime", "BEAR")
    regimes_summary = {}

    if market in results:
        for regime in ["BULL", "BEAR", "RANGE"]:
            opt_reg = results[market].get(regime)
            if not opt_reg:
                continue
            if ticker == "USDT" and regime == "RANGE":
                pick = cfg.get("usdt_auto_pick")
                pick_reason = cfg.get("usdt_auto_pick_reason", "")
                if not pick and "fixed_fusion_strategy" in opt_reg:
                    pick, pick_reason = _compute_usdt_auto_pick(opt_reg)
                variant = pick or "fixed_fusion"
                key_map = {"fixed_fusion": "fixed_fusion_strategy", "learned_mixed": "learned_mixed_strategy"}
                src = opt_reg.get(key_map.get(variant, "fixed_fusion_strategy")) or opt_reg
                summary = _summarize_usdt_variant(src)
                summary["usdt_variant"] = variant
                summary["usdt_pick_reason"] = pick_reason
                regimes_summary[regime] = summary
                continue
            if regime == "BEAR":
                pick = cfg.get("bear_auto_pick")
                pick_reason = cfg.get("bear_auto_pick_reason", "")
                if not pick and "custom_bear_strategy" in opt_reg and "mixed_strategy" in opt_reg:
                    pick, pick_reason = _compute_bear_auto_pick(opt_reg, cfg.get("cycle_analysis"))
                variant = pick or "custom_bear"
                key_map = {"custom_bear": "custom_bear_strategy", "mixed": "mixed_strategy"}
                src = opt_reg.get(key_map.get(variant, "custom_bear_strategy")) or opt_reg
                summary = _summarize_opt_reg_for_ui(src, bear_variant=variant)
                summary["bear_pick_reason"] = pick_reason
                regimes_summary[regime] = summary
            else:
                regimes_summary[regime] = _summarize_opt_reg_for_ui(opt_reg)

    active_tactic = cfg.get("tactics", {}).get(current_regime, {})
    active_strats = [s.get("name") for s in active_tactic.get("strategies", []) if s.get("enabled")]
    cycle = cfg.get("cycle_analysis") or {}

    return {
        "applied_at": results.get("last_updated"),
        "source": results.get("source", "backtest"),
        "current_regime": current_regime,
        "active_logic": active_tactic.get("logic"),
        "active_strategies": active_strats,
        "active_risk_type": active_tactic.get("risk", {}).get("type", "None"),
        "resolved_bear_strategy": cfg.get("resolved_bear_strategy"),
        "bear_auto_pick_reason": cfg.get("bear_auto_pick_reason"),
        "resolved_usdt_strategy": cfg.get("resolved_usdt_strategy"),
        "usdt_auto_pick_reason": cfg.get("usdt_auto_pick_reason"),
        "usdt_strategy_compare": cfg.get("usdt_strategy_compare"),
        "cycle_match_cycle": cycle.get("matched_cycle_number"),
        "cycle_similarity_pct": cycle.get("similarity_pct"),
        "regimes": regimes_summary,
    }


def update_configs_with_optimized_params():
    global active_ticker_configs
    results = get_latest_results()
    if not results:
        return

    updated = False
    for market in [f"KRW-{t}" for t in SUPPORTED_TICKERS]:
        ticker = market.replace("KRW-", "")
        if market not in results or ticker not in active_ticker_configs:
            continue

        if ticker == "USDT":
            if _apply_usdt_optimization_results(results[market], ticker):
                updated = True
            active_ticker_configs[ticker]["optimized_default"] = _build_optimized_default_snapshot(ticker, results)
            updated = True
            continue

        cycle_match = active_ticker_configs[ticker].get("cycle_analysis")

        for regime in ["BULL", "BEAR", "RANGE"]:
            opt_reg = results[market].get(regime)
            if not opt_reg:
                continue

            if regime == "BEAR":
                bear_cache = {}
                if "custom_bear_strategy" in opt_reg:
                    bear_cache["custom_bear"] = _build_tactics_from_opt_reg(opt_reg["custom_bear_strategy"])
                if "mixed_strategy" in opt_reg:
                    bear_cache["mixed"] = _build_tactics_from_opt_reg(opt_reg["mixed_strategy"])
                if bear_cache:
                    if "custom_bear_strategy" in opt_reg and "mixed_strategy" in opt_reg:
                        pick, reason = _compute_bear_auto_pick(opt_reg, cycle_match)
                        active_ticker_configs[ticker]["bear_auto_pick"] = pick
                        active_ticker_configs[ticker]["bear_auto_pick_reason"] = reason
                    active_ticker_configs[ticker]["bear_tactics_cache"] = bear_cache
                    _apply_bear_strategy_selection(ticker)
                    updated = True
                continue

            active_ticker_configs[ticker]["tactics"][regime] = _build_tactics_from_opt_reg(opt_reg)
            updated = True

        active_ticker_configs[ticker]["optimized_default"] = _build_optimized_default_snapshot(ticker, results)
        updated = True

    if updated:
        save_persisted_configs()
        print("[Storage] 종목별 최적화 파라미터(BULL/RANGE/mix·CustomBear) 및 디폴트 스냅샷이 반영되었습니다.")

def _bar_seconds_for_timeframe(timeframe: str) -> int:
    return TIMEFRAME_BAR_SECONDS.get(timeframe, STRATEGY_BAR_SECONDS)


def _migrate_config_to_15m(cfg: dict) -> None:
    """저장 설정의 30m 전략 봉 → 15m (time_cut은 동일 실시간 유지 위해 2배)."""
    if not isinstance(cfg, dict):
        return
    for tactic in (cfg.get("tactics") or {}).values():
        if not isinstance(tactic, dict):
            continue
        for strat in tactic.get("strategies") or []:
            if not isinstance(strat, dict):
                continue
            if strat.get("timeframe") == "30m":
                strat["timeframe"] = "15m"
            if strat.get("name") == "CustomBear":
                params = strat.setdefault("params", {})
                tc = params.get("time_cut")
                if isinstance(tc, (int, float)) and tc < 96 and tc % 2 == 0:
                    params["time_cut"] = int(tc * 2)
    usdt_fx = cfg.get("usdt_fx")
    if isinstance(usdt_fx, dict) and usdt_fx.get("bb_timeframe") == "30m":
        usdt_fx["bb_timeframe"] = "15m"


def _get_min_strategy_sell_profit_pct(tactic: dict) -> float:
    """학습된 지표매도 최소 순수익 (0이면 기존과 동일)."""
    risk = tactic.get("risk", {}) if tactic else {}
    return float(risk.get("min_strategy_sell_profit_pct", 0))


def update_configs_and_apply_to_engine():
    update_configs_with_optimized_params()
    if engine_instance:
        for ticker in SUPPORTED_TICKERS:
            if ticker in engine_instance._workers:
                worker = engine_instance._workers[ticker]
                worker.config = active_ticker_configs[ticker]
                override = worker.config.get("regime_override", "AUTO")
                curr_reg = worker.regime_detector.detect_regime()
                if override != "AUTO":
                    curr_reg = override
                worker.switch_regime(curr_reg, log_to_ui=True)
                worker.current_regime = curr_reg
        print("[Engine] 최적화 파라미터가 구동 중인 엔진 워커들에 핫스왑 적용되었습니다.")

# 초기 구동 시 저장된 설정 로드 적용
load_persisted_configs()
update_configs_with_optimized_params()

# ══════════════════════════════════════════════════════════════
# 0. 전역 스레드 세이프 UI 이벤트 큐
# ══════════════════════════════════════════════════════════════
ui_event_queue = queue.Queue()


# ══════════════════════════════════════════════════════════════
# 1. 지표별 단일 전략 클래스 구현
# ══════════════════════════════════════════════════════════════

class BithumbRsiStrategy(BaseStrategy):
    NAME = "RSI"
    PARAMS = {"period": 5, "oversold": 35, "overbought": 65}

    def __init__(self, candle_manager, timeframe="5m", params=None):
        super().__init__(params)
        self.candle_manager = candle_manager
        self.timeframe = timeframe

    def generate_signal(self, data: dict) -> str:
        prices = self.candle_manager.get_prices(self.timeframe)
        if not prices: return "HOLD"
        
        period = self.params["period"]
        if len(prices) <= period: return "HOLD"

        delta = pd.Series(prices).diff()
        gain = delta.clip(lower=0).rolling(window=period).mean()
        loss = (-delta.clip(upper=0)).rolling(window=period).mean()
        rs = gain / (loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))
        curr_rsi = rsi.iloc[-1]

        data["rsi_val"] = f"{curr_rsi:.1f}"

        if self.params.get("require_reversal") and len(rsi) >= 2:
            prev_rsi = float(rsi.iloc[-2])
            if curr_rsi < self.params["oversold"] and curr_rsi > prev_rsi:
                return "BUY"
        elif curr_rsi < self.params["oversold"]:
            return "BUY"
        if curr_rsi > self.params["overbought"]: return "SELL"
        return "HOLD"


class BithumbMacdStrategy(BaseStrategy):
    NAME = "MACD"
    PARAMS = {"fast": 5, "slow": 10, "signal_period": 3}

    def __init__(self, candle_manager, timeframe="1h", params=None):
        super().__init__(params)
        self.candle_manager = candle_manager
        self.timeframe = timeframe

    def generate_signal(self, data: dict) -> str:
        prices = self.candle_manager.get_prices(self.timeframe)
        if not prices: return "HOLD"
        
        slow = self.params["slow"]
        if len(prices) <= slow: return "HOLD"

        df = pd.Series(prices)
        ema_fast = df.ewm(span=self.params["fast"], adjust=False).mean()
        ema_slow = df.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=self.params["signal_period"], adjust=False).mean()

        curr_macd = macd.iloc[-1]
        curr_sig = macd_signal.iloc[-1]
        diff = curr_macd - curr_sig

        data["macd_val"] = f"{diff:.3f}"

        if len(macd) >= 2:
            prev_diff = macd.iloc[-2] - macd_signal.iloc[-2]
            if prev_diff <= 0 and diff > 0: return "BUY"
            if prev_diff >= 0 and diff < 0: return "SELL"
        return "HOLD"


class BithumbBollingerStrategy(BaseStrategy):
    NAME = "Bollinger"
    PARAMS = {"period": 5, "std_dev": 1.5}

    def __init__(self, candle_manager, timeframe="D", params=None):
        super().__init__(params)
        self.candle_manager = candle_manager
        self.timeframe = timeframe

    def generate_signal(self, data: dict) -> str:
        prices = self.candle_manager.get_prices(self.timeframe)
        if not prices: return "HOLD"
        
        period = self.params["period"]
        if len(prices) <= period: return "HOLD"

        df = pd.Series(prices)
        ma = df.rolling(window=period).mean()
        std = df.rolling(window=period).std()
        upper = ma + self.params["std_dev"] * std
        lower = ma - self.params["std_dev"] * std

        curr_upper = upper.iloc[-1]
        curr_lower = lower.iloc[-1]
        price = float(data.get("price") or prices[-1])

        data["bb_val"] = f"U:{curr_upper:.1f}/L:{curr_lower:.1f}"

        if self.params.get("bounce_buy") and len(prices) >= 2 and len(lower) >= 2:
            if prices[-2] < lower.iloc[-2] and price >= curr_lower:
                return "BUY"
        elif price < curr_lower:
            return "BUY"
        if price > curr_upper: return "SELL"
        return "HOLD"


class BithumbUsdtReversePremiumStrategy(BaseStrategy):
    """
    역프리미엄(할인) 매매 — 수수료·최소목표수익 반영:
    (기준환율 - USDT가) > 매수수수료 + 매도수수료 + 최소목표수익 일 때만 매수.
    """
    NAME = "ReversePremium"
    PARAMS = {
        "min_target_profit_pct": 0.2,
        "sell_premium_pct": 0.15,
        "reference_krw": 0,
        "fx_refresh_minutes": USDT_FX_REFRESH_MINUTES,
        "fee_one_way_pct": 0.25,
    }

    def generate_signal(self, data: dict) -> str:
        tick = float(data.get("price") or 0)
        if tick <= 0:
            return "HOLD"

        manual = float(self.params.get("reference_krw") or 0)
        ttl = int(self.params.get("fx_refresh_minutes", USDT_FX_REFRESH_MINUTES)) * 60
        fair, source = get_usd_krw_rate(manual_krw=manual, cache_ttl_sec=ttl)
        fx = calc_reverse_premium_pct(tick, fair)
        rev = fx["reverse_premium_pct"]
        prem = fx["premium_pct"]

        fee_one_way = float(self.params.get("fee_one_way_pct", BITHUMB_FEE_RATE_ONE_WAY * 100)) / 100.0
        min_profit_pct = float(self.params.get("min_target_profit_pct", 0.2))
        sell_thr = float(self.params.get("sell_premium_pct", 0.15))

        edge = calc_usdt_fee_aware_edge(fair, tick, fee_one_way, min_profit_pct)

        if edge["is_buy_profitable"]:
            status = f"역프 순이익 +{edge['net_edge_krw']:.1f}원"
        elif rev > 0:
            status = f"역프 {rev:.2f}% (수수료·목표 미달)"
        elif prem > 0.05:
            status = f"프리미엄 {prem:.2f}%"
        else:
            status = "정상권"

        data["usdt_fx_val"] = (
            f"{status} | 기준:{fair:,.1f}원 | 현재:{tick:,.2f}원 | "
            f"필요스프레드:{edge['required_spread_krw']:.1f}원(수수료+목표)"
        )
        data["usdt_fx_status"] = {
            **fx,
            **edge,
            "fair_source": source,
            "min_target_profit_pct": min_profit_pct,
            "fee_one_way_pct": fee_one_way * 100,
            "sell_threshold_pct": sell_thr,
        }

        qty = data.get("position_qty", 0.0)
        if qty == 0:
            if edge["is_buy_profitable"]:
                return "BUY"
        else:
            avg = float(data.get("avg_price") or tick)
            net_pnl = calc_net_pnl_pct(avg, tick)
            min_profit_dec = min_profit_pct / 100.0
            if net_pnl >= min_profit_dec:
                return "SELL"
            if prem >= sell_thr and net_pnl > 0:
                return "SELL"
        return "HOLD"


class BithumbUsdtFxBollingerStrategy(BaseStrategy):
    """
    USDT 융합 전략:
    1차 — 기준환율 대비 gap >= min_consider_gap_krw 일 때만 매수 '검토' 시작
    2차 — 검토 구역 + 수수료·목표 충족 + 볼린저 하단 터치 시 매수
    청산 — 순수익 목표 / 밴드 중심·상단 / 프리미엄
    """
    NAME = "UsdtFxBollinger"
    PARAMS = _usdt_fx_bollinger_params()

    def __init__(self, candle_manager, timeframe="15m", params=None):
        super().__init__(params)
        self.candle_manager = candle_manager
        self.timeframe = timeframe

    def _calc_bollinger(self, prices: list):
        period = int(self.params.get("bb_period", 20))
        std_dev = float(self.params.get("bb_std_dev", 2.0))
        if len(prices) <= period:
            return None
        series = pd.Series(prices)
        ma = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        mid = float(ma.iloc[-1])
        sd = float(std.iloc[-1])
        return {
            "upper": mid + std_dev * sd,
            "middle": mid,
            "lower": mid - std_dev * sd,
        }

    def generate_signal(self, data: dict) -> str:
        tick = float(data.get("price") or 0)
        if tick <= 0:
            return "HOLD"

        manual = float(self.params.get("reference_krw") or 0)
        ttl = int(self.params.get("fx_refresh_minutes", USDT_FX_REFRESH_MINUTES)) * 60
        fair, source = get_usd_krw_rate(manual_krw=manual, cache_ttl_sec=ttl)
        fx = calc_reverse_premium_pct(tick, fair)
        rev = fx["reverse_premium_pct"]
        prem = fx["premium_pct"]
        gap_krw = fx["gap_krw"]

        fee_one_way = float(self.params.get("fee_one_way_pct", BITHUMB_FEE_RATE_ONE_WAY * 100)) / 100.0
        min_profit_pct = float(self.params.get("min_target_profit_pct", 0.2))
        sell_thr = float(self.params.get("sell_premium_pct", 0.15))
        min_consider = float(self.params.get("min_consider_gap_krw", USDT_DEFAULT_MIN_CONSIDER_GAP_KRW))

        edge = calc_usdt_fee_aware_edge(fair, tick, fee_one_way, min_profit_pct)
        phase_info = calc_usdt_consideration_phase(gap_krw, min_consider, edge)

        prices = self.candle_manager.get_prices(self.timeframe)
        bb = self._calc_bollinger(prices) if prices else None
        at_lower = bool(bb and tick <= bb["lower"])
        at_middle = bool(bb and tick >= bb["middle"])
        at_upper = bool(bb and tick >= bb["upper"])

        if bb:
            data["bb_val"] = (
                f"U:{bb['upper']:.1f}/M:{bb['middle']:.1f}/L:{bb['lower']:.1f}"
            )
        else:
            data["bb_val"] = "데이터 부족"

        phase = phase_info["phase"]
        if phase == "idle":
            fx_status = f"관심 구역 밖 (차이 {gap_krw:.0f}원 < {min_consider:.0f}원)"
        elif phase == "consider":
            fx_status = f"검토 중 · 수수료·목표 미달 (차이 {gap_krw:.0f}원)"
        elif at_lower:
            fx_status = f"매수 타점 (역프 {gap_krw:.0f}원 + BB하단)"
        else:
            fx_status = f"대기 · BB하단 타점 (차이 {gap_krw:.0f}원, 하단 {bb['lower']:.1f}원)" if bb else f"대기 · BB 데이터 부족 (차이 {gap_krw:.0f}원)"

        data["usdt_fx_val"] = (
            f"{fx_status} | 기준:{fair:,.1f}원 | 현재:{tick:,.2f}원 | "
            f"순이익여유:{edge['net_edge_krw']:+.1f}원"
        )
        data["usdt_fx_status"] = {
            **fx,
            **edge,
            **phase_info,
            "fair_source": source,
            "min_target_profit_pct": min_profit_pct,
            "fee_one_way_pct": fee_one_way * 100,
            "sell_threshold_pct": sell_thr,
            "bb_timeframe": self.timeframe,
            "bb_period": int(self.params.get("bb_period", 20)),
            "bb_std_dev": float(self.params.get("bb_std_dev", 2.0)),
            "bb_upper": round(bb["upper"], 2) if bb else None,
            "bb_middle": round(bb["middle"], 2) if bb else None,
            "bb_lower": round(bb["lower"], 2) if bb else None,
            "at_bb_lower": at_lower,
            "at_bb_middle": at_middle,
            "at_bb_upper": at_upper,
        }

        qty = data.get("position_qty", 0.0)
        if qty == 0:
            if phase_info["in_consideration"] and edge["is_buy_profitable"] and at_lower:
                return "BUY"
        else:
            avg = float(data.get("avg_price") or tick)
            net_pnl = calc_net_pnl_pct(avg, tick)
            min_profit_dec = min_profit_pct / 100.0
            if net_pnl >= min_profit_dec:
                return "SELL"
            if prem >= sell_thr and net_pnl >= min_profit_dec:
                return "SELL"
        return "HOLD"


class BithumbCustomBearStrategy(BaseStrategy):
    NAME = "CustomBear"
    PARAMS = {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "trail_pct": 0.015, "stop_loss": 0.015, "time_cut": 48}

    def __init__(self, candle_manager, timeframe="15m", params=None):
        super().__init__(params)
        self.candle_manager = candle_manager
        self.timeframe = timeframe
        self.entry_price = 0.0
        self.entry_time = None
        self.peak_price = 0.0

    def generate_signal(self, data: dict) -> str:
        # candle_manager로부터 15분봉 캔들 조회
        candles = self.candle_manager.get_candles(self.timeframe)
        if not candles or len(candles) < max(self.params.get("lookback", 8), 20):
            return "HOLD"

        closes = [c["close"] for c in candles]
        opens = [c["open"] for c in candles]
        volumes = [c["volume"] for c in candles]
        highs = [c.get("high", c["close"]) for c in candles]
        tick_price = float(data.get("price") or closes[-1])
        
        qty = data.get("position_qty", 0.0)
        
        if qty == 0.0:
            self.peak_price = 0.0
            # 매수 조건
            lookback = self.params["lookback"]
            window_closes = closes[-lookback-1:-1]
            highest = max(window_closes) if window_closes else closes[-1]
            curr_price = tick_price
            drop = (highest - curr_price) / highest if highest > 0 else 0
            
            vol_window = volumes[-11:-1]
            avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1.0
            curr_vol = volumes[-1]
            
            is_bullish = tick_price > opens[-1]
            bounced = tick_price > closes[-2] if len(closes) >= 2 else is_bullish
            require_bounce = self.params.get("require_bounce", False)

            buy = (drop >= self.params["drop_pct"]) and (curr_vol >= avg_vol * self.params["volume_ratio"]) and is_bullish
            if require_bounce:
                buy = buy and bounced and (len(closes) >= 3 and closes[-2] <= closes[-3])
            
            # 실시간 지표 상태 기록
            data["custom_bear_val"] = f"낙폭:{drop*100:.1f}%(기준:{self.params['drop_pct']*100:.1f}%) / 거래량비율:{curr_vol/avg_vol:.1f}배(기준:{self.params['volume_ratio']:.1f}배)"
            
            if buy:
                self.entry_price = curr_price
                self.entry_time = datetime.now()
                self.peak_price = curr_price
                return "BUY"
        else:
            # 매도 조건 (트레일링 스톱, 손절, 시간청산) — 틱 가격으로 즉시 평가
            curr_price = tick_price
            if self.entry_price == 0.0:
                self.entry_price = data.get("avg_price", curr_price)
                
            if self.peak_price == 0.0:
                self.peak_price = max(self.entry_price, curr_price)
                
            # 실시간 최고가 업데이트 (형성 중 봉 high + 현재 체결가)
            curr_high = max(highs[-1] if highs else curr_price, curr_price)
            self.peak_price = max(self.peak_price, curr_high)
                
            pnl = (curr_price - self.entry_price) / self.entry_price
            drawdown = (self.peak_price - curr_price) / self.peak_price
            avg = float(data.get("avg_price") or self.entry_price)
            net_pnl = calc_net_pnl_pct(avg, curr_price)

            exit_ts = drawdown >= self.params["trail_pct"] and net_pnl >= BEAR_LIVE_MIN_NET_PROFIT
            exit_sl = pnl <= -self.params["stop_loss"]
            
            exit_tc = False
            if self.entry_time:
                elapsed_sec = (datetime.now() - self.entry_time).total_seconds()
                bar_sec = _bar_seconds_for_timeframe(self.timeframe)
                if elapsed_sec >= self.params["time_cut"] * bar_sec:
                    exit_tc = True
            
            # 실시간 지표 상태 기록
            data["custom_bear_val"] = f"익절(TS):{drawdown*100:.1f}%(기준:{self.params['trail_pct']*100:.1f}%) / 손절(SL):{pnl*100:.1f}%(기준:-{self.params['stop_loss']*100:.1f}%)"
            
            if exit_ts or exit_sl or exit_tc:
                self.entry_price = 0.0
                self.entry_time = None
                self.peak_price = 0.0
                return "SELL"
                
        return "HOLD"


class VerboseCompositeStrategy(CompositeStrategy):
    def __init__(self, strategies=None, logic="AND", weights=None, threshold=0.5, sell_logic=None):
        super().__init__(strategies, logic)
        self.weights = weights or {}
        self.threshold = threshold
        self.sell_logic = sell_logic

    @staticmethod
    def _resolve_side(signals: dict, logic: str, weights: dict, threshold: float) -> tuple:
        sig_list = list(signals.values())
        n = len(sig_list)
        if n == 0:
            return False, False
        if logic == "AND":
            return all(s == "BUY" for s in sig_list), all(s == "SELL" for s in sig_list)
        if logic == "OR":
            return any(s == "BUY" for s in sig_list), any(s == "SELL" for s in sig_list)
        if logic == "VOTE":
            majority = n // 2 + 1
            return sig_list.count("BUY") >= majority, sig_list.count("SELL") >= majority
        if logic == "WEIGHTED_VOTE":
            buy_weight = sell_weight = total_weight = 0.0
            for name, sig in signals.items():
                w = weights.get(name, 1.0)
                total_weight += w
                if sig == "BUY":
                    buy_weight += w
                elif sig == "SELL":
                    sell_weight += w
            if total_weight <= 0:
                return False, False
            return buy_weight / total_weight >= threshold, sell_weight / total_weight >= threshold
        return any(s == "BUY" for s in sig_list), any(s == "SELL" for s in sig_list)

    def generate_signal(self, data: Dict[str, Any]) -> str:
        signals = {}
        for s in self.strategies:
            sig = s.generate_signal(data)
            signals[s.NAME] = sig

        sell_logic = self.sell_logic or self.logic
        buy_ok, _ = self._resolve_side(signals, self.logic, self.weights, self.threshold)
        _, sell_ok = self._resolve_side(signals, sell_logic, self.weights, self.threshold)

        has_position = float(data.get("position_qty", 0) or 0) > 0
        if has_position and sell_ok:
            final_signal = "SELL"
        elif buy_ok:
            final_signal = "BUY"
        else:
            final_signal = "HOLD"

        indicators = {}
        for s in self.strategies:
            if s.NAME == "RSI":
                indicators["RSI"] = data.get("rsi_val", "-")
            elif s.NAME == "MACD":
                indicators["MACD"] = data.get("macd_val", "-")
            elif s.NAME == "Bollinger":
                indicators["Bollinger"] = data.get("bb_val", "-")
            elif s.NAME == "CustomBear":
                indicators["CustomBear"] = data.get("custom_bear_val", "-")
            elif s.NAME == "ReversePremium":
                indicators["역프리미엄"] = data.get("usdt_fx_val", "-")
            elif s.NAME == "UsdtFxBollinger":
                indicators["역프+BB"] = data.get("usdt_fx_val", "-")
                indicators["Bollinger"] = data.get("bb_val", "-")

        data["composite_details"] = {
            "logic": self.logic,
            "sell_logic": sell_logic,
            "sub_signals": signals,
            "indicators": indicators,
            "weights": self.weights,
            "threshold": self.threshold,
            "final": final_signal
        }
        return final_signal


class AllowAllRiskManager(BaseRiskManager):
    def is_allowed(self, signal: str, position: dict) -> bool:
        return True
    def check_risk_signal(self, position: dict) -> str:
        return "HOLD"

class StopLossRiskManager(BaseRiskManager):
    def __init__(self, stop_loss_pct: float = 0.03, take_profit_pct: float = 0.06):
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct

    def is_allowed(self, signal: str, position: dict) -> bool:
        return True

    def check_risk_signal(self, position: dict) -> str:
        qty = position.get("quantity", 0.0)
        if qty <= 0:
            return "HOLD"
            
        pnl_pct = position.get("pnl_pct", 0.0)
        if pnl_pct <= -self.stop_loss_pct:
            return "FORCE_SELL_STOP_LOSS"
        if pnl_pct >= self.take_profit_pct:
            return "FORCE_SELL_TAKE_PROFIT"
        return "HOLD"

class TrailingStopRiskManager(BaseRiskManager):
    def __init__(self, trail_pct: float = 0.02, trail_activate_profit_pct: float = 0.0):
        self.trail_pct = trail_pct
        self.trail_activate_profit_pct = trail_activate_profit_pct
        self.peak_price = 0.0

    def is_allowed(self, signal: str, position: dict) -> bool:
        return True

    def check_risk_signal(self, position: dict) -> str:
        qty = position.get("quantity", 0.0)
        if qty <= 0:
            self.peak_price = 0.0
            return "HOLD"
            
        curr_price = position.get("current_price", 0.0)
        avg_price = position.get("avg_price", 0.0)
        pnl_pct = position.get("pnl_pct", 0.0)
        
        if self.peak_price == 0.0:
            self.peak_price = curr_price
        else:
            self.peak_price = max(self.peak_price, curr_price)

        if self.trail_activate_profit_pct > 0 and pnl_pct < self.trail_activate_profit_pct:
            return "HOLD"
            
        drawdown_pct = (self.peak_price - curr_price) / self.peak_price if self.peak_price > 0 else 0.0
        if drawdown_pct >= self.trail_pct:
            return "FORCE_SELL_TRAILING_STOP"
        return "HOLD"


class BearScalpRiskManager(BaseRiskManager):
    """하락장 실거래: 손절 + 낮은 익절 + 고점 트레일링 복합 청산."""

    def __init__(
        self,
        stop_loss_pct: float = 0.03,
        take_profit_pct: float = 0.006,
        trail_pct: float = 0.005,
        trail_activate_profit_pct: float = 0.0,
    ):
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.trail_pct = trail_pct
        self.trail_activate_profit_pct = trail_activate_profit_pct
        self.peak_price = 0.0

    def is_allowed(self, signal: str, position: dict) -> bool:
        return True

    def check_risk_signal(self, position: dict) -> str:
        qty = position.get("quantity", 0.0)
        if qty <= 0:
            self.peak_price = 0.0
            return "HOLD"

        curr_price = position.get("current_price", 0.0)
        pnl_pct = position.get("pnl_pct", 0.0)

        if pnl_pct <= -self.stop_loss_pct:
            return "FORCE_SELL_STOP_LOSS"
        if pnl_pct >= self.take_profit_pct:
            return "FORCE_SELL_TAKE_PROFIT"

        if self.peak_price == 0.0:
            self.peak_price = curr_price
        else:
            self.peak_price = max(self.peak_price, curr_price)

        if self.trail_activate_profit_pct > 0 and pnl_pct < self.trail_activate_profit_pct:
            return "HOLD"

        drawdown_pct = (self.peak_price - curr_price) / self.peak_price if self.peak_price > 0 else 0.0
        if drawdown_pct >= self.trail_pct and pnl_pct >= BEAR_LIVE_MIN_NET_PROFIT:
            return "FORCE_SELL_TRAILING_STOP"
        return "HOLD"

class AveragingDownRiskManager(BaseRiskManager):
    def __init__(self, drop_trigger_pct: float = 0.06, max_add_count: int = 1, stop_loss_pct: float = 0.03, take_profit_pct: float = 0.02):
        self.drop_trigger_pct = drop_trigger_pct
        self.max_add_count = max_add_count
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.add_count = 0

    def is_allowed(self, signal: str, position: dict) -> bool:
        return True

    def check_risk_signal(self, position: dict) -> str:
        qty = position.get("quantity", 0.0)
        if qty <= 0:
            self.add_count = 0
            return "HOLD"
            
        pnl_pct = position.get("pnl_pct", 0.0)
        
        if pnl_pct <= -self.stop_loss_pct:
            return "FORCE_SELL_STOP_LOSS"
            
        if pnl_pct >= self.take_profit_pct:
            return "FORCE_SELL_TAKE_PROFIT"

        if pnl_pct <= -self.drop_trigger_pct and self.add_count < self.max_add_count:
            self.add_count += 1
            return "FORCE_ADD_BUY_AVERAGING"
            
        return "HOLD"


# ══════════════════════════════════════════════════════════════
# 2. 커스텀 워커 스레드 (실시간 체결 유입을 UI 큐로 전달)
# ══════════════════════════════════════════════════════════════
class UITickerWorker(TickerWorker):
    def __init__(self, ticker: str, config: dict, order_queue: queue.Queue, account_manager: AccountManager):
        # switch_regime에서 실질적으로 세팅할 것이므로 임시 전략/리스크 매니저 주입
        dummy_manager = CandleManager(ticker, timeframes=["1m"])
        dummy_strategy = BithumbRsiStrategy(dummy_manager, timeframe="1m")
        dummy_risk = AllowAllRiskManager()
        super().__init__(ticker, dummy_strategy, dummy_risk, order_queue, account_manager)
        
        self.config = config
        self.trading_active = config.get("active", True)
        self.long_term_ma_period = config.get("long_term_ma_period", 250)
        self.order_amount = 5100.0  # 회당 기본 매매 주문금액 (빗썸 최소 5,000원 이상 + 여유분 반영)
        
        # 1. 백엔드 CandleManager 장착 및 웜업 구동
        self.candle_manager = CandleManager(ticker, timeframes=["1m", "5m", "15m", "30m", "1h", "D", "W", "M"], max_candles=4500)
        self.candle_manager.warmup()
        
        # 2. 장세 감지기 장착 (전략 설정 파일에서 임계치 획득)
        bull_limit = config.get("bull_duration_limit_days", 100)
        bear_limit = config.get("bear_duration_limit_days", 200)
        self.regime_detector = MarketRegimeDetector(
            self.candle_manager, 
            long_term_ma_period=self.long_term_ma_period,
            bull_duration_limit_days=bull_limit,
            bear_duration_limit_days=bear_limit
        )
        self.current_regime = "RANGE"  # 초기 상태
        
        # 3. 초기 장세를 진단하여 첫 작전(Tactics) 로딩 및 스위칭
        initial_regime = self.regime_detector.detect_regime()
        override = self.config.get("regime_override", "AUTO")
        if override != "AUTO":
            initial_regime = override
        self.switch_regime(initial_regime, log_to_ui=False)
        self.current_regime = initial_regime
        
        # 기동 시각 KST 기준 일자 기록 (매일 01시 갱신 체크 기준 마련)
        from datetime import datetime, timezone, timedelta
        kst_now = datetime.now(timezone(timedelta(hours=9)))
        self.last_regime_check_date = kst_now.strftime("%Y-%m-%d")

    def _total_portfolio_value(self) -> float:
        global engine_instance
        krw_balance = self.account.get_balance()
        total_crypto_value = 0.0
        if engine_instance:
            for worker in engine_instance._workers.values():
                qty = worker.position.quantity
                curr_price = worker.position.current_price
                if qty > 0 and curr_price > 0:
                    total_crypto_value += qty * curr_price
        return krw_balance + total_crypto_value

    def _ticker_allocation_cap_krw(self) -> float:
        """4코인 1:1:1:1 → 종목당 전체 자산의 1/N."""
        total = self._total_portfolio_value()
        if total <= 0:
            return 0.0
        return total / len(SUPPORTED_TICKERS)

    def compute_strategy_buy_amount(self) -> float:
        """
        전략 매수 1회 금액: 종목 할당(1/N)의 절반 이내, 할당 상한까지 추가 매수 허용.
        """
        ticker_cap = self._ticker_allocation_cap_krw()
        if ticker_cap <= 0:
            return 0.0
        current_val = self.position.quantity * self.position.current_price
        room = max(0.0, ticker_cap - current_val)
        single_max = ticker_cap * PORTFOLIO_MAX_SINGLE_BUY_OF_SHARE
        balance = self.account.get_balance()
        amount = min(single_max, room, balance)
        if amount < MIN_BITHUMB_ORDER_KRW:
            if room >= MIN_BITHUMB_ORDER_KRW and balance >= MIN_BITHUMB_ORDER_KRW:
                print(f"[Portfolio] {self.ticker} 1회 매수 상한({single_max:,.0f}원) 미달 — 할당 절반 규칙 적용 중")
            elif room < MIN_BITHUMB_ORDER_KRW and current_val > 0:
                print(f"[Portfolio Guard] {self.ticker} 할당 한도({ticker_cap:,.0f}원) 도달 — 추가 매수 불가")
            return 0.0
        return amount

    def check_portfolio_allocation_limit(self, buy_amount: float) -> bool:
        """레거시 호환 — compute_strategy_buy_amount() 사용 권장."""
        return self.compute_strategy_buy_amount() >= MIN_BITHUMB_ORDER_KRW

    def switch_regime(self, regime: str, log_to_ui=True):
        """
        감지된 장세(BULL, BEAR, RANGE)에 맞춰 전략 조합 및 리스크 매니저를 실시간 전환합니다.
        """
        if self.ticker in STABLECOIN_TICKERS:
            regime = "RANGE"

        tactics = self.config.get("tactics", {})
        t_cfg = tactics.get(regime)
        if not t_cfg:
            print(f"[Worker-{self.ticker}] 장세 {regime}에 대응하는 작전 설정이 존재하지 않습니다.")
            return

        if regime == "BEAR" and self.ticker not in STABLECOIN_TICKERS:
            t_cfg = _apply_bear_live_profile(t_cfg)

        self._live_tactic = t_cfg

        # 전략 오브젝트 재생성
        strategies = []
        weights = {}
        for s_cfg in t_cfg.get("strategies", []):
            if not s_cfg.get("enabled", False):
                continue
            name = s_cfg["name"]
            weight = s_cfg.get("weight", 1.0)
            params = s_cfg.get("params", {})
            tf = s_cfg.get("timeframe", "5m")
            weights[name] = weight
            
            if name == "RSI":
                strategies.append(BithumbRsiStrategy(self.candle_manager, timeframe=tf, params=params))
            elif name == "MACD":
                strategies.append(BithumbMacdStrategy(self.candle_manager, timeframe=tf, params=params))
            elif name == "Bollinger":
                strategies.append(BithumbBollingerStrategy(self.candle_manager, timeframe=tf, params=params))
            elif name == "CustomBear":
                strategies.append(BithumbCustomBearStrategy(self.candle_manager, timeframe=tf, params=params))
            elif name == "ReversePremium":
                fx_cfg = self.config.get("usdt_fx", {})
                merged = {**params, **fx_cfg}
                strategies.append(BithumbUsdtReversePremiumStrategy(params=merged))
            elif name == "UsdtFxBollinger":
                fx_cfg = self.config.get("usdt_fx", {})
                merged = {**params, **fx_cfg}
                strategies.append(BithumbUsdtFxBollingerStrategy(self.candle_manager, timeframe=tf, params=merged))

        logic = t_cfg.get("logic", "AND")
        sell_logic = t_cfg.get("sell_logic")
        threshold = t_cfg.get("threshold", 0.5)

        self.strategy = VerboseCompositeStrategy(
            strategies=strategies,
            logic=logic,
            weights=weights,
            threshold=threshold,
            sell_logic=sell_logic,
        )

        # 리스크 매니저 재생성
        risk_cfg = t_cfg.get("risk", {"type": "None"})
        r_type = risk_cfg.get("type", "None")
        if r_type == "BearScalp":
            self.risk_manager = BearScalpRiskManager(
                stop_loss_pct=risk_cfg.get("stop_loss_pct", BEAR_LIVE_STOP_LOSS),
                take_profit_pct=risk_cfg.get("take_profit_pct", BEAR_RISK_FALLBACK["take_profit_pct"]),
                trail_pct=risk_cfg.get("trail_pct", BEAR_RISK_FALLBACK["trail_pct"]),
                trail_activate_profit_pct=risk_cfg.get("trail_activate_profit_pct", BEAR_RISK_FALLBACK["trail_activate_profit_pct"]),
            )
        elif r_type == "StopLoss":
            self.risk_manager = StopLossRiskManager(
                stop_loss_pct=risk_cfg.get("stop_loss_pct", 0.03),
                take_profit_pct=risk_cfg.get("take_profit_pct", 0.06)
            )
        elif r_type == "TrailingStop":
            self.risk_manager = TrailingStopRiskManager(
                trail_pct=risk_cfg.get("trail_pct", 0.02),
                trail_activate_profit_pct=risk_cfg.get("trail_activate_profit_pct", 0.0),
            )
        elif r_type == "AveragingDown":
            self.risk_manager = AveragingDownRiskManager(
                drop_trigger_pct=risk_cfg.get("drop_trigger_pct", 0.06),
                max_add_count=risk_cfg.get("max_add_count", 1),
                stop_loss_pct=risk_cfg.get("stop_loss_pct", 0.03),
                take_profit_pct=risk_cfg.get("take_profit_pct", 0.02)
            )
        else:
            self.risk_manager = AllowAllRiskManager()

        self.config["current_regime"] = regime
        if self.ticker in active_ticker_configs:
            active_ticker_configs[self.ticker]["current_regime"] = regime

        # UI 상에 감지된 변경사항 로깅
        if log_to_ui:
            ui_event = {
                "type": "regime_change",
                "data": {
                    "ticker": self.ticker,
                    "regime": regime,
                    "logic": logic,
                    "risk_type": r_type,
                    "timestamp": int(time.time() * 1000)
                }
            }
            ui_event_queue.put(ui_event)
            print(f"[Worker-{self.ticker}] 시장 국면 변경 감지 -> {regime} 작전으로 스위칭 완료! (전략: {logic}" + (f"/매도:{sell_logic}" if sell_logic else "") + f", 리스크: {r_type})")

    def _process(self, data: dict) -> None:
        # 1. 틱 정보로 CandleManager 실시간 캔들 업데이트
        self.candle_manager.update(data)
        
        self.position.current_price = data.get("price", self.position.current_price)
        timestamp = data.get("timestamp", int(time.time() * 1000))

        if self.ticker not in STABLECOIN_TICKERS:
            # 2. 시장 국면 감지 및 동적 작전 스위칭 (매일 KST 01시 이후 1회만 연산하여 CPU 부하 최적화)
            from datetime import datetime, timezone, timedelta
            kst_now = datetime.now(timezone(timedelta(hours=9)))
            current_date_str = kst_now.strftime("%Y-%m-%d")

            # 2-1. 장세 강제 고정 만료 여부 체크
            expires_at_str = self.config.get("regime_override_expires_at", None)
            if expires_at_str and self.config.get("regime_override", "AUTO") != "AUTO":
                try:
                    expires_at = datetime.fromisoformat(expires_at_str)
                    if kst_now >= expires_at:
                        self.config["regime_override"] = "AUTO"
                        self.config["regime_override_expires_at"] = None
                        active_ticker_configs[self.ticker]["regime_override"] = "AUTO"
                        active_ticker_configs[self.ticker]["regime_override_expires_at"] = None
                        save_persisted_configs()
                        
                        ui_event = {
                            "type": "regime_change",
                            "data": {
                                "ticker": self.ticker,
                                "regime": "AUTO",
                                "logic": "AUTO",
                                "risk_type": "AUTO",
                                "timestamp": int(time.time() * 1000)
                            }
                        }
                        ui_event_queue.put(ui_event)
                        print(f"[Worker-{self.ticker}] 장세 고정 만료 -> AUTO 모드로 자동 전환 완료!")
                except Exception as e:
                    print(f"[Worker-{self.ticker}] 장세 고정 만료 체크 중 오류: {e}")
            
            # 2. 시장 국면 감지 및 동적 작전 스위칭 (수동 장세 고정 오버라이드 반영)
            override = self.config.get("regime_override", "AUTO")
            
            detailed = self.regime_detector.detect_regime_detailed()
            detected_regime = detailed["regime"]
            regime_reason = detailed["reason"]
            cycle_match = detailed.get("cycle_match")
            bear_pattern = detailed.get("bear_pattern")
            if override == "AUTO":
                if cycle_match:
                    self.config["cycle_analysis"] = cycle_match
                if bear_pattern:
                    self.config["bear_pattern_analysis"] = bear_pattern
                if self.ticker in active_ticker_configs:
                    if cycle_match:
                        active_ticker_configs[self.ticker]["cycle_analysis"] = cycle_match
                    if bear_pattern:
                        active_ticker_configs[self.ticker]["bear_pattern_analysis"] = bear_pattern
                    od = active_ticker_configs[self.ticker].get("optimized_default")
                    if od and cycle_match:
                        od["cycle_match_cycle"] = cycle_match.get("matched_cycle_number")
                        od["cycle_similarity_pct"] = cycle_match.get("similarity_pct")
                    if od and bear_pattern:
                        timing = bear_pattern.get("entry_timing", {})
                        od["bear_entry_score"] = timing.get("entry_score")
                        od["bear_entry_style"] = timing.get("entry_style")
                        od["bear_episode_count"] = bear_pattern.get("episode_count")
                    if detected_regime == "BEAR":
                        _update_bear_entry_ranking()
            
            if override != "AUTO":
                detected_regime = override
                regime_reason = f"사용자 지정 장세 강제 고정 중 ({override})"

            should_update_regime = False
            if not hasattr(self, 'last_regime_check_date'):
                should_update_regime = True
            elif kst_now.hour >= 1 and self.last_regime_check_date != current_date_str:
                should_update_regime = True
            elif override != "AUTO" and self.current_regime != override:
                should_update_regime = True
            elif override == "AUTO" and self.current_regime != detected_regime:
                should_update_regime = True
                
            if should_update_regime:
                self.last_regime_check_date = current_date_str
                self.switch_regime(detected_regime, log_to_ui=(override == "AUTO"))
                self.current_regime = detected_regime
        else:
            from datetime import datetime, timezone, timedelta
            kst_now = datetime.now(timezone(timedelta(hours=9)))
            current_date_str = kst_now.strftime("%Y-%m-%d")
            override = self.config.get("regime_override", "AUTO")

            detailed = self.regime_detector.detect_regime_detailed()
            cycle_match = detailed.get("cycle_match")
            peg_reason = detailed.get("reason", "")
            if cycle_match and override == "AUTO":
                self.config["cycle_analysis"] = cycle_match
                if self.ticker in active_ticker_configs:
                    active_ticker_configs[self.ticker]["cycle_analysis"] = cycle_match

            should_refresh = False
            if not hasattr(self, "last_usdt_strategy_check_date"):
                should_refresh = True
            elif kst_now.hour >= 1 and self.last_usdt_strategy_check_date != current_date_str:
                should_refresh = True

            if should_refresh:
                self.last_usdt_strategy_check_date = current_date_str
                self.switch_regime("RANGE", log_to_ui=True)
                self.current_regime = "RANGE"

            resolved = self.config.get("resolved_usdt_strategy", "fixed_fusion")
            pick_reason = self.config.get("usdt_auto_pick_reason", "")
            if resolved == "fixed_fusion":
                regime_reason = f"USDT 고정 융합 (환율+BB) · {pick_reason}"
            else:
                regime_reason = f"USDT 학습 3대지표 · {pick_reason}"
            if peg_reason:
                regime_reason = f"{regime_reason} | {peg_reason}"

        # 3. 갱신된 전략 세팅으로 시그널 도출
        data["position_qty"] = self.position.quantity
        data["avg_price"] = self.position.avg_price
        final_signal = self.strategy.generate_signal(data)
        composite_details = data.get("composite_details", {
            "logic": "UNKNOWN", "sub_signals": {}, "indicators": {}, "final": final_signal
        })

        # 평가 금액 및 수익률 계산 (수수료 반영 순수익률)
        krw_value = self.position.quantity * self.position.current_price
        pnl_pct = calc_net_pnl_pct(self.position.avg_price, self.position.current_price)

        # 4. 실시간 UI 이벤트 데이터 전송 (국면 정보 및 상세 사유 탑재)
        ui_event = {
            "type": "trade",
            "data": {
                "ticker": self.ticker,
                "price": self.position.current_price,
                "volume": data.get("volume", 0),
                "composite": composite_details,
                "regime": self.current_regime,
                "regime_reason": regime_reason,
                "regime_override": override,
                "cycle_match": cycle_match,
                "bear_pattern": self.config.get("bear_pattern_analysis") if self.ticker not in STABLECOIN_TICKERS else None,
                "bear_entry_ranking": bear_entry_ranking if self.ticker not in STABLECOIN_TICKERS else None,
                "optimized_default": active_ticker_configs.get(self.ticker, {}).get("optimized_default"),
                "usdt_fx_status": data.get("usdt_fx_status") if self.ticker in STABLECOIN_TICKERS else None,
                "resolved_usdt_strategy": self.config.get("resolved_usdt_strategy") if self.ticker in STABLECOIN_TICKERS else None,
                "usdt_auto_pick_reason": self.config.get("usdt_auto_pick_reason") if self.ticker in STABLECOIN_TICKERS else None,
                "timestamp": timestamp,
                "trading_active": self.trading_active,
                "balance": self.account.get_balance(),
                "position": {
                    "quantity": self.position.quantity,
                    "avg_price": self.position.avg_price,
                    "krw_value": krw_value,
                    "pnl_pct": pnl_pct * 100.0
                }
            }
        }
        ui_event_queue.put(ui_event)

        # 비활성화 상태면 실제 주문 무시
        if not self.trading_active:
            return

        # 5. 리스크 관리자 강제 시그널 처리
        pos_info = {
            "avg_price": self.position.avg_price,
            "current_price": self.position.current_price,
            "quantity": self.position.quantity,
            "pnl_pct": pnl_pct
        }
        
        risk_signal = "HOLD"
        if hasattr(self.risk_manager, "check_risk_signal"):
            risk_signal = self.risk_manager.check_risk_signal(pos_info)
            
        if risk_signal != "HOLD":
            if risk_signal.startswith("FORCE_SELL"):
                print(f"[Risk Engine] {self.ticker} 강제 청산 집행: {risk_signal}")
                self.order_queue.put({
                    "ticker": self.ticker,
                    "signal": "SELL",
                    "price": self.position.current_price,
                    "risk_reason": risk_signal
                })
                return
            elif risk_signal == "FORCE_ADD_BUY_AVERAGING":
                add_amount = self.compute_strategy_buy_amount()
                if add_amount <= 0:
                    return
                if self.account.reserve(add_amount):
                    print(f"[Risk Engine] {self.ticker} 물타기 추가 매수 집행 ({add_amount:,.0f}원)")
                    self.order_queue.put({
                        "ticker": self.ticker,
                        "signal": "BUY",
                        "price": self.position.current_price,
                        "risk_reason": risk_signal,
                        "manual_amount": add_amount,
                    })
                return

        # 6. 전략 매매 주문 처리
        if final_signal == "HOLD":
            return

        if final_signal == "BUY":
            if self.ticker in STABLECOIN_TICKERS and not _usdt_live_fx_allows_buy(self.config, self.position.current_price):
                return

            if self.current_regime == "BEAR" and self.ticker not in STABLECOIN_TICKERS:
                tactic = getattr(self, "_live_tactic", None) or self.config.get("tactics", {}).get("BEAR", {})
                tf = OPTIM_BACKTEST_TIMEFRAME
                for s in tactic.get("strategies", []):
                    if s.get("enabled") and s.get("timeframe"):
                        tf = s["timeframe"]
                        break
                ok, block_reason = _bear_buy_entry_allowed(
                    self.candle_manager,
                    tf,
                    self.position.current_price,
                    tactic,
                    bear_timing=(self.config.get("bear_pattern_analysis") or {}).get("entry_timing"),
                )
                if not ok:
                    print(f"[Bear Entry Gate] {self.ticker} 매수 차단: {block_reason}")
                    return

            buy_amount = self.compute_strategy_buy_amount()
            if buy_amount <= 0:
                return

            now_ts = time.time()
            last_order_ts = getattr(self, "_last_order_ts", 0)
            last_order_signal = getattr(self, "_last_order_signal", "")
            if last_order_signal == "BUY" and (now_ts - last_order_ts) < ORDER_SAME_DIR_COOLDOWN_SEC:
                return

            if not self.account.reserve(buy_amount):
                return

        elif final_signal == "SELL":
            # ★ 포지션 없으면 매도 불필요
            if self.position.quantity <= 0:
                return

            tactic = getattr(self, "_live_tactic", None) if self.current_regime == "BEAR" else None
            if not tactic:
                tactic = self.config.get("tactics", {}).get(self.current_regime, {})
            min_sell = _get_min_strategy_sell_profit_pct(tactic)
            if self.current_regime == "BEAR" and self.ticker not in STABLECOIN_TICKERS:
                min_sell = max(min_sell, _bear_min_net_profit(tactic))
            if min_sell > 0 and pnl_pct < min_sell:
                return  # 순이익(수수료 포함) 미달 — 손절만 리스크 엔진

            # 지표 매도: 손절선 외에는 순손실 구간에서 매도 불가
            if pnl_pct < (min_sell if min_sell > 0 else 0):
                gross_pnl = self.position.pnl_pct
                risk_cfg = tactic.get("risk", {})
                sl_thresh = None
                if risk_cfg.get("type") in ("StopLoss", "BearScalp"):
                    sl_thresh = risk_cfg.get("stop_loss_pct", 0.03)
                elif tactic.get("logic") == "CUSTOM_BEAR":
                    for s in tactic.get("strategies", []):
                        if s.get("name") == "CustomBear":
                            sl_thresh = s.get("params", {}).get("stop_loss", 0.015)
                            break
                if sl_thresh is None or gross_pnl > -sl_thresh:
                    return
            
            # ★ 매도 쿨다운: 동일 틱 중복 매도만 방지
            now_ts = time.time()
            last_order_ts = getattr(self, "_last_order_ts", 0)
            last_order_signal = getattr(self, "_last_order_signal", "")
            if last_order_signal == "SELL" and (now_ts - last_order_ts) < ORDER_SAME_DIR_COOLDOWN_SEC:
                return  # 30초 쿨다운 중

        # 주문 타임스탬프 및 방향 기록 (쿨다운 기준)
        self._last_order_ts = time.time()
        self._last_order_signal = final_signal

        order_payload = {
            "ticker": self.ticker,
            "signal": final_signal,
            "price": self.position.current_price,
        }
        if final_signal == "BUY":
            order_payload["manual_amount"] = buy_amount
        self.order_queue.put(order_payload)


# ══════════════════════════════════════════════════════════════
# 3. 다중 종목 실시간 웹소켓 리스너
# ══════════════════════════════════════════════════════════════
class MultiTickerWebSocketListener:
    def __init__(self, dispatcher, markets=None):
        if markets is None:
            markets = [f"KRW-{t}" for t in SUPPORTED_TICKERS]
        self.dispatcher = dispatcher
        self.markets = markets
        self.url = "wss://ws-api.bithumb.com/websocket/v1"
        self._ws = None
        self._running = False

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, name="WS-UI-TimeframeCheck", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()

    def update_markets(self, new_markets):
        self.markets = new_markets
        print(f"[WS Listener] 구독 마켓 목록이 변경되어 소켓을 재연결합니다. ({new_markets})")
        if self._ws:
            self._ws.close()

    def _run_loop(self):
        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self._ws.run_forever()
            except Exception:
                pass
            if self._running:
                time.sleep(3)

    def _on_open(self, ws):
        print(f"[WS-UI-TimeframeCheck] 빗썸 소켓 연결 완료. 구독 마켓: {self.markets}")
        sub_msg = [
            {"ticket": str(uuid.uuid4())},
            {"type": "trade", "codes": self.markets}
        ]
        ws.send(json.dumps(sub_msg))

    def _on_message(self, ws, message):
        if message == "PING":
            ws.send("PONG")
            return

        try:
            msg = json.loads(message)
            if msg.get("type") == "trade":
                market_code = msg.get("code", "")
                ticker = market_code.replace("KRW-", "")
                
                data = {
                    "ticker": ticker,
                    "price": float(msg["trade_price"]),
                    "volume": float(msg["trade_volume"]),
                    "timestamp": msg.get("timestamp", int(time.time() * 1000))
                }
                self.dispatcher.dispatch(ticker, data)
        except Exception:
            pass

    def _on_error(self, ws, error):
        pass

    def _on_close(self, ws, close_code, close_msg):
        pass


# ══════════════════════════════════════════════════════════════
# 3.5. Bithumb 실제 잔고 동기화 계좌 관리자
# ══════════════════════════════════════════════════════════════
class BithumbRealAccountManager(AccountManager):
    def __init__(self, initial_balance: float, engine_ref=None):
        super().__init__(initial_balance)
        self.engine_ref = engine_ref
        self._stop_event = threading.Event()
        self._thread = None
        self._last_sync_time = 0

    def start_sync(self):
        if BITHUMB_ACCESS_KEY and BITHUMB_SECRET_KEY:
            self._thread = threading.Thread(target=self._sync_loop, name="BithumbAccountSync", daemon=True)
            self._thread.start()
            print("[AccountManager] 빗썸 실시간 잔고 동기화 스레드가 기동되었습니다.")
        else:
            print("[AccountManager] API Key가 설정되지 않아 가상 잔고 모드로만 작동합니다.")

    def stop_sync(self):
        self._stop_event.set()

    def _sync_loop(self):
        while not self._stop_event.is_set():
            try:
                self.sync_from_bithumb()
            except Exception as e:
                print(f"[AccountManager] 빗썸 잔고 동기화 중 예외 발생: {e}")
            time.sleep(10)

    def sync_from_bithumb(self):
        url = "https://api.bithumb.com/v1/accounts"
        headers = create_bithumb_headers()
        if not headers:
            return

        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            krw_balance = 0.0
            positions = {} # {ticker: {"quantity": qty, "avg_price": price}}

            for item in data:
                curr = item.get("currency")
                balance = float(item.get("balance", 0.0))
                locked = float(item.get("locked", 0.0))
                avg_buy_price = float(item.get("avg_buy_price", 0.0))
                
                if curr == "KRW":
                    # 빗썸 API의 KRW 'balance' 필드는 이미 Locked(사용중)가 제외된 '주문가능원화'를 반환하므로, locked를 이중으로 뺄 필요가 없습니다.
                    krw_balance = balance
                else:
                    # 보유 코인 정보
                    positions[curr] = {
                        "quantity": balance - locked,
                        "avg_price": avg_buy_price
                    }

            # 원화 잔고 업데이트
            self.sync(krw_balance)

            # 엔진의 워커들 포지션 업데이트
            if self.engine_ref and hasattr(self.engine_ref, "_workers"):
                for ticker, worker in self.engine_ref._workers.items():
                    if ticker in positions:
                        pos_info = positions[ticker]
                        qty = pos_info["quantity"]
                        avg_price = pos_info["avg_price"]
                        
                        # 먼지 잔고(Dust) 처리: 평가금액이 2,000원 미만인 경우 포지션을 없는 것으로 처리
                        curr_price = getattr(worker.position, "current_price", 0.0) or avg_price
                        est_value = qty * curr_price
                        
                        if est_value < 2000.0:
                            # 2000원 미만 소액은 DUST로 무시
                            worker.position.quantity = 0.0
                            worker.position.avg_price = 0.0
                        else:
                            worker.position.quantity = qty
                            worker.position.avg_price = avg_price
                    else:
                        # 빗썸에 잔고가 없는 경우
                        worker.position.quantity = 0.0
                        worker.position.avg_price = 0.0

            self._last_sync_time = time.time()
        else:
            print(f"[AccountManager] 빗썸 잔고 조회 실패 (Status: {resp.status_code}, Body: {resp.text})")


# ══════════════════════════════════════════════════════════════
# 4. FastAPI 및 WebSocket 브로드캐스트 브로커
# ══════════════════════════════════════════════════════════════
app = FastAPI(title="Bithumb Multi-Timeframe Regime Switching PoC")

# CORS 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[UI Broker] UI 연결 성공. 연결 수: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"[UI Broker] UI 연결 해제. 연결 수: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

class MultiTickerUIEngine(TradingEngine):
    def register_ticker(self, ticker: str, config: dict) -> None:
        worker = UITickerWorker(
            ticker=ticker,
            config=config,
            order_queue=self.order_queue,
            account_manager=self.account
        )
        self._workers[ticker] = worker
        self.dispatcher.register(ticker, worker)

    def _execute_order(self, order: dict):
        try:
            super()._execute_order(order)
            ticker = order["ticker"]
            signal = order["signal"]
            price = order["price"]
            risk_reason = order.get("risk_reason", None)
            
            worker = self._workers.get(ticker)
            if not worker:
                return
                
            pos = worker.position
            order_amount = order.get("manual_amount", getattr(worker, "order_amount", 5000.0))
            
            # 빗썸 원화 마켓 최소 주문 금액 (보통 500 KRW 이상)
            MIN_BITHUMB_ORDER_KRW = 500.0
            
            if BITHUMB_ORDER_MODE == "real":
                url = "https://api.bithumb.com/v2/orders"
                
                if signal == "BUY":
                    # 실제 원화 잔고 체크
                    balance = self.account.get_balance()
                    buy_amount = min(balance, order_amount)
                    
                    if buy_amount < MIN_BITHUMB_ORDER_KRW:
                        print(f"[REAL ORDER SHIELD] 잔고 부족 또는 최소 주문금액({MIN_BITHUMB_ORDER_KRW}원) 미달로 주문을 생략합니다. (가능 금액: {buy_amount:,.0f} KRW)")
                        return
                    
                    volume = int((buy_amount / price) * 100000000) / 100000000
                    if volume <= 0:
                        print(f"[REAL ORDER SHIELD] 계산된 매수 수량이 0이하입니다. 주문 취소. (금액: {buy_amount:,.0f} KRW)")
                        return
                    
                    params = {
                        "market": f"KRW-{ticker}",
                        "side": "bid",
                        "volume": str(volume),
                        "price": str(int(price)),  # 빗썸 KRW 마켓 가격은 정수
                        "order_type": "limit"
                    }
                    
                    headers = create_bithumb_headers(params)
                    try:
                        resp = requests.post(url, data=json.dumps(params), headers=headers)
                        resp_data = resp.json()
                        if resp.status_code in [200, 201] and "error" not in resp_data:
                            print(f"[REAL ORDER SUCCESS] BUY {ticker} | {volume} @ {price:,.0f} | Response: {resp_data}")
                        else:
                            print(f"[REAL ORDER FAIL] BUY {ticker} 실패 | Response: {resp_data}")
                    except Exception as e:
                        print(f"[ORDER SYSTEM ERROR] 실제 매수 API 호출 오류: {e}")
                        
                elif signal == "SELL":
                    sell_qty = pos.quantity
                    if sell_qty <= 0:
                        print(f"[REAL ORDER SHIELD] 보유 코인 수량이 없어 실제 매도 주문을 전송하지 않습니다.")
                        return
                    
                    # ★ 먼지(Dust) 포지션 처리: 원화 환산 가치가 최소 주문금액 미달이면 포지션만 초기화
                    krw_value = sell_qty * price
                    MIN_SELL_KRW = 5000.0  # 빗썸 최소 주문금액
                    if krw_value < MIN_SELL_KRW:
                        print(f"[DUST CLEANUP] {ticker} 포지션 가치({krw_value:,.0f} KRW)가 최소 주문금액({MIN_SELL_KRW:,.0f} KRW) 미달. 포지션 강제 초기화 처리.")
                        pos.quantity = 0.0
                        pos.avg_price = 0.0
                        if hasattr(worker.risk_manager, "peak_price"):
                            worker.risk_manager.peak_price = 0.0
                        if hasattr(worker.risk_manager, "add_count"):
                            worker.risk_manager.add_count = 0
                        # 쿨다운 리셋
                        worker._last_order_ts = 0
                        worker._last_order_signal = ""
                        return
                    
                    volume = int(sell_qty * 100000000) / 100000000
                    if volume <= 0:
                        print(f"[REAL ORDER SHIELD] 계산된 매도 수량이 너무 작아 주문을 취소합니다. (수량: {sell_qty})")
                        return
                    
                    params = {
                        "market": f"KRW-{ticker}",
                        "side": "ask",
                        "volume": str(volume),
                        "price": str(int(price)),
                        "order_type": "limit"
                    }
                    
                    headers = create_bithumb_headers(params)
                    try:
                        resp = requests.post(url, data=json.dumps(params), headers=headers)
                        resp_data = resp.json()
                        if resp.status_code in [200, 201] and "error" not in resp_data:
                            print(f"[REAL ORDER SUCCESS] SELL {ticker} | {volume} @ {price:,.0f} | Response: {resp_data}")
                            # 성공 시 포지션 초기화
                            pos.quantity = 0.0
                            pos.avg_price = 0.0
                            if hasattr(worker.risk_manager, "peak_price"):
                                worker.risk_manager.peak_price = 0.0
                            if hasattr(worker.risk_manager, "add_count"):
                                worker.risk_manager.add_count = 0
                        else:
                            print(f"[REAL ORDER FAIL] SELL {ticker} 실패 | Response: {resp_data}")
                    except Exception as e:
                        print(f"[ORDER SYSTEM ERROR] 실제 매도 API 호출 오류: {e}")
            else:
                # simulation 모드 (기존 모의 체결 로직 유지 및 order_amount 반영)
                if signal == "BUY":
                    buy_qty = order_amount / price
                    total_cost = (pos.quantity * pos.avg_price) + (buy_qty * price)
                    pos.quantity += buy_qty
                    pos.avg_price = total_cost / pos.quantity
                    print(f"[SIMULATION ORDER] BUY {ticker} | {buy_qty:.4f} @ {price:,.0f}")
                elif signal == "SELL":
                    if risk_reason and risk_reason.startswith("FORCE_SELL"):
                        sell_qty = pos.quantity
                    else:
                        sell_qty = min(pos.quantity, order_amount / price)

                    if sell_qty > 0:
                        pos.quantity -= sell_qty
                        self.account.release(sell_qty * price)
                        if pos.quantity == 0:
                            pos.avg_price = 0.0
                            if hasattr(worker.risk_manager, "peak_price"):
                                worker.risk_manager.peak_price = 0.0
                            if hasattr(worker.risk_manager, "add_count"):
                                worker.risk_manager.add_count = 0
                        print(f"[SIMULATION ORDER] SELL {ticker} | {sell_qty:.4f} @ {price:,.0f}")

            ui_event = {
                "type": "order",
                "data": {
                    "ticker": ticker,
                    "signal": signal,
                    "price": price,
                    "risk_reason": risk_reason,
                    "timestamp": int(time.time() * 1000)
                }
            }
            ui_event_queue.put(ui_event)
        except Exception as outer_e:
            import traceback
            print(f"[ORDER SYSTEM FATAL] _execute_order 예외 발생!")
            traceback.print_exc()

    def update_ticker_config(self, ticker: str, config: dict) -> bool:
        worker = self._workers.get(ticker)
        if not worker: return False
        
        worker.config = config
        worker.trading_active = config.get("active", True)
        worker.long_term_ma_period = config.get("long_term_ma_period", 250)
        
        worker.regime_detector.sync_regression_period(worker.long_term_ma_period)
        
        curr_reg = worker.regime_detector.detect_regime()
        worker.switch_regime(curr_reg, log_to_ui=True)
        worker.current_regime = curr_reg
        
        print(f"[Engine] {ticker} 설정 변경 및 장세 작전 재적용 완료: {curr_reg}")
        return True

    def add_new_ticker(self, ticker: str) -> bool:
        if ticker in self._workers:
            return False
            
        if ticker not in active_ticker_configs:
            _profile = TICKER_CYCLE_PROFILE.get(ticker, TICKER_CYCLE_PROFILE["BTC"])
            active_ticker_configs[ticker] = {
                "active": True,
                "current_regime": "BEAR",
                "regime_override": DEFAULT_REGIME_OVERRIDE,
                "selected_bear_strategy": DEFAULT_BEAR_STRATEGY,
                "long_term_ma_period": _profile["regression_days"],
                "tactics": {
                    "BULL": {
                        "logic": "OR",
                        "threshold": 0.5,
                        "strategies": [
                            {"name": "RSI", "enabled": True, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 38, "overbought": 65}},
                            {"name": "Bollinger", "enabled": False, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                            {"name": "MACD", "enabled": True, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                        ],
                        "risk": {"type": "None"}
                    },
                    "BEAR": {
                        "logic": "CUSTOM_BEAR",
                        "threshold": 0.5,
                        "strategies": [
                            {"name": "CustomBear", "enabled": True, "weight": 1.0, "timeframe": "15m", "params": {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "trail_pct": 0.015, "stop_loss": 0.015, "time_cut": 48}}
                        ],
                        "risk": {"type": "StopLoss", "stop_loss_pct": 0.015, "take_profit_pct": 0.02}
                    },
                    "RANGE": {
                        "logic": "OR",
                        "threshold": 0.5,
                        "strategies": [
                            {"name": "RSI", "enabled": False, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 35, "overbought": 65}},
                            {"name": "Bollinger", "enabled": True, "weight": 1.0, "timeframe": "D", "params": {"period": 5, "std_dev": 1.5}},
                            {"name": "MACD", "enabled": False, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                        ],
                        "risk": {"type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.03}
                    }
                }
            }
            
        config = active_ticker_configs[ticker]
        self.register_ticker(ticker, config)
        
        if hasattr(self, 'ws_listener') and self.ws_listener:
            markets = [f"KRW-{t}" for t in self._workers.keys()]
            self.ws_listener.update_markets(markets)
            
        print(f"[Engine] 신규 종목 {ticker} 가동 성공.")
        return True


# ══════════════════════════════════════════════════════════════
# 5. REST API: 빗썸 과거 캔들 조회 (UI 차트 전용 — 빗썸 API 직통, 빠른 표시)
#    백테스트·장세분석은 15분 캐시 집계 경로와 별개
# ══════════════════════════════════════════════════════════════
@app.get("/api/candles")
def get_historical_candles(
    market: str = Query(..., description="예: KRW-BTC, KRW-ETH"),
    timeframe: str = Query(..., description="1m, 3m, 5m, 10m, 15m, 30m, 1h, 4h, D, W, M")
):
    base_url = "https://api.bithumb.com/v1"
    url = ""
    
    if timeframe == "1m":
        url = f"{base_url}/candles/minutes/1"
    elif timeframe == "3m":
        url = f"{base_url}/candles/minutes/3"
    elif timeframe == "5m":
        url = f"{base_url}/candles/minutes/5"
    elif timeframe == "10m":
        url = f"{base_url}/candles/minutes/10"
    elif timeframe == "15m":
        url = f"{base_url}/candles/minutes/15"
    elif timeframe == "30m":
        url = f"{base_url}/candles/minutes/30"
    elif timeframe == "1h":
        url = f"{base_url}/candles/minutes/60"
    elif timeframe == "4h":
        url = f"{base_url}/candles/minutes/240"
    elif timeframe == "D":
        url = f"{base_url}/candles/days"
    elif timeframe == "W":
        url = f"{base_url}/candles/weeks"
    elif timeframe == "M":
        url = f"{base_url}/candles/months"
    else:
        return {"error": "Invalid timeframe"}

    def _parse_candle_time(c: dict) -> int:
        if timeframe in ["D", "W", "M"]:
            dt = datetime.strptime(c["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        ts_ms = c.get("timestamp")
        if ts_ms:
            return int(ts_ms // 1000)
        kst_str = c.get("candle_date_time_kst") or c.get("candle_date_time_utc", "")
        dt = datetime.fromisoformat(kst_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return int(dt.timestamp())

    try:
        raw_candles = []
        to_cursor = None
        max_pages = 3 if timeframe not in ["D", "W", "M"] else 1
        per_page = 200

        for _ in range(max_pages):
            params = {"market": market, "count": per_page}
            if to_cursor:
                params["to"] = to_cursor
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return {"error": f"Bithumb API Error: {resp.text}"}
            page = resp.json()
            if not page:
                break
            raw_candles.extend(page)
            if len(page) < per_page:
                break
            to_cursor = page[-1].get("candle_date_time_utc")
            if not to_cursor:
                break

        formatted_candles = []
        seen_times = set()
        for c in reversed(raw_candles):
            try:
                t_val = _parse_candle_time(c)
                o = float(c["opening_price"])
                h = float(c["high_price"])
                l = float(c["low_price"])
                cl = float(c["trade_price"])
            except (KeyError, TypeError, ValueError):
                continue
            if t_val <= 0 or o <= 0 or cl <= 0 or t_val in seen_times:
                continue
            seen_times.add(t_val)
            formatted_candles.append({
                "time": t_val,
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": float(c.get("candle_acc_trade_volume", 0))
            })

        if not formatted_candles:
            return {"error": "No valid candle data returned from Bithumb"}
        return formatted_candles
    except Exception as e:
        return {"error": f"Internal Server Error: {str(e)}"}


@app.post("/api/tickers/regime-override")
def override_ticker_regime(payload: dict):
    ticker = payload.get("ticker")
    regime = payload.get("regime", "AUTO")  # "AUTO", "BULL", "BEAR", "RANGE"
    duration_days = payload.get("duration_days", 0)  # 만료 기한 (일수, 0은 무기한)
    
    if not ticker or ticker not in active_ticker_configs:
        return {"success": False, "error": "존재하지 않거나 올바르지 않은 심볼입니다."}
        
    expires_at_str = None
    if regime != "AUTO" and duration_days > 0:
        from datetime import datetime, timezone, timedelta
        kst_now = datetime.now(timezone(timedelta(hours=9)))
        expires_at = kst_now + timedelta(days=duration_days)
        expires_at_str = expires_at.isoformat()
        
    active_ticker_configs[ticker]["regime_override"] = regime
    active_ticker_configs[ticker]["regime_override_expires_at"] = expires_at_str
    save_persisted_configs()
    
    if engine_instance:
        worker = engine_instance._workers.get(ticker)
        if worker:
            worker.config["regime_override"] = regime
            worker.config["regime_override_expires_at"] = expires_at_str
            # 워커가 즉시 강제 전환하도록 유도
            if regime != "AUTO":
                worker.switch_regime(regime, log_to_ui=True)
                worker.current_regime = regime
            else:
                # 자동 모드로 돌아갈 시 즉각 재진단
                auto_regime = worker.regime_detector.detect_regime()
                worker.switch_regime(auto_regime, log_to_ui=True)
                worker.current_regime = auto_regime
                
            print(f"[Engine] {ticker} 장세 수동 오버라이드 변경 완료: {regime}")
            return {"success": True}
            
    return {"success": False, "error": "엔진이 기동되지 않았습니다."}

@app.post("/api/trade/manual")
def manual_trade_order(payload: dict):
    ticker = payload.get("ticker")
    side = payload.get("side") # "BUY" or "SELL"
    amount = payload.get("amount", 5000.0) # 매수 금액 (원화) 혹은 매도 수량
    
    if not ticker or ticker not in active_ticker_configs:
        return {"success": False, "error": "존재하지 않거나 올바르지 않은 심볼입니다."}
    if side not in ["BUY", "SELL"]:
        return {"success": False, "error": "주문 방향이 올바르지 않습니다."}
        
    if engine_instance:
        worker = engine_instance._workers.get(ticker)
        if not worker:
            return {"success": False, "error": "종목 워커를 찾을 수 없습니다."}
            
        current_price = worker.position.current_price
        if current_price <= 0:
            return {"success": False, "error": "현재 시세 정보가 없어 수동 주문이 불가합니다."}
            
        # 주문 객체 빌드
        order = {
            "ticker": ticker,
            "signal": side,
            "price": current_price,
            "risk_reason": "사용자 수동 즉시 주문"
        }
        
        if side == "BUY":
            # 가용 잔고 예치 예약
            if not engine_instance.account.reserve(float(amount)):
                return {"success": False, "error": f"잔고가 부족합니다. (가용: {engine_instance.account.get_balance():,.0f} KRW)"}
            order["manual_amount"] = float(amount)
        elif side == "SELL":
            # 전량 매도
            if worker.position.quantity <= 0:
                return {"success": False, "error": "보유 수량이 없어 매도할 수 없습니다."}
            
        # 주문 큐에 삽입
        engine_instance.order_queue.put(order)
        return {"success": True}
        
    return {"success": False, "error": "엔진이 기동되지 않았습니다."}

@app.get("/api/trade/history")
def get_trade_history(limit: int = 20):
    url = "https://api.bithumb.com/v1/orders"
    params = {
        "state": "done",
        "limit": limit
    }
    headers = create_bithumb_headers(params)
    if not headers:
        return []
    
    from urllib.parse import urlencode
    import requests
    
    query_string = urlencode(params)
    full_url = f"{url}?{query_string}"
    
    try:
        resp = requests.get(full_url, headers=headers)
        if resp.status_code == 200:
            raw_orders = resp.json()
            formatted_orders = []
            for ord in raw_orders:
                market = ord.get("market", "")
                if not market.startswith("KRW-"):
                    continue
                ticker = market.split("-")[1]
                
                # 수량과 금액 구하기
                executed_volume = float(ord.get("executed_volume", ord.get("volume", 0.0) or 0.0))
                executed_funds = float(ord.get("executed_funds", 0.0) or 0.0)
                
                price_str = ord.get("price")
                if price_str is not None:
                    price = float(price_str)
                elif executed_volume > 0:
                    price = executed_funds / executed_volume
                else:
                    price = 0.0
                
                side = ord.get("side", "")
                side_kr = "매수" if side == "bid" else ("매도" if side == "ask" else side)
                
                # datetime 포맷팅 (원시 데이터 예: 2026-06-15T20:24:47+09:00 -> 2026-06-15 20:24:47)
                created_at = ord.get("created_at", "")
                if created_at and "T" in created_at:
                    dt_part = created_at.split("+")[0]
                    formatted_time = dt_part.replace("T", " ")
                else:
                    formatted_time = created_at
                
                formatted_orders.append({
                    "timestamp": formatted_time,
                    "ticker": ticker,
                    "side": side_kr,
                    "price": price,
                    "volume": executed_volume,
                    "amount": executed_funds,
                    "reason": "빗썸 실제 체결"
                })
            return formatted_orders
        else:
            print(f"[API] 빗썸 주문 이력 조회 실패 (Status: {resp.status_code}, Body: {resp.text})")
            return []
    except Exception as e:
        print(f"[API] 빗썸 주문 이력 조회 중 예외 발생: {e}")
        return []

@app.get("/api/config")
def get_configs():
    for ticker in SUPPORTED_TICKERS:
        if ticker not in active_ticker_configs or not isinstance(active_ticker_configs.get(ticker), dict):
            if ticker == "USDT":
                active_ticker_configs[ticker] = _build_default_usdt_config()
            continue
        if ticker == "USDT":
            fx = active_ticker_configs[ticker].setdefault("usdt_fx", {})
            if fx.get("min_consider_gap_krw", 0) < USDT_DEFAULT_MIN_CONSIDER_GAP_KRW:
                fx["min_consider_gap_krw"] = USDT_DEFAULT_MIN_CONSIDER_GAP_KRW
    return active_ticker_configs

@app.get("/api/usdt/fx-status")
def usdt_fx_status():
    """USDT 역프리미엄 현황 (기준환율 vs 빗썸 체결가)."""
    cfg = active_ticker_configs.get("USDT", {})
    fx_cfg = cfg.get("usdt_fx", {})
    price = 0.0
    worker = None
    if engine_instance and "USDT" in engine_instance._workers:
        worker = engine_instance._workers["USDT"]
        price = worker.position.current_price
    manual = float(fx_cfg.get("reference_krw") or 0)
    ttl = int(fx_cfg.get("fx_refresh_minutes", USDT_FX_REFRESH_MINUTES)) * 60
    fair, source = get_usd_krw_rate(manual_krw=manual, cache_ttl_sec=ttl)
    fx = calc_reverse_premium_pct(price, fair) if price > 0 else calc_reverse_premium_pct(0, fair)
    fee_one_way = float(fx_cfg.get("fee_one_way_pct", BITHUMB_FEE_RATE_ONE_WAY * 100)) / 100.0
    min_profit_pct = float(fx_cfg.get("min_target_profit_pct", 0.2))
    min_consider = float(fx_cfg.get("min_consider_gap_krw", USDT_DEFAULT_MIN_CONSIDER_GAP_KRW))
    edge = calc_usdt_fee_aware_edge(fair, price, fee_one_way, min_profit_pct) if price > 0 else calc_usdt_fee_aware_edge(fair, fair, fee_one_way, min_profit_pct)
    phase_info = calc_usdt_consideration_phase(fx.get("gap_krw", 0), min_consider, edge)

    bb_info = {}
    if worker and price > 0:
        tf = fx_cfg.get("bb_timeframe", "15m")
        period = int(fx_cfg.get("bb_period", 20))
        std_dev = float(fx_cfg.get("bb_std_dev", 2.0))
        prices = worker.candle_manager.get_prices(tf)
        if prices and len(prices) > period:
            series = pd.Series(prices)
            ma = series.rolling(window=period).mean()
            std = series.rolling(window=period).std()
            mid = float(ma.iloc[-1])
            sd = float(std.iloc[-1])
            upper = mid + std_dev * sd
            lower = mid - std_dev * sd
            bb_info = {
                "bb_timeframe": tf,
                "bb_upper": round(upper, 2),
                "bb_middle": round(mid, 2),
                "bb_lower": round(lower, 2),
                "at_bb_lower": price <= lower,
                "at_bb_middle": price >= mid,
                "at_bb_upper": price >= upper,
            }

    return {
        "ok": True,
        **fx,
        **edge,
        **phase_info,
        **bb_info,
        "fair_source": source,
        "min_target_profit_pct": min_profit_pct,
        "min_consider_gap_krw": min_consider,
        "sell_premium_pct": fx_cfg.get("sell_premium_pct", 0.15),
        "fee_one_way_pct": fee_one_way * 100,
        "reference_krw_manual": manual,
        "fx_refresh_minutes": int(fx_cfg.get("fx_refresh_minutes", USDT_FX_REFRESH_MINUTES)),
    }

def _sync_usdt_strategy_params(fx: dict) -> None:
    """usdt_fx 설정을 USDT 전술 파라미터에 반영."""
    param_keys = (
        "reference_krw", "min_consider_gap_krw", "min_target_profit_pct",
        "sell_premium_pct", "fx_refresh_minutes", "fee_one_way_pct",
        "bb_period", "bb_std_dev",
    )
    synced = {k: fx[k] for k in param_keys if k in fx}
    for regime in ("BULL", "BEAR", "RANGE"):
        t = active_ticker_configs["USDT"]["tactics"][regime]
        for s in t.get("strategies", []):
            if s.get("name") in ("ReversePremium", "UsdtFxBollinger"):
                s["params"].update(synced)


@app.post("/api/usdt/fx-config")
def usdt_fx_config(payload: dict):
    """USDT 역프 매매 임계값·수동 기준환율 저장."""
    if "USDT" not in active_ticker_configs:
        return {"success": False, "error": "USDT not configured"}
    fx = active_ticker_configs["USDT"].setdefault("usdt_fx", {})
    for key in (
        "reference_krw", "min_consider_gap_krw", "min_target_profit_pct",
        "sell_premium_pct", "fx_refresh_minutes", "fee_one_way_pct",
        "bb_period", "bb_std_dev",
    ):
        if key in payload:
            fx[key] = payload[key]
    _sync_usdt_strategy_params(fx)
    save_persisted_configs()
    if engine_instance:
        engine_instance.update_ticker_config("USDT", active_ticker_configs["USDT"])
    return {"success": True, "usdt_fx": fx}


@app.post("/api/usdt/select-strategy")
def select_usdt_strategy(payload: dict):
    """USDT 전략 선택: auto | fixed_fusion | learned_mixed"""
    strategy = payload.get("strategy", "auto")
    if strategy not in ("auto", "fixed_fusion", "learned_mixed"):
        return {"success": False, "error": "Invalid strategy type"}
    if "USDT" not in active_ticker_configs:
        return {"success": False, "error": "USDT not configured"}

    cfg = active_ticker_configs["USDT"]
    cfg["selected_usdt_strategy"] = strategy
    if strategy == "auto":
        results = get_latest_results()
        if results and "KRW-USDT" in results:
            opt = results["KRW-USDT"].get("RANGE", {})
            if "fixed_fusion_strategy" in opt:
                pick, reason = _compute_usdt_auto_pick(opt)
                cfg["usdt_auto_pick"] = pick
                cfg["usdt_auto_pick_reason"] = reason
    _apply_usdt_strategy_selection("USDT")
    save_persisted_configs()

    if engine_instance and "USDT" in engine_instance._workers:
        worker = engine_instance._workers["USDT"]
        worker.config = cfg
        worker.switch_regime("RANGE", log_to_ui=True)
        worker.current_regime = "RANGE"

    return {
        "success": True,
        "selected_usdt_strategy": strategy,
        "resolved_usdt_strategy": cfg.get("resolved_usdt_strategy"),
        "usdt_auto_pick_reason": cfg.get("usdt_auto_pick_reason"),
    }


@app.post("/api/config/select-bear-strategy")
def select_bear_strategy(payload: dict):
    ticker = payload.get("ticker")
    strategy = payload.get("strategy")  # "auto", "custom_bear", or "mixed"
    if not ticker or not strategy:
        return {"success": False, "error": "Ticker or strategy missing"}
        
    if ticker not in active_ticker_configs:
        return {"success": False, "error": "Ticker not found"}
        
    if strategy not in ["auto", "custom_bear", "mixed"]:
        return {"success": False, "error": "Invalid strategy type"}
        
    active_ticker_configs[ticker]["selected_bear_strategy"] = strategy
    if not _apply_bear_strategy_selection(ticker):
        update_configs_with_optimized_params()
        _apply_bear_strategy_selection(ticker)
    save_persisted_configs()
    update_configs_and_apply_to_engine()
    labels = {
        "auto": "AI 자동(백테스트 우수 전략)",
        "custom_bear": "CustomBear(나만의기법)",
        "mixed": "Mixed(믹스기법)",
    }
    return {"success": True, "message": f"{ticker}의 하락장 전략이 {labels[strategy]}로 변경 및 적용되었습니다.",
            "resolved_bear_strategy": active_ticker_configs[ticker].get("resolved_bear_strategy")}

@app.post("/api/config/update")
def update_config(payload: dict):
    ticker = payload.get("ticker")
    if not ticker:
        return {"success": False, "error": "Ticker is missing"}
    
    if ticker not in active_ticker_configs:
        return {"success": False, "error": "Ticker not found"}
        
    is_active = active_ticker_configs[ticker].get("active", True)
    
    cur_regime = active_ticker_configs[ticker].get("current_regime", "BEAR")
    active_ticker_configs[ticker]["active"] = is_active
    active_ticker_configs[ticker]["tactics"][cur_regime] = {
        "logic": payload.get("logic", "AND"),
        "threshold": payload.get("threshold", 0.5),
        "strategies": payload.get("strategies", []),
        "risk": payload.get("risk", {"type": "None"})
    }
    
    save_persisted_configs()
    
    if engine_instance:
        success = engine_instance.update_ticker_config(ticker, active_ticker_configs[ticker])
        return {"success": success}
    return {"success": False, "error": "Engine not running"}

@app.post("/api/tickers")
def add_ticker(payload: dict):
    ticker = payload.get("ticker")
    if not ticker:
        return {"success": False, "error": "Ticker is missing"}
    
    ticker = ticker.upper().strip()
    if engine_instance:
        success = engine_instance.add_new_ticker(ticker)
        if success:
            save_persisted_configs()
        return {"success": success}
    return {"success": False, "error": "Engine not running"}

@app.post("/api/tickers/control")
def control_ticker(payload: dict):
    ticker = payload.get("ticker")
    action = payload.get("action")
    if not ticker or not action:
        return {"success": False, "error": "Parameters missing"}
    
    if ticker not in active_ticker_configs:
        return {"success": False, "error": "Ticker not found"}
        
    active_ticker_configs[ticker]["active"] = (action == "start")
    save_persisted_configs()
    
    if engine_instance:
        worker = engine_instance._workers.get(ticker)
        if worker:
            worker.trading_active = (action == "start")
            print(f"[Engine] {ticker} 매매 제어: {action}")
            return {"success": True}
        return {"success": False, "error": "Ticker worker not found"}
    return {"success": False, "error": "Engine not running"}

@app.post("/api/tickers/delete")
def delete_ticker(payload: dict):
    ticker = payload.get("ticker")
    if not ticker:
        return {"success": False, "error": "Ticker is missing"}
    if ticker in SUPPORTED_TICKERS:
        return {"success": False, "error": f"{ticker}는 시스템 기본 종목이라 삭제할 수 없습니다."}
    
    if ticker in active_ticker_configs:
        del active_ticker_configs[ticker]
        save_persisted_configs()
        
    if engine_instance:
        worker = engine_instance._workers.get(ticker)
        if worker:
            worker.stop()
            if ticker in engine_instance._workers:
                del engine_instance._workers[ticker]
            if ticker in engine_instance.dispatcher._workers:
                del engine_instance.dispatcher._workers[ticker]
                
            if hasattr(engine_instance, 'ws_listener') and engine_instance.ws_listener:
                markets = [f"KRW-{t}" for t in engine_instance._workers.keys()]
                engine_instance.ws_listener.update_markets(markets)
                
            print(f"[Engine] 종목 {ticker} 삭제 성공.")
            return {"success": True}
        return {"success": False, "error": "Ticker worker not found"}
    return {"success": False, "error": "Engine not running"}


# ══════════════════════════════════════════════════════════════
# 6. 비동기 큐 소비기 (스레드 안전 이벤트 브로커)
# ══════════════════════════════════════════════════════════════
async def ui_event_broadcaster():
    while True:
        try:
            event = await asyncio.to_thread(ui_event_queue.get)
            await manager.broadcast(event)
            ui_event_queue.task_done()
        except Exception:
            await asyncio.sleep(0.1)


# ══════════════════════════════════════════════════════════════
# 7. 프리미엄 트레이딩뷰 대시보드 마크업 (lightweight-charts 연동)
# ══════════════════════════════════════════════════════════════
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <title>NEXUS ALGO DASHBOARD - Multi-Timeframe Charts</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <!-- Google Fonts Outfit & JetBrains Mono 로드 -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
    
    <!-- TradingView Lightweight Charts CDN 로드 -->
    <script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    
    <style>
        :root {
            --bg-main: #030712;
            --bg-card: rgba(17, 24, 39, 0.75);
            --bg-sidebar: rgba(11, 17, 34, 0.85);
            --border-color: rgba(255, 255, 255, 0.08);
            --accent-blue: #3b82f6;
            --accent-green: #10b981;
            --accent-red: #ef4444;
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
            --glow-blue: 0 0 15px rgba(59, 130, 246, 0.45);
            --glow-green: 0 0 15px rgba(16, 185, 129, 0.45);
            --glow-red: 0 0 15px rgba(239, 68, 68, 0.45);
        }
        
        .bear-strategy-card {
            transition: all 0.2s ease;
        }
        .bear-strategy-card:hover {
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.2) !important;
            background: rgba(255, 255, 255, 0.05) !important;
        }
        .bear-strategy-card.selected {
            border-color: var(--accent-green) !important;
            background: rgba(16, 185, 129, 0.05) !important;
            box-shadow: 0 0 12px rgba(16, 185, 129, 0.15);
        }

        .status-indicator {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: var(--accent-red);
            display: inline-block;
            box-shadow: 0 0 5px rgba(239, 68, 68, 0.5);
            transition: all 0.3s ease;
        }
        .status-indicator.active {
            background-color: var(--accent-green);
            box-shadow: var(--glow-green);
        }
        .balance-display {
            background-color: rgba(59, 130, 246, 0.12);
            border: 1px solid rgba(59, 130, 246, 0.25);
            color: #60a5fa;
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 13px;
            font-weight: 700;
            box-shadow: 0 0 10px rgba(59, 130, 246, 0.05);
            font-family: 'JetBrains Mono', monospace;
        }

        * {
            box-sizing: border-box;
            scrollbar-width: thin;
            scrollbar-color: rgba(255, 255, 255, 0.1) transparent;
        }

        body {
            background-color: var(--bg-main);
            color: var(--text-primary);
            font-family: 'Outfit', -apple-system, BlinkMacSystemFont, sans-serif;
            margin: 0;
            padding: 24px;
            min-height: 100vh;
            display: flex;
            justify-content: center;
        }

        .container {
            width: 100%;
            max-width: 1650px;
            display: flex;
            flex-direction: column;
            gap: 20px;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 20px;
        }

        .logo-area {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .logo-icon {
            width: 12px;
            height: 12px;
            background-color: var(--accent-blue);
            border-radius: 50%;
            box-shadow: var(--glow-blue);
            animation: pulse-glow 2s infinite ease-in-out;
        }

        .logo-text {
            font-size: 24px;
            font-weight: 800;
            letter-spacing: -0.5px;
            background: linear-gradient(135deg, #ffffff 30%, #9ca3af 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .status-badge {
            background-color: rgba(239, 68, 68, 0.15);
            border: 1px solid rgba(239, 68, 68, 0.3);
            color: var(--accent-red);
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 13px;
            font-weight: 600;
            box-shadow: 0 0 10px rgba(239, 68, 68, 0.1);
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .status-badge.connected {
            background-color: rgba(16, 185, 129, 0.15);
            border: 1px solid rgba(16, 185, 129, 0.3);
            color: var(--accent-green);
            box-shadow: 0 0 10px rgba(16, 185, 129, 0.1);
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: currentColor;
            display: inline-block;
        }

        .status-badge-inline {
            padding: 4px 8px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 700;
            display: inline-flex;
            align-items: center;
            gap: 4px;
            transition: all 0.3s ease;
            margin-left: 8px;
        }
        .status-badge-inline.default {
            background-color: rgba(16, 185, 129, 0.15);
            border: 1px solid rgba(16, 185, 129, 0.3);
            color: var(--accent-green);
            box-shadow: 0 0 10px rgba(16, 185, 129, 0.1);
        }
        .status-badge-inline.custom {
            background-color: rgba(245, 158, 11, 0.15);
            border: 1px solid rgba(245, 158, 11, 0.3);
            color: #f59e0b;
            box-shadow: 0 0 10px rgba(245, 158, 11, 0.1);
        }

        .main-layout {
            display: grid;
            grid-template-columns: 320px 1fr 360px;
            gap: 20px;
            align-items: start;
        }

        /* 1350px 이하 해상도 대응 반응형 레이아웃 */
        @media (max-width: 1350px) {
            .main-layout {
                grid-template-columns: 300px 1fr;
            }
            .strategy-card {
                grid-column: span 2;
            }
        }

        /* 900px 이하 모바일/태블릿 대응 */
        @media (max-width: 900px) {
            .main-layout {
                grid-template-columns: 1fr;
            }
            .sidebar {
                grid-column: span 1;
            }
            .chart-card {
                grid-column: span 1;
            }
            .strategy-card {
                grid-column: span 1;
            }
        }

        .sidebar {
            display: flex;
            flex-direction: column;
            gap: 16px;
            max-height: calc(100vh - 88px);
            overflow-y: auto;
            overflow-x: hidden;
            padding-right: 6px;
            position: sticky;
            top: 72px;
            scrollbar-width: thin;
            scrollbar-color: rgba(255, 255, 255, 0.2) transparent;
        }

        .sidebar::-webkit-scrollbar {
            width: 6px;
        }

        .sidebar::-webkit-scrollbar-thumb {
            background: rgba(255, 255, 255, 0.18);
            border-radius: 4px;
        }

        .ticker-card {
            background: var(--bg-sidebar);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 16px;
            cursor: pointer;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: visible;
            flex-shrink: 0;
        }

        .sidebar-info-block {
            font-size: 10px;
            line-height: 1.5;
            padding: 6px 10px;
            border-radius: 8px;
            margin-bottom: 8px;
            overflow-wrap: anywhere;
            word-break: break-word;
            white-space: normal;
        }

        .sidebar-info-block--reason {
            color: var(--text-secondary);
            background: rgba(255, 255, 255, 0.02);
            border-left: 2px solid var(--accent-blue);
        }

        .sidebar-info-block--cycle {
            color: #a5b4fc;
            background: rgba(99, 102, 241, 0.06);
            border-left: 2px solid #6366f1;
        }

        .sidebar-info-block--opt {
            color: #86efac;
            background: rgba(34, 197, 94, 0.06);
            border-left: 2px solid var(--accent-green);
        }

        .ticker-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 4px;
            height: 100%;
            background: transparent;
            transition: background-color 0.2s ease;
        }

        .ticker-card:hover {
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.15);
            box-shadow: 0 8px 20px rgba(0,0,0,0.4);
        }

        .ticker-card.active {
            border-color: rgba(59, 130, 246, 0.4);
            background: rgba(59, 130, 246, 0.04);
            box-shadow: var(--glow-blue);
        }

        .ticker-card.active::before {
            background: var(--accent-blue);
        }

        .ticker-info {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }

        .ticker-symbol {
            font-size: 18px;
            font-weight: 700;
            color: #ffffff;
        }

        .strategy-badge {
            font-size: 10px;
            font-weight: 600;
            padding: 3px 8px;
            border-radius: 6px;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .badge-btc { color: #f59e0b; border-color: rgba(245, 158, 11, 0.3); }
        .badge-eth { color: #818cf8; border-color: rgba(129, 140, 248, 0.3); }
        .badge-xrp { color: #60a5fa; border-color: rgba(96, 165, 250, 0.3); }
        .badge-usdt { color: #22c55e; border-color: rgba(34, 197, 94, 0.35); }
        .badge-other { color: #a855f7; border-color: rgba(168, 85, 247, 0.3); }

        .price-display {
            font-size: 26px;
            font-weight: 800;
            color: var(--text-primary);
            margin-bottom: 12px;
            letter-spacing: -0.5px;
            transition: color 0.15s ease;
        }

        .price-display.flash-up {
            color: var(--accent-green) !important;
            text-shadow: 0 0 10px rgba(16, 185, 129, 0.3);
        }

        .price-display.flash-down {
            color: var(--accent-red) !important;
            text-shadow: 0 0 10px rgba(239, 68, 68, 0.3);
        }

        .strategy-details-table {
            width: 100%;
            font-size: 11px;
            color: var(--text-secondary);
            font-family: 'JetBrains Mono', monospace;
            border-collapse: collapse;
        }

        .strategy-details-table td {
            padding: 4px 0;
            border-bottom: 1px dashed rgba(255, 255, 255, 0.03);
        }

        .strategy-details-table td:last-child {
            text-align: right;
            color: #ffffff;
            font-weight: 600;
        }

        .chart-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            padding: 24px;
            display: flex;
            flex-direction: column;
            gap: 20px;
            min-height: 560px;
            box-shadow: 0 12px 28px rgba(0,0,0,0.3);
        }

        .chart-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
        }

        .chart-title {
            font-size: 20px;
            font-weight: 700;
            color: #ffffff;
        }

        .timeframe-bar {
            display: flex;
            gap: 4px;
            background: rgba(0, 0, 0, 0.3);
            padding: 4px;
            border-radius: 10px;
            border: 1px solid rgba(255, 255, 255, 0.05);
            flex-wrap: wrap;
        }

        .tf-btn {
            background: transparent;
            border: none;
            color: var(--text-secondary);
            padding: 6px 12px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 12px;
            font-weight: 600;
            font-family: inherit;
            transition: all 0.2s ease;
        }

        .tf-btn:hover {
            color: #ffffff;
            background-color: rgba(255, 255, 255, 0.05);
        }

        .tf-btn.active {
            background: var(--accent-blue);
            color: #ffffff;
            box-shadow: var(--glow-blue);
        }

        .chart-area {
            flex-grow: 1;
            border: 1px solid var(--border-color);
            border-radius: 12px;
            background-color: #040814;
            min-height: 400px;
            position: relative;
            overflow: hidden;
        }

        .chart-loader {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(4, 8, 20, 0.8);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 10;
            font-size: 14px;
            color: var(--text-secondary);
            backdrop-filter: blur(4px);
            display: none;
        }

        /* Strategy Config panel */
        .strategy-card {
            background: var(--bg-sidebar);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            padding: 20px;
            box-shadow: 0 12px 28px rgba(0,0,0,0.3);
            display: flex;
            flex-direction: column;
            gap: 16px;
            /* 고정 min-height를 유연하게 제거하여 짤림 해결 */
            min-height: auto;
            height: fit-content;
        }

        .strategy-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .strategy-panel-title {
            font-size: 15px;
            font-weight: 800;
            letter-spacing: -0.5px;
            color: #ffffff;
        }

        .strategy-tabs {
            display: flex;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 4px;
        }

        .strategy-tab {
            flex: 1;
            text-align: center;
            padding: 8px 0;
            cursor: pointer;
            font-size: 13px;
            font-weight: 600;
            color: var(--text-secondary);
            border-bottom: 2px solid transparent;
            transition: all 0.2s ease;
        }

        .strategy-tab:hover {
            color: #ffffff;
        }

        .strategy-tab.active {
            color: var(--accent-blue);
            border-bottom-color: var(--accent-blue);
        }

        .tab-content {
            display: flex;
            flex-direction: column;
            gap: 12px;
            /* 고정 height 대신 max-height를 사용하여 오버플로우 시 내부 스크롤 제공 */
            max-height: 480px;
            overflow-y: auto;
            padding-right: 4px;
        }

        .strategy-item-card {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.04);
            border-radius: 12px;
            padding: 12px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }

        .strategy-item-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .strategy-icon-title {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .strategy-icon {
            font-size: 9px;
            font-weight: 800;
            width: 32px;
            height: 32px;
            border-radius: 50%;
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: 'JetBrains Mono', monospace;
        }

        .icon-rsi { color: #f59e0b; background: rgba(245, 158, 11, 0.1); }
        .icon-bb { color: #10b981; background: rgba(16, 185, 129, 0.1); }
        .icon-macd { color: #818cf8; background: rgba(129, 140, 248, 0.1); }

        .strategy-label-desc {
            display: flex;
            flex-direction: column;
        }

        .strategy-item-name {
            font-size: 13px;
            font-weight: 700;
            color: #ffffff;
        }

        .strategy-item-desc {
            font-size: 10px;
            color: var(--text-secondary);
        }

        /* Toggle switch */
        .toggle-switch {
            position: relative;
            display: inline-block;
            width: 44px;
            height: 24px;
            flex-shrink: 0;
        }

        .toggle-switch input {
            opacity: 0;
            width: 0;
            height: 0;
        }

        .slider-round {
            position: absolute;
            cursor: pointer;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-color: rgba(255, 255, 255, 0.1);
            transition: .3s;
            border-radius: 24px;
        }

        .slider-round:before {
            position: absolute;
            content: "";
            height: 18px;
            width: 18px;
            left: 3px;
            bottom: 3px;
            background-color: white;
            transition: .3s;
            border-radius: 50%;
        }

        .toggle-switch input:checked + .slider-round {
            background-color: var(--accent-green);
        }

        .toggle-switch input:checked + .slider-round:before {
            transform: translateX(20px);
        }

        .strategy-item-controls {
            display: flex;
            flex-direction: column;
            gap: 8px;
            border-top: 1px dashed rgba(255, 255, 255, 0.04);
            padding-top: 8px;
            transition: all 0.2s ease;
        }

        .control-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            font-size: 11px;
            color: var(--text-secondary);
        }

        .control-label {
            width: 80px;
            flex-shrink: 0;
        }

        .param-slider {
            flex-grow: 1;
            height: 4px;
            background: rgba(255, 255, 255, 0.1);
            outline: none;
            border-radius: 2px;
            -webkit-appearance: none;
        }

        .param-slider::-webkit-slider-thumb {
            -webkit-appearance: none;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: var(--accent-blue);
            cursor: pointer;
            box-shadow: 0 0 5px rgba(59, 130, 246, 0.5);
            transition: transform 0.1s ease;
        }

        .param-slider::-webkit-slider-thumb:hover {
            transform: scale(1.2);
        }

        .weight-slider::-webkit-slider-thumb {
            background: #a855f7 !important;
            box-shadow: 0 0 5px rgba(168, 85, 247, 0.5) !important;
        }

        .param-val {
            width: 32px;
            text-align: right;
            font-family: 'JetBrains Mono', monospace;
            color: #ffffff;
            font-weight: 600;
        }

        .weight-row {
            border-top: 1px solid rgba(255, 255, 255, 0.03);
            padding-top: 4px;
            margin-top: 2px;
        }

        .logic-settings-card {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.04);
            border-radius: 12px;
            padding: 16px;
            display: flex;
            flex-direction: column;
            gap: 16px;
        }

        .control-group {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .control-label-large {
            font-size: 12px;
            font-weight: 700;
            color: #ffffff;
        }

        .logic-dropdown {
            background: #090d16;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            color: #ffffff;
            padding: 8px;
            font-size: 12px;
            outline: none;
            cursor: pointer;
            font-family: inherit;
            width: 100%;
        }

        .logic-desc {
            font-size: 10px;
            color: var(--text-secondary);
            font-style: italic;
        }

        .slider-val-row {
            display: flex;
            align-items: center;
            gap: 12px;
            width: 100%;
        }

        .apply-btn {
            background: var(--accent-blue);
            color: #ffffff;
            border: none;
            padding: 12px;
            border-radius: 12px;
            font-size: 13px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: var(--glow-blue);
            font-family: inherit;
            margin-top: auto;
            width: 100%;
            text-align: center;
        }

        .apply-btn:hover {
            transform: translateY(-1px);
            box-shadow: 0 0 20px rgba(59, 130, 246, 0.6);
        }

        .add-ticker-btn {
            background: rgba(255, 255, 255, 0.02);
            border: 1px dashed rgba(255, 255, 255, 0.15);
            border-radius: 16px;
            padding: 12px;
            text-align: center;
            font-size: 13px;
            font-weight: 600;
            color: var(--text-secondary);
            cursor: pointer;
            transition: all 0.2s ease;
            margin-top: 8px;
        }

        .add-ticker-btn:hover {
            background: rgba(255, 255, 255, 0.04);
            border-color: var(--accent-blue);
            color: #ffffff;
        }

        .log-section {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            padding: 24px;
            box-shadow: 0 12px 28px rgba(0,0,0,0.3);
        }

        .manual-trade-section {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            padding: 20px;
            box-shadow: 0 12px 28px rgba(0,0,0,0.3);
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        .log-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 12px;
        }

        .log-header h2 {
            margin: 0;
            font-size: 16px;
            font-weight: 700;
            color: #ffffff;
        }

        .log-box {
            background-color: #020617;
            border: 1px solid rgba(255, 255, 255, 0.03);
            border-radius: 12px;
            height: 200px;
            overflow-y: auto;
            padding: 16px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            display: flex;
            flex-direction: column;
            gap: 6px;
        }

        .log-entry {
            line-height: 1.6;
            word-break: break-all;
            padding: 2px 0;
        }

        .log-time {
            color: #6b7280;
            margin-right: 8px;
        }

        .log-ticker {
            font-weight: 700;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 10px;
            margin-right: 8px;
            text-transform: uppercase;
        }

        .ticker-log-btc { background: rgba(245, 158, 11, 0.15); color: #f59e0b; }
        .ticker-log-eth { background: rgba(129, 140, 248, 0.15); color: #818cf8; }
        .ticker-log-xrp { background: rgba(96, 165, 250, 0.15); color: #60a5fa; }
        .ticker-log-other { background: rgba(168, 85, 247, 0.15); color: #a855f7; }

        .log-buy { color: var(--accent-green); font-weight: 600; }
        .log-sell { color: var(--accent-red); font-weight: 600; }
        .log-system { color: var(--accent-blue); }
        .log-trade { color: var(--text-secondary); }

        @keyframes pulse-glow {
            0%, 100% {
                opacity: 0.6;
                transform: scale(0.95);
                box-shadow: 0 0 10px rgba(59, 130, 246, 0.3);
            }
            50% {
                opacity: 1;
                transform: scale(1.05);
                box-shadow: 0 0 20px rgba(59, 130, 246, 0.7);
            }
        }

        .backtest-section {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            padding: 20px;
            box-shadow: 0 12px 28px rgba(0,0,0,0.3);
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        .progress-container {
            width: 100%;
            background-color: rgba(255, 255, 255, 0.05);
            border-radius: 9999px;
            height: 12px;
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .progress-bar-fill {
            height: 100%;
            width: 0%;
            background: linear-gradient(90deg, var(--accent-blue) 0%, #10b981 100%);
            box-shadow: 0 0 10px rgba(59, 130, 246, 0.5);
            transition: width 0.4s ease;
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- 상단 헤더 -->
        <header>
            <div class="logo-area">
                <div class="logo-icon"></div>
                <div class="logo-text">빗썸 실시간 다중 타임프레임 자동매매 PoC</div>
            </div>
            <div style="display:flex; align-items:center; gap:16px; flex-wrap:wrap;">
                <div class="balance-display" id="total-balance">예수금: 10,000,000 KRW</div>
                <!-- 보유 자산 및 한국 시세 실시간 요약 카드 -->
                <div id="assets-summary" style="display:flex; align-items:center; gap:10px; font-size:12px; font-weight:700; color:var(--text-secondary); background:rgba(255,255,255,0.03); border:1px solid var(--border-color); padding:6px 14px; border-radius:9999px; font-family:'JetBrains Mono', monospace;">
                    보유 코인 조회 중...
                </div>
                <div id="kst-clock" style="font-family:'JetBrains Mono',monospace; font-size:12px; font-weight:700; color:var(--text-secondary); background:rgba(255,255,255,0.03); border:1px solid var(--border-color); padding:6px 12px; border-radius:9999px;">KST --:--:--</div>
                <div class="status-badge" id="ws-status">
                    <span class="status-dot"></span>
                    <span id="ws-status-text">소켓 연결 중...</span>
                </div>
            </div>
        </header>

        <!-- 메인 그리드 레이아웃 (3열) -->
        <div class="main-layout">
            <!-- 1열: 좌측 사이드바 (종목 카드) -->
            <div class="sidebar" id="ticker-sidebar">
                <div class="add-ticker-btn" id="add-ticker-btn" onclick="addNewTicker()">+ 종목 추가 (KRW 마켓)</div>
            </div>

            <!-- 2열: 중앙 (차트) -->
            <div class="chart-card">
                <div class="chart-header">
                    <div class="chart-title" id="chart-title">BTC/KRW 1분봉 차트</div>
                    <div class="timeframe-bar">
                        <button class="tf-btn" id="tf-1m" onclick="changeTimeframe('1m')">1분</button>
                        <button class="tf-btn" id="tf-3m" onclick="changeTimeframe('3m')">3분</button>
                        <button class="tf-btn" id="tf-5m" onclick="changeTimeframe('5m')">5분</button>
                        <button class="tf-btn" id="tf-10m" onclick="changeTimeframe('10m')">10분</button>
                        <button class="tf-btn active" id="tf-15m" onclick="changeTimeframe('15m')">15분</button>
                        <button class="tf-btn" id="tf-30m" onclick="changeTimeframe('30m')">30분</button>
                        <button class="tf-btn" id="tf-1h" onclick="changeTimeframe('1h')">1시간</button>
                        <button class="tf-btn" id="tf-4h" onclick="changeTimeframe('4h')">4시간</button>
                        <button class="tf-btn" id="tf-D" onclick="changeTimeframe('D')">일봉</button>
                        <button class="tf-btn" id="tf-W" onclick="changeTimeframe('W')">주봉</button>
                        <button class="tf-btn" id="tf-M" onclick="changeTimeframe('M')">월봉</button>
                    </div>
                </div>
                <div class="chart-area" id="chart-container">
                    <div class="chart-loader" id="chart-loader">로딩 중...</div>
                </div>
            </div>

            <!-- 3열: 우측 (전략 설정 패널) -->
            <div class="strategy-card glass-card" id="strategy-panel">
                <div class="strategy-header" style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span class="strategy-panel-title">전략 구성 및 설정</span>
                        <span id="config-status-badge"></span>
                    </div>
                    <span class="refresh-icon" onclick="resetStrategyUI()" style="cursor:pointer; opacity:0.7;" title="원래대로 설정 초기화">&#8635; 설정 복구</span>
                </div>

                <!-- USDT 환율+볼린저 융합 패널 (USDT 선택 시 우측 전략 패널 상단) -->
                <div id="usdt-reverse-panel" style="display:none; margin-bottom:12px; padding:14px; background:rgba(34,197,94,0.06); border:1px solid rgba(34,197,94,0.25); border-radius:12px;">
                    <h2 style="margin:0 0 8px 0; font-size:13px; font-weight:700; color:#86efac; display:flex; align-items:center; gap:8px;">
                        <span style="display:inline-block; width:6px; height:6px; background:#22c55e; border-radius:50%;"></span>
                        USDT 환율+볼린저 융합 (역프 할인 · BB 타점)
                    </h2>
                    <p style="font-size:10px; color:var(--text-secondary); margin:0 0 10px 0; line-height:1.5;">
                        <strong>기준환율</strong> 10분마다 API 갱신 · <strong>빗썸 USDT</strong> WebSocket 실시간 비교<br>
                        <strong>1차</strong> 환율차 ≥ 검토 gap(기본 40원) → <strong>2차</strong> 수수료·목표 + 15m BB 하단 → 매수
                    </p>
                    <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap:8px; margin-bottom:10px; font-size:11px;">
                        <div style="background:rgba(0,0,0,0.2); padding:8px; border-radius:8px;">
                            <div style="color:var(--text-secondary); font-size:9px;">기준 USD/KRW</div>
                            <div id="usdt-fair-krw" style="font-weight:700; font-size:14px;">-</div>
                            <div id="usdt-fair-source" style="font-size:8px; color:#94a3b8;">-</div>
                        </div>
                        <div style="background:rgba(0,0,0,0.2); padding:8px; border-radius:8px;">
                            <div style="color:var(--text-secondary); font-size:9px;">빗썸 USDT</div>
                            <div id="usdt-market-krw" style="font-weight:700; font-size:14px;">-</div>
                        </div>
                        <div style="background:rgba(0,0,0,0.2); padding:8px; border-radius:8px;">
                            <div style="color:var(--text-secondary); font-size:9px;">환율 차이</div>
                            <div id="usdt-reverse-pct" style="font-weight:700; font-size:14px; color:#86efac;">-</div>
                        </div>
                        <div style="background:rgba(0,0,0,0.2); padding:8px; border-radius:8px;">
                            <div style="color:var(--text-secondary); font-size:9px;">순이익 여유</div>
                            <div id="usdt-net-edge" style="font-weight:700; font-size:14px;">-</div>
                        </div>
                        <div style="background:rgba(0,0,0,0.2); padding:8px; border-radius:8px;">
                            <div style="color:var(--text-secondary); font-size:9px;">최적화 검토 gap</div>
                            <div id="usdt-opt-gap-krw" style="font-weight:700; font-size:14px; color:#fbbf24;">-</div>
                            <div id="usdt-opt-gap-source" style="font-size:8px; color:#94a3b8;">백테스트 후 갱신</div>
                        </div>
                        <div style="background:rgba(0,0,0,0.2); padding:8px; border-radius:8px;">
                            <div style="color:var(--text-secondary); font-size:9px;">볼린저 (15m)</div>
                            <div id="usdt-bb-levels" style="font-weight:700; font-size:11px;">-</div>
                        </div>
                        <div style="background:rgba(0,0,0,0.2); padding:8px; border-radius:8px;">
                            <div style="color:var(--text-secondary); font-size:9px;">신호</div>
                            <div id="usdt-signal-hint" style="font-weight:700; font-size:12px;">-</div>
                        </div>
                    </div>
                    <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:8px; font-size:10px;">
                        <label>검토 시작 ≥ <input type="number" id="usdt-min-gap" step="1" min="0" max="200" style="width:44px; margin:0 4px;"> 원
                        </label>
                        <label>목표 수익 <input type="number" id="usdt-min-profit" step="0.05" min="0" max="3" style="width:48px; margin:0 4px;"> %
                        </label>
                        <label>매도 프리미엄 ≥ <input type="number" id="usdt-sell-premium" step="0.05" min="0" max="3" style="width:48px; margin:0 4px;"> %
                        </label>
                        <label>편도 수수료 <input type="number" id="usdt-fee-oneway" step="0.01" min="0.1" max="1" style="width:48px; margin:0 4px;"> %
                        </label>
                        <label>수동 환율(0=자동) <input type="number" id="usdt-ref-krw" step="1" min="0" style="width:64px; margin-left:4px;"> 원
                        </label>
                    </div>
                    <div style="margin-top:10px; padding:10px; background:rgba(0,0,0,0.2); border-radius:8px; border:1px solid rgba(255,255,255,0.06);">
                        <div style="font-size:11px; font-weight:700; color:#fff; margin-bottom:6px;">전략 비교 · 매일 자동 선택</div>
                        <div id="usdt-strategy-auto-card" onclick="selectUsdtStrategy('auto')" style="background:rgba(59,130,246,0.08); border:1px solid rgba(59,130,246,0.35); border-radius:8px; padding:8px; margin-bottom:6px; cursor:pointer;">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <span style="font-size:11px; font-weight:700;">🤖 AI 자동 선택</span>
                                <span id="badge-usdt-auto" style="display:none; font-size:9px; background:#3b82f6; color:#fff; padding:2px 6px; border-radius:4px;">적용 중</span>
                            </div>
                            <div id="usdt-auto-pick-reason" style="font-size:9px; color:#a5b4fc; margin-top:4px;">-</div>
                        </div>
                        <div style="display:grid; grid-template-columns:1fr 1fr; gap:6px; font-size:9px;">
                            <div id="usdt-card-fixed" onclick="selectUsdtStrategy('fixed_fusion')" style="background:rgba(34,197,94,0.08); border:1px solid rgba(34,197,94,0.25); border-radius:8px; padding:8px; cursor:pointer;">
                                <div style="font-weight:700; margin-bottom:3px;">고정 융합 <span id="badge-usdt-fixed" style="display:none; font-size:8px; background:#22c55e; color:#fff; padding:1px 4px; border-radius:3px;">적용</span></div>
                                <div>1일: <span id="usdt-fixed-1d">-</span></div>
                                <div>3일: <span id="usdt-fixed-3d">-</span></div>
                                <div>1주: <span id="usdt-fixed-1w">-</span></div>
                                <div>3주: <span id="usdt-fixed-3w">-</span></div>
                            </div>
                            <div id="usdt-card-learned" onclick="selectUsdtStrategy('learned_mixed')" style="background:rgba(255,255,255,0.03); border:1px solid rgba(255,255,255,0.1); border-radius:8px; padding:8px; cursor:pointer;">
                                <div style="font-weight:700; margin-bottom:3px;">학습 3대지표 <span id="badge-usdt-learned" style="display:none; font-size:8px; background:#3b82f6; color:#fff; padding:1px 4px; border-radius:3px;">적용</span></div>
                                <div>1일: <span id="usdt-learned-1d">-</span></div>
                                <div>3일: <span id="usdt-learned-3d">-</span></div>
                                <div>1주: <span id="usdt-learned-1w">-</span></div>
                                <div>3주: <span id="usdt-learned-3w">-</span></div>
                            </div>
                        </div>
                    </div>
                    <button class="apply-btn" onclick="saveUsdtFxConfig()" style="margin-top:10px; padding:6px 14px; font-size:11px; width:auto;">USDT 융합 설정 저장</button>
                </div>
                
                <div class="strategy-tabs">
                    <div class="strategy-tab active" id="tab-strategies" onclick="switchStrategyTab('strategies')">개별 전략</div>
                    <div class="strategy-tab" id="tab-template" onclick="switchStrategyTab('template')">조합 논리 및 가중치</div>
                    <div class="strategy-tab" id="tab-risk" onclick="switchStrategyTab('risk')">리스크 관리 설정</div>
                </div>
                
                <div id="strategies-tab-content" class="tab-content">
                    <!-- RSI 전략 -->
                    <div class="strategy-item-card" id="card-item-rsi">
                        <div class="strategy-item-header">
                            <div class="strategy-icon-title">
                                <span class="strategy-icon icon-rsi">RSI</span>
                                <div class="strategy-label-desc">
                                    <span class="strategy-item-name">RSI</span>
                                    <span class="strategy-item-desc">상대강도지수 (Relative Strength Index)</span>
                                </div>
                            </div>
                            <label class="toggle-switch">
                                <input type="checkbox" id="toggle-rsi" onchange="toggleStrategy('RSI')">
                                <span class="slider-round"></span>
                            </label>
                        </div>
                        <div class="strategy-item-controls" id="controls-rsi">
                            <div class="control-row">
                                <span class="control-label">분석 기간</span>
                                <input type="range" class="param-slider" id="rsi-period" min="2" max="25" value="5" oninput="updateParamValue('rsi-period-val', this.value)">
                                <span class="param-val" id="rsi-period-val">5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">과매수 기준</span>
                                <input type="range" class="param-slider" id="rsi-overbought" min="50" max="95" value="65" oninput="updateParamValue('rsi-overbought-val', this.value)">
                                <span class="param-val" id="rsi-overbought-val">65</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">과매도 기준</span>
                                <input type="range" class="param-slider" id="rsi-oversold" min="5" max="50" value="35" oninput="updateParamValue('rsi-oversold-val', this.value)">
                                <span class="param-val" id="rsi-oversold-val">35</span>
                            </div>
                            <div class="control-row weight-row">
                                <span class="control-label" style="color:var(--accent-blue); font-weight:700;">전략 가중치</span>
                                <input type="range" class="param-slider weight-slider" id="rsi-weight" min="0.1" max="3.0" step="0.1" value="1.0" oninput="updateParamValue('rsi-weight-val', this.value)">
                                <span class="param-val" id="rsi-weight-val">1.0</span>
                            </div>
                        </div>
                    </div>

                    <!-- Bollinger Bands 전략 -->
                    <div class="strategy-item-card" id="card-item-bollinger">
                        <div class="strategy-item-header">
                            <div class="strategy-icon-title">
                                <span class="strategy-icon icon-bb">BB</span>
                                <div class="strategy-label-desc">
                                    <span class="strategy-item-name">Bollinger Bands</span>
                                    <span class="strategy-item-desc">볼린저 밴드 (Volatility Bandwidth)</span>
                                </div>
                            </div>
                            <label class="toggle-switch">
                                <input type="checkbox" id="toggle-bollinger" onchange="toggleStrategy('Bollinger')">
                                <span class="slider-round"></span>
                            </label>
                        </div>
                        <div class="strategy-item-controls" id="controls-bollinger">
                            <div class="control-row">
                                <span class="control-label">분석 기간</span>
                                <input type="range" class="param-slider" id="bollinger-period" min="2" max="25" value="5" oninput="updateParamValue('bollinger-period-val', this.value)">
                                <span class="param-val" id="bollinger-period-val">5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">표준편차</span>
                                <input type="range" class="param-slider" id="bollinger-stddev" min="0.5" max="3.5" step="0.1" value="1.5" oninput="updateParamValue('bollinger-stddev-val', this.value)">
                                <span class="param-val" id="bollinger-stddev-val">1.5</span>
                            </div>
                            <div class="control-row weight-row">
                                <span class="control-label" style="color:var(--accent-blue); font-weight:700;">전략 가중치</span>
                                <input type="range" class="param-slider weight-slider" id="bollinger-weight" min="0.1" max="3.0" step="0.1" value="1.0" oninput="updateParamValue('bollinger-weight-val', this.value)">
                                <span class="param-val" id="bollinger-weight-val">1.0</span>
                            </div>
                        </div>
                    </div>

                    <!-- MACD 전략 -->
                    <div class="strategy-item-card" id="card-item-macd">
                        <div class="strategy-item-header">
                            <div class="strategy-icon-title">
                                <span class="strategy-icon icon-macd">MACD</span>
                                <div class="strategy-label-desc">
                                    <span class="strategy-item-name">MACD</span>
                                    <span class="strategy-item-desc">이동평균 수렴확산 (Trend Momentum)</span>
                                </div>
                            </div>
                            <label class="toggle-switch">
                                <input type="checkbox" id="toggle-macd" onchange="toggleStrategy('MACD')">
                                <span class="slider-round"></span>
                            </label>
                        </div>
                        <div class="strategy-item-controls" id="controls-macd">
                            <div class="control-row">
                                <span class="control-label">단기 평균</span>
                                <input type="range" class="param-slider" id="macd-fast" min="2" max="15" value="5" oninput="updateParamValue('macd-fast-val', this.value)">
                                <span class="param-val" id="macd-fast-val">5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">장기 평균</span>
                                <input type="range" class="param-slider" id="macd-slow" min="5" max="30" value="10" oninput="updateParamValue('macd-slow-val', this.value)">
                                <span class="param-val" id="macd-slow-val">10</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">시그널 기간</span>
                                <input type="range" class="param-slider" id="macd-signal" min="2" max="10" value="3" oninput="updateParamValue('macd-signal-val', this.value)">
                                <span class="param-val" id="macd-signal-val">3</span>
                            </div>
                            <div class="control-row weight-row">
                                <span class="control-label" style="color:var(--accent-blue); font-weight:700;">전략 가중치</span>
                                <input type="range" class="param-slider weight-slider" id="macd-weight" min="0.1" max="3.0" step="0.1" value="1.0" oninput="updateParamValue('macd-weight-val', this.value)">
                                <span class="param-val" id="macd-weight-val">1.0</span>
                            </div>
                        </div>
                    </div>

                    <!-- CustomBear 전략 -->
                    <div class="strategy-item-card" id="card-item-custombear" style="display:none;">
                        <div class="strategy-item-header">
                            <div class="strategy-icon-title">
                                <span class="strategy-icon icon-rsi" style="color: #ef4444; background: rgba(239, 68, 68, 0.1);">BEAR</span>
                                <div class="strategy-label-desc">
                                    <span class="strategy-item-name">CustomBear (나만의 하락장 전략)</span>
                                    <span class="strategy-item-desc">급락 후 기술적 반등의 최적 순간 포착</span>
                                </div>
                            </div>
                            <label class="toggle-switch">
                                <input type="checkbox" id="toggle-custombear" onchange="toggleStrategy('CustomBear')">
                                <span class="slider-round"></span>
                            </label>
                        </div>
                        <div class="strategy-item-controls" id="controls-custombear">
                            <div class="control-row">
                                <span class="control-label">분석 캔들수(lookback)</span>
                                <input type="range" class="param-slider" id="custombear-lookback" min="3" max="20" value="8" oninput="updateParamValue('custombear-lookback-val', this.value)">
                                <span class="param-val" id="custombear-lookback-val">8</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">낙폭 임계치(drop_pct %)</span>
                                <input type="range" class="param-slider" id="custombear-drop_pct" min="1.0" max="15.0" step="0.5" value="5.0" oninput="updateParamValue('custombear-drop_pct-val', this.value)">
                                <span class="param-val" id="custombear-drop_pct-val">5.0</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">거래량 비율(volume_ratio)</span>
                                <input type="range" class="param-slider" id="custombear-volume_ratio" min="1.0" max="5.0" step="0.1" value="2.0" oninput="updateParamValue('custombear-volume_ratio-val', this.value)">
                                <span class="param-val" id="custombear-volume_ratio-val">2.0</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">트레일링스탑(trail_pct %)</span>
                                <input type="range" class="param-slider" id="custombear-trail_pct" min="0.5" max="5.0" step="0.1" value="1.5" oninput="updateParamValue('custombear-trail_pct-val', this.value)">
                                <span class="param-val" id="custombear-trail_pct-val">1.5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">손절 기준선(stop_loss %)</span>
                                <input type="range" class="param-slider" id="custombear-stop_loss" min="0.5" max="5.0" step="0.1" value="1.5" oninput="updateParamValue('custombear-stop_loss-val', this.value)">
                                <span class="param-val" id="custombear-stop_loss-val">1.5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">시간청산(time_cut 캔들수)</span>
                                <input type="range" class="param-slider" id="custombear-time_cut" min="2" max="48" value="24" oninput="updateParamValue('custombear-time_cut-val', this.value)">
                                <span class="param-val" id="custombear-time_cut-val">24</span>
                            </div>
                            <div class="control-row weight-row">
                                <span class="control-label" style="color:var(--accent-blue); font-weight:700;">전략 가중치</span>
                                <input type="range" class="param-slider weight-slider" id="custombear-weight" min="0.1" max="3.0" step="0.1" value="1.0" oninput="updateParamValue('custombear-weight-val', this.value)">
                                <span class="param-val" id="custombear-weight-val">1.0</span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Template/Logic 설정 콘텐츠 -->
                <div id="template-tab-content" class="tab-content" style="display:none;">
                    <div class="logic-settings-card">
                        <div class="control-group">
                            <label class="control-label-large">의사결정 조합 Logic</label>
                            <select id="logic-select" class="logic-dropdown" onchange="toggleThresholdDisplay(this.value)">
                                <option value="AND">AND (모든 전략이 같은 신호)</option>
                                <option value="OR">OR (하나라도 신호)</option>
                                <option value="VOTE">VOTE (단순 다수결)</option>
                                <option value="WEIGHTED_VOTE">WEIGHTED VOTE (가중치 투표)</option>
                            </select>
                            <div class="logic-desc" id="logic-desc-text">활성화된 모든 전략이 BUY이면 매수, 모두 SELL이면 매도합니다 (AND).</div>
                        </div>
                        
                        <div class="control-group" id="threshold-control-group" style="display:none; margin-top:10px;">
                            <label class="control-label-large">가중 합의 임계값 (Threshold)</label>
                            <div class="slider-val-row">
                                <input type="range" class="param-slider" id="weighted-threshold" min="0.1" max="1.0" step="0.05" value="0.5" oninput="updateParamValue('threshold-val', this.value)">
                                <span class="param-val" id="threshold-val" style="font-size:14px; width:45px;">0.5</span>
                            </div>
                            <div class="logic-desc">활성화된 전략들의 총 가중치 중 BUY 또는 SELL 신호 가중치 합의 비율이 이 값 이상이어야 신호를 도출합니다.</div>
                        </div>
                    </div>
                </div>

                <!-- Risk Settings 설정 콘텐츠 -->
                <div id="risk-tab-content" class="tab-content" style="display:none;">
                    <div class="logic-settings-card">
                        <!-- 일반 전략(Mixed/BULL/RANGE)용 리스크 UI -->
                        <div id="risk-standard-panel">
                            <div class="control-group">
                                <label class="control-label-large">리스크 관리 기법</label>
                                <select id="risk-type-select" class="logic-dropdown" onchange="toggleRiskParamsDisplay(this.value)">
                                    <option value="None">None (미적용)</option>
                                    <option value="StopLoss">StopLoss (고정 손절/익절)</option>
                                    <option value="TrailingStop">TrailingStop (트레일링 스탑)</option>
                                    <option value="AveragingDown">AveragingDown (물타기)</option>
                                </select>
                            </div>

                            <!-- StopLoss 파라미터 -->
                            <div class="control-group" id="risk-params-stoploss" style="display:none; margin-top:10px;">
                                <label class="control-label-large">Stop Loss (손절 비율 %)</label>
                                <div class="slider-val-row">
                                    <input type="range" class="param-slider" id="risk-stoploss-pct" min="0.5" max="10" step="0.5" value="3.0" oninput="updateParamValue('risk-stoploss-pct-val', this.value)">
                                    <span class="param-val" id="risk-stoploss-pct-val" style="font-size:14px; width:45px;">3.0</span>
                                </div>
                                <label class="control-label-large" style="margin-top: 10px;">Take Profit (익절 비율 %)</label>
                                <div class="slider-val-row">
                                    <input type="range" class="param-slider" id="risk-takeprofit-pct" min="1.0" max="20" step="0.5" value="6.0" oninput="updateParamValue('risk-takeprofit-pct-val', this.value)">
                                    <span class="param-val" id="risk-takeprofit-pct-val" style="font-size:14px; width:45px;">6.0</span>
                                </div>
                            </div>

                            <!-- TrailingStop 파라미터 -->
                            <div class="control-group" id="risk-params-trailing" style="display:none; margin-top:10px;">
                                <label class="control-label-large">Trailing Stop (하락폭 비율 %)</label>
                                <div class="slider-val-row">
                                    <input type="range" class="param-slider" id="risk-trail-pct" min="0.5" max="10" step="0.5" value="2.0" oninput="updateParamValue('risk-trail-pct-val', this.value)">
                                    <span class="param-val" id="risk-trail-pct-val" style="font-size:14px; width:45px;">2.0</span>
                                </div>
                            </div>

                            <!-- AveragingDown 파라미터 -->
                            <div class="control-group" id="risk-params-averaging" style="display:none; margin-top:10px;">
                                <label class="control-label-large">Drop Trigger (추가 매수 발동 %)</label>
                                <div class="slider-val-row">
                                    <input type="range" class="param-slider" id="risk-drop-trigger-pct" min="1.0" max="15" step="0.5" value="4.0" oninput="updateParamValue('risk-drop-trigger-val', this.value)">
                                    <span class="param-val" id="risk-drop-trigger-val" style="font-size:14px; width:45px;">4.0</span>
                                </div>
                                <label class="control-label-large" style="margin-top: 10px;">Max Add Count (최대 추가 매수 횟수)</label>
                                <div class="slider-val-row">
                                    <input type="range" class="param-slider" id="risk-max-add-count" min="1" max="5" step="1" value="3" oninput="updateParamValue('risk-max-add-count-val', this.value)">
                                    <span class="param-val" id="risk-max-add-count-val" style="font-size:14px; width:45px;">3</span>
                                </div>
                            </div>
                        </div>

                        <!-- CustomBear 전용: 실제 청산 규칙 표시 -->
                        <div id="risk-custombear-panel" style="display:none;">
                            <div class="control-group">
                                <label class="control-label-large">리스크 관리 기법</label>
                                <div style="padding:10px 12px; background:rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.25); border-radius:8px; font-size:13px; font-weight:700; color:#fca5a5;">
                                    CustomBear 내장 청산 (최적화값)
                                </div>
                            </div>
                            <div style="display:grid; gap:8px; margin-top:10px; font-size:12px; color:var(--text-secondary);">
                                <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:10px 12px;">
                                    <div style="color:#fff; font-weight:600; margin-bottom:4px;">트레일링 익절</div>
                                    <div>진입 후 최고가 대비 <strong id="risk-cb-trail-val" style="color:var(--accent-green);">-</strong> 하락 시 매도</div>
                                </div>
                                <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:10px 12px;">
                                    <div style="color:#fff; font-weight:600; margin-bottom:4px;">손절 (전략)</div>
                                    <div>진입가 대비 <strong id="risk-cb-stop-val" style="color:var(--accent-red);">-</strong> 이하 시 매도</div>
                                </div>
                                <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:10px 12px;">
                                    <div style="color:#fff; font-weight:600; margin-bottom:4px;">시간 청산</div>
                                    <div><strong id="risk-cb-timecut-val" style="color:var(--accent-blue);">-</strong> (<span id="risk-cb-timecut-hint">-</span>)</div>
                                </div>
                                <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:10px 12px;">
                                    <div style="color:#fff; font-weight:600; margin-bottom:4px;">보조 안전손절 (리스크매니저)</div>
                                    <div>진입가 대비 <strong id="risk-cb-backup-sl-val" style="color:var(--accent-red);">-</strong> 이하 강제 청산</div>
                                </div>
                            </div>
                            <div class="logic-desc" style="margin-top:10px; color:#94a3b8;">
                                고정 Take Profit(익절 %)은 사용하지 않습니다. 위 규칙으로 매수→매도가 결정됩니다.
                            </div>
                        </div>

                        <div class="logic-desc" id="risk-desc-text" style="margin-top:10px;">리스크 관리를 수행하지 않고 전략 신호만을 따릅니다.</div>
                        <div class="logic-desc" id="backtest-hold-summary" style="margin-top:8px; color:var(--accent-blue); font-weight:600;">
                            최적화 백테스트 평균 보유: -
                        </div>
                    </div>
                </div>
                
                <button class="apply-btn" onclick="applySettingsToEngine()">엔진에 변경 설정 적용</button>
            </div>
        </div>

        <!-- 수동 거래 데스크 패널 -->
        <!-- 백테스팅 및 파라미터 최적화 현황 패널 -->
        <div class="backtest-section" id="backtest-progress-section" style="margin-top: 10px; margin-bottom: 10px;">
            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
                <h2 style="margin:0; font-size:14px; font-weight:700; color:#fff; display:flex; align-items:center; gap:8px;">
                    <span style="display:inline-block; width:6px; height:6px; background-color:#10b981; border-radius:50%; box-shadow:var(--glow-green);"></span>
                    백그라운드 백테스팅 및 최적화 현황
                </h2>
                <div style="display:flex; gap:16px; align-items:center;">
                    <span style="font-size:12px; color:var(--text-secondary);">현재 진행 단계:</span>
                    <span id="bt-stage" style="font-size:12px; font-weight:bold; color:#fff; font-family:inherit;">
                        <span style="display:inline-block; width:8px; height:8px; background-color:#94a3b8; border-radius:50%; margin-right:6px;"></span>대기 중
                    </span>
                    <span id="bt-percent" style="font-size:12px; font-weight:bold; color:var(--accent-blue); font-family:'JetBrains Mono', monospace;">0.0%</span>
                </div>
            </div>
            
            <div style="display:flex; align-items:center; gap:16px; width:100%; margin-top:4px;">
                <div class="progress-container" style="flex:1;">
                    <div class="progress-bar-fill" id="bt-bar-fill" style="width:0%;"></div>
                </div>
                <button class="apply-btn" id="bt-run-btn" onclick="runBacktestNow()" style="margin:0; padding:8px 16px; border-radius:8px; font-size:12px; width:auto; height:36px; line-height:20px; font-weight:700; min-width:140px;">최적화 즉시 실행</button>
            </div>
            
            <div style="font-size:11px; color:var(--text-secondary); margin-top:-4px;" id="bt-message">
                대기 상태
            </div>
        </div>

        <!-- 하락장 극복 전략 선택 및 비교 모니터 패널 -->
        <div class="bear-strategy-compare-section" id="bear-compare-panel" style="margin-top: 10px; margin-bottom: 10px; padding:15px; background:rgba(255,255,255,0.02); border:1px solid rgba(255,255,255,0.05); border-radius:12px; display:none;">
            <h2 style="margin:0 0 12px 0; font-size:14px; font-weight:700; color:#fff; display:flex; align-items:center; gap:8px;">
                <span style="display:inline-block; width:6px; height:6px; background-color:var(--accent-red); border-radius:50%; box-shadow:var(--glow-red);"></span>
                하락장(BEAR) 최적화 전략 선택 및 성능 비교 데스크
            </h2>
            
            <div class="bear-strategy-cards-grid" style="display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:16px;">
                <!-- 카드 0: AI 자동 선택 (기본) -->
                <div class="bear-strategy-card" id="bear-card-auto" onclick="selectBearStrategy('auto')" style="background:rgba(59,130,246,0.08); border:1px solid rgba(59,130,246,0.35); border-radius:10px; padding:15px; cursor:pointer; grid-column: 1 / -1;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                        <span style="font-size:14px; font-weight:700; color:#fff;">🤖 AI 자동 선택 (기본)</span>
                        <span class="active-badge" id="badge-auto" style="display:none; background:var(--accent-blue); color:#fff; font-size:10px; padding:2px 6px; border-radius:4px; font-weight:bold;">적용 중</span>
                    </div>
                    <p style="font-size:11px; color:var(--text-secondary); margin:0; line-height:1.5;">
                        하락장(BEAR) 판정 시에만 적용됩니다. 백테스트 예상 수익을 <strong>1일 → 3일 → 1주 → 2주 → 1달 → 3달 → 6달</strong> 순으로 비교해 더 높은 전략을 매일 자동 선택합니다.
                        현재 AI 선택: <strong id="bear-resolved-label" style="color:var(--accent-green);">-</strong><br>
                        <span id="bear-auto-pick-reason" style="font-size:10px; color:#a5b4fc; margin-top:4px; display:inline-block;">-</span>
                    </p>
                </div>

                <!-- 카드 1: 기존 3대 지표 믹스 전략 -->
                <div class="bear-strategy-card" id="bear-card-mixed" onclick="selectBearStrategy('mixed')" style="background:rgba(255,255,255,0.03); border:1px solid rgba(255,255,255,0.1); border-radius:10px; padding:15px; cursor:pointer;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <span style="font-size:14px; font-weight:700; color:#fff;">기존 3대 지표 결합 (Mixed)</span>
                        <span class="active-badge" id="badge-mixed" style="display:none; background:var(--accent-blue); color:#fff; font-size:10px; padding:2px 6px; border-radius:4px; font-weight:bold;">적용 중</span>
                    </div>
                    <p style="font-size:11px; color:var(--text-secondary); margin:0 0 12px 0; line-height:1.4;">RSI, 볼린저밴드, MACD 보조지표의 최적 조합을 바탕으로 횡보/반등/추세를 필터링하여 이윤을 도출하는 기존 핵심 전략입니다.</p>
                    <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:8px 12px;">
                        <div style="font-size:11px; color:#94a3b8; font-weight:bold; margin-bottom:4px;">예상 기대 이윤 (복리 환산)</div>
                        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:6px; font-size:11px;">
                            <div>1일: <span id="mixed-profit-1d" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>3일: <span id="mixed-profit-3d" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>1주: <span id="mixed-profit-1w" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>2주: <span id="mixed-profit-2w" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>1개월: <span id="mixed-profit-1m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>3개월: <span id="mixed-profit-3m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>6개월: <span id="mixed-profit-6m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                        </div>
                    </div>
                </div>

                <!-- 카드 2: 나만의 하락장 롱테일 극복 전략 -->
                <div class="bear-strategy-card" id="bear-card-custom" onclick="selectBearStrategy('custom_bear')" style="background:rgba(255,255,255,0.03); border:1px solid rgba(255,255,255,0.1); border-radius:10px; padding:15px; cursor:pointer;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <span style="font-size:14px; font-weight:700; color:#fff; display:flex; align-items:center; gap:6px;">
                            나만의 하락장 전략 (Custom Bear) 🔥
                        </span>
                        <span class="active-badge" id="badge-custom" style="display:none; background:var(--accent-green); color:#fff; font-size:10px; padding:2px 6px; border-radius:4px; font-weight:bold;">적용 중</span>
                    </div>
                    <p style="font-size:11px; color:var(--text-secondary); margin:0 0 12px 0; line-height:1.4;">롱테일 우하향 패턴 극복을 위해, 급락 후 거래량이 실린 기술적 반등(Dead-cat)의 최적 순간만 발라먹는 전용 단독 전략입니다.</p>
                    <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:8px 12px;">
                        <div style="font-size:11px; color:#94a3b8; font-weight:bold; margin-bottom:4px;">예상 기대 이윤 (복리 환산)</div>
                        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:6px; font-size:11px;">
                            <div>1일: <span id="custom-profit-1d" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>3일: <span id="custom-profit-3d" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>1주: <span id="custom-profit-1w" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>2주: <span id="custom-profit-2w" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>1개월: <span id="custom-profit-1m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>3개월: <span id="custom-profit-3m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>6개월: <span id="custom-profit-6m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 백테스팅 과거 데이터 하락장/상승장/횡보장 종합 성과 모니터 -->
            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap:12px; margin-top:12px;">
                <!-- 1. 하락장 (BEAR) 성과 비교 카드 -->
                <div id="bear-regime-compare-table" style="background:rgba(0,0,0,0.2); border-radius:8px; padding:12px;">
                    <div style="font-size:12px; font-weight:700; color:#fff; margin-bottom:8px; display:flex; align-items:center; gap:6px;">
                        <span style="display:inline-block; width:6px; height:6px; background-color:var(--accent-red); border-radius:50%; box-shadow:var(--glow-red);"></span>
                        과거 9년 하락장(BEAR) 성과 비교
                    </div>
                    <table style="width:100%; border-collapse:collapse; font-size:11px; text-align:left; color:var(--text-secondary);">
                        <thead>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.1); color:#fff;">
                                <th style="padding:6px 4px;">지표</th>
                                <th style="padding:6px 4px;">기존 믹스</th>
                                <th style="padding:6px 4px; color:var(--accent-green);">Custom Bear</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">누적 수익률</td>
                                <td style="padding:6px 4px;" id="compare-mixed-ret">-</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-ret">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">Sharpe Ratio</td>
                                <td style="padding:6px 4px;" id="compare-mixed-sharpe">-</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-sharpe">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">최대 낙폭(MDD)</td>
                                <td style="padding:6px 4px;" id="compare-mixed-mdd">-</td>
                                <td style="padding:6px 4px; font-weight:bold;" id="compare-custom-mdd">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">거래 횟수/승률</td>
                                <td style="padding:6px 4px;" id="compare-mixed-trades">-</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-trades">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">평균 보유 기간</td>
                                <td style="padding:6px 4px;" id="compare-mixed-hold">-</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-hold">-</td>
                            </tr>
                            <tr>
                                <td style="padding:6px 4px;">최적 리스크 기법</td>
                                <td style="padding:6px 4px;" id="compare-mixed-risk">-</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-risk">-</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- 2. 상승장 (BULL) 성과 카드 -->
                <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:12px;">
                    <div style="font-size:12px; font-weight:700; color:#fff; margin-bottom:8px; display:flex; align-items:center; gap:6px;">
                        <span style="display:inline-block; width:6px; height:6px; background-color:var(--accent-green); border-radius:50%; box-shadow:var(--glow-green);"></span>
                        과거 9년 상승장(BULL) 최적화 성과
                    </div>
                    <table style="width:100%; border-collapse:collapse; font-size:11px; text-align:left; color:var(--text-secondary);">
                        <thead>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.1); color:#fff;">
                                <th style="padding:6px 4px;">지표</th>
                                <th style="padding:6px 4px; color:var(--accent-green);">최적화 믹스 전략</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">누적 수익률</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="bull-ret">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">Sharpe Ratio</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="bull-sharpe">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">최대 낙폭(MDD)</td>
                                <td style="padding:6px 4px; font-weight:bold;" id="bull-mdd">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">거래 횟수/승률</td>
                                <td style="padding:6px 4px;" id="bull-trades">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">평균 보유 기간</td>
                                <td style="padding:6px 4px;" id="bull-hold">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">최적 리스크 기법</td>
                                <td style="padding:6px 4px;" id="bull-risk">-</td>
                            </tr>
                            <tr>
                                <td style="padding:6px 4px;">기대 이윤 (3M/6M)</td>
                                <td style="padding:6px 4px;" id="bull-expected">-</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- 3. 횡보장 (RANGE) 성과 카드 -->
                <div style="background:rgba(0,0,0,0.2); border-radius:8px; padding:12px;">
                    <div style="font-size:12px; font-weight:700; color:#fff; margin-bottom:8px; display:flex; align-items:center; gap:6px;">
                        <span style="display:inline-block; width:6px; height:6px; background-color:var(--accent-blue); border-radius:50%; box-shadow:var(--glow-blue);"></span>
                        과거 9년 횡보장(RANGE) 최적화 성과
                    </div>
                    <table style="width:100%; border-collapse:collapse; font-size:11px; text-align:left; color:var(--text-secondary);">
                        <thead>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.1); color:#fff;">
                                <th style="padding:6px 4px;">지표</th>
                                <th style="padding:6px 4px; color:var(--accent-blue);">최적화 믹스 전략</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">누적 수익률</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-blue);" id="range-ret">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">Sharpe Ratio</td>
                                <td style="padding:6px 4px; font-weight:bold; color:var(--accent-blue);" id="range-sharpe">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">최대 낙폭(MDD)</td>
                                <td style="padding:6px 4px; font-weight:bold;" id="range-mdd">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">거래 횟수/승률</td>
                                <td style="padding:6px 4px;" id="range-trades">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">평균 보유 기간</td>
                                <td style="padding:6px 4px;" id="range-hold">-</td>
                            </tr>
                            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                                <td style="padding:6px 4px;">최적 리스크 기법</td>
                                <td style="padding:6px 4px;" id="range-risk">-</td>
                            </tr>
                            <tr>
                                <td style="padding:6px 4px;">기대 이윤 (3M/6M)</td>
                                <td style="padding:6px 4px;" id="range-expected">-</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="manual-trade-section" style="margin-top: 10px; margin-bottom: 10px;">
            <h2 style="margin:0; font-size:14px; font-weight:700; color:#fff; display:flex; align-items:center; gap:8px;">
                <span style="display:inline-block; width:6px; height:6px; background-color:var(--accent-blue); border-radius:50%; box-shadow:var(--glow-blue);"></span>
                수동 주문 데스크
            </h2>
            <div style="display:flex; align-items:center; gap:16px; flex-wrap:wrap; margin-top:8px;">
                <div style="display:flex; align-items:center; gap:8px;">
                    <span style="font-size:12px; color:var(--text-secondary);">주문 종목:</span>
                    <select id="manual-ticker-select" style="background:#090d16; border:1px solid var(--border-color); border-radius:8px; color:#fff; padding:6px 12px; font-size:12px; cursor:pointer; font-family:inherit; outline:none;">
                    </select>
                </div>
                <div style="display:flex; align-items:center; gap:8px;">
                    <span style="font-size:12px; color:var(--text-secondary);">주문 금액:</span>
                    <input type="number" id="manual-order-amount" value="5000" step="1000" min="500" style="background:#090d16; border:1px solid var(--border-color); border-radius:8px; color:#fff; padding:6px 12px; font-size:12px; width:120px; font-family:'JetBrains Mono', monospace; outline:none;" placeholder="금액 (KRW)">
                    <span style="font-size:11px; color:var(--text-secondary);">KRW (매수 시 적용)</span>
                </div>
                <div style="display:flex; gap:8px; margin-left:auto;">
                    <button class="apply-btn" onclick="submitManualOrder('BUY')" style="background:var(--accent-green); box-shadow:var(--glow-green); margin:0; padding:8px 16px; border-radius:8px; font-size:12px; width:auto; height:36px; line-height:20px;">즉시 매수(BUY)</button>
                    <button class="apply-btn" onclick="submitManualOrder('SELL')" style="background:var(--accent-red); box-shadow:var(--glow-red); margin:0; padding:8px 16px; border-radius:8px; font-size:12px; width:auto; height:36px; line-height:20px;">즉시 매도(SELL 전량)</button>
                </div>
            </div>
        </div>

        <!-- 하단: 로그 -->
        <div class="log-section">
            <div class="log-header" style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;">
                <div style="display:flex; align-items:center; gap:16px;">
                    <h2 style="margin:0; font-size:15px; font-weight:700; color:#fff;">종합 실시간 트레이딩 로그</h2>
                    <div style="display:flex; background:rgba(0,0,0,0.3); padding:2px; border-radius:8px; border:1px solid rgba(255,255,255,0.05);">
                        <button class="tf-btn active" id="log-tab-all" onclick="switchLogTab('all')" style="font-size:11px; padding:4px 8px; border-radius:6px;">전체 로그</button>
                        <button class="tf-btn" id="log-tab-trade" onclick="switchLogTab('trade')" style="font-size:11px; padding:4px 8px; border-radius:6px;">매매 체결만</button>
                    </div>
                </div>
                <button class="tf-btn" onclick="clearLogs()" style="padding: 2px 8px;">지우기</button>
            </div>
            <div class="log-box" id="log-container">
                <div class="log-entry is-system-log"><span class="log-time">[System]</span> 빗썸 다중 시세 트레이딩 엔진 가동 준비 중...</div>
            </div>
        </div>
    </div>

    <script>
        const wsStatusEl = document.getElementById('ws-status');
        const wsStatusText = document.getElementById('ws-status-text');
        const logContainer = document.getElementById('log-container');
        const chartTitleEl = document.getElementById('chart-title');
        const chartLoaderEl = document.getElementById('chart-loader');
        
        let activeTicker = 'BTC';
        let activeTimeframe = '15m';
        
        // 캐싱 저장소
        const candleCache = {};
        const lastPrices = { BTC: 0, ETH: 0, XRP: 0, USDT: 0 };
        let tickerConfigs = {};

        // 보유 코인 자산 정보 글로벌 객체 및 헤더 동적 요약 갱신 함수
        const userAssets = {};

        function updateHeaderAssetsSummary() {
            const summaryEl = document.getElementById('assets-summary');
            if (!summaryEl) return;
            
            let html = '';
            const tickers = Object.keys(userAssets).sort();
            
            // 보유 수량이 0보다 큰 활성 자산만 필터링
            const activeAssets = tickers.filter(t => userAssets[t] && userAssets[t].quantity > 0);
            
            if (activeAssets.length === 0) {
                html = '<span style="color:var(--text-secondary);">보유 코인: 없음</span>';
            } else {
                html = activeAssets.map(t => {
                    const asset = userAssets[t];
                    const price = lastPrices[t] || 0;
                    const priceStr = price > 0 ? `${price.toLocaleString()}원` : '시세 대기';
                    
                    let color = '#ffffff';
                    if (t === 'BTC') color = '#f59e0b';
                    else if (t === 'ETH') color = '#818cf8';
                    
                    return `<span style="color:${color}; font-weight:800;">${t}</span>: ${asset.quantity.toFixed(4)}개 (현재가: ${priceStr})`;
                }).join(' | ');
            }
            summaryEl.innerHTML = html;
        }
        
        // 타임프레임별 초단위 매핑
        const timeframeIntervals = {
            "1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800,
            "1h": 3600, "4h": 14400
        };

        // ══════════════════════════════════════════════════════════════
        // 한국 표준시(KST) 표시 — 빗썸 거래내역·차트와 동일 기준
        // ══════════════════════════════════════════════════════════════
        const KST_TIMEZONE = 'Asia/Seoul';
        const KST_OFFSET_SEC = 9 * 3600;

        function _toEpochMs(input) {
            if (input == null) return Date.now();
            if (typeof input === 'number') return input < 1e12 ? input * 1000 : input;
            const parsed = Date.parse(input);
            return Number.isNaN(parsed) ? Date.now() : parsed;
        }

        function formatKstTime(input) {
            return new Date(_toEpochMs(input)).toLocaleTimeString('ko-KR', {
                timeZone: KST_TIMEZONE,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false
            });
        }

        function formatKstDateTime(input) {
            return new Date(_toEpochMs(input)).toLocaleString('ko-KR', {
                timeZone: KST_TIMEZONE,
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false
            });
        }

        function formatKstChartLabel(unixSec) {
            const d = new Date(unixSec * 1000);
            const datePart = d.toLocaleDateString('ko-KR', {
                timeZone: KST_TIMEZONE,
                month: 'numeric',
                day: 'numeric'
            });
            const timePart = d.toLocaleTimeString('ko-KR', {
                timeZone: KST_TIMEZONE,
                hour: '2-digit',
                minute: '2-digit',
                hour12: false
            });
            return `${datePart} ${timePart}`;
        }

        function alignCandleTimeKst(timestampSec, intervalSec) {
            return Math.floor((timestampSec + KST_OFFSET_SEC) / intervalSec) * intervalSec - KST_OFFSET_SEC;
        }

        function updateKstClock() {
            const el = document.getElementById('kst-clock');
            if (el) el.innerText = `KST ${formatKstTime()}`;
        }
        updateKstClock();
        setInterval(updateKstClock, 1000);
        const USDT_FX_REFRESH_MS = 10 * 60 * 1000;
        setInterval(refreshUsdtFxStatus, USDT_FX_REFRESH_MS);

        // ══════════════════════════════════════════════════════════════
        // 로그 출력 헬퍼
        // ══════════════════════════════════════════════════════════════
        let activeLogTab = 'all';

        function switchLogTab(tab) {
            activeLogTab = tab;
            document.getElementById('log-tab-all').classList.remove('active');
            document.getElementById('log-tab-trade').classList.remove('active');
            document.getElementById(`log-tab-${tab}`).classList.add('active');
            
            const entries = logContainer.getElementsByClassName('log-entry');
            for (let entry of entries) {
                if (tab === 'all') {
                    entry.style.display = 'block';
                } else {
                    if (entry.classList.contains('is-trade-log')) {
                        entry.style.display = 'block';
                    } else {
                        entry.style.display = 'none';
                    }
                }
            }
        }

        function addLog(time, ticker, type, message) {
            const entry = document.createElement('div');
            const isTradeLog = ['BUY', 'SELL', 'ORDER'].includes(type);
            entry.className = `log-entry ${isTradeLog ? 'is-trade-log' : 'is-system-log'}`;
            
            if (activeLogTab === 'trade' && !isTradeLog) {
                entry.style.display = 'none';
            }
            
            let tickerBadge = '';
            if (ticker) {
                const isSystemTicker = ['BTC', 'ETH', 'XRP', 'USDT'].includes(ticker);
                const badgeClass = isSystemTicker ? `ticker-log-${ticker.toLowerCase()}` : 'ticker-log-other';
                tickerBadge = `<span class="log-ticker ${badgeClass}">${ticker}</span>`;
            }
            
            let typeClass = 'log-trade';
            if (type === 'BUY') typeClass = 'log-buy';
            else if (type === 'SELL') typeClass = 'log-sell';
            else if (type === 'SYSTEM') typeClass = 'log-system';
            
            entry.innerHTML = `<span class="log-time">[${time}]</span>${tickerBadge}<span class="${typeClass}">[${type}] ${message}</span>`;
            
            logContainer.appendChild(entry);
            logContainer.scrollTop = logContainer.scrollHeight;
            
            while (logContainer.childElementCount > 200) {
                logContainer.removeChild(logContainer.firstChild);
            }
        }

        function clearLogs() {
            logContainer.innerHTML = '';
            addLog(formatKstTime(), null, 'SYSTEM', '로그 콘솔이 초기화되었습니다.');
        }

        // ══════════════════════════════════════════════════════════════
        // TradingView Lightweight Charts 구성
        // ══════════════════════════════════════════════════════════════
        const chartElement = document.getElementById('chart-container');
        let chart = null;
        let candlestickSeries = null;
        
        try {
            if (typeof LightweightCharts !== 'undefined') {
                chart = LightweightCharts.createChart(chartElement, {
                    width: chartElement.clientWidth,
                    height: chartElement.clientHeight,
                    layout: {
                        background: { type: 'solid', color: '#040814' },
                        textColor: '#9ca3af',
                        fontSize: 11,
                        fontFamily: 'Outfit, sans-serif'
                    },
                    grid: {
                        vertLines: { color: 'rgba(255, 255, 255, 0.03)' },
                        horzLines: { color: 'rgba(255, 255, 255, 0.03)' },
                    },
                    localization: {
                        locale: 'ko-KR',
                        timeFormatter: (time) => {
                            if (typeof time === 'object' && time.year) {
                                return `${time.year}-${String(time.month).padStart(2, '0')}-${String(time.day).padStart(2, '0')}`;
                            }
                            return formatKstChartLabel(time);
                        },
                    },
                    timeScale: {
                        timeVisible: true,
                        secondsVisible: false,
                        borderColor: 'rgba(255, 255, 255, 0.08)'
                    },
                    rightPriceScale: {
                        borderColor: 'rgba(255, 255, 255, 0.08)'
                    }
                });

                candlestickSeries = chart.addCandlestickSeries({
                    upColor: '#10b981',
                    downColor: '#ef4444',
                    borderUpColor: '#10b981',
                    borderDownColor: '#ef4444',
                    wickUpColor: '#10b981',
                    wickDownColor: '#ef4444',
                });

                window.addEventListener('resize', () => {
                    if (chart) {
                        chart.resize(chartElement.clientWidth, chartElement.clientHeight);
                    }
                });
            } else {
                throw new Error("TradingView 라이브러리가 로드되지 않았습니다.");
            }
        } catch (e) {
            console.error("TradingView 차트 로딩 오류:", e);
            chartElement.innerHTML = `<div style="display:flex; justify-content:center; align-items:center; height:100%; color:#ef4444; font-size:13px; font-weight:600; padding:20px; text-align:center;">차트 라이브러리 초기화 실패. 실시간 덤프 로그를 확인하세요.</div>`;
        }

        // ══════════════════════════════════════════════════════════════
        // 과거 데이터 REST 로드
        // ══════════════════════════════════════════════════════════════
        async function loadHistoricalCandles(ticker, timeframe) {
            const cacheKey = `${ticker}_${timeframe}`;
            chartLoaderEl.style.display = 'flex';
            
            try {
                const response = await fetch(`/api/candles?market=KRW-${ticker}&timeframe=${timeframe}`);
                const data = await response.json();
                
                if (data.error) {
                    addLog(formatKstTime(), null, 'SYSTEM', `캔들 로드 실패: ${data.error}`);
                    chartLoaderEl.style.display = 'none';
                    return;
                }

                if (!Array.isArray(data) || data.length === 0) {
                    addLog(formatKstTime(), null, 'SYSTEM', `${ticker} ${timeframe} 캔들 데이터가 비어 있습니다.`);
                    chartLoaderEl.style.display = 'none';
                    return;
                }

                const validData = data.filter(c => c.time > 0 && c.open > 0 && c.close > 0);
                if (validData.length === 0) {
                    addLog(formatKstTime(), null, 'SYSTEM', `${ticker} ${timeframe} 유효한 캔들이 없습니다.`);
                    chartLoaderEl.style.display = 'none';
                    return;
                }
                
                candleCache[cacheKey] = validData;
                
                if (ticker === activeTicker && timeframe === activeTimeframe && candlestickSeries) {
                    candlestickSeries.setData(validData);
                    chart.timeScale().fitContent();
                    addLog(formatKstTime(), null, 'SYSTEM', `${ticker}의 ${timeframe} 과거 캔들 ${validData.length}개 렌더링 완료.`);
                }
            } catch (e) {
                console.error("Historical candles fetch error:", e);
                addLog(formatKstTime(), null, 'SYSTEM', `${ticker} ${timeframe} 과거 캔들 연동에 실패했습니다.`);
            } finally {
                chartLoaderEl.style.display = 'none';
            }
        }

        // ══════════════════════════════════════════════════════════════
        // 실시간 가격 데이터 캔들 병합 알고리즘
        // ══════════════════════════════════════════════════════════════
        function mergeRealtimePriceToCandle(ticker, price, timestampMs) {
            const cacheKey = `${ticker}_${activeTimeframe}`;
            const cache = candleCache[cacheKey];
            if (!cache || cache.length === 0) return;
            
            let candleTime;
            
            if (["D", "W", "M"].includes(activeTimeframe)) {
                const kstOffset = 9 * 60 * 60 * 1000;
                const kstTime = new Date(timestampMs + kstOffset);
                
                if (activeTimeframe === 'D') {
                    const year = kstTime.getUTCFullYear();
                    const month = kstTime.getUTCMonth();
                    const day = kstTime.getUTCDate();
                    candleTime = Date.UTC(year, month, day) / 1000;
                } else if (activeTimeframe === 'W') {
                    const dayOfWeek = kstTime.getUTCDay();
                    const daysToMonday = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
                    const mondayTime = new Date(kstTime.getTime() + daysToMonday * 24 * 60 * 60 * 1000);
                    const year = mondayTime.getUTCFullYear();
                    const month = mondayTime.getUTCMonth();
                    const day = mondayTime.getUTCDate();
                    candleTime = Date.UTC(year, month, day) / 1000;
                } else {
                    const year = kstTime.getUTCFullYear();
                    const month = kstTime.getUTCMonth();
                    candleTime = Date.UTC(year, month, 1) / 1000;
                }
            } else {
                const interval = timeframeIntervals[activeTimeframe] || 60;
                const timestampSec = Math.floor(timestampMs / 1000);
                candleTime = alignCandleTimeKst(timestampSec, interval);
            }
            
            const lastCandle = cache[cache.length - 1];
            let targetCandle;
            
            if (lastCandle.time === candleTime) {
                lastCandle.high = Math.max(lastCandle.high, price);
                lastCandle.low = Math.min(lastCandle.low, price);
                lastCandle.close = price;
                targetCandle = lastCandle;
            } else {
                const newCandle = {
                    time: candleTime,
                    open: price,
                    high: price,
                    low: price,
                    close: price
                };
                cache.push(newCandle);
                if (cache.length > 300) cache.shift();
                targetCandle = newCandle;
                addLog(formatKstTime(), ticker, 'SYSTEM', `${activeTimeframe} 신규 봉 형성 완료. (${formatKstDateTime(candleTime * 1000)})`);
            }
            
            if (ticker === activeTicker && candlestickSeries) {
                candlestickSeries.update(targetCandle);
            }
        }

        // ══════════════════════════════════════════════════════════════
        // 종목 동적 렌더링 및 UI 스위칭
        // ══════════════════════════════════════════════════════════════
        const SYSTEM_TICKERS = ['BTC', 'ETH', 'XRP', 'USDT'];
        const USDT_DEFAULT_GAP = 40;

        function upgradeUsdtSidebarBlock(ticker) {
            if (ticker !== 'USDT') return;
            if (document.getElementById('usdt-sidebar-fx-USDT')) return;
            const priceEl = document.getElementById('price-USDT');
            if (!priceEl) return;
            const fxBlock = document.createElement('div');
            fxBlock.id = 'usdt-sidebar-fx-USDT';
            fxBlock.style.cssText = 'background:rgba(34,197,94,0.08); border:1px solid rgba(34,197,94,0.2); border-radius:8px; padding:8px 10px; margin-bottom:8px; font-size:10px;';
            fxBlock.innerHTML = `
                <div style="font-weight:700; color:#86efac; margin-bottom:4px;">실시간 환율 (USD/KRW)</div>
                <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">기준</span><span id="usdt-sb-fair-USDT" style="font-family:'JetBrains Mono',monospace;">-</span></div>
                <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">USDT가</span><span id="usdt-sb-market-USDT" style="font-family:'JetBrains Mono',monospace;">-</span></div>
                <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">환율차</span><span id="usdt-sb-gap-USDT" style="font-family:'JetBrains Mono',monospace; font-weight:700;">-</span></div>
                <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">순이익 여유</span><span id="usdt-sb-edge-USDT" style="font-family:'JetBrains Mono',monospace;">-</span></div>
                <div style="display:flex; justify-content:space-between; margin-top:2px;"><span style="color:var(--text-secondary);">검토 gap</span><span id="usdt-sb-optgap-USDT" style="font-family:'JetBrains Mono',monospace; color:#fbbf24;">${USDT_DEFAULT_GAP}원</span></div>
            `;
            priceEl.parentNode.insertBefore(fxBlock, priceEl);
        }

        function ensureSystemTickerCards() {
            for (const ticker of SYSTEM_TICKERS) {
                const cfg = tickerConfigs[ticker] || {};
                const regime = cfg.current_regime || (ticker === 'USDT' ? 'RANGE' : 'BEAR');
                const tactic = cfg.tactics ? cfg.tactics[regime] : null;
                const logic = tactic ? tactic.logic : (ticker === 'USDT' ? 'OR' : 'AND');
                createTickerCardIfNotExist(ticker, logic, cfg.active !== false);
                upgradeUsdtSidebarBlock(ticker);
            }
        }

        function createTickerCardIfNotExist(ticker, logic, active = true) {
            if (document.getElementById(`card-${ticker}`)) return;
            
            const sidebar = document.getElementById('ticker-sidebar');
            const card = document.createElement('div');
            card.className = `ticker-card ${activeTicker === ticker ? 'active' : ''}`;
            card.id = `card-${ticker}`;
            card.onclick = (e) => {
                if (e.target.tagName !== 'BUTTON' && !e.target.classList.contains('delete-btn')) {
                    selectTicker(ticker);
                }
            };
            
            let badgeClass = 'badge-other';
            if (ticker === 'BTC') badgeClass = 'badge-btc';
            else if (ticker === 'ETH') badgeClass = 'badge-eth';
            else if (ticker === 'XRP') badgeClass = 'badge-xrp';
            else if (ticker === 'USDT') badgeClass = 'badge-usdt';
            
            const badgeLabel = ticker === 'USDT' ? '환율+BB 융합' : `${logic} 결합`;
            const canDelete = !SYSTEM_TICKERS.includes(ticker);
            const deleteBtnHtml = canDelete
                ? `<span class="delete-btn" onclick="event.stopPropagation(); deleteTicker('${ticker}')" style="color:var(--accent-red); font-size:14px; font-weight:bold; cursor:pointer; margin-left:4px; padding:2px 4px;" title="종목 삭제">&#10006;</span>`
                : '';
            const usdtFxSidebarHtml = ticker === 'USDT' ? `
                <div id="usdt-sidebar-fx-${ticker}" style="background:rgba(34,197,94,0.08); border:1px solid rgba(34,197,94,0.2); border-radius:8px; padding:8px 10px; margin-bottom:8px; font-size:10px;">
                    <div style="font-weight:700; color:#86efac; margin-bottom:4px;">실시간 환율 (USD/KRW)</div>
                    <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">기준</span><span id="usdt-sb-fair-${ticker}" style="font-family:'JetBrains Mono',monospace;">-</span></div>
                    <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">USDT가</span><span id="usdt-sb-market-${ticker}" style="font-family:'JetBrains Mono',monospace;">-</span></div>
                    <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">환율차</span><span id="usdt-sb-gap-${ticker}" style="font-family:'JetBrains Mono',monospace; font-weight:700;">-</span></div>
                    <div style="display:flex; justify-content:space-between;"><span style="color:var(--text-secondary);">순이익 여유</span><span id="usdt-sb-edge-${ticker}" style="font-family:'JetBrains Mono',monospace;">-</span></div>
                    <div style="display:flex; justify-content:space-between; margin-top:2px;"><span style="color:var(--text-secondary);">최적 gap</span><span id="usdt-sb-optgap-${ticker}" style="font-family:'JetBrains Mono',monospace; color:#fbbf24;">-</span></div>
                </div>
            ` : '';
            
            card.innerHTML = `
                <div class="ticker-header" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                    <span class="ticker-symbol" style="font-size:18px; font-weight:700; color:#fff;">${ticker}/KRW</span>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span class="status-indicator ${active ? 'active' : ''}" id="status-ind-${ticker}"></span>
                        <button class="control-btn" id="ctrl-btn-${ticker}" onclick="toggleTickerActive('${ticker}')" style="background:rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.1); color:#fff; border-radius:6px; font-size:10px; padding:3px 6px; cursor:pointer;">${active ? '정지' : '시작'}</button>
                        ${deleteBtnHtml}
                    </div>
                </div>
                <div class="ticker-info" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                    <span class="strategy-badge ${badgeClass}" id="badge-text-${ticker}">${badgeLabel}</span>
                    <div style="display:flex; align-items:center; gap:6px;">
                        <span id="regime-${ticker}" style="font-size:12px; font-weight:bold; color:var(--text-secondary);">진단 중...</span>
                        <select id="regime-select-${ticker}" onchange="changeRegimeOverride('${ticker}', this.value)" style="background:#090d16; border:1px solid rgba(255,255,255,0.1); border-radius:4px; color:#fff; font-size:10px; padding:2px; cursor:pointer; font-family:inherit; outline:none;">
                            <option value="AUTO">자동 판정</option>
                            <option value="BULL">상승장 고정</option>
                            <option value="BEAR">하락장 고정</option>
                            <option value="RANGE">횡보장 고정</option>
                        </select>
                        <select id="regime-duration-${ticker}" onchange="changeRegimeOverride('${ticker}', document.getElementById('regime-select-${ticker}').value)" style="display:none; background:#090d16; border:1px solid rgba(255,255,255,0.1); border-radius:4px; color:#fff; font-size:10px; padding:2px; cursor:pointer; font-family:inherit; outline:none;">
                            <option value="90" selected>3개월 고정</option>
                            <option value="30">1개월 고정</option>
                            <option value="7">1주일 고정</option>
                            <option value="0">무기한 고정</option>
                        </select>
                    </div>
                </div>
                <!-- 장세 오버라이드 만료 타이머 표시 영역 -->
                <div id="regime-expiry-container-${ticker}" style="display:none; font-size:10px; margin-bottom:8px; align-items:center; gap:4px; background:rgba(239,68,68,0.05); padding:4px 8px; border-radius:6px; border:1px solid rgba(239,68,68,0.1);">
                    <span style="background:rgba(239,68,68,0.15); color:var(--accent-red); padding:1px 4px; border-radius:4px; font-weight:600;" id="regime-expiry-badge-${ticker}">만료 예정</span>
                    <span id="regime-expiry-time-${ticker}" style="color:var(--text-secondary); font-family:'JetBrains Mono', monospace;">-</span>
                </div>
                
                <!-- AI 장세 판정 근거 설명 박스 -->
                <div id="regime-reason-${ticker}" class="sidebar-info-block sidebar-info-block--reason">
                    AI 분석 대기 중...
                </div>
                <div id="cycle-match-${ticker}" class="sidebar-info-block sidebar-info-block--cycle">
                    ${ticker === 'USDT' ? '400일 페그 회귀·사이클 각도 분석 대기 중...' : '사이클 회귀각 분석 대기 중...'}
                </div>
                <div id="bear-pattern-${ticker}" class="sidebar-info-block sidebar-info-block--bear" style="display:${ticker === 'USDT' ? 'none' : 'block'};">
                    월봉 하락 패턴·바닥선 분석 대기 중...
                </div>
                <div id="optimized-default-${ticker}" class="sidebar-info-block sidebar-info-block--opt">
                    일일 최적화 디폴트 전략 대기 중...
                </div>

                ${usdtFxSidebarHtml}

                <div class="price-display" id="price-${ticker}">- KRW</div>
                
                <!-- 실시간 보유 수량 및 수익률 원화 영역 -->
                <div class="position-info-box" style="background:rgba(0,0,0,0.25); border-radius:10px; padding:8px 12px; margin-top:8px; margin-bottom:8px; font-size:11px; display:flex; flex-direction:column; gap:4px; border: 1px solid rgba(255,255,255,0.03);">
                    <div style="display:flex; justify-content:space-between;">
                        <span style="color:var(--text-secondary);">수량</span>
                        <span id="pos-qty-${ticker}" style="font-family:'JetBrains Mono', monospace; font-weight:600;">0.00000000 ${ticker}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                        <span style="color:var(--text-secondary);">평단가</span>
                        <span id="pos-avg-${ticker}" style="font-family:'JetBrains Mono', monospace; font-weight:600;">0 KRW</span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                        <span style="color:var(--text-secondary);">평가금</span>
                        <span id="pos-val-${ticker}" style="font-family:'JetBrains Mono', monospace; font-weight:600;">0 KRW</span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                        <span style="color:var(--text-secondary);">수익률</span>
                        <span id="pos-pnl-${ticker}" style="font-family:'JetBrains Mono', monospace; font-weight:800;">0.00%</span>
                    </div>
                </div>
                
                <table class="strategy-details-table" id="table-${ticker}"></table>
            `;
            
            const addBtn = document.getElementById('add-ticker-btn');
            sidebar.insertBefore(card, addBtn);
        }

        function selectTicker(ticker) {
            if (activeTicker === ticker) return;
            
            const oldCard = document.getElementById(`card-${activeTicker}`);
            if (oldCard) oldCard.classList.remove('active');
            
            const newCard = document.getElementById(`card-${ticker}`);
            if (newCard) newCard.classList.add('active');
            
            activeTicker = ticker;
            updateChartInfo();
            
            // 시리즈 재생성으로 타입 크래시 회방
            recreateSeries();
            
            // 캐시 로드 혹은 REST 호출
            const cacheKey = `${ticker}_${activeTimeframe}`;
            if (candleCache[cacheKey] && candleCache[cacheKey].length > 0) {
                candlestickSeries.setData(candleCache[cacheKey]);
                chart.timeScale().fitContent();
            } else {
                loadHistoricalCandles(activeTicker, activeTimeframe);
            }
            
            // UI 설정 동기화
            loadConfigForTicker(ticker);
            syncUsdtPanel(ticker);
        }

        function syncUsdtPanel(ticker) {
            const usdtPanel = document.getElementById('usdt-reverse-panel');
            const bearPanel = document.getElementById('bear-compare-panel');
            if (!usdtPanel) return;
            const isUsdt = ticker === 'USDT';
            usdtPanel.style.display = isUsdt ? 'block' : 'none';
            if (isUsdt && bearPanel) bearPanel.style.display = 'none';
            if (isUsdt) {
                const fx = tickerConfigs.USDT?.usdt_fx || {};
                const gapEl = document.getElementById('usdt-min-gap');
                const minEl = document.getElementById('usdt-min-profit');
                const sellEl = document.getElementById('usdt-sell-premium');
                const feeEl = document.getElementById('usdt-fee-oneway');
                const refEl = document.getElementById('usdt-ref-krw');
                if (gapEl) gapEl.value = fx.min_consider_gap_krw ?? USDT_DEFAULT_GAP;
                if (minEl) minEl.value = fx.min_target_profit_pct ?? 0.2;
                if (sellEl) sellEl.value = fx.sell_premium_pct ?? 0.15;
                if (feeEl) feeEl.value = fx.fee_one_way_pct ?? 0.25;
                if (refEl) refEl.value = fx.reference_krw ?? 0;
                updateUsdtStrategyCompare();
                refreshUsdtFxStatus();
            }
        }

        function updateUsdtStrategyCompare() {
            const cfg = tickerConfigs.USDT || {};
            const cmp = cfg.usdt_strategy_compare || {};
            const fixed = cmp.fixed_fusion || {};
            const learned = cmp.learned_mixed || {};
            const fmt = (v) => (v != null ? `${v >= 0 ? '+' : ''}${Number(v).toFixed(2)}%` : '-');
            ['1d', '3d', '1w', '3w'].forEach(h => {
                const fk = `expected_profit_${h}`;
                const fe = document.getElementById(`usdt-fixed-${h}`);
                const le = document.getElementById(`usdt-learned-${h}`);
                if (fe) fe.innerText = fmt(fixed[fk]);
                if (le) le.innerText = fmt(learned[fk]);
            });
            const reasonEl = document.getElementById('usdt-auto-pick-reason');
            if (reasonEl) {
                reasonEl.innerText = cfg.usdt_auto_pick_reason
                    ? `선택 근거: ${cfg.usdt_auto_pick_reason}`
                    : '최적화 실행 후 자동 비교됩니다.';
            }
            const selected = cfg.selected_usdt_strategy || 'auto';
            const resolved = cfg.resolved_usdt_strategy || 'fixed_fusion';
            const badges = {
                auto: document.getElementById('badge-usdt-auto'),
                fixed: document.getElementById('badge-usdt-fixed'),
                learned: document.getElementById('badge-usdt-learned'),
            };
            if (badges.auto) badges.auto.style.display = selected === 'auto' ? 'inline' : 'none';
            if (badges.fixed) badges.fixed.style.display = (selected === 'fixed_fusion' || (selected === 'auto' && resolved === 'fixed_fusion')) ? 'inline' : 'none';
            if (badges.learned) badges.learned.style.display = (selected === 'learned_mixed' || (selected === 'auto' && resolved === 'learned_mixed')) ? 'inline' : 'none';
            updateUsdtOptGapDisplay();
        }

        async function selectUsdtStrategy(strategy) {
            try {
                const res = await fetch('/api/usdt/select-strategy', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ strategy })
                });
                const data = await res.json();
                if (data.success) {
                    if (tickerConfigs.USDT) {
                        tickerConfigs.USDT.selected_usdt_strategy = strategy;
                        tickerConfigs.USDT.resolved_usdt_strategy = data.resolved_usdt_strategy;
                        if (data.usdt_auto_pick_reason) tickerConfigs.USDT.usdt_auto_pick_reason = data.usdt_auto_pick_reason;
                    }
                    updateUsdtStrategyCompare();
                } else alert(data.error || '전략 선택 실패');
            } catch (e) { console.error(e); }
        }

        function updateUsdtOptGapDisplay() {
            const cfg = tickerConfigs.USDT || {};
            const cmp = cfg.usdt_strategy_compare || {};
            const resolved = cfg.resolved_usdt_strategy || cfg.usdt_auto_pick || 'fixed_fusion';
            const variant = resolved === 'learned_mixed' ? cmp.learned_mixed : cmp.fixed_fusion;
            const optGap = variant?.best_gap_krw ?? cfg.usdt_fx?.min_consider_gap_krw ?? USDT_DEFAULT_GAP;
            const hasBt = variant?.best_gap_krw != null;
            const optEl = document.getElementById('usdt-opt-gap-krw');
            const srcEl = document.getElementById('usdt-opt-gap-source');
            const sbOpt = document.getElementById('usdt-sb-optgap-USDT');
            const gapText = `${optGap}원`;
            if (optEl) optEl.innerText = gapText;
            if (srcEl) srcEl.innerText = hasBt ? '백테스트 최적화' : '설정값 (최적화 전)';
            if (sbOpt) sbOpt.innerText = gapText;
        }

        function updateUsdtSidebarFx(d) {
            const fairEl = document.getElementById('usdt-sb-fair-USDT');
            const mktEl = document.getElementById('usdt-sb-market-USDT');
            const gapEl = document.getElementById('usdt-sb-gap-USDT');
            const edgeEl = document.getElementById('usdt-sb-edge-USDT');
            if (fairEl) fairEl.innerText = d.fair_krw ? `${Math.round(d.fair_krw).toLocaleString()}원` : '-';
            if (mktEl) mktEl.innerText = d.usdt_krw ? `${d.usdt_krw.toLocaleString()}원` : '-';
            if (gapEl) {
                const gap = d.gap_krw ?? 0;
                const minGap = d.min_consider_gap_krw ?? USDT_DEFAULT_GAP;
                gapEl.innerText = gap > 0 ? `${gap.toFixed(0)}원↓` : `+${Math.abs(gap).toFixed(0)}원↑`;
                gapEl.style.color = gap >= minGap ? '#86efac' : '#94a3b8';
            }
            if (edgeEl) {
                const net = d.net_edge_krw;
                if (net != null) {
                    edgeEl.innerText = `${net >= 0 ? '+' : ''}${net.toFixed(1)}원`;
                    edgeEl.style.color = net > 0 ? '#86efac' : '#f87171';
                } else edgeEl.innerText = '-';
            }
            updateUsdtOptGapDisplay();
        }

        function updateUsdtFxPanel(d) {
            const fairEl = document.getElementById('usdt-fair-krw');
            const mktEl = document.getElementById('usdt-market-krw');
            const revEl = document.getElementById('usdt-reverse-pct');
            const edgeEl = document.getElementById('usdt-net-edge');
            const bbEl = document.getElementById('usdt-bb-levels');
            const hintEl = document.getElementById('usdt-signal-hint');
            const srcEl = document.getElementById('usdt-fair-source');
            if (fairEl) fairEl.innerText = d.fair_krw ? `${d.fair_krw.toLocaleString()}원` : '-';
            if (srcEl) {
                const mins = d.fx_refresh_minutes || 10;
                if (d.fair_source === 'manual') srcEl.innerText = '수동 입력';
                else if (d.fair_source === 'api' || (d.fair_source || '').includes('api')) srcEl.innerText = `${mins}분마다 API 갱신`;
                else srcEl.innerText = '기본값';
            }
            if (mktEl) mktEl.innerText = d.usdt_krw ? `${d.usdt_krw.toLocaleString()}원` : '-';
            if (revEl) {
                const gap = d.gap_krw ?? 0;
                const minGap = d.min_consider_gap_krw ?? USDT_DEFAULT_GAP;
                revEl.innerText = gap > 0 ? `${gap.toFixed(0)}원 (할인)` : `+${Math.abs(gap).toFixed(0)}원 (프리미엄)`;
                revEl.style.color = gap >= minGap ? '#86efac' : '#94a3b8';
            }
            if (bbEl) {
                if (d.bb_lower != null) {
                    const tag = d.at_bb_lower ? '🟢하단' : (d.at_bb_upper ? '🔴상단' : '⚪중간');
                    bbEl.innerText = `${tag} L:${d.bb_lower} M:${d.bb_middle} U:${d.bb_upper}`;
                } else bbEl.innerText = '캔들 로딩 중';
            }
            if (edgeEl) {
                const net = d.net_edge_krw;
                if (net != null && d.usdt_krw) {
                    edgeEl.innerText = `${net >= 0 ? '+' : ''}${net.toFixed(1)}원 (${(d.net_edge_pct || 0).toFixed(2)}%)`;
                    edgeEl.style.color = net > 0 ? '#86efac' : '#f87171';
                } else edgeEl.innerText = '-';
            }
            if (hintEl) {
                const minGap = d.min_consider_gap_krw ?? USDT_DEFAULT_GAP;
                if (d.phase === 'idle') {
                    const need = d.gap_to_consider_krw ?? Math.max(0, minGap - (d.gap_krw || 0));
                    hintEl.innerText = `⚫ 관심 구역 밖 (${need.toFixed(0)}원 더 필요)`;
                } else if (d.phase === 'consider') {
                    hintEl.innerText = '🟡 검토 중 (수수료·목표 미달)';
                } else if (d.is_buy_profitable && d.at_bb_lower) {
                    hintEl.innerText = '🟢 매수 타점 (환율+BB하단)';
                } else if (d.is_buy_profitable) {
                    hintEl.innerText = '🟡 대기 (BB 하단 터치 대기)';
                } else {
                    hintEl.innerText = '⚪ 대기';
                }
            }
            updateUsdtSidebarFx(d);
            updateUsdtOptGapDisplay();
        }

        async function refreshUsdtFxStatus() {
            try {
                const res = await fetch('/api/usdt/fx-status');
                const d = await res.json();
                if (!d.ok) return;
                updateUsdtFxPanel(d);
            } catch (e) { console.error(e); }
        }

        async function saveUsdtFxConfig() {
            const payload = {
                min_consider_gap_krw: parseFloat(document.getElementById('usdt-min-gap')?.value || USDT_DEFAULT_GAP),
                min_target_profit_pct: parseFloat(document.getElementById('usdt-min-profit')?.value || 0.2),
                sell_premium_pct: parseFloat(document.getElementById('usdt-sell-premium')?.value || 0.15),
                fee_one_way_pct: parseFloat(document.getElementById('usdt-fee-oneway')?.value || 0.25),
                reference_krw: parseFloat(document.getElementById('usdt-ref-krw')?.value || 0),
            };
            try {
                const res = await fetch('/api/usdt/fx-config', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (data.success) {
                    if (tickerConfigs.USDT) tickerConfigs.USDT.usdt_fx = data.usdt_fx;
                    alert('USDT 융합 설정이 저장·적용되었습니다.');
                    refreshUsdtFxStatus();
                } else alert('저장 실패: ' + (data.error || ''));
            } catch (e) {
                alert('저장 중 오류가 발생했습니다.');
            }
        }

        function recreateSeries() {
            if (!chart) return;
            if (candlestickSeries) {
                chart.removeSeries(candlestickSeries);
            }
            candlestickSeries = chart.addCandlestickSeries({
                upColor: '#10b981',
                downColor: '#ef4444',
                borderUpColor: '#10b981',
                borderDownColor: '#ef4444',
                wickUpColor: '#10b981',
                wickDownColor: '#ef4444',
            });
        }

        function changeTimeframe(timeframe) {
            if (activeTimeframe === timeframe) return;
            
            document.getElementById(`tf-${activeTimeframe}`).classList.remove('active');
            document.getElementById(`tf-${timeframe}`).classList.add('active');
            
            activeTimeframe = timeframe;
            updateChartInfo();
            
            if (chart) {
                if (["D", "W", "M"].includes(timeframe)) {
                    chart.applyOptions({
                        timeScale: { secondsVisible: false, timeVisible: false }
                    });
                } else {
                    chart.applyOptions({
                        timeScale: { secondsVisible: false, timeVisible: true }
                    });
                }
            }

            recreateSeries();

            const cacheKey = `${activeTicker}_${timeframe}`;
            if (candleCache[cacheKey] && candleCache[cacheKey].length > 0) {
                candlestickSeries.setData(candleCache[cacheKey]);
                chart.timeScale().fitContent();
            } else {
                loadHistoricalCandles(activeTicker, activeTimeframe);
            }
        }

        function updateChartInfo() {
            const tfText = document.getElementById(`tf-${activeTimeframe}`).innerText;
            chartTitleEl.innerText = `${activeTicker}/KRW 실시간 ${tfText} 차트 (KST)`;
        }

        // ══════════════════════════════════════════════════════════════
        // UI 설정 제어 및 로딩/갱신
        // ══════════════════════════════════════════════════════════════
        function switchStrategyTab(tab) {
            document.getElementById('tab-strategies').classList.remove('active');
            document.getElementById('tab-template').classList.remove('active');
            document.getElementById('tab-risk').classList.remove('active');
            document.getElementById('strategies-tab-content').style.display = 'none';
            document.getElementById('template-tab-content').style.display = 'none';
            document.getElementById('risk-tab-content').style.display = 'none';
            
            document.getElementById(`tab-${tab}`).classList.add('active');
            document.getElementById(`${tab}-tab-content`).style.display = 'flex';
        }

        function toggleRiskParamsDisplay(riskType) {
            const desc = document.getElementById('risk-desc-text');
            const stoplossDiv = document.getElementById('risk-params-stoploss');
            const trailingDiv = document.getElementById('risk-params-trailing');
            const averagingDiv = document.getElementById('risk-params-averaging');

            stoplossDiv.style.display = 'none';
            trailingDiv.style.display = 'none';
            averagingDiv.style.display = 'none';

            if (riskType === 'None') {
                desc.innerText = '리스크 관리를 수행하지 않고 전략 신호만을 따릅니다.';
            } else if (riskType === 'StopLoss') {
                desc.innerText = '설정한 손절선 이하 또는 익절선 이상 도달 시 강제로 포지션을 전량 매도 청산합니다.';
                stoplossDiv.style.display = 'flex';
            } else if (riskType === 'TrailingStop') {
                desc.innerText = '포지션 진입 후 최고가 대비 설정한 하락폭 비율만큼 가격이 밀릴 때 동적 익절/손절을 실행합니다.';
                trailingDiv.style.display = 'flex';
            } else if (riskType === 'AveragingDown') {
                desc.innerText = '평균 단가 대비 기준 하락률 이탈 시 추가 매수를 실행하여 평단을 낮춥니다. (최대 횟수 도달 전까지)';
                averagingDiv.style.display = 'flex';
            }
        }

        function _timeframeBarHours(tf) {
            const map = { '1m': 1/60, '3m': 3/60, '5m': 5/60, '10m': 10/60, '15m': 15/60, '30m': 0.5, '1h': 1, '4h': 4, 'D': 24, 'W': 168, 'M': 720 };
            return map[tf] || 0.5;
        }

        function syncRiskPanelForTactic(tactic) {
            const stdPanel = document.getElementById('risk-standard-panel');
            const cbPanel = document.getElementById('risk-custombear-panel');
            const desc = document.getElementById('risk-desc-text');
            if (!stdPanel || !cbPanel || !tactic) return;

            const isCustomBear = tactic.logic === 'CUSTOM_BEAR';

            if (isCustomBear) {
                stdPanel.style.display = 'none';
                cbPanel.style.display = 'block';

                const cbStrat = (tactic.strategies || []).find(s => s.name === 'CustomBear');
                const p = cbStrat ? cbStrat.params : {};
                const tf = cbStrat ? (cbStrat.timeframe || '15m') : '15m';
                const trailPct = ((p.trail_pct ?? 0.02) * 100).toFixed(1);
                const stopPct = ((p.stop_loss ?? 0.015) * 100).toFixed(1);
                const timeCut = p.time_cut ?? 24;
                const barHours = _timeframeBarHours(tf);
                const holdHours = (timeCut * barHours).toFixed(1);
                const backupSl = ((tactic.risk?.stop_loss_pct ?? p.stop_loss ?? 0.015) * 100).toFixed(1);

                document.getElementById('risk-cb-trail-val').innerText = trailPct + '%';
                document.getElementById('risk-cb-stop-val').innerText = '-' + stopPct + '%';
                document.getElementById('risk-cb-timecut-val').innerText = `${timeCut}봉 (${tf})`;
                document.getElementById('risk-cb-timecut-hint').innerText = `약 ${holdHours}시간 경과 시 무조건 청산`;
                document.getElementById('risk-cb-backup-sl-val').innerText = '-' + backupSl + '%';

                desc.innerText = 'CustomBear: 급락 반등 매수 후 트레일링·손절·시간청산으로 매도합니다. 고정 익절(TP)은 사용하지 않습니다.';
            } else {
                cbPanel.style.display = 'none';
                stdPanel.style.display = 'block';

                const risk = tactic.risk || { type: 'None' };
                document.getElementById('risk-type-select').value = risk.type;
                toggleRiskParamsDisplay(risk.type);

                if (risk.type === 'StopLoss') {
                    const slVal = ((risk.stop_loss_pct || 0.03) * 100).toFixed(1);
                    const tpVal = ((risk.take_profit_pct || 0.06) * 100).toFixed(1);
                    document.getElementById('risk-stoploss-pct').value = slVal;
                    document.getElementById('risk-stoploss-pct-val').innerText = slVal;
                    document.getElementById('risk-takeprofit-pct').value = tpVal;
                    document.getElementById('risk-takeprofit-pct-val').innerText = tpVal;
                } else if (risk.type === 'TrailingStop') {
                    const trailVal = ((risk.trail_pct || 0.02) * 100).toFixed(1);
                    document.getElementById('risk-trail-pct').value = trailVal;
                    document.getElementById('risk-trail-pct-val').innerText = trailVal;
                } else if (risk.type === 'AveragingDown') {
                    const dropVal = ((risk.drop_trigger_pct || 0.04) * 100).toFixed(1);
                    const maxAddCount = risk.max_add_count || 3;
                    document.getElementById('risk-drop-trigger-pct').value = dropVal;
                    document.getElementById('risk-drop-trigger-val').innerText = dropVal;
                    document.getElementById('risk-max-add-count').value = maxAddCount;
                    document.getElementById('risk-max-add-count-val').innerText = maxAddCount;
                }
            }
        }

        function updateParamValue(elId, val) {
            document.getElementById(elId).innerText = val;
        }

        function toggleStrategy(name) {
            const enabled = document.getElementById(`toggle-${name.toLowerCase()}`).checked;
            toggleStrategyControlsState(name, enabled);
        }

        function toggleStrategyControlsState(name, enabled) {
            const prefix = name.toLowerCase();
            const controlsDiv = document.getElementById(`controls-${prefix}`);
            if (controlsDiv) {
                const inputs = controlsDiv.getElementsByTagName('input');
                for (let input of inputs) {
                    input.disabled = !enabled;
                    input.style.opacity = enabled ? '1' : '0.4';
                }
                controlsDiv.style.opacity = enabled ? '1' : '0.6';
            }
        }

        function toggleThresholdDisplay(logic) {
            const group = document.getElementById('threshold-control-group');
            const desc = document.getElementById('logic-desc-text');
            
            if (logic === 'WEIGHTED_VOTE') {
                group.style.display = 'flex';
                desc.innerText = '각 지표 신호별 가중치를 부여해 합의 비율(임계치) 이상일 때 거래 주문을 생성합니다.';
            } else {
                group.style.display = 'none';
                if (logic === 'AND') desc.innerText = 'AND: 모든 활성 전략이 BUY이면 매수, 모두 SELL이면 매도합니다.';
                else if (logic === 'OR') desc.innerText = 'OR: 활성 전략 중 하나라도 BUY이면 매수, 하나라도 SELL이면 매도합니다.';
                else if (logic === 'VOTE') desc.innerText = '활성화된 전략들의 다수결(과반수 이상 합의)을 기준으로 판단합니다.';
            }
        }

        async function changeRegimeOverride(ticker, regime) {
            const durationEl = document.getElementById(`regime-duration-${ticker}`);
            const durationDays = durationEl ? parseInt(durationEl.value) : 0;
            
            if (durationEl) {
                durationEl.style.display = (regime === 'AUTO') ? 'none' : 'inline-block';
            }
            
            addLog(formatKstTime(), ticker, 'SYSTEM', `장세 강제 고정 요청 -> ${regime} (기간: ${durationDays}일)`);
            try {
                const response = await fetch('/api/tickers/regime-override', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ 
                        ticker: ticker, 
                        regime: regime,
                        duration_days: durationDays
                    })
                });
                const res = await response.json();
                if (res.success) {
                    if (tickerConfigs[ticker]) {
                        tickerConfigs[ticker].regime_override = regime;
                    }
                    addLog(formatKstTime(), ticker, 'SYSTEM', `장세가 ${regime}(으)로 성공적으로 반영되었습니다.`);
                    await loadAllConfigs();
                } else {
                    alert("장세 변경 실패: " + res.error);
                }
            } catch(e) {
                console.error(e);
            }
        }

        async function submitManualOrder(side) {
            const ticker = document.getElementById('manual-ticker-select').value;
            const amount = parseFloat(document.getElementById('manual-order-amount').value);
            
            if (!ticker) {
                alert("주문 종목을 선택하세요.");
                return;
            }
            
            if (side === 'BUY' && (isNaN(amount) || amount < 500)) {
                alert("매수 금액은 최소 500 KRW 이상이어야 합니다.");
                return;
            }
            
            const confirmMsg = side === 'BUY' ? 
                `[수동 매수] ${ticker} 종목을 ${amount.toLocaleString()}원만큼 즉시 매수 주문하시겠습니까?` : 
                `[수동 매도] ${ticker} 종목의 보유 물량을 전량 즉시 매도 주문하시겠습니까?`;
                
            if (!confirm(confirmMsg)) return;
            
            try {
                const response = await fetch('/api/trade/manual', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker: ticker, side: side, amount: amount })
                });
                const res = await response.json();
                if (res.success) {
                    addLog(formatKstTime(), ticker, side, `[수동 주문 성공] 사용자가 직접 발송한 ${side} 즉시 주문이 정상 접수되었습니다.`);
                } else {
                    alert("수동 주문 실패: " + res.error);
                }
            } catch (e) {
                console.error(e);
                alert("네트워크 통신 오류가 발생했습니다.");
            }
        }

        function updateCycleMatchPanel(ticker, cycleMatch) {
            const el = document.getElementById(`cycle-match-${ticker}`);
            if (!el) return;
            if (!cycleMatch || !cycleMatch.summary) {
                el.innerText = ticker === 'USDT'
                    ? '400일 페그 회귀·사이클 각도 분석 대기 중...'
                    : '사이클 회귀각 분석 대기 중...';
                return;
            }
            const hist = (cycleMatch.historical_cycles || [])
                .map(c => `#${c.cycle_number} ${c.angle_pct_per_month >= 0 ? '+' : ''}${c.angle_pct_per_month}%/월`)
                .join('<br>');
            el.innerHTML = `🔄 ${cycleMatch.summary}<br><span style="opacity:0.75; font-size:9px; display:block; margin-top:4px;">과거 사이클 각도:<br>${hist || '-'}</span>`;
        }

        function updateBearPatternPanel(ticker, bearPattern, ranking) {
            const el = document.getElementById(`bear-pattern-${ticker}`);
            if (!el || ticker === 'USDT') return;
            if (!bearPattern || !bearPattern.entry_timing) {
                el.innerText = '월봉 하락 패턴·바닥선 분석 대기 중...';
                return;
            }
            const t = bearPattern.entry_timing;
            const styleLabel = {
                scalp_fast: '단타(빠른 익절)',
                swing_bottom: '바닥권 스윙',
                cautious_swing: '바닥 접근 보수',
                wait: '진입 대기',
            }[t.entry_style] || t.entry_style;
            const eps = (bearPattern.historical_episodes || [])
                .slice(-4)
                .map(e => `#${e.episode_number} ${(e.peak_date || '').slice(0, 7)} -${e.drawdown_pct}%`)
                .join(', ');
            let rankHtml = '';
            if (ranking && ranking.summary) {
                rankHtml = `<br><span style="opacity:0.8; font-size:9px;">📊 ${ranking.summary}</span>`;
            }
            el.innerHTML = `📉 월봉 하락 ${bearPattern.episode_count || 0}회 · 진입점수 <b>${t.entry_score}</b> (${styleLabel})<br>`
                + `<span style="opacity:0.85; font-size:9px;">${t.summary}</span>`
                + (eps ? `<br><span style="opacity:0.7; font-size:9px;">역사 패턴: ${eps}</span>` : '')
                + rankHtml;
        }

        function formatProfitPct(val) {
            if (val === undefined || val === null) return '-';
            return `${val >= 0 ? '+' : ''}${val}%`;
        }

        function fillBearProfitHorizons(prefix, profits, backtest) {
            const keys = ['1d', '3d', '1w', '2w', '1m', '3m', '6m'];
            const ret = backtest?.total_return_pct ?? 0;
            const daily = ret !== 0 ? Math.pow(1 + ret / 100, 1 / 365) - 1 : 0;
            const dayMap = { '1d': 1, '3d': 3, '1w': 7, '2w': 14, '1m': 30, '3m': 90, '6m': 180 };
            keys.forEach(k => {
                const el = document.getElementById(`${prefix}-profit-${k}`);
                if (!el) return;
                let v = profits?.[k];
                if (v === undefined && daily !== 0) {
                    v = Math.round(((Math.pow(1 + daily, dayMap[k]) - 1) * 100) * 100) / 100;
                }
                el.innerText = formatProfitPct(v ?? 0);
            });
        }

        function updateOptimizedDefaultPanel(ticker, configOrData) {
            const el = document.getElementById(`optimized-default-${ticker}`);
            if (!el) return;
            if (ticker === 'USDT') {
                const cfg = (configOrData && configOrData.resolved_usdt_strategy !== undefined)
                    ? configOrData
                    : (tickerConfigs.USDT || configOrData || {});
                const resolved = cfg.resolved_usdt_strategy || cfg.usdt_auto_pick || 'fixed_fusion';
                const label = resolved === 'learned_mixed' ? '학습 3대지표' : '고정 융합';
                const reason = cfg.usdt_auto_pick_reason || '백테스트 최적화 후 자동 갱신';
                const gap = cfg.usdt_fx?.min_consider_gap_krw ?? USDT_DEFAULT_GAP;
                const minProfit = cfg.usdt_fx?.min_target_profit_pct ?? '-';
                el.innerHTML = `✅ <b>USDT ${label}</b> · 검토 gap ${gap}원 · 익절≥${minProfit}%<br>`
                    + `<span style="opacity:0.8;">${reason}</span>`;
                updateUsdtOptGapDisplay();
                return;
            }
            const od = configOrData?.optimized_default || configOrData;
            if (!od || !od.regimes) {
                el.innerText = '일일 최적화 디폴트 전략 대기 중... (02:00 KST 자동 갱신)';
                return;
            }
            const regime = od.current_regime || 'BEAR';
            const active = od.regimes[regime] || {};
            const strats = (active.strategies || []).join('+') || '-';
            const bearNote = active.bear_variant ? ` (${active.bear_variant === 'mixed' ? 'Mixed' : 'CustomBear'})` : '';
            const cycleNote = od.cycle_match_cycle
                ? ` · ${od.cycle_match_cycle}번째 사이클 참고(${od.cycle_similarity_pct || 0}%)`
                : '';
            const applied = od.applied_at ? formatKstDateTime(od.applied_at) : '-';
            const sellInfo = active.sell_strategy || active.risk_type || '-';
            const holdInfo = active.avg_hold_days ? ` · 평균보유 ${active.avg_hold_days}일` : '';
            el.innerHTML = `✅ <b>디폴트 적용</b> ${regime}: ${active.logic || '-'} ${strats}${bearNote}<br>`
                + `<span style="opacity:0.8;">매도 ${sellInfo}${holdInfo} · 1D예상 ${active.expected_profit_1d ?? '-'}% · 1M예상 ${active.expected_profit_1m ?? '-'}% · Sharpe ${active.sharpe ?? '-'}${cycleNote}</span><br>`
                + (od.bear_auto_pick_reason ? `<span style="opacity:0.75; font-size:9px;">BEAR 선택: ${od.bear_auto_pick_reason}</span><br>` : '')
                + `<span style="opacity:0.65; font-size:9px;">갱신: ${applied} (02:00 KST 학습·완료 시 자동)</span>`;
        }

        async function loadAllConfigs() {
            ensureSystemTickerCards();
            try {
                const response = await fetch('/api/config');
                tickerConfigs = await response.json();
                
                // 수동 거래소 종목 선택 셀렉트 옵션 갱신
                const selectBox = document.getElementById('manual-ticker-select');
                if (selectBox) {
                    let optionsHtml = '';
                    Object.keys(tickerConfigs).forEach(t => {
                        if (t === 'initial_balance' || t === 'global_settings') return;
                        optionsHtml += `<option value="${t}">${t}/KRW</option>`;
                    });
                    selectBox.innerHTML = optionsHtml;
                }
                
                // 만약 initial_balance 설정이 있다면 화면의 예수금 초기 텍스트 갱신
                if (tickerConfigs.initial_balance !== undefined) {
                    const balEl = document.getElementById('total-balance');
                    if (balEl) {
                        const initBal = typeof tickerConfigs.initial_balance === 'object' ? 
                            tickerConfigs.initial_balance.value : tickerConfigs.initial_balance;
                        balEl.innerText = `예수금: ${Math.floor(initBal).toLocaleString()} KRW`;
                    }
                }
                
                Object.entries(tickerConfigs).forEach(([ticker, config]) => {
                    if (ticker === 'initial_balance' || ticker === 'global_settings') return;
                    const currentRegime = config.current_regime || 'BEAR';
                    const tactic = config.tactics ? config.tactics[currentRegime] : null;
                    const logic = tactic ? tactic.logic : 'AND';
                    createTickerCardIfNotExist(ticker, logic, config.active !== false);
                    updateStrategyDetailsTable(ticker, config);
                    updateCycleMatchPanel(ticker, config.cycle_analysis);
                    updateOptimizedDefaultPanel(ticker, config);
                    const regSelect = document.getElementById(`regime-select-${ticker}`);
                    const durSelect = document.getElementById(`regime-duration-${ticker}`);
                    if (regSelect && config.regime_override) {
                        regSelect.value = config.regime_override;
                        if (durSelect) {
                            durSelect.style.display = (config.regime_override === 'AUTO') ? 'none' : 'inline-block';
                        }
                    }
                    
                    // 만료 시간 뱃지 동적 갱신
                    const expiryContainer = document.getElementById(`regime-expiry-container-${ticker}`);
                    const expiryTimeEl = document.getElementById(`regime-expiry-time-${ticker}`);
                    if (expiryContainer && expiryTimeEl) {
                        if (config.regime_override && config.regime_override !== 'AUTO' && config.regime_override_expires_at) {
                            expiryContainer.style.display = 'flex';
                            
                            const expiresAt = new Date(config.regime_override_expires_at);
                            const now = new Date();
                            const diffMs = expiresAt - now;
                            if (diffMs > 0) {
                                const diffDays = Math.ceil(diffMs / (1000 * 60 * 60 * 24));
                                const dateStr = expiresAt.getFullYear() + '-' + 
                                    String(expiresAt.getMonth() + 1).padStart(2, '0') + '-' + 
                                    String(expiresAt.getDate()).padStart(2, '0') + ' ' + 
                                    String(expiresAt.getHours()).padStart(2, '0') + ':' + 
                                    String(expiresAt.getMinutes()).padStart(2, '0');
                                expiryTimeEl.innerText = `${dateStr} (${diffDays}일 남음)`;
                            } else {
                                expiryContainer.style.display = 'none';
                            }
                        } else {
                            expiryContainer.style.display = 'none';
                        }
                    }
                });

                for (const ticker of SYSTEM_TICKERS) {
                    const config = tickerConfigs[ticker];
                    if (!config) continue;
                    const currentRegime = config.current_regime || (ticker === 'USDT' ? 'RANGE' : 'BEAR');
                    const tactic = config.tactics ? config.tactics[currentRegime] : null;
                    const logic = tactic ? tactic.logic : (ticker === 'USDT' ? 'OR' : 'AND');
                    createTickerCardIfNotExist(ticker, logic, config.active !== false);
                    updateStrategyDetailsTable(ticker, config);
                    updateCycleMatchPanel(ticker, config.cycle_analysis);
                    updateOptimizedDefaultPanel(ticker, config);
                    upgradeUsdtSidebarBlock(ticker);
                }
                
                loadConfigForTicker(activeTicker);
                syncUsdtPanel(activeTicker);
                refreshUsdtFxStatus();
            } catch (e) {
                console.error("Failed to load configs:", e);
                ensureSystemTickerCards();
            }
        }

        function updateStrategyDetailsTable(ticker, config) {
            const tableEl = document.getElementById(`table-${ticker}`);
            if (tableEl) {
                let rowsHtml = '';
                const currentRegime = config.current_regime || 'BEAR';
                const tactic = config.tactics ? config.tactics[currentRegime] : null;
                const strategies = tactic ? tactic.strategies : [];
                strategies.forEach(s => {
                    if (s.enabled) {
                        const label = s.name === 'UsdtFxBollinger' ? '환율+BB 융합' : s.name;
                        rowsHtml += `<tr><td>${label}</td><td>대기 중</td></tr>`;
                    }
                });
                rowsHtml += `<tr><td>종합 판단</td><td>HOLD</td></tr>`;
                tableEl.innerHTML = rowsHtml;
            }
        }

        function loadConfigForTicker(ticker) {
            const config = tickerConfigs[ticker];
            if (!config) return;
            
            const currentRegime = getTickerLiveRegime(ticker);
            config.current_regime = currentRegime;
            const tactic = config.tactics ? config.tactics[currentRegime] : null;
            if (!tactic) return;

            if (ticker === 'USDT') {
                const resolved = config.resolved_usdt_strategy || 'fixed_fusion';
                const badgeEl = document.getElementById(`badge-text-${ticker}`);
                if (badgeEl) {
                    badgeEl.innerText = resolved === 'learned_mixed' ? formatStrategyBadge(tactic.logic) : '환율+BB 융합';
                }
            } else {
                updateTickerStrategyBadge(ticker, tactic.logic);
            }
            
            document.getElementById('logic-select').value = (tactic.logic === 'CUSTOM_BEAR') ? 'OR' : tactic.logic;
            toggleThresholdDisplay(tactic.logic);
            
            // 수동 장세 고정 드롭다운 값 맞춤
            const regSelect = document.getElementById(`regime-select-${ticker}`);
            if (regSelect && config.regime_override) {
                regSelect.value = config.regime_override;
            }
            
            document.getElementById('weighted-threshold').value = tactic.threshold || 0.5;
            document.getElementById('threshold-val').innerText = tactic.threshold || 0.5;

            // 리스크 설정 동기화 (CustomBear는 전용 패널)
            syncRiskPanelForTactic(tactic);

            const holdEl = document.getElementById('backtest-hold-summary');
            if (holdEl) {
                const meta = tactic.backtest_meta;
                if (meta && meta.avg_hold_hours > 0) {
                    holdEl.innerText = `최적화 백테스트 평균 보유: ${formatHoldDuration(meta.avg_hold_hours, meta.avg_hold_days)} (매수→매도)`;
                } else {
                    holdEl.innerText = '최적화 백테스트 평균 보유: 최적화 실행 후 표시됩니다';
                }
            }
            
            // 모든 전략 카드 숨기기
            document.querySelectorAll('#strategies-tab-content .strategy-item-card').forEach(card => {
                card.style.display = 'none';
            });
            
            const strategies = tactic.strategies || [];
            strategies.forEach(s => {
                const prefix = s.name.toLowerCase();
                
                const cardEl = document.getElementById(`card-item-${prefix}`);
                if (cardEl) {
                    cardEl.style.display = 'flex';
                }
                
                const toggle = document.getElementById(`toggle-${prefix}`);
                if (toggle) {
                    toggle.checked = s.enabled;
                    toggleStrategyControlsState(s.name, s.enabled);
                }
                
                if (s.params) {
                    Object.entries(s.params).forEach(([paramName, paramVal]) => {
                        const htmlParamName = paramName === 'std_dev' ? 'stddev' : paramName;
                        const inputEl = document.getElementById(`${prefix}-${htmlParamName}`);
                        if (inputEl) {
                            let displayVal = paramVal;
                            if (prefix === 'custombear' && ['drop_pct', 'trail_pct', 'stop_loss'].includes(paramName)) {
                                displayVal = (paramVal * 100).toFixed(1);
                            }
                            inputEl.value = displayVal;
                            const valEl = document.getElementById(`${prefix}-${htmlParamName}-val`);
                            if (valEl) valEl.innerText = displayVal;
                        }
                    });
                }
                
                const weightEl = document.getElementById(`${prefix}-weight`);
                if (weightEl) {
                    weightEl.value = s.weight || 1.0;
                    const valEl = document.getElementById(`${prefix}-weight-val`);
                    if (valEl) valEl.innerText = s.weight || 1.0;
                }
            });
            updateConfigStatusBadge();
            syncBearStrategyUI(ticker);
            syncUsdtPanel(ticker);
            loadBacktestCompareData(ticker);
        }

        /* DYNAMIC_FACTORY_DEFAULTS_START */
        const FACTORY_DEFAULTS = {
            "BTC": {
                "BULL": {
                    "logic": "OR",
                    "threshold": 0.5,
                    "strategies": {
                        "RSI": {"enabled": true, "weight": 1.0, "period": 5, "oversold": 38, "overbought": 65},
                        "Bollinger": {"enabled": false, "weight": 1.0, "period": 5, "std_dev": 1.5},
                        "MACD": {"enabled": true, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
                    },
                    "risk": {"type": "None"}
                },
                "BEAR": {
                    "logic": "OR",
                    "threshold": 0.5,
                    "strategies": {
                        "RSI": {"enabled": true, "weight": 1.0, "period": 5, "oversold": 25, "overbought": 60},
                        "Bollinger": {"enabled": true, "weight": 1.0, "period": 10, "std_dev": 2.0},
                        "MACD": {"enabled": false, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
                    },
                    "risk": {"type": "StopLoss", "stop_loss_pct": 0.02, "take_profit_pct": 0.03}
                },
                "RANGE": {
                    "logic": "OR",
                    "threshold": 0.5,
                    "strategies": {
                        "RSI": {"enabled": false, "weight": 1.0, "period": 5, "oversold": 35, "overbought": 65},
                        "Bollinger": {"enabled": true, "weight": 1.0, "period": 5, "std_dev": 1.5},
                        "MACD": {"enabled": false, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
                    },
                    "risk": {"type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.03}
                }
            },
            "ETH": {
                "BULL": {
                    "logic": "OR",
                    "threshold": 0.5,
                    "strategies": {
                        "RSI": {"enabled": true, "weight": 1.0, "period": 5, "oversold": 38, "overbought": 65},
                        "Bollinger": {"enabled": false, "weight": 1.0, "period": 5, "std_dev": 1.5},
                        "MACD": {"enabled": true, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
                    },
                    "risk": {"type": "None"}
                },
                "BEAR": {
                    "logic": "OR",
                    "threshold": 0.5,
                    "strategies": {
                        "RSI": {"enabled": true, "weight": 1.0, "period": 5, "oversold": 25, "overbought": 60},
                        "Bollinger": {"enabled": true, "weight": 1.0, "period": 10, "std_dev": 2.0},
                        "MACD": {"enabled": false, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
                    },
                    "risk": {"type": "StopLoss", "stop_loss_pct": 0.02, "take_profit_pct": 0.03}
                },
                "RANGE": {
                    "logic": "OR",
                    "threshold": 0.5,
                    "strategies": {
                        "RSI": {"enabled": false, "weight": 1.0, "period": 5, "oversold": 35, "overbought": 65},
                        "Bollinger": {"enabled": true, "weight": 1.0, "period": 5, "std_dev": 1.5},
                        "MACD": {"enabled": false, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
                    },
                    "risk": {"type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.03}
                }
            }
        };
        /* DYNAMIC_FACTORY_DEFAULTS_END */

        function tacticToStrategyMap(tactic) {
            const map = {};
            (tactic?.strategies || []).forEach(s => {
                map[s.name] = { enabled: !!s.enabled, weight: s.weight ?? 1.0, ...(s.params || {}) };
            });
            return map;
        }

        function savedTacticMatchesDefaults(tactic, defaults) {
            if (!tactic || !defaults) return true;

            const savedLogic = tactic.logic || 'OR';
            const defaultLogic = defaults.logic || 'OR';
            if (savedLogic !== defaultLogic) return false;

            const savedThreshold = tactic.threshold ?? 0.5;
            const defaultThreshold = defaults.threshold ?? 0.5;
            if (Math.abs(savedThreshold - defaultThreshold) > 0.001) return false;

            const defStrats = defaults.strategies || {};
            const savedStrats = tacticToStrategyMap(tactic);
            const isCustomBearMode = defaultLogic === 'CUSTOM_BEAR' || !!defStrats.CustomBear;

            if (isCustomBearMode && defStrats.CustomBear) {
                const defCB = defStrats.CustomBear;
                const savedCB = savedStrats.CustomBear;
                if (!savedCB) return false;
                if (savedCB.enabled !== defCB.enabled) return false;
                if (Math.abs((savedCB.weight ?? 1) - (defCB.weight ?? 1)) > 0.001) return false;
                const cbKeys = ['lookback', 'drop_pct', 'volume_ratio', 'trail_pct', 'stop_loss', 'time_cut'];
                for (const key of cbKeys) {
                    if (Math.abs(Number(savedCB[key]) - Number(defCB[key])) > 0.001) return false;
                }
            } else {
                for (const [sName, defS] of Object.entries(defStrats)) {
                    const savedS = savedStrats[sName];
                    if (!savedS) return false;
                    if (savedS.enabled !== defS.enabled) return false;
                    if (Math.abs((savedS.weight ?? 1) - (defS.weight ?? 1)) > 0.001) return false;
                    if (sName === 'RSI') {
                        if (savedS.period !== defS.period) return false;
                        if (savedS.oversold !== defS.oversold) return false;
                        if (savedS.overbought !== defS.overbought) return false;
                    } else if (sName === 'Bollinger') {
                        if (savedS.period !== defS.period) return false;
                        if (Math.abs(Number(savedS.std_dev) - Number(defS.std_dev)) > 0.001) return false;
                    } else if (sName === 'MACD') {
                        if (savedS.fast !== defS.fast) return false;
                        if (savedS.slow !== defS.slow) return false;
                        if (savedS.signal_period !== defS.signal_period) return false;
                    }
                }
            }

            const savedRisk = tactic.risk || { type: 'None' };
            const defRisk = defaults.risk || { type: 'None' };
            if (savedRisk.type !== defRisk.type) return false;

            if (savedRisk.type === 'StopLoss') {
                if (Math.abs((savedRisk.stop_loss_pct ?? 0) - (defRisk.stop_loss_pct ?? 0)) > 0.001) return false;
                if (!isCustomBearMode) {
                    if (Math.abs((savedRisk.take_profit_pct ?? 0) - (defRisk.take_profit_pct ?? 0)) > 0.001) return false;
                }
            } else if (savedRisk.type === 'TrailingStop') {
                if (Math.abs((savedRisk.trail_pct ?? 0) - (defRisk.trail_pct ?? 0)) > 0.001) return false;
            } else if (savedRisk.type === 'AveragingDown') {
                if (Math.abs((savedRisk.drop_trigger_pct ?? 0) - (defRisk.drop_trigger_pct ?? 0)) > 0.001) return false;
                if ((savedRisk.max_add_count ?? 0) !== (defRisk.max_add_count ?? 0)) return false;
            }

            return true;
        }

        function checkIfConfigIsDefault() {
            if (!activeTicker || !tickerConfigs[activeTicker]) return true;
            const config = tickerConfigs[activeTicker];

            // 장세/하락장 전략을 수동 고정한 경우 → 사용자 설정
            if ((config.regime_override || 'AUTO') !== 'AUTO') return false;
            if ((config.selected_bear_strategy || 'auto') !== 'auto') return false;

            const currentRegime = getTickerLiveRegime(activeTicker);
            const tickerDefaults = FACTORY_DEFAULTS[activeTicker] || FACTORY_DEFAULTS["BTC"];
            const defaults = tickerDefaults ? tickerDefaults[currentRegime] : null;
            if (!defaults) return true;

            const tactic = config.tactics?.[currentRegime];
            return savedTacticMatchesDefaults(tactic, defaults);
        }

        function updateConfigStatusBadge() {
            const badge = document.getElementById('config-status-badge');
            if (!badge) return;
            const isDefault = checkIfConfigIsDefault();
            if (isDefault) {
                badge.className = 'status-badge-inline default';
                badge.innerHTML = '🤖 [AI 자동 최적 설정]';
            } else {
                badge.className = 'status-badge-inline custom';
                badge.innerHTML = '🟠 [사용자임의 설정]';
            }
        }

        function resetStrategyUI() {
            if (!activeTicker || !tickerConfigs[activeTicker]) return;
            const currentRegime = tickerConfigs[activeTicker].current_regime || 'BEAR';
            const tickerDefaults = FACTORY_DEFAULTS[activeTicker] || FACTORY_DEFAULTS["BTC"];
            const defaults = tickerDefaults ? tickerDefaults[currentRegime] : null;
            if (!defaults) return;

            // UI 값을 디폴트로 업데이트
            document.getElementById('logic-select').value = (defaults.logic === 'CUSTOM_BEAR') ? 'OR' : defaults.logic;
            toggleThresholdDisplay(defaults.logic);
            
            document.getElementById('weighted-threshold').value = defaults.threshold;
            document.getElementById('threshold-val').innerText = defaults.threshold;

            // 리스크 설정
            syncRiskPanelForTactic(defaults);

            // 개별 전략
            Object.entries(defaults.strategies).forEach(([sName, sCfg]) => {
                const prefix = sName.toLowerCase();
                const toggle = document.getElementById(`toggle-${prefix}`);
                if (toggle) {
                    toggle.checked = sCfg.enabled;
                    toggleStrategyControlsState(sName, sCfg.enabled);
                }
                
                if (sName === 'RSI') {
                    document.getElementById('rsi-period').value = sCfg.period;
                    document.getElementById('rsi-period-val').innerText = sCfg.period;
                    document.getElementById('rsi-overbought').value = sCfg.overbought;
                    document.getElementById('rsi-overbought-val').innerText = sCfg.overbought;
                    document.getElementById('rsi-oversold').value = sCfg.oversold;
                    document.getElementById('rsi-oversold-val').innerText = sCfg.oversold;
                } else if (sName === 'Bollinger') {
                    document.getElementById('bollinger-period').value = sCfg.period;
                    document.getElementById('bollinger-period-val').innerText = sCfg.period;
                    document.getElementById('bollinger-stddev').value = sCfg.std_dev;
                    document.getElementById('bollinger-stddev-val').innerText = sCfg.std_dev;
                } else if (sName === 'MACD') {
                    document.getElementById('macd-fast').value = sCfg.fast;
                    document.getElementById('macd-fast-val').innerText = sCfg.fast;
                    document.getElementById('macd-slow').value = sCfg.slow;
                    document.getElementById('macd-slow-val').innerText = sCfg.slow;
                    document.getElementById('macd-signal').value = sCfg.signal_period;
                    document.getElementById('macd-signal-val').innerText = sCfg.signal_period;
                } else if (sName === 'CustomBear') {
                    document.getElementById('custombear-lookback').value = sCfg.lookback;
                    document.getElementById('custombear-lookback-val').innerText = sCfg.lookback;
                    
                    const dropPctVal = (sCfg.drop_pct * 100).toFixed(1);
                    document.getElementById('custombear-drop_pct').value = dropPctVal;
                    document.getElementById('custombear-drop_pct-val').innerText = dropPctVal;
                    
                    document.getElementById('custombear-volume_ratio').value = sCfg.volume_ratio;
                    document.getElementById('custombear-volume_ratio-val').innerText = sCfg.volume_ratio;
                    
                    const trailPctVal = (sCfg.trail_pct * 100).toFixed(1);
                    document.getElementById('custombear-trail_pct').value = trailPctVal;
                    document.getElementById('custombear-trail_pct-val').innerText = trailPctVal;
                    
                    const stopLossVal = (sCfg.stop_loss * 100).toFixed(1);
                    document.getElementById('custombear-stop_loss').value = stopLossVal;
                    document.getElementById('custombear-stop_loss-val').innerText = stopLossVal;
                    
                    document.getElementById('custombear-time_cut').value = sCfg.time_cut;
                    document.getElementById('custombear-time_cut-val').innerText = sCfg.time_cut;
                }

                const weightEl = document.getElementById(`${prefix}-weight`);
                if (weightEl) {
                    weightEl.value = sCfg.weight;
                    const valEl = document.getElementById(`${prefix}-weight-val`);
                    if (valEl) valEl.innerText = sCfg.weight;
                }
            });

            // 엔진에 적용 및 로그 기록
            applySettingsToEngine();
            addLog(formatKstTime(), activeTicker, 'SYSTEM', '전략 UI 및 엔진 설정이 검증된 기본 설정으로 복구되었습니다.');
        }

        async function applySettingsToEngine() {
            const logic = document.getElementById('logic-select').value;
            const threshold = parseFloat(document.getElementById('weighted-threshold').value);
            
            const strategies = [];
            
            // RSI
            const toggleRsi = document.getElementById('toggle-rsi');
            if (toggleRsi) {
                strategies.push({
                    name: 'RSI',
                    enabled: toggleRsi.checked,
                    weight: parseFloat(document.getElementById('rsi-weight').value),
                    params: {
                        period: parseInt(document.getElementById('rsi-period').value),
                        oversold: parseInt(document.getElementById('rsi-oversold').value),
                        overbought: parseInt(document.getElementById('rsi-overbought').value)
                    }
                });
            }
            
            // Bollinger
            const toggleBb = document.getElementById('toggle-bollinger');
            if (toggleBb) {
                strategies.push({
                    name: 'Bollinger',
                    enabled: toggleBb.checked,
                    weight: parseFloat(document.getElementById('bollinger-weight').value),
                    params: {
                        period: parseInt(document.getElementById('bollinger-period').value),
                        std_dev: parseFloat(document.getElementById('bollinger-stddev').value)
                    }
                });
            }
            
            // MACD
            const toggleMacd = document.getElementById('toggle-macd');
            if (toggleMacd) {
                strategies.push({
                    name: 'MACD',
                    enabled: toggleMacd.checked,
                    weight: parseFloat(document.getElementById('macd-weight').value),
                    params: {
                        fast: parseInt(document.getElementById('macd-fast').value),
                        slow: parseInt(document.getElementById('macd-slow').value),
                        signal_period: parseInt(document.getElementById('macd-signal').value)
                    }
                });
            }
            
            // CustomBear
            const toggleCustomBear = document.getElementById('toggle-custombear');
            if (toggleCustomBear) {
                strategies.push({
                    name: 'CustomBear',
                    enabled: toggleCustomBear.checked,
                    weight: parseFloat(document.getElementById('custombear-weight').value),
                    params: {
                        lookback: parseInt(document.getElementById('custombear-lookback').value),
                        drop_pct: parseFloat(document.getElementById('custombear-drop_pct').value) / 100,
                        volume_ratio: parseFloat(document.getElementById('custombear-volume_ratio').value),
                        trail_pct: parseFloat(document.getElementById('custombear-trail_pct').value) / 100,
                        stop_loss: parseFloat(document.getElementById('custombear-stop_loss').value) / 100,
                        time_cut: parseInt(document.getElementById('custombear-time_cut').value)
                    }
                });
            }
            
            // Risk Settings 파싱
            const isCustomBearMode = toggleCustomBear && toggleCustomBear.checked &&
                strategies.filter(s => s.enabled).every(s => s.name === 'CustomBear');
            let effectiveLogic = isCustomBearMode ? 'CUSTOM_BEAR' : logic;

            let risk;
            if (effectiveLogic === 'CUSTOM_BEAR') {
                const sl = parseFloat(document.getElementById('custombear-stop_loss').value) / 100;
                risk = { type: 'StopLoss', stop_loss_pct: sl, take_profit_pct: 9.9 };
            } else {
                const riskType = document.getElementById('risk-type-select').value;
                risk = { type: riskType };
                if (riskType === 'StopLoss') {
                    risk.stop_loss_pct = parseFloat(document.getElementById('risk-stoploss-pct').value) / 100;
                    risk.take_profit_pct = parseFloat(document.getElementById('risk-takeprofit-pct').value) / 100;
                } else if (riskType === 'TrailingStop') {
                    risk.trail_pct = parseFloat(document.getElementById('risk-trail-pct').value) / 100;
                } else if (riskType === 'AveragingDown') {
                    risk.drop_trigger_pct = parseFloat(document.getElementById('risk-drop-trigger-pct').value) / 100;
                    risk.max_add_count = parseInt(document.getElementById('risk-max-add-count').value);
                }
            }

            const newConfig = {
                ticker: activeTicker,
                logic: effectiveLogic,
                threshold: threshold,
                strategies: strategies,
                risk: risk
            };
            
            try {
                const response = await fetch('/api/config/update', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(newConfig)
                });
                const res = await response.json();
                if (res.success) {
                    const currentRegime = tickerConfigs[activeTicker].current_regime || 'BEAR';
                    if (!tickerConfigs[activeTicker].tactics) {
                        tickerConfigs[activeTicker].tactics = {};
                    }
                    tickerConfigs[activeTicker].tactics[currentRegime] = {
                        logic: effectiveLogic,
                        threshold: threshold,
                        strategies: strategies,
                        risk: risk
                    };
                    syncRiskPanelForTactic(tickerConfigs[activeTicker].tactics[currentRegime]);
                    const badgeEl = document.getElementById(`badge-text-${activeTicker}`);
                    if (badgeEl) badgeEl.innerText = formatStrategyBadge(effectiveLogic);
                    addLog(formatKstTime(), activeTicker, 'SYSTEM', `복합 전략 설정이 런타임 엔진에 실시간 적용되었습니다. (${effectiveLogic})`);
                    updateStrategyDetailsTable(activeTicker, tickerConfigs[activeTicker]);
                    updateConfigStatusBadge();
                } else {
                    addLog(formatKstTime(), activeTicker, 'SYSTEM', `설정 적용 실패: ${res.error}`);
                }
            } catch (e) {
                console.error("Apply settings error:", e);
                addLog(formatKstTime(), activeTicker, 'SYSTEM', `설정 적용 요청 실패.`);
            }
        }

        async function addNewTicker() {
            let ticker = prompt("추가할 KRW 마켓 종목 심볼을 입력하세요 (예: DOGE, SOL, ADA, SAND):");
            if (!ticker) return;
            ticker = ticker.toUpperCase().trim().replace("KRW-", "");
            
            if (document.getElementById(`card-${ticker}`)) {
                alert("이미 등록된 종목입니다.");
                return;
            }
            
            addLog(formatKstTime(), null, 'SYSTEM', `KRW-${ticker} 종목 추가 요청 중...`);
            
            try {
                const response = await fetch('/api/tickers', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker: ticker })
                });
                const res = await response.json();
                if (res.success) {
                    addLog(formatKstTime(), ticker, 'SYSTEM', `종목이 성공적으로 추가되었습니다. 웹소켓 구독을 갱신합니다.`);
                    await loadAllConfigs();
                    selectTicker(ticker);
                } else {
                    addLog(formatKstTime(), null, 'SYSTEM', `종목 추가 실패: ${res.error}`);
                }
            } catch (e) {
                console.error("Add ticker error:", e);
                addLog(formatKstTime(), null, 'SYSTEM', `종목 추가 요청 중 네트워크 에러 발생.`);
            }
        }

        // ══════════════════════════════════════════════════════════════
        // WebSocket 커넥션 및 실시간 파이프라인
        // ══════════════════════════════════════════════════════════════
        let ws = null;
        
        async function loadTradeHistory() {
            try {
                const response = await fetch('/api/trade/history');
                const data = await response.json();
                if (data && data.length > 0) {
                    const reversed = [...data].reverse();
                    reversed.forEach(ord => {
                        const timeDisplay = ord.timestamp;
                        const signalType = ord.side === '매수' ? 'BUY' : 'SELL';
                        addLog(
                            timeDisplay,
                            ord.ticker,
                            signalType,
                            `[빗썸 실제거래] 체결 완료! 수량: ${ord.volume} | 단가: ${ord.price.toLocaleString()} KRW | 총액: ${ord.amount.toLocaleString()} KRW`
                        );
                    });
                }
            } catch (e) {
                console.error("Failed to load trade history:", e);
                addLog(formatKstTime(), null, 'SYSTEM', '빗썸 과거 거래 내역을 불러오는데 실패했습니다.');
            }
        }

        function connectWebSocket() {
            const loc = window.location;
            const wsUri = (loc.protocol === "https:" ? "wss:" : "ws:") + "//" + loc.host + "/ws/trading-status";
            
            ws = new WebSocket(wsUri);
            
            ws.onopen = () => {
                wsStatusEl.className = 'status-badge connected';
                wsStatusText.innerText = '연결됨';
                addLog(formatKstTime(), null, 'SYSTEM', '다중 타임프레임 PoC 웹소켓 브로커에 접속 성공.');
                
                // 엔진 설정을 조회하여 UI와 동기화
                loadAllConfigs();
                
                // 빗썸 과거 체결 내역 불러와 트레이딩 로그에 초기화 로드
                loadTradeHistory();
                
                // 첫 진입 시 기본 종목 캐시 준비물 로드
                loadHistoricalCandles('BTC', activeTimeframe);
                loadHistoricalCandles('ETH', activeTimeframe);
                loadHistoricalCandles('XRP', activeTimeframe);
                loadHistoricalCandles('USDT', activeTimeframe);
            };

            ws.onmessage = (event) => {
                const msg = jsonParseSafe(event.data);
                if (!msg) return;
                
                const timeStr = formatKstTime(msg.data.timestamp);
                const ticker = msg.data.ticker;
                
                if (msg.type === 'regime_change') {
                    // 장세 국면 변경 알림 처리
                    const regime = msg.data.regime;
                    const logic = msg.data.logic;
                    const rType = msg.data.risk_type;
                    const logMsg = `시장 국면 변경 감지 -> ${regime} 작전으로 스위칭 완료! (전략 logic: ${logic}, 리스크: ${rType})`;
                    addLog(timeStr, ticker, 'SYSTEM', logMsg);

                    if (tickerConfigs[ticker]) {
                        tickerConfigs[ticker].current_regime = regime;
                    }
                    updateTickerStrategyBadge(ticker, logic);
                    
                    const regimeEl = document.getElementById(`regime-${ticker}`);
                    if (regimeEl) {
                        let emoji = '↔️';
                        let color = 'var(--accent-blue)';
                        if (regime === 'BULL') { emoji = '📈'; color = 'var(--accent-green)'; }
                        else if (regime === 'BEAR') { emoji = '📉'; color = 'var(--accent-red)'; }
                        regimeEl.innerText = `${emoji} ${regime}`;
                        regimeEl.style.color = color;
                    }

                    if (ticker === activeTicker) {
                        loadConfigForTicker(ticker);
                        loadBacktestCompareData(ticker);
                    }
                    return;
                }
                
                if (msg.type === 'trade') {
                    const price = msg.data.price;
                    const priceEl = document.getElementById(`price-${ticker}`);
                    const comp = msg.data.composite;
                    const regime = msg.data.regime;
                    const regimeReason = msg.data.regime_reason;
                    const override = msg.data.regime_override;
                    const compLogic = msg.data.composite?.logic;

                    if (tickerConfigs[ticker] && regime) {
                        const prevRegime = tickerConfigs[ticker].current_regime;
                        tickerConfigs[ticker].current_regime = regime;
                        if (ticker === activeTicker && prevRegime !== regime) {
                            loadConfigForTicker(ticker);
                        }
                    }
                    if (compLogic) {
                        updateTickerStrategyBadge(ticker, compLogic);
                    }
                    
                    // 장세 국면 표시 실시간 업데이트
                    const regimeEl = document.getElementById(`regime-${ticker}`);
                    if (regimeEl && regime) {
                        let emoji = '↔️';
                        let color = 'var(--accent-blue)';
                        if (regime === 'BULL') { emoji = '📈'; color = 'var(--accent-green)'; }
                        else if (regime === 'BEAR') { emoji = '📉'; color = 'var(--accent-red)'; }
                        regimeEl.innerText = `${emoji} ${regime}`;
                        regimeEl.style.color = color;
                    }

                    // 상세 판단 사유 노출
                    const reasonEl = document.getElementById(`regime-reason-${ticker}`);
                    if (reasonEl && regimeReason) {
                        reasonEl.innerText = regimeReason;
                        if (override !== "AUTO") {
                            reasonEl.style.borderLeftColor = 'var(--accent-red)';
                        } else {
                            reasonEl.style.borderLeftColor = 'var(--accent-blue)';
                        }
                    }

                    if (msg.data.cycle_match) {
                        if (tickerConfigs[ticker]) {
                            tickerConfigs[ticker].cycle_analysis = msg.data.cycle_match;
                        }
                        updateCycleMatchPanel(ticker, msg.data.cycle_match);
                    }
                    if (msg.data.bear_pattern) {
                        if (tickerConfigs[ticker]) {
                            tickerConfigs[ticker].bear_pattern_analysis = msg.data.bear_pattern;
                        }
                        updateBearPatternPanel(ticker, msg.data.bear_pattern, msg.data.bear_entry_ranking);
                    }
                    if (msg.data.optimized_default) {
                        if (tickerConfigs[ticker]) {
                            tickerConfigs[ticker].optimized_default = msg.data.optimized_default;
                        }
                        updateOptimizedDefaultPanel(ticker, msg.data.optimized_default);
                    }

                    // 장세 드롭다운 동기화 (사용자가 클릭해 변경한 상태가 소켓 메시지로 재수신될 때 동기화)
                    const regSelect = document.getElementById(`regime-select-${ticker}`);
                    if (regSelect && override) {
                        regSelect.value = override;
                    }
                    
                    // 1. 실시간 캔들 병합 알고리즘
                    mergeRealtimePriceToCandle(ticker, price, msg.data.timestamp);
                    
                    // 2. 가격 판 갱신 및 플래시 이펙트
                    if (ticker === 'USDT' && msg.data.usdt_fx_status) {
                        updateUsdtFxPanel(msg.data.usdt_fx_status);
                    }

                    if (priceEl) {
                        const formattedPrice = ['XRP', 'USDT', 'DOGE', 'SAND'].includes(ticker) ? price.toFixed(2) : price.toLocaleString();
                        priceEl.innerText = formattedPrice + ' KRW';
                        
                        if (lastPrices[ticker] !== 0 && lastPrices[ticker] !== undefined) {
                            if (price > lastPrices[ticker]) {
                                priceEl.className = 'price-display flash-up';
                            } else if (price < lastPrices[ticker]) {
                                priceEl.className = 'price-display flash-down';
                            }
                            setTimeout(() => { priceEl.className = 'price-display'; }, 180);
                        }
                        lastPrices[ticker] = price;
                        // 현재가 갱신 시 상단 요약도 실시간 반영
                        updateHeaderAssetsSummary();
                    }
                    
                    // 3. 포지션 세부 내역 실시간 바인딩
                    const pos = msg.data.position;
                    if (pos) {
                        const qtyEl = document.getElementById(`pos-qty-${ticker}`);
                        const avgEl = document.getElementById(`pos-avg-${ticker}`);
                        const valEl = document.getElementById(`pos-val-${ticker}`);
                        const pnlEl = document.getElementById(`pos-pnl-${ticker}`);
                        
                        if (qtyEl) qtyEl.innerText = `${parseFloat(pos.quantity.toFixed(8))} ${ticker}`;
                        if (avgEl) avgEl.innerText = `${Math.floor(pos.avg_price).toLocaleString()} KRW`;
                        if (valEl) valEl.innerText = `${Math.floor(pos.krw_value).toLocaleString()} KRW`;
                        
                        if (pnlEl) {
                            const pnlVal = pos.pnl_pct;
                            pnlEl.innerText = `${pnlVal >= 0 ? '+' : ''}${pnlVal.toFixed(2)}%`;
                            if (pnlVal > 0) {
                                pnlEl.style.color = 'var(--accent-green)';
                            } else if (pnlVal < 0) {
                                pnlEl.style.color = 'var(--accent-red)';
                            } else {
                                pnlEl.style.color = 'var(--text-secondary)';
                            }
                        }

                        // 글로벌 자산 객체 업데이트 및 헤더 요약 정보 갱신
                        userAssets[ticker] = {
                            quantity: pos.quantity,
                            avgPrice: pos.avg_price,
                            krwValue: pos.krw_value,
                            pnlPct: pos.pnl_pct
                        };
                        updateHeaderAssetsSummary();
                    }
                    
                    // 4. 총 계좌 원화 예수금 정보 갱신
                    if (msg.data.balance !== undefined) {
                        const balEl = document.getElementById('total-balance');
                        if (balEl) {
                            balEl.innerText = `예수금: ${Math.floor(msg.data.balance).toLocaleString()} KRW`;
                        }
                    }
                    
                    // 5. 지표 상세 텍스트 및 테이블 동적 업데이트
                    if (comp && comp.indicators) {
                        const tableEl = document.getElementById(`table-${ticker}`);
                        if (tableEl) {
                            let rowsHtml = '';
                            Object.entries(comp.indicators).forEach(([indName, indVal]) => {
                                const sig = comp.sub_signals ? (comp.sub_signals[indName] || 'HOLD') : 'HOLD';
                                let sigColor = 'var(--text-secondary)';
                                if (sig === 'BUY') sigColor = 'var(--accent-green)';
                                else if (sig === 'SELL') sigColor = 'var(--accent-red)';
                                
                                rowsHtml += `<tr><td>${indName}</td><td style="color:${sigColor}">${indVal} (${sig})</td></tr>`;
                            });
                            
                            let finalColor = 'var(--text-primary)';
                            if (comp.final === 'BUY') finalColor = 'var(--accent-green)';
                            else if (comp.final === 'SELL') finalColor = 'var(--accent-red)';
                            rowsHtml += `<tr><td>종합 판단</td><td style="color:${finalColor}; font-weight:bold;">${comp.final}</td></tr>`;
                            
                            tableEl.innerHTML = rowsHtml;
                        }
                    }

                    // 10% Throttling 로그 기록
                    if (comp && Math.random() < 0.10) {
                        const subSigStr = Object.entries(comp.sub_signals)
                            .map(([name, s]) => `${name}(${s})`)
                            .join(' + ');
                        addLog(
                            timeStr, 
                            ticker, 
                            'TRADE', 
                            `[${comp.logic} 복합] ${subSigStr} -> 합의: ${comp.final} (현재가: ${price.toLocaleString()} KRW)`
                        );
                    }
                } else if (msg.type === 'order') {
                    const signal = msg.data.signal;
                    const price = msg.data.price;
                    const riskReason = msg.data.risk_reason;
                    
                    if (riskReason) {
                        let riskDesc = '';
                        let logType = signal;
                        if (riskReason === 'FORCE_SELL_STOP_LOSS') {
                            riskDesc = '🚨🚨 [리스크 관리자 강제 손절발송] 손절 기준선 초과 전량 청산!';
                            logType = 'SELL';
                        } else if (riskReason === 'FORCE_SELL_TAKE_PROFIT') {
                            riskDesc = '💰💰 [리스크 관리자 강제 익절발송] 익절 기준선 초과 전량 청산!';
                            logType = 'SELL';
                        } else if (riskReason === 'FORCE_SELL_TRAILING_STOP') {
                            riskDesc = '📈📉 [리스크 관리자 트레일링스탑] 최고가 대비 하락폭 초과 전량 청산!';
                            logType = 'SELL';
                        } else if (riskReason === 'FORCE_ADD_BUY_AVERAGING') {
                            riskDesc = '💧💧 [리스크 관리자 물타기 집행] 평단 낮추기 추가 매수!';
                            logType = 'BUY';
                        } else if (riskReason.startsWith('PORTFOLIO_LIMIT_EXCEEDED')) {
                            riskDesc = `🛡️🛡️ [자산 배분 차단] 한 종목당 최대 투자 한도(35%) 초과로 주문 차단! (${riskReason.split('(')[1] ? riskReason.split('(')[1].replace(')', '') : ''})`;
                            logType = 'SYSTEM';
                        }
                        addLog(
                            timeStr,
                            ticker,
                            logType,
                            `${riskDesc} 가격: ${price.toLocaleString()} KRW`
                        );
                    } else {
                        addLog(
                            timeStr, 
                            ticker, 
                            signal, 
                            `★★ [자동매매 주문발송] 복합 결합 조건 충족! 신호: ${signal} @ ${price.toLocaleString()} KRW`
                        );
                    }
                }
            };

            ws.onclose = () => {
                wsStatusEl.className = 'status-badge';
                wsStatusText.innerText = '연결 끊김';
                addLog(formatKstTime(), null, 'SYSTEM', '브로커 연결 종료. 3초 후 재연결 시도...');
                setTimeout(connectWebSocket, 3000);
            };

            ws.onerror = (e) => {
                console.error("WebSocket Error:", e);
            };
        }

        async function toggleTickerActive(ticker) {
            const btn = document.getElementById(`ctrl-btn-${ticker}`);
            const isStarting = btn.innerText === '시작';
            const action = isStarting ? 'start' : 'stop';
            
            try {
                const response = await fetch('/api/tickers/control', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker: ticker, action: action })
                });
                const res = await response.json();
                if (res.success) {
                    btn.innerText = isStarting ? '정지' : '시작';
                    const ind = document.getElementById(`status-ind-${ticker}`);
                    if (isStarting) {
                        ind.classList.add('active');
                        addLog(formatKstTime(), ticker, 'SYSTEM', '자동매매 가동 시작.');
                    } else {
                        ind.classList.remove('active');
                        addLog(formatKstTime(), ticker, 'SYSTEM', '자동매매 일시정지.');
                    }
                    if (tickerConfigs[ticker]) {
                        tickerConfigs[ticker].active = isStarting;
                    }
                } else {
                    alert("상태 제어 실패: " + res.error);
                }
            } catch (e) {
                console.error("Ticker control error:", e);
            }
        }

        async function deleteTicker(ticker) {
            if (!confirm(`${ticker} 종목을 정말 삭제하시겠습니까?`)) return;
            
            try {
                const response = await fetch('/api/tickers/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker: ticker })
                });
                const res = await response.json();
                if (res.success) {
                    addLog(formatKstTime(), ticker, 'SYSTEM', '종목이 완전히 삭제되었습니다.');
                    const card = document.getElementById(`card-${ticker}`);
                    if (card) card.remove();
                    
                    delete tickerConfigs[ticker];
                    
                    if (activeTicker === ticker) {
                        const remainingKeys = Object.keys(tickerConfigs);
                        if (remainingKeys.length > 0) {
                            selectTicker(remainingKeys[0]);
                        } else {
                            activeTicker = '';
                            chartTitleEl.innerText = "등록된 종목이 없습니다. 종목을 추가하세요.";
                            if (candlestickSeries) {
                                candlestickSeries.setData([]);
                            }
                        }
                    }
                } else {
                    alert("종목 삭제 실패: " + res.error);
                }
            } catch (e) {
                console.error("Ticker delete error:", e);
            }
        }

        function jsonParseSafe(str) {
            try {
                return JSON.parse(str);
            } catch (e) {
                return null;
            }
        }

        // 초기 시작
        // 백테스트 진행률 조회 및 제어 함수 정의
        async function fetchBacktestProgress() {
            try {
                const res = await fetch('/api/backtest/progress');
                if (!res.ok) return;
                const data = await res.json();
                
                const progressSection = document.getElementById('backtest-progress-section');
                if (!progressSection) return;
                
                const stageEl = document.getElementById('bt-stage');
                const percentEl = document.getElementById('bt-percent');
                const messageEl = document.getElementById('bt-message');
                const barFillEl = document.getElementById('bt-bar-fill');
                const runBtn = document.getElementById('bt-run-btn');
                
                const pct = data.percent || 0;
                const stage = data.stage || 'idle';
                const message = data.message || '';
                
                percentEl.innerText = `${pct.toFixed(1)}%`;
                barFillEl.style.width = `${pct}%`;
                
                let stageStr = '대기 중';
                let dotColor = '#94a3b8'; // grey
                let isRunning = data.is_running;
                
                if (stage === 'collecting') {
                    stageStr = '데이터 수집 중';
                    dotColor = '#3b82f6'; // blue
                    if (isRunning === undefined) isRunning = true;
                } else if (stage === 'optimizing') {
                    stageStr = '그리드 최적화 중';
                    dotColor = '#f59e0b'; // orange
                    if (isRunning === undefined) isRunning = true;
                } else if (stage === 'done') {
                    stageStr = '최근 최적화 완료';
                    dotColor = '#10b981'; // green
                    if (isRunning === undefined) isRunning = false;
                } else if (stage === 'error') {
                    stageStr = '최적화 실패';
                    dotColor = '#ef4444'; // red
                    isRunning = false;
                }
                isRunning = !!isRunning;
                
                stageEl.innerHTML = `<span style="display:inline-block; width:8px; height:8px; background-color:${dotColor}; border-radius:50%; margin-right:6px;"></span>${stageStr}`;
                messageEl.innerText = message || (isRunning ? '진행 중...' : '대기 상태');
                
                if (isRunning) {
                    runBtn.disabled = true;
                    runBtn.innerText = '최적화 진행 중...';
                    runBtn.style.opacity = 0.6;
                    runBtn.style.cursor = 'not-allowed';
                } else {
                    runBtn.disabled = false;
                    runBtn.innerText = '최적화 즉시 실행';
                    runBtn.style.opacity = 1;
                    runBtn.style.cursor = 'pointer';
                }
            } catch (e) {
                console.error("Failed to fetch backtest progress:", e);
            }
        }
        
        async function runBacktestNow() {
            if (!confirm(`백그라운드에서 15분봉(96포인트/일) 9년 데이터 최적화를 즉시 시작하시겠습니까?
(약 10~20분 소요, 캐시 재수집 포함. 거래 엔진은 중단 없이 동작하며 WebSocket 틱마다 조건 충족 시 즉시 매매합니다)`)) return;
            try {
                const res = await fetch('/api/backtest/run-now', { method: 'POST' });
                const data = await res.json();
                alert(data.message || "최적화를 시작했습니다.");
                fetchBacktestProgress();
            } catch (e) {
                alert("최적화 요청 중 오류가 발생했습니다.");
                console.error(e);
            }
        }

        ensureSystemTickerCards();
        loadAllConfigs();
        connectWebSocket();
        
        // 백테스트 진행률 주기적 체크 (4초 주기)
        fetchBacktestProgress();
        setInterval(fetchBacktestProgress, 4000);

        // 실시간 사용자 조작 감지 이벤트 바인딩
        const strategyPanel = document.getElementById('strategy-panel');
        if (strategyPanel) {
            strategyPanel.addEventListener('input', updateConfigStatusBadge);
            strategyPanel.addEventListener('change', updateConfigStatusBadge);
        }

        // ══════════════════════════════════════════════════════════════
        // 하락장 전용 최적화 전략 제어 및 시각화 연동
        // ══════════════════════════════════════════════════════════════
        function formatStrategyBadge(logic) {
            if (logic === 'CUSTOM_BEAR') return 'CustomBear';
            return `${logic} 결합`;
        }

        function getTickerLiveRegime(ticker) {
            const config = tickerConfigs[ticker];
            if (config && config.current_regime) return config.current_regime;
            const regimeEl = document.getElementById(`regime-${ticker}`);
            if (!regimeEl) return 'RANGE';
            const text = regimeEl.innerText || '';
            if (text.includes('BULL')) return 'BULL';
            if (text.includes('BEAR')) return 'BEAR';
            return 'RANGE';
        }

        function updateTickerStrategyBadge(ticker, logic) {
            const badgeEl = document.getElementById(`badge-text-${ticker}`);
            if (badgeEl && logic) badgeEl.innerText = formatStrategyBadge(logic);
        }

        function formatHoldDuration(hours, days) {
            if (!hours || hours <= 0) return '-';
            if (hours < 24) return `약 ${hours.toFixed(1)}시간`;
            if (days < 7) return `약 ${days.toFixed(1)}일 (${hours.toFixed(0)}h)`;
            const weeks = days / 7;
            return `약 ${weeks.toFixed(1)}주 (${days.toFixed(1)}일)`;
        }

        function formatRiskLabel(risk, logic) {
            if (logic === 'CUSTOM_BEAR') {
                return 'CustomBear 내장 (트레일링·손절·시간청산)';
            }
            if (!risk || risk.type === 'None') return 'None (미적용)';
            if (risk.take_profit_pct >= 5) {
                return `StopLoss SL ${((risk.stop_loss_pct || 0) * 100).toFixed(1)}% (익절: 전략 내장)`;
            }
            if (risk.type === 'StopLoss') {
                const sl = ((risk.stop_loss_pct || 0) * 100).toFixed(1);
                const tp = ((risk.take_profit_pct || 0) * 100).toFixed(1);
                return `StopLoss ${sl}% / TP ${tp}%`;
            }
            if (risk.type === 'TrailingStop') {
                return `Trailing ${((risk.trail_pct || 0) * 100).toFixed(1)}%`;
            }
            return risk.type;
        }

        async function loadBacktestCompareData(ticker) {
            try {
                const comparePanel = document.getElementById('bear-compare-panel');
                if (!comparePanel) return;

                const liveRegime = getTickerLiveRegime(ticker);
                const response = await fetch('/api/backtest/results');
                const resData = await response.json();
                
                if (resData.status !== 'ok' || !resData.data) {
                    comparePanel.style.display = 'none';
                    return;
                }

                const market = `KRW-${ticker}`;
                const optData = resData.data[market];
                if (!optData) {
                    comparePanel.style.display = 'none';
                    return;
                }

                comparePanel.style.display = 'block';

                if (optData.BULL && optData.BULL.backtest) {
                    const b = optData.BULL.backtest;
                    const bProfits = optData.BULL.period_expected_profits || { "3m": 0, "6m": 0 };
                    document.getElementById('bull-ret').innerText = `${b.total_return_pct >= 0 ? '+' : ''}${b.total_return_pct.toFixed(2)}%`;
                    document.getElementById('bull-sharpe').innerText = b.sharpe_ratio.toFixed(4);
                    document.getElementById('bull-mdd').innerText = `-${b.mdd_pct.toFixed(1)}%`;
                    document.getElementById('bull-trades').innerText = `${b.trade_count}회 / ${b.win_rate_pct.toFixed(1)}%`;
                    document.getElementById('bull-hold').innerText = formatHoldDuration(b.avg_hold_hours, b.avg_hold_days);
                    document.getElementById('bull-risk').innerText = formatRiskLabel(optData.BULL.risk, optData.BULL.logic);
                    document.getElementById('bull-expected').innerText = `3m: +${bProfits["3m"]}% / 6m: +${bProfits["6m"]}%`;
                }

                if (optData.RANGE && optData.RANGE.backtest) {
                    const r = optData.RANGE.backtest;
                    const rProfits = optData.RANGE.period_expected_profits || { "3m": 0, "6m": 0 };
                    document.getElementById('range-ret').innerText = `${r.total_return_pct >= 0 ? '+' : ''}${r.total_return_pct.toFixed(2)}%`;
                    document.getElementById('range-sharpe').innerText = r.sharpe_ratio.toFixed(4);
                    document.getElementById('range-mdd').innerText = `-${r.mdd_pct.toFixed(1)}%`;
                    document.getElementById('range-trades').innerText = `${r.trade_count}회 / ${r.win_rate_pct.toFixed(1)}%`;
                    document.getElementById('range-hold').innerText = formatHoldDuration(r.avg_hold_hours, r.avg_hold_days);
                    document.getElementById('range-risk').innerText = formatRiskLabel(optData.RANGE.risk, optData.RANGE.logic);
                    document.getElementById('range-expected').innerText = `3m: +${rProfits["3m"]}% / 6m: +${rProfits["6m"]}%`;
                }

                const bearCards = comparePanel.querySelector('.bear-strategy-cards-grid');
                const bearCompareTable = document.getElementById('bear-regime-compare-table');

                if (liveRegime === 'BEAR' && optData.BEAR && optData.BEAR.mixed_strategy && optData.BEAR.custom_bear_strategy) {
                    if (bearCards) bearCards.style.display = '';
                    if (bearCompareTable) bearCompareTable.style.display = '';

                    const mixed = optData.BEAR.mixed_strategy;
                    const custom = optData.BEAR.custom_bear_strategy;
                    const mixedProfits = mixed.period_expected_profits || {};
                    const customProfits = custom.period_expected_profits || {};

                    fillBearProfitHorizons('mixed', mixedProfits, mixed.backtest);
                    fillBearProfitHorizons('custom', customProfits, custom.backtest);

                    const reasonEl = document.getElementById('bear-auto-pick-reason');
                    const cfg = tickerConfigs[activeTicker];
                    if (reasonEl && cfg?.bear_auto_pick_reason) {
                        reasonEl.innerText = `선택 근거: ${cfg.bear_auto_pick_reason}`;
                    }

                    document.getElementById('compare-mixed-ret').innerText = `${mixed.backtest.total_return_pct >= 0 ? '+' : ''}${mixed.backtest.total_return_pct.toFixed(2)}%`;
                    document.getElementById('compare-custom-ret').innerText = `${custom.backtest.total_return_pct >= 0 ? '+' : ''}${custom.backtest.total_return_pct.toFixed(2)}%`;
                    document.getElementById('compare-mixed-sharpe').innerText = mixed.backtest.sharpe_ratio.toFixed(4);
                    document.getElementById('compare-custom-sharpe').innerText = custom.backtest.sharpe_ratio.toFixed(4);
                    document.getElementById('compare-mixed-mdd').innerText = `-${mixed.backtest.mdd_pct.toFixed(1)}%`;
                    document.getElementById('compare-custom-mdd').innerText = `-${custom.backtest.mdd_pct.toFixed(1)}%`;
                    document.getElementById('compare-mixed-trades').innerText = `${mixed.backtest.trade_count}회 / ${mixed.backtest.win_rate_pct.toFixed(1)}%`;
                    document.getElementById('compare-custom-trades').innerText = `${custom.backtest.trade_count}회 / ${custom.backtest.win_rate_pct.toFixed(1)}%`;
                    document.getElementById('compare-mixed-hold').innerText = formatHoldDuration(
                        mixed.backtest.avg_hold_hours, mixed.backtest.avg_hold_days);
                    document.getElementById('compare-custom-hold').innerText = formatHoldDuration(
                        custom.backtest.avg_hold_hours, custom.backtest.avg_hold_days);
                    document.getElementById('compare-mixed-risk').innerText = formatRiskLabel(mixed.risk, mixed.logic);
                    document.getElementById('compare-custom-risk').innerText = formatRiskLabel(custom.risk, 'CUSTOM_BEAR');
                } else {
                    if (bearCards) bearCards.style.display = 'none';
                    if (bearCompareTable) bearCompareTable.style.display = 'none';
                }
            } catch (e) {
                console.error("Failed to load backtest compare data:", e);
            }
        }

        function syncBearStrategyUI(ticker) {
            const config = tickerConfigs[ticker];
            if (!config) return;
            
            const selected = config.selected_bear_strategy || 'auto';
            const resolved = config.resolved_bear_strategy || 'custom_bear';
            
            const cardAuto = document.getElementById('bear-card-auto');
            const cardMixed = document.getElementById('bear-card-mixed');
            const cardCustom = document.getElementById('bear-card-custom');
            const badgeAuto = document.getElementById('badge-auto');
            const badgeMixed = document.getElementById('badge-mixed');
            const badgeCustom = document.getElementById('badge-custom');
            const resolvedLabel = document.getElementById('bear-resolved-label');
            const reasonEl = document.getElementById('bear-auto-pick-reason');

            const resolvedName = resolved === 'mixed' ? '3대 지표 결합 (Mixed)' : 'CustomBear';
            if (resolvedLabel) resolvedLabel.innerText = resolvedName;
            if (reasonEl) {
                reasonEl.innerText = config.bear_auto_pick_reason
                    ? `선택 근거: ${config.bear_auto_pick_reason}`
                    : (selected === 'auto' ? '선택 근거: 백테스트 갱신 후 표시됩니다.' : '');
            }

            if (cardAuto) cardAuto.className = selected === 'auto' ? 'bear-strategy-card selected' : 'bear-strategy-card';
            if (cardMixed) cardMixed.className = (selected === 'mixed' || (selected === 'auto' && resolved === 'mixed')) ? 'bear-strategy-card selected' : 'bear-strategy-card';
            if (cardCustom) cardCustom.className = (selected === 'custom_bear' || (selected === 'auto' && resolved === 'custom_bear')) ? 'bear-strategy-card selected' : 'bear-strategy-card';

            if (badgeAuto) badgeAuto.style.display = selected === 'auto' ? 'inline-block' : 'none';
            if (badgeMixed) badgeMixed.style.display = (selected === 'mixed' || (selected === 'auto' && resolved === 'mixed')) ? 'inline-block' : 'none';
            if (badgeCustom) badgeCustom.style.display = (selected === 'custom_bear' || (selected === 'auto' && resolved === 'custom_bear')) ? 'inline-block' : 'none';

            if (selected === 'auto') {
                if (badgeMixed && resolved === 'mixed') badgeMixed.innerText = 'AI 선택';
                if (badgeCustom && resolved === 'custom_bear') badgeCustom.innerText = 'AI 선택';
            } else {
                if (badgeMixed) badgeMixed.innerText = '적용 중';
                if (badgeCustom) badgeCustom.innerText = '적용 중';
            }
        }

        async function selectBearStrategy(strategy) {
            if (!activeTicker) return;
            try {
                const response = await fetch('/api/config/select-bear-strategy', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker: activeTicker, strategy: strategy })
                });
                const data = await response.json();
                if (data.success) {
                    if (tickerConfigs[activeTicker]) {
                        tickerConfigs[activeTicker].selected_bear_strategy = strategy;
                        if (data.resolved_bear_strategy) {
                            tickerConfigs[activeTicker].resolved_bear_strategy = data.resolved_bear_strategy;
                        }
                    }
                    syncBearStrategyUI(activeTicker);
                    loadConfigForTicker(activeTicker);
                    const labels = { auto: 'AI 자동', custom_bear: 'CustomBear', mixed: 'Mixed(3대지표)' };
                    addLog(formatKstTime(), activeTicker, 'SYSTEM', `${activeTicker} 하락장 전략을 ${labels[strategy] || strategy}으로 변경 적용했습니다.`);
                } else {
                    alert("전략 변경 실패: " + data.error);
                }
            } catch (e) {
                console.error("Failed to select bear strategy:", e);
            }
        }
    </script>
</body>
</html>
"""

def load_dynamic_factory_defaults():
    results = get_latest_results()
    fallback_defaults = {
        "BULL": {
            "logic": "OR",
            "threshold": 0.5,
            "strategies": {
                "RSI": {"enabled": True, "weight": 1.0, "period": 5, "oversold": 38, "overbought": 65},
                "Bollinger": {"enabled": False, "weight": 1.0, "period": 5, "std_dev": 1.5},
                "MACD": {"enabled": True, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
            },
            "risk": {"type": "None"}
        },
        "BEAR": {
            "logic": "OR",
            "threshold": 0.5,
            "strategies": {
                "RSI": {"enabled": True, "weight": 1.0, "period": 5, "oversold": 25, "overbought": 60},
                "Bollinger": {"enabled": True, "weight": 1.0, "period": 10, "std_dev": 2.0},
                "MACD": {"enabled": False, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
            },
            "risk": {"type": "StopLoss", "stop_loss_pct": 0.02, "take_profit_pct": 0.03}
        },
        "RANGE": {
            "logic": "OR",
            "threshold": 0.5,
            "strategies": {
                "RSI": {"enabled": False, "weight": 1.0, "period": 5, "oversold": 35, "overbought": 65},
                "Bollinger": {"enabled": True, "weight": 1.0, "period": 5, "std_dev": 1.5},
                "MACD": {"enabled": False, "weight": 1.0, "fast": 5, "slow": 10, "signal_period": 3}
            },
            "risk": {"type": "StopLoss", "stop_loss_pct": 0.025, "take_profit_pct": 0.03}
        }
    }
    
    defaults = {t: fallback_defaults for t in SUPPORTED_TICKERS}
    
    if results:
        for market in [f"KRW-{t}" for t in SUPPORTED_TICKERS]:
            ticker = market.replace("KRW-", "")
            if market in results:
                ticker_data = {}
                for regime in ["BULL", "BEAR", "RANGE"]:
                    reg_data = results[market].get(regime)
                    if reg_data:
                        if regime == "BEAR":
                            selected = active_ticker_configs.get(ticker, {}).get("selected_bear_strategy", DEFAULT_BEAR_STRATEGY)
                            if selected == "auto":
                                pick = active_ticker_configs.get(ticker, {}).get("bear_auto_pick")
                                if not pick and "custom_bear_strategy" in reg_data and "mixed_strategy" in reg_data:
                                    pick, _reason = _compute_bear_auto_pick(
                                        reg_data,
                                        active_ticker_configs.get(ticker, {}).get("cycle_analysis"),
                                    )
                                pick = pick or "custom_bear"
                                if pick == "custom_bear" and "custom_bear_strategy" in reg_data:
                                    reg_data = reg_data["custom_bear_strategy"]
                                elif pick == "mixed" and "mixed_strategy" in reg_data:
                                    reg_data = reg_data["mixed_strategy"]
                            elif selected == "custom_bear" and "custom_bear_strategy" in reg_data:
                                reg_data = reg_data["custom_bear_strategy"]
                            elif selected == "mixed" and "mixed_strategy" in reg_data:
                                reg_data = reg_data["mixed_strategy"]
                        
                        strats_src = reg_data.get("strategies", {})
                        strats_dest = {}
                        for s_name, s_src in strats_src.items():
                            if s_name == "RSI":
                                strats_dest["RSI"] = {
                                    "enabled": s_src.get("enabled", False),
                                    "weight": s_src.get("weight", 1.0),
                                    "period": s_src.get("period", 14),
                                    "oversold": s_src.get("oversold", 30),
                                    "overbought": s_src.get("overbought", 70)
                                }
                            elif s_name == "Bollinger":
                                strats_dest["Bollinger"] = {
                                    "enabled": s_src.get("enabled", False),
                                    "weight": s_src.get("weight", 1.0),
                                    "period": s_src.get("period", 20),
                                    "std_dev": s_src.get("std_dev", 2.0)
                                }
                            elif s_name == "MACD":
                                strats_dest["MACD"] = {
                                    "enabled": s_src.get("enabled", False),
                                    "weight": s_src.get("weight", 1.0),
                                    "fast": s_src.get("fast", 12),
                                    "slow": s_src.get("slow", 26),
                                    "signal_period": s_src.get("signal_period", 9)
                                }
                            elif s_name == "CustomBear":
                                strats_dest["CustomBear"] = {
                                    "enabled": s_src.get("enabled", False),
                                    "weight": s_src.get("weight", 1.0),
                                    "lookback": s_src.get("lookback", 8),
                                    "drop_pct": s_src.get("drop_pct", 0.05),
                                    "volume_ratio": s_src.get("volume_ratio", 2.0),
                                    "trail_pct": s_src.get("trail_pct", 0.015),
                                    "stop_loss": s_src.get("stop_loss", 0.015),
                                    "time_cut": s_src.get("time_cut", 24)
                                }
                        ticker_data[regime] = {
                            "logic": reg_data.get("logic", "OR"),
                            "threshold": reg_data.get("threshold", 0.5),
                            "strategies": strats_dest,
                            "risk": reg_data.get("risk", {"type": "None"})
                        }
                    else:
                        ticker_data[regime] = fallback_defaults[regime]
                defaults[ticker] = ticker_data
    return defaults

@app.get("/")
async def get_dashboard():
    import re
    defaults = load_dynamic_factory_defaults()
    defaults_json = json.dumps(defaults, ensure_ascii=False, indent=12)
    defaults_js = f"const FACTORY_DEFAULTS = {defaults_json};"
    
    pattern = r"/\* DYNAMIC_FACTORY_DEFAULTS_START \*/.*?/\* DYNAMIC_FACTORY_DEFAULTS_END \*/"
    replacement = f"/* DYNAMIC_FACTORY_DEFAULTS_START */\n        {defaults_js}\n        /* DYNAMIC_FACTORY_DEFAULTS_END */"
    
    modified_html = re.sub(pattern, replacement, HTML_CONTENT, flags=re.DOTALL)
    return HTMLResponse(modified_html)

@app.websocket("/ws/trading-status")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ══════════════════════════════════════════════════════════════
# 8. 백그라운드 다중 종목 복합 전략 엔진 가동
# ══════════════════════════════════════════════════════════════
def start_composite_engine():
    global engine_instance
    init_bal = active_ticker_configs.get("initial_balance", 10_000_000)
    if isinstance(init_bal, dict):
        init_bal = init_bal.get("value", 10_000_000)
    try:
        init_bal = int(init_bal)
    except Exception:
        init_bal = 10_000_000
    account = BithumbRealAccountManager(initial_balance=init_bal)
    engine = MultiTickerUIEngine(account)
    account.engine_ref = engine
    
    # active_ticker_configs 기반으로 초기 종목 및 전략 동적 등록
    for ticker, config in active_ticker_configs.items():
        if ticker in ["initial_balance", "global_settings"]:
            continue
            
        engine.register_ticker(ticker, config)
        
    # 엔진 스레드 구동
    engine.start()
    account.start_sync()
    print("[UI Server] 다중 타임프레임 및 장세 감지형 복합 전략 엔진 가동 (실시간 잔고 동기화 포함).")
    
    # 빗썸 다중 웹소켓 연결
    markets = [f"KRW-{t}" for t in active_ticker_configs.keys() if t not in ["initial_balance", "global_settings"]]
    ws_listener = MultiTickerWebSocketListener(engine.dispatcher, markets=markets)
    ws_listener.start()
    engine.ws_listener = ws_listener # 엔진 객체에 바인딩하여 dynamic subscribe 지원
    print("[UI Server] 빗썸 실시간 다중 마켓 피드 가동.")
    
    engine_instance = engine


# ─────────────────────────────────────────────────────────────────────────────
# 백테스팅 일일 스케줄러
# ─────────────────────────────────────────────────────────────────────────────
_optimizer_running = False
_optimizer_thread: threading.Thread = None

def _run_optimizer_bg():
    global _optimizer_running
    _optimizer_running = True
    print("[DailyOptimizer] 백테스팅 최적화 시작 (백그라운드)")
    try:
        result = run_optimization()
        
        # 최적화 완료 후 엔진 설정 및 워커 전략 핫스왑 동적 적용
        try:
            update_configs_and_apply_to_engine()
        except Exception as apply_err:
            print(f"[DailyOptimizer] 최적화 결과 엔진 적용 중 오류: {apply_err}")

        msg = {"type": "optimizer_done", "message": "일일 백테스팅 최적화 완료", "updated_at": datetime.now(timezone(timedelta(hours=9))).isoformat()}
        ui_event_queue.put_nowait(json.dumps(msg))
        print("[DailyOptimizer] 최적화 완료 — UI 알림 전송")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[DailyOptimizer] 최적화 중 오류: {e}")
        if BACKTEST_AVAILABLE and _bt_mod:
            _bt_mod._save_progress("error", 0, f"최적화 실패: {e}")
    finally:
        _optimizer_running = False

def _daily_optimizer_scheduler():
    """매일 새벽 02:00 KST에 자동 최적화 실행."""
    print("[DailyOptimizer] 스케줄러 시작 (매일 02:00 KST 자동 최적화)")
    while True:
        try:
            _KST = timezone(timedelta(hours=9))
            now = datetime.now(_KST)
            # 오늘 02:00 타겟
            target = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_sec = (target - now).total_seconds()
            print(f"[DailyOptimizer] 다음 실행까지 {wait_sec/3600:.1f}시간 대기 ({target.strftime('%Y-%m-%d %H:%M')} KST)")
            time.sleep(wait_sec)
            if BACKTEST_AVAILABLE and not _optimizer_running:
                t = threading.Thread(target=_run_optimizer_bg, daemon=True, name="OptimizerBG")
                t.start()
        except Exception as e:
            print(f"[DailyOptimizer] 스케줄러 오류: {e}")
            time.sleep(60)

# ─────────────────────────────────────────────────────────────────────────────
# 백테스팅 API 엔드포인트
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/backtest/results")
def api_backtest_results():
    """최신 백테스팅 최적화 결과 반환."""
    result = get_latest_results()
    if result is None:
        return {"status": "no_data", "message": "아직 최적화 결과가 없습니다. 처음 실행 시 약 5~20분 소요됩니다.",
                "available": BACKTEST_AVAILABLE}
    return {"status": "ok", "data": result, "available": BACKTEST_AVAILABLE}

@app.post("/api/backtest/run-now")
def api_backtest_run_now():
    """즉시 백테스팅 최적화 트리거 (비동기 백그라운드 실행)."""
    global _optimizer_running, _optimizer_thread
    if not BACKTEST_AVAILABLE:
        return {"success": False, "message": "backtester.py 모듈을 찾을 수 없습니다."}
    if _optimizer_running:
        return {"success": False, "message": "이미 최적화가 실행 중입니다. 완료 후 다시 시도하세요."}
    _optimizer_thread = threading.Thread(target=_run_optimizer_bg, daemon=True, name="OptimizerBG")
    _optimizer_thread.start()
    return {"success": True, "message": "백테스팅 최적화가 백그라운드에서 시작되었습니다."}

@app.get("/api/backtest/progress")
def api_backtest_progress():
    """백테스팅 진행 상태 반환."""
    prog = get_progress()
    prog["is_running"] = _optimizer_running
    prog["available"]  = BACKTEST_AVAILABLE
    return prog


@app.on_event("startup")
def startup_event():
    asyncio.create_task(ui_event_broadcaster())
    start_composite_engine()
    # 백테스팅 일일 스케줄러 시작
    if BACKTEST_AVAILABLE:
        sched_thread = threading.Thread(target=_daily_optimizer_scheduler, daemon=True, name="DailyOptimizerScheduler")
        sched_thread.start()
        print("[DailyOptimizer] 일일 스케줄러 가동 완료 (매일 02:00 KST)")

if __name__ == "__main__":
    import uvicorn
    # 검증을 위한 로컬 서버 가동 (127.0.0.1:8006)
    uvicorn.run(app, host="127.0.0.1", port=8006)

import os
import sys
import json
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
import sys
import importlib

# ── 백테스팅 모듈 임포트 (같은 폴더) ──
try:
    _bt_spec = importlib.util.spec_from_file_location(
        "backtester",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtester.py")
    )
    _bt_mod = importlib.util.module_from_spec(_bt_spec)
    _bt_spec.loader.exec_module(_bt_mod)
    run_optimization    = _bt_mod.run_optimization
    get_latest_results  = _bt_mod.get_latest_results
    get_progress        = _bt_mod.get_progress
    BACKTEST_AVAILABLE  = True
    print("[Backtester] 모듈 로드 성공")
except Exception as _bt_err:
    BACKTEST_AVAILABLE = False
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
from regime_detector import MarketRegimeDetector

# 글로벌 설정 및 엔진 인스턴스 홀더
active_ticker_configs = {
    "BTC": {
        "active": True,
        "current_regime": "BEAR",
        "regime_override": "AUTO",
        "selected_bear_strategy": "custom_bear",
        "long_term_ma_period": 250,
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
                    {"name": "CustomBear", "enabled": True, "weight": 1.0, "timeframe": "30m", "params": {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "take_profit": 0.02, "stop_loss": 0.015, "time_cut": 24}}
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
        "selected_bear_strategy": "custom_bear",
        "long_term_ma_period": 250,
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
                    {"name": "CustomBear", "enabled": True, "weight": 1.0, "timeframe": "30m", "params": {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "take_profit": 0.02, "stop_loss": 0.015, "time_cut": 24}}
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
        "selected_bear_strategy": "custom_bear",
        "long_term_ma_period": 250,
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
                    {"name": "CustomBear", "enabled": True, "weight": 1.0, "timeframe": "30m", "params": {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "take_profit": 0.02, "stop_loss": 0.015, "time_cut": 24}}
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
}
engine_instance = None

CONFIG_FILE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 
    "strategy_config.json"
))

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

def save_persisted_configs():
    try:
        with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(active_ticker_configs, f, indent=4, ensure_ascii=False)
            print(f"[Storage] 설정을 {CONFIG_FILE_PATH}에 백업 저장했습니다.")
    except Exception as e:
        print(f"[Storage] 설정 파일 저장 실패: {e}")

def update_configs_with_optimized_params():
    global active_ticker_configs
    results = get_latest_results()
    if not results:
        return
        
    updated = False
    for market in ["KRW-BTC", "KRW-ETH", "KRW-XRP"]:
        ticker = market.replace("KRW-", "")
        if market not in results or ticker not in active_ticker_configs:
            continue
            
        for regime in ["BULL", "BEAR", "RANGE"]:
            opt_reg = results[market].get(regime)
            if not opt_reg:
                continue
                
            # BEAR 장세인 경우 selected_bear_strategy 에 맞춰 추출
            if regime == "BEAR":
                selected = active_ticker_configs[ticker].get("selected_bear_strategy", "custom_bear")
                if selected == "custom_bear" and "custom_bear_strategy" in opt_reg:
                    opt_reg = opt_reg["custom_bear_strategy"]
                elif selected == "mixed" and "mixed_strategy" in opt_reg:
                    opt_reg = opt_reg["mixed_strategy"]
                    
            opt_strats = opt_reg.get("strategies", {})
            new_strategies = []
            for s_name, s_data in opt_strats.items():
                if s_name == "RSI":
                    tf = "5m"
                    params = {"period": s_data.get("period", 14), "oversold": s_data.get("oversold", 30), "overbought": s_data.get("overbought", 70)}
                elif s_name == "Bollinger":
                    tf = "D"
                    params = {"period": s_data.get("period", 20), "std_dev": s_data.get("std_dev", 2.0)}
                elif s_name == "MACD":
                    tf = "1h"
                    params = {"fast": s_data.get("fast", 12), "slow": s_data.get("slow", 26), "signal_period": s_data.get("signal_period", 9)}
                elif s_name == "CustomBear":
                    tf = "30m"
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
            
            risk_info = opt_reg.get("risk", {"type": "None"})
            
            active_ticker_configs[ticker]["tactics"][regime] = {
                "logic": opt_reg.get("logic", "OR"),
                "threshold": opt_reg.get("threshold", 0.5),
                "strategies": new_strategies,
                "risk": risk_info
            }
            updated = True
            
    if updated:
        save_persisted_configs()
        print("[Storage] 최적화 파라미터가 최신 결과에 맞춰 강제 갱신/병합되었습니다.")
                
    if updated:
        save_persisted_configs()
        print("[Storage] 최적화 파라미터가 기존 설정에 자동 병합 및 반영되었습니다.")

def update_configs_and_apply_to_engine():
    update_configs_with_optimized_params()
    if engine_instance:
        for ticker in ["BTC", "ETH", "XRP"]:
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

        if curr_rsi < self.params["oversold"]: return "BUY"
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
        price = prices[-1]

        data["bb_val"] = f"U:{curr_upper:.1f}/L:{curr_lower:.1f}"

        if price < curr_lower: return "BUY"
        if price > curr_upper: return "SELL"
        return "HOLD"


class BithumbCustomBearStrategy(BaseStrategy):
    NAME = "CustomBear"
    PARAMS = {"lookback": 8, "drop_pct": 0.05, "volume_ratio": 2.0, "trail_pct": 0.015, "stop_loss": 0.015, "time_cut": 24}

    def __init__(self, candle_manager, timeframe="30m", params=None):
        super().__init__(params)
        self.candle_manager = candle_manager
        self.timeframe = timeframe
        self.entry_price = 0.0
        self.entry_time = None
        self.peak_price = 0.0

    def generate_signal(self, data: dict) -> str:
        # candle_manager로부터 30분봉 캔들 조회
        candles = self.candle_manager.get_candles(self.timeframe)
        if not candles or len(candles) < max(self.params.get("lookback", 8), 20):
            return "HOLD"

        closes = [c["close"] for c in candles]
        opens = [c["open"] for c in candles]
        volumes = [c["volume"] for c in candles]
        highs = [c.get("high", c["close"]) for c in candles]
        
        qty = data.get("position_qty", 0.0)
        
        if qty == 0.0:
            self.peak_price = 0.0
            # 매수 조건
            lookback = self.params["lookback"]
            window_closes = closes[-lookback-1:-1]
            highest = max(window_closes) if window_closes else closes[-1]
            curr_price = closes[-1]
            drop = (highest - curr_price) / highest if highest > 0 else 0
            
            vol_window = volumes[-11:-1]
            avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1.0
            curr_vol = volumes[-1]
            
            is_bullish = closes[-1] > opens[-1]
            
            buy = (drop >= self.params["drop_pct"]) and (curr_vol >= avg_vol * self.params["volume_ratio"]) and is_bullish
            
            # 실시간 지표 상태 기록
            data["custom_bear_val"] = f"낙폭:{drop*100:.1f}%(기준:{self.params['drop_pct']*100:.1f}%) / 거래량비율:{curr_vol/avg_vol:.1f}배(기준:{self.params['volume_ratio']:.1f}배)"
            
            if buy:
                self.entry_price = curr_price
                self.entry_time = datetime.now()
                self.peak_price = curr_price
                return "BUY"
        else:
            # 매도 조건 (트레일링 스톱, 손절, 시간청산)
            curr_price = closes[-1]
            if self.entry_price == 0.0:
                self.entry_price = data.get("avg_price", curr_price)
                
            if self.peak_price == 0.0:
                self.peak_price = max(self.entry_price, curr_price)
                
            # 실시간 최고가 업데이트
            curr_high = highs[-1]
            self.peak_price = max(self.peak_price, curr_high)
                
            pnl = (curr_price - self.entry_price) / self.entry_price
            drawdown = (self.peak_price - curr_price) / self.peak_price
            
            exit_ts = drawdown >= self.params["trail_pct"]
            exit_sl = pnl <= -self.params["stop_loss"]
            
            exit_tc = False
            if self.entry_time:
                elapsed_sec = (datetime.now() - self.entry_time).total_seconds()
                if elapsed_sec >= self.params["time_cut"] * 1800: # 30분봉 N개
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
    def __init__(self, strategies=None, logic="AND", weights=None, threshold=0.5):
        super().__init__(strategies, logic)
        self.weights = weights or {}
        self.threshold = threshold

    def generate_signal(self, data: Dict[str, Any]) -> str:
        signals = {}
        for s in self.strategies:
            sig = s.generate_signal(data)
            signals[s.NAME] = sig
            
        final_signal = "HOLD"
        sig_list = list(signals.values())

        if self.logic == "AND":
            if all(s == "BUY" for s in sig_list): final_signal = "BUY"
            elif any(s == "SELL" for s in sig_list): final_signal = "SELL"
        elif self.logic == "OR":
            if any(s == "BUY" for s in sig_list): final_signal = "BUY"
            elif all(s == "SELL" for s in sig_list): final_signal = "SELL"
        elif self.logic == "VOTE":
            majority = len(sig_list) // 2 + 1
            if sig_list.count("BUY") >= majority: final_signal = "BUY"
            elif sig_list.count("SELL") >= majority: final_signal = "SELL"
        elif self.logic == "WEIGHTED_VOTE":
            buy_weight = 0.0
            sell_weight = 0.0
            total_weight = 0.0
            for s in self.strategies:
                w = self.weights.get(s.NAME, 1.0)
                total_weight += w
                if signals[s.NAME] == "BUY":
                    buy_weight += w
                elif signals[s.NAME] == "SELL":
                    sell_weight += w
            
            if total_weight > 0:
                buy_ratio = buy_weight / total_weight
                sell_ratio = sell_weight / total_weight
                if buy_ratio >= self.threshold:
                    final_signal = "BUY"
                elif sell_ratio >= self.threshold:
                    final_signal = "SELL"

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

        data["composite_details"] = {
            "logic": self.logic,
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
    def __init__(self, trail_pct: float = 0.02):
        self.trail_pct = trail_pct
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
        
        if self.peak_price == 0.0:
            self.peak_price = curr_price
        else:
            self.peak_price = max(self.peak_price, curr_price)
            
        drawdown_pct = (self.peak_price - curr_price) / self.peak_price
        if drawdown_pct >= self.trail_pct and curr_price > avg_price * 0.90:
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
        self.order_amount = 6000.0  # 회당 기본 매매 주문금액 (빗썸 최소 5,000원 이상 + 여유분 반영)
        
        # 1. 백엔드 CandleManager 장착 및 웜업 구동
        self.candle_manager = CandleManager(ticker, timeframes=["1m", "5m", "1h", "D"], max_candles=4500)
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

    def check_portfolio_allocation_limit(self, buy_amount: float) -> bool:
        """
        신규 매수(buy_amount) 실행 시, 
        해당 종목의 총 가치가 전체 자산(가용원화 + 총 포지션 평가금)의 50%를 초과하는지 여부를 검증합니다.
        True: 한도 내 (허용) / False: 한도 초과 (차단)
        """
        global engine_instance
        if not engine_instance:
            return True
            
        # 1. 전체 자산 가치 계산
        krw_balance = self.account.get_balance()
        total_crypto_value = 0.0
        
        for ticker, worker in engine_instance._workers.items():
            qty = worker.position.quantity
            curr_price = worker.position.current_price
            if qty > 0 and curr_price > 0:
                total_crypto_value += qty * curr_price
                
        total_asset_value = krw_balance + total_crypto_value
        
        # 2. 현재 종목의 예상 평가 금액 (현재 포지션 평가 금액 + 추가 매수 예정 금액)
        current_position_value = self.position.quantity * self.position.current_price
        expected_position_value = current_position_value + buy_amount
        
        if total_asset_value <= 0:
            return True
            
        ratio = expected_position_value / total_asset_value
        if ratio > 0.50:
            print(f"[Portfolio Guard] {self.ticker} 매수 차단: 예상 비중 {ratio*100:.1f}%가 포트폴리오 한도(50.0%)를 초과합니다.")
            
            # UI 로그 큐에 차단 이벤트 발송
            ui_event = {
                "type": "order",
                "data": {
                    "ticker": self.ticker,
                    "signal": "HOLD",
                    "price": self.position.current_price,
                    "risk_reason": f"PORTFOLIO_LIMIT_EXCEEDED ({ratio*100:.1f}% > 50%)",
                    "timestamp": int(time.time() * 1000)
                }
            }
            ui_event_queue.put(ui_event)
            return False
            
        return True

    def switch_regime(self, regime: str, log_to_ui=True):
        """
        감지된 장세(BULL, BEAR, RANGE)에 맞춰 전략 조합 및 리스크 매니저를 실시간 전환합니다.
        """
        tactics = self.config.get("tactics", {})
        t_cfg = tactics.get(regime)
        if not t_cfg:
            print(f"[Worker-{self.ticker}] 장세 {regime}에 대응하는 작전 설정이 존재하지 않습니다.")
            return

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
                strategies.append(BithumbCustomBearStrategy(self.candle_manager, timeframe="30m", params=params))

        logic = t_cfg.get("logic", "AND")
        threshold = t_cfg.get("threshold", 0.5)

        self.strategy = VerboseCompositeStrategy(
            strategies=strategies,
            logic=logic,
            weights=weights,
            threshold=threshold
        )

        # 리스크 매니저 재생성
        risk_cfg = t_cfg.get("risk", {"type": "None"})
        r_type = risk_cfg.get("type", "None")
        if r_type == "StopLoss":
            self.risk_manager = StopLossRiskManager(
                stop_loss_pct=risk_cfg.get("stop_loss_pct", 0.03),
                take_profit_pct=risk_cfg.get("take_profit_pct", 0.06)
            )
        elif r_type == "TrailingStop":
            self.risk_manager = TrailingStopRiskManager(
                trail_pct=risk_cfg.get("trail_pct", 0.02)
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
            print(f"[Worker-{self.ticker}] 시장 국면 변경 감지 -> {regime} 작전으로 스위칭 완료! (전략: {logic}, 리스크: {r_type})")

    def _process(self, data: dict) -> None:
        # 1. 틱 정보로 CandleManager 실시간 캔들 업데이트
        self.candle_manager.update(data)
        
        self.position.current_price = data.get("price", self.position.current_price)
        timestamp = data.get("timestamp", int(time.time() * 1000))

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
                    
                    # UI 로그 전송
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
        
        # 10초 주기로 세부 판단 근거와 metrics를 새로고침하여 웹소켓 전송용으로 활용
        detailed = self.regime_detector.detect_regime_detailed()
        detected_regime = detailed["regime"]
        regime_reason = detailed["reason"]
        
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

        # 3. 갱신된 전략 세팅으로 시그널 도출
        data["position_qty"] = self.position.quantity
        data["avg_price"] = self.position.avg_price
        final_signal = self.strategy.generate_signal(data)
        composite_details = data.get("composite_details", {
            "logic": "UNKNOWN", "sub_signals": {}, "indicators": {}, "final": final_signal
        })

        # 평가 금액 및 수익률 계산
        krw_value = self.position.quantity * self.position.current_price
        pnl_pct = self.position.pnl_pct

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
                # 포트폴리오 비중 한도 검증
                if not self.check_portfolio_allocation_limit(self.order_amount):
                    return
                    
                if self.account.reserve(self.order_amount):
                    print(f"[Risk Engine] {self.ticker} 물타기 추가 매수 집행")
                    self.order_queue.put({
                        "ticker": self.ticker,
                        "signal": "BUY",
                        "price": self.position.current_price,
                        "risk_reason": risk_signal
                    })
                return

        # 6. 전략 매매 주문 처리
        if final_signal == "HOLD":
            return

        if final_signal == "BUY":
            # ★ 핵심 버그 수정: 이미 포지션을 보유 중이면 추가 BUY 금지 (물타기 전략은 리스크 매니저가 담당)
            if self.position.quantity > 0:
                return  # 이미 보유 중 - 중복 매수 방지
            
            # ★ 주문 쿨다운: 같은 방향 주문이 30초 이내에 연속 발생하는 것 방지
            now_ts = time.time()
            last_order_ts = getattr(self, "_last_order_ts", 0)
            last_order_signal = getattr(self, "_last_order_signal", "")
            if last_order_signal == "BUY" and (now_ts - last_order_ts) < 30:
                return  # 30초 쿨다운 중
            
            # 포트폴리오 비중 한도 검증
            if not self.check_portfolio_allocation_limit(self.order_amount):
                return
                
            if not self.account.reserve(self.order_amount):
                return  # 잔고 부족

        elif final_signal == "SELL":
            # ★ 포지션 없으면 매도 불필요
            if self.position.quantity <= 0:
                return
            
            # ★ 매도 쿨다운: 30초 이내 연속 매도 방지
            now_ts = time.time()
            last_order_ts = getattr(self, "_last_order_ts", 0)
            last_order_signal = getattr(self, "_last_order_signal", "")
            if last_order_signal == "SELL" and (now_ts - last_order_ts) < 30:
                return  # 30초 쿨다운 중

        # 주문 타임스탬프 및 방향 기록 (쿨다운 기준)
        self._last_order_ts = time.time()
        self._last_order_signal = final_signal

        self.order_queue.put({
            "ticker": self.ticker,
            "signal": final_signal,
            "price": self.position.current_price,
        })


# ══════════════════════════════════════════════════════════════
# 3. 다중 종목 실시간 웹소켓 리스너
# ══════════════════════════════════════════════════════════════
class MultiTickerWebSocketListener:
    def __init__(self, dispatcher, markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"]):
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
        
        worker.regime_detector.long_term_ma_period = worker.long_term_ma_period
        
        curr_reg = worker.regime_detector.detect_regime()
        worker.switch_regime(curr_reg, log_to_ui=True)
        worker.current_regime = curr_reg
        
        print(f"[Engine] {ticker} 설정 변경 및 장세 작전 재적용 완료: {curr_reg}")
        return True

    def add_new_ticker(self, ticker: str) -> bool:
        if ticker in self._workers:
            return False
            
        if ticker not in active_ticker_configs:
            active_ticker_configs[ticker] = {
                "active": True,
                "current_regime": "BEAR",
                "regime_override": "AUTO",
                "long_term_ma_period": 250,
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
                        "logic": "OR",
                        "threshold": 0.5,
                        "strategies": [
                            {"name": "RSI", "enabled": True, "weight": 1.0, "timeframe": "5m", "params": {"period": 5, "oversold": 25, "overbought": 60}},
                            {"name": "Bollinger", "enabled": True, "weight": 1.0, "timeframe": "15m", "params": {"period": 10, "std_dev": 2.0}},
                            {"name": "MACD", "enabled": False, "weight": 1.0, "timeframe": "1h", "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                        ],
                        "risk": {"type": "StopLoss", "stop_loss_pct": 0.02, "take_profit_pct": 0.03}
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
# 5. REST API: 빗썸 과거 캔들 조회 및 포맷 정규화
# ══════════════════════════════════════════════════════════════
@app.get("/api/candles")
def get_historical_candles(
    market: str = Query(..., description="예: KRW-BTC, KRW-ETH"),
    timeframe: str = Query(..., description="1m, 3m, 5m, 10m, 15m, 30m, 1h, 4h, D, W, M")
):
    base_url = "https://api.bithumb.com/v1"
    url = ""
    params = {"market": market, "count": 120}
    
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

    try:
        resp = requests.get(url, params=params, timeout=5)
        if resp.status_code != 200:
            return {"error": f"Bithumb API Error: {resp.text}"}
        
        raw_candles = resp.json()
        formatted_candles = []
        
        for c in reversed(raw_candles):
            if (timeframe in ["D", "W", "M"]):
                dt = datetime.strptime(c["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
                t_val = int(dt.replace(tzinfo=timezone.utc).timestamp())
            else:
                t_val = c["timestamp"] // 1000
                
            formatted_candles.append({
                "time": t_val,
                "open": float(c["opening_price"]),
                "high": float(c["high_price"]),
                "low": float(c["low_price"]),
                "close": float(c["trade_price"]),
                "volume": float(c["candle_acc_trade_volume"])
            })
            
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
    return active_ticker_configs

@app.post("/api/config/select-bear-strategy")
def select_bear_strategy(payload: dict):
    ticker = payload.get("ticker")
    strategy = payload.get("strategy") # "custom_bear" or "mixed"
    if not ticker or not strategy:
        return {"success": False, "error": "Ticker or strategy missing"}
        
    if ticker not in active_ticker_configs:
        return {"success": False, "error": "Ticker not found"}
        
    if strategy not in ["custom_bear", "mixed"]:
        return {"success": False, "error": "Invalid strategy type"}
        
    active_ticker_configs[ticker]["selected_bear_strategy"] = strategy
    save_persisted_configs()
    update_configs_and_apply_to_engine()
    return {"success": True, "message": f"{ticker}의 하락장 전략이 {strategy}로 변경 및 적용되었습니다."}

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
        }

        .ticker-card {
            background: var(--bg-sidebar);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 20px;
            cursor: pointer;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
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
                        <button class="tf-btn active" id="tf-1m" onclick="changeTimeframe('1m')">1분</button>
                        <button class="tf-btn" id="tf-3m" onclick="changeTimeframe('3m')">3분</button>
                        <button class="tf-btn" id="tf-5m" onclick="changeTimeframe('5m')">5분</button>
                        <button class="tf-btn" id="tf-10m" onclick="changeTimeframe('10m')">10분</button>
                        <button class="tf-btn" id="tf-15m" onclick="changeTimeframe('15m')">15분</button>
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
                                <option value="AND">AND (모든 전략 합의)</option>
                                <option value="OR">OR (최소 한 전략 진입)</option>
                                <option value="VOTE">VOTE (단순 다수결)</option>
                                <option value="WEIGHTED_VOTE">WEIGHTED VOTE (가중치 투표)</option>
                            </select>
                            <div class="logic-desc" id="logic-desc-text">활성화된 모든 개별 전략이 일치할 때 매수/매수 주문을 넣습니다.</div>
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
                        <div class="control-group">
                            <label class="control-label-large">리스크 관리 기법</label>
                            <select id="risk-type-select" class="logic-dropdown" onchange="toggleRiskParamsDisplay(this.value)">
                                <option value="None">None (미적용)</option>
                                <option value="StopLoss">StopLoss (고정 손절/익절)</option>
                                <option value="TrailingStop">TrailingStop (트레일링 스탑)</option>
                                <option value="AveragingDown">AveragingDown (물타기)</option>
                            </select>
                            <div class="logic-desc" id="risk-desc-text">리스크 관리를 수행하지 않고 전략 신호만을 따릅니다.</div>
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
                            <label class="control-label-large">Drop Trigger (물타기 기준 하락폭 %)</label>
                            <div class="slider-val-row">
                                <input type="range" class="param-slider" id="risk-drop-trigger-pct" min="1.0" max="15" step="0.5" value="4.0" oninput="updateParamValue('risk-drop-trigger-val', this.value)">
                                <span class="param-val" id="risk-drop-trigger-val" style="font-size:14px; width:45px;">4.0</span>
                            </div>
                            <label class="control-label-large" style="margin-top: 10px;">Max Add Count (최대 물타기 횟수)</label>
                            <div class="slider-val-row">
                                <input type="range" class="param-slider" id="risk-max-add-count" min="1" max="5" step="1" value="3" oninput="updateParamValue('risk-max-add-count-val', this.value)">
                                <span class="param-val" id="risk-max-add-count-val" style="font-size:14px; width:45px;">3</span>
                            </div>
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
            
            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:16px;">
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
                            <div>1주일: <span id="mixed-profit-1w" style="font-weight:700; color:var(--accent-green);">-</span></div>
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
                            <div>1주일: <span id="custom-profit-1w" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>1개월: <span id="custom-profit-1m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>3개월: <span id="custom-profit-3m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                            <div>6개월: <span id="custom-profit-6m" style="font-weight:700; color:var(--accent-green);">-</span></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 백테스팅 과거 데이터 하락장 성과 비교 보고서 -->
            <div style="margin-top:12px; background:rgba(0,0,0,0.2); border-radius:8px; padding:12px;">
                <div style="font-size:12px; font-weight:700; color:#fff; margin-bottom:8px;">과거 9년 하락장(BEAR) 비교 백테스팅 성과 비교표</div>
                <table style="width:100%; border-collapse:collapse; font-size:11px; text-align:left; color:var(--text-secondary);">
                    <thead>
                        <tr style="border-bottom:1px solid rgba(255,255,255,0.1); color:#fff;">
                            <th style="padding:6px 4px;">지표</th>
                            <th style="padding:6px 4px;">기존 믹스 전략 (Mixed)</th>
                            <th style="padding:6px 4px; color:var(--accent-green);">나만의 하락장 전략 (Custom Bear)</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                            <td style="padding:6px 4px;">하락장 누적 수익률</td>
                            <td style="padding:6px 4px;" id="compare-mixed-ret">-</td>
                            <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-ret">-</td>
                        </tr>
                        <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                            <td style="padding:6px 4px;">Sharpe Ratio (위험대비수익)</td>
                            <td style="padding:6px 4px;" id="compare-mixed-sharpe">-</td>
                            <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-sharpe">-</td>
                        </tr>
                        <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                            <td style="padding:6px 4px;">최대 낙폭 (MDD)</td>
                            <td style="padding:6px 4px;" id="compare-mixed-mdd">-</td>
                            <td style="padding:6px 4px; font-weight:bold;" id="compare-custom-mdd">-</td>
                        </tr>
                        <tr>
                            <td style="padding:6px 4px;">총 거래 횟수 / 승률</td>
                            <td style="padding:6px 4px;" id="compare-mixed-trades">-</td>
                            <td style="padding:6px 4px; font-weight:bold; color:var(--accent-green);" id="compare-custom-trades">-</td>
                        </tr>
                    </tbody>
                </table>
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
        let activeTimeframe = '1m';
        
        // 캐싱 저장소
        const candleCache = {};
        const lastPrices = { BTC: 0, ETH: 0 };
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
                const isSystemTicker = ['BTC', 'ETH'].includes(ticker);
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
            addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', '로그 콘솔이 초기화되었습니다.');
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
                    addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `캔들 로드 실패: ${data.error}`);
                    chartLoaderEl.style.display = 'none';
                    return;
                }
                
                candleCache[cacheKey] = data;
                
                if (ticker === activeTicker && timeframe === activeTimeframe && candlestickSeries) {
                    candlestickSeries.setData(data);
                    chart.timeScale().fitContent();
                    addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `${ticker}의 ${timeframe} 과거 캔들 ${data.length}개 렌더링 완료.`);
                }
            } catch (e) {
                console.error("Historical candles fetch error:", e);
                addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `${ticker} ${timeframe} 과거 캔들 연동에 실패했습니다.`);
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
                candleTime = Math.floor(timestampSec / interval) * interval;
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
                addLog(new Date().toLocaleTimeString(), ticker, 'SYSTEM', `${activeTimeframe} 신규 봉 형성 완료. (${new Date(candleTime * 1000).toLocaleString()})`);
            }
            
            if (ticker === activeTicker && candlestickSeries) {
                candlestickSeries.update(targetCandle);
            }
        }

        // ══════════════════════════════════════════════════════════════
        // 종목 동적 렌더링 및 UI 스위칭
        // ══════════════════════════════════════════════════════════════
        function createTickerCardIfNotExist(ticker, logic, active) {
            if (document.getElementById(`card-${ticker}`)) return;
            
            const sidebar = document.getElementById('ticker-sidebar');
            const card = document.createElement('div');
            card.className = `ticker-card ${activeTicker === ticker ? 'active' : ''}`;
            card.id = `card-${ticker}`;
            card.onclick = (e) => {
                // 버튼이나 삭제 ✕ 영역 클릭이 아닌 경우에만 탭 이동
                if (e.target.tagName !== 'BUTTON' && !e.target.classList.contains('delete-btn')) {
                    selectTicker(ticker);
                }
            };
            
            let badgeClass = 'badge-other';
            if (ticker === 'BTC') badgeClass = 'badge-btc';
            else if (ticker === 'ETH') badgeClass = 'badge-eth';
            else if (ticker === 'XRP') badgeClass = 'badge-xrp';
            
            card.innerHTML = `
                <div class="ticker-header" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                    <span class="ticker-symbol" style="font-size:18px; font-weight:700; color:#fff;">${ticker}/KRW</span>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span class="status-indicator ${active ? 'active' : ''}" id="status-ind-${ticker}"></span>
                        <button class="control-btn" id="ctrl-btn-${ticker}" onclick="toggleTickerActive('${ticker}')" style="background:rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.1); color:#fff; border-radius:6px; font-size:10px; padding:3px 6px; cursor:pointer;">${active ? '정지' : '시작'}</button>
                        <span class="delete-btn" onclick="event.stopPropagation(); deleteTicker('${ticker}')" style="color:var(--accent-red); font-size:14px; font-weight:bold; cursor:pointer; margin-left:4px; padding:2px 4px;" title="종목 삭제">&#10006;</span>
                    </div>
                </div>
                <div class="ticker-info" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                    <span class="strategy-badge ${badgeClass}" id="badge-text-${ticker}">${logic} 결합</span>
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
                <div id="regime-reason-${ticker}" style="font-size:10px; color:var(--text-secondary); background:rgba(255,255,255,0.02); padding:6px 10px; border-radius:8px; margin-bottom:8px; line-height:1.4; border-left:2px solid var(--accent-blue); word-break:keep-all;">
                    AI 분석 대기 중...
                </div>

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
            if (candleCache[cacheKey]) {
                candlestickSeries.setData(candleCache[cacheKey]);
                chart.timeScale().fitContent();
            } else {
                loadHistoricalCandles(activeTicker, activeTimeframe);
            }
            
            // UI 설정 동기화
            loadConfigForTicker(ticker);
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
            if (candleCache[cacheKey]) {
                candlestickSeries.setData(candleCache[cacheKey]);
                chart.timeScale().fitContent();
            } else {
                loadHistoricalCandles(activeTicker, activeTimeframe);
            }
        }

        function updateChartInfo() {
            const tfText = document.getElementById(`tf-${activeTimeframe}`).innerText;
            chartTitleEl.innerText = `${activeTicker}/KRW 실시간 ${tfText} 차트`;
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
                if (logic === 'AND') desc.innerText = '활성화된 모든 개별 전략이 BUY/SELL 일치할 때 매수/매수 주문을 넣습니다.';
                else if (logic === 'OR') desc.innerText = '활성화된 개별 전략 중 단 하나라도 BUY/SELL 신호를 주면 진입합니다.';
                else if (logic === 'VOTE') desc.innerText = '활성화된 전략들의 다수결(과반수 이상 합의)을 기준으로 판단합니다.';
            }
        }

        async function changeRegimeOverride(ticker, regime) {
            const durationEl = document.getElementById(`regime-duration-${ticker}`);
            const durationDays = durationEl ? parseInt(durationEl.value) : 0;
            
            if (durationEl) {
                durationEl.style.display = (regime === 'AUTO') ? 'none' : 'inline-block';
            }
            
            addLog(new Date().toLocaleTimeString(), ticker, 'SYSTEM', `장세 강제 고정 요청 -> ${regime} (기간: ${durationDays}일)`);
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
                    addLog(new Date().toLocaleTimeString(), ticker, 'SYSTEM', `장세가 ${regime}(으)로 성공적으로 반영되었습니다.`);
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
                    addLog(new Date().toLocaleTimeString(), ticker, side, `[수동 주문 성공] 사용자가 직접 발송한 ${side} 즉시 주문이 정상 접수되었습니다.`);
                } else {
                    alert("수동 주문 실패: " + res.error);
                }
            } catch (e) {
                console.error(e);
                alert("네트워크 통신 오류가 발생했습니다.");
            }
        }

        async function loadAllConfigs() {
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
                    
                    // 장세 고정 및 기간 드롭다운 동기화
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
                
                loadConfigForTicker(activeTicker);
            } catch (e) {
                console.error("Failed to load configs:", e);
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
                        rowsHtml += `<tr><td>${s.name}</td><td>대기 중</td></tr>`;
                    }
                });
                rowsHtml += `<tr><td>종합 판단</td><td>HOLD</td></tr>`;
                tableEl.innerHTML = rowsHtml;
            }
        }

        function loadConfigForTicker(ticker) {
            const config = tickerConfigs[ticker];
            if (!config) return;
            
            const currentRegime = config.current_regime || 'BEAR';
            const tactic = config.tactics ? config.tactics[currentRegime] : null;
            if (!tactic) return;
            
            document.getElementById('logic-select').value = tactic.logic;
            toggleThresholdDisplay(tactic.logic);
            
            // 수동 장세 고정 드롭다운 값 맞춤
            const regSelect = document.getElementById(`regime-select-${ticker}`);
            if (regSelect && config.regime_override) {
                regSelect.value = config.regime_override;
            }
            
            document.getElementById('weighted-threshold').value = tactic.threshold || 0.5;
            document.getElementById('threshold-val').innerText = tactic.threshold || 0.5;

            // 리스크 설정 동기화
            const risk = tactic.risk || { type: "None" };
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
            
            // 모든 전략 카드 숨기기
            document.querySelectorAll('#strategies-tab-content .strategy-item-card').forEach(card => {
                card.style.display = 'none';
            });
            
            const strategies = tactic.strategies || [];
            strategies.forEach(s => {
                const prefix = s.name.toLowerCase();
                
                // 해당 전략 카드 보여주기
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

        function checkIfConfigIsDefault() {
            if (!activeTicker || !tickerConfigs[activeTicker]) return true;
            const currentRegime = tickerConfigs[activeTicker].current_regime || 'BEAR';
            const tickerDefaults = FACTORY_DEFAULTS[activeTicker] || FACTORY_DEFAULTS["BTC"];
            const defaults = tickerDefaults ? tickerDefaults[currentRegime] : null;
            if (!defaults) return true;

            // 1. Logic comparison
            const uiLogic = document.getElementById('logic-select').value;
            if (uiLogic !== defaults.logic) return false;

            // 2. Threshold comparison
            const uiThreshold = parseFloat(document.getElementById('weighted-threshold').value);
            if (Math.abs(uiThreshold - defaults.threshold) > 0.001) return false;

            // 3. Strategies comparison
            // RSI
            const rsiEnabled = document.getElementById('toggle-rsi').checked;
            const rsiWeight = parseFloat(document.getElementById('rsi-weight').value);
            const rsiPeriod = parseInt(document.getElementById('rsi-period').value);
            const rsiOverbought = parseInt(document.getElementById('rsi-overbought').value);
            const rsiOversold = parseInt(document.getElementById('rsi-oversold').value);
            
            const defRsi = defaults.strategies.RSI;
            if (rsiEnabled !== defRsi.enabled) return false;
            if (Math.abs(rsiWeight - defRsi.weight) > 0.001) return false;
            if (rsiPeriod !== defRsi.period) return false;
            if (rsiOverbought !== defRsi.overbought) return false;
            if (rsiOversold !== defRsi.oversold) return false;

            // Bollinger
            const bbEnabled = document.getElementById('toggle-bb').checked;
            const bbWeight = parseFloat(document.getElementById('bb-weight').value);
            const bbPeriod = parseInt(document.getElementById('bb-period').value);
            const bbStddev = parseFloat(document.getElementById('bb-stddev').value);

            const defBb = defaults.strategies.Bollinger;
            if (bbEnabled !== defBb.enabled) return false;
            if (Math.abs(bbWeight - defBb.weight) > 0.001) return false;
            if (bbPeriod !== defBb.period) return false;
            if (Math.abs(bbStddev - defBb.std_dev) > 0.001) return false;

            // MACD
            const macdEnabled = document.getElementById('toggle-macd').checked;
            const macdWeight = parseFloat(document.getElementById('macd-weight').value);
            const macdFast = parseInt(document.getElementById('macd-fast').value);
            const macdSlow = parseInt(document.getElementById('macd-slow').value);
            const macdSignal = parseInt(document.getElementById('macd-signal').value);

            const defMacd = defaults.strategies.MACD;
            if (macdEnabled !== defMacd.enabled) return false;
            if (Math.abs(macdWeight - defMacd.weight) > 0.001) return false;
            if (macdFast !== defMacd.fast) return false;
            if (macdSlow !== defMacd.slow) return false;
            if (macdSignal !== defMacd.signal_period) return false;

            // 4. Risk comparison
            const uiRiskType = document.getElementById('risk-type-select').value;
            if (uiRiskType !== defaults.risk.type) return false;

            if (uiRiskType === 'StopLoss') {
                const uiStopLoss = parseFloat(document.getElementById('risk-stoploss-pct').value) / 100;
                const uiTakeProfit = parseFloat(document.getElementById('risk-takeprofit-pct').value) / 100;
                if (Math.abs(uiStopLoss - defaults.risk.stop_loss_pct) > 0.001) return false;
                if (Math.abs(uiTakeProfit - defaults.risk.take_profit_pct) > 0.001) return false;
            } else if (uiRiskType === 'TrailingStop') {
                const uiTrail = parseFloat(document.getElementById('risk-trail-pct').value) / 100;
                if (Math.abs(uiTrail - defaults.risk.trail_pct) > 0.001) return false;
            } else if (uiRiskType === 'AveragingDown') {
                const uiDrop = parseFloat(document.getElementById('risk-drop-trigger-pct').value) / 100;
                const uiMaxAdd = parseInt(document.getElementById('risk-max-add-count').value);
                if (Math.abs(uiDrop - defaults.risk.drop_trigger_pct) > 0.001) return false;
                if (uiMaxAdd !== defaults.risk.max_add_count) return false;
            }

            return true;
        }

        function updateConfigStatusBadge() {
            const badge = document.getElementById('config-status-badge');
            if (!badge) return;
            const isDefault = checkIfConfigIsDefault();
            if (isDefault) {
                badge.className = 'status-badge-inline default';
                badge.innerHTML = '🟢 [검증된 기본 설정]';
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
            document.getElementById('logic-select').value = defaults.logic;
            toggleThresholdDisplay(defaults.logic);
            
            document.getElementById('weighted-threshold').value = defaults.threshold;
            document.getElementById('threshold-val').innerText = defaults.threshold;

            // 리스크 설정
            document.getElementById('risk-type-select').value = defaults.risk.type;
            toggleRiskParamsDisplay(defaults.risk.type);

            if (defaults.risk.type === 'StopLoss') {
                const slVal = (defaults.risk.stop_loss_pct * 100).toFixed(1);
                const tpVal = (defaults.risk.take_profit_pct * 100).toFixed(1);
                document.getElementById('risk-stoploss-pct').value = slVal;
                document.getElementById('risk-stoploss-pct-val').innerText = slVal;
                document.getElementById('risk-takeprofit-pct').value = tpVal;
                document.getElementById('risk-takeprofit-pct-val').innerText = tpVal;
            } else if (defaults.risk.type === 'TrailingStop') {
                const trailVal = (defaults.risk.trail_pct * 100).toFixed(1);
                document.getElementById('risk-trail-pct').value = trailVal;
                document.getElementById('risk-trail-pct-val').innerText = trailVal;
            } else if (defaults.risk.type === 'AveragingDown') {
                const dropVal = (defaults.risk.drop_trigger_pct * 100).toFixed(1);
                const maxAddCount = defaults.risk.max_add_count;
                document.getElementById('risk-drop-trigger-pct').value = dropVal;
                document.getElementById('risk-drop-trigger-val').innerText = dropVal;
                document.getElementById('risk-max-add-count').value = maxAddCount;
                document.getElementById('risk-max-add-count-val').innerText = maxAddCount;
            }

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
                    document.getElementById('bb-period').value = sCfg.period;
                    document.getElementById('bb-period-val').innerText = sCfg.period;
                    document.getElementById('bb-stddev').value = sCfg.std_dev;
                    document.getElementById('bb-stddev-val').innerText = sCfg.std_dev;
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
            addLog(new Date().toLocaleTimeString(), activeTicker, 'SYSTEM', '전략 UI 및 엔진 설정이 검증된 기본 설정으로 복구되었습니다.');
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
            const riskType = document.getElementById('risk-type-select').value;
            const risk = { type: riskType };

            if (riskType === 'StopLoss') {
                risk.stop_loss_pct = parseFloat(document.getElementById('risk-stoploss-pct').value) / 100;
                risk.take_profit_pct = parseFloat(document.getElementById('risk-takeprofit-pct').value) / 100;
            } else if (riskType === 'TrailingStop') {
                risk.trail_pct = parseFloat(document.getElementById('risk-trail-pct').value) / 100;
            } else if (riskType === 'AveragingDown') {
                risk.drop_trigger_pct = parseFloat(document.getElementById('risk-drop-trigger-pct').value) / 100;
                risk.max_add_count = parseInt(document.getElementById('risk-max-add-count').value);
            }

            const newConfig = {
                ticker: activeTicker,
                logic: logic,
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
                        logic: logic,
                        threshold: threshold,
                        strategies: strategies,
                        risk: risk
                    };
                    const badgeEl = document.getElementById(`badge-text-${activeTicker}`);
                    if (badgeEl) badgeEl.innerText = `${logic} 결합`;
                    addLog(new Date().toLocaleTimeString(), activeTicker, 'SYSTEM', `복합 전략 설정이 런타임 엔진에 실시간 적용되었습니다. (${logic})`);
                    updateStrategyDetailsTable(activeTicker, tickerConfigs[activeTicker]);
                    updateConfigStatusBadge();
                } else {
                    addLog(new Date().toLocaleTimeString(), activeTicker, 'SYSTEM', `설정 적용 실패: ${res.error}`);
                }
            } catch (e) {
                console.error("Apply settings error:", e);
                addLog(new Date().toLocaleTimeString(), activeTicker, 'SYSTEM', `설정 적용 요청 실패.`);
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
            
            addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `KRW-${ticker} 종목 추가 요청 중...`);
            
            try {
                const response = await fetch('/api/tickers', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker: ticker })
                });
                const res = await response.json();
                if (res.success) {
                    addLog(new Date().toLocaleTimeString(), ticker, 'SYSTEM', `종목이 성공적으로 추가되었습니다. 웹소켓 구독을 갱신합니다.`);
                    await loadAllConfigs();
                    selectTicker(ticker);
                } else {
                    addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `종목 추가 실패: ${res.error}`);
                }
            } catch (e) {
                console.error("Add ticker error:", e);
                addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `종목 추가 요청 중 네트워크 에러 발생.`);
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
                addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', '빗썸 과거 거래 내역을 불러오는데 실패했습니다.');
            }
        }

        function connectWebSocket() {
            const loc = window.location;
            const wsUri = (loc.protocol === "https:" ? "wss:" : "ws:") + "//" + loc.host + "/ws/trading-status";
            
            ws = new WebSocket(wsUri);
            
            ws.onopen = () => {
                wsStatusEl.className = 'status-badge connected';
                wsStatusText.innerText = '연결됨';
                addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', '다중 타임프레임 PoC 웹소켓 브로커에 접속 성공.');
                
                // 엔진 설정을 조회하여 UI와 동기화
                loadAllConfigs();
                
                // 빗썸 과거 체결 내역 불러와 트레이딩 로그에 초기화 로드
                loadTradeHistory();
                
                // 첫 진입 시 기본 종목 캐시 준비물 로드
                loadHistoricalCandles('BTC', activeTimeframe);
                loadHistoricalCandles('ETH', activeTimeframe);
                loadHistoricalCandles('XRP', activeTimeframe);
            };

            ws.onmessage = (event) => {
                const msg = jsonParseSafe(event.data);
                if (!msg) return;
                
                const timeStr = new Date(msg.data.timestamp).toLocaleTimeString();
                const ticker = msg.data.ticker;
                
                if (msg.type === 'regime_change') {
                    // 장세 국면 변경 알림 처리
                    const regime = msg.data.regime;
                    const rType = msg.data.risk_type;
                    const logMsg = `시장 국면 변경 감지 -> ${regime} 작전으로 스위칭 완료! (전략 logic: ${msg.data.logic}, 리스크: ${rType})`;
                    addLog(timeStr, ticker, 'SYSTEM', logMsg);
                    
                    const regimeEl = document.getElementById(`regime-${ticker}`);
                    if (regimeEl) {
                        let emoji = '↔️';
                        let color = 'var(--accent-blue)';
                        if (regime === 'BULL') { emoji = '📈'; color = 'var(--accent-green)'; }
                        else if (regime === 'BEAR') { emoji = '📉'; color = 'var(--accent-red)'; }
                        regimeEl.innerText = `${emoji} ${regime}`;
                        regimeEl.style.color = color;
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

                    // 장세 드롭다운 동기화 (사용자가 클릭해 변경한 상태가 소켓 메시지로 재수신될 때 동기화)
                    const regSelect = document.getElementById(`regime-select-${ticker}`);
                    if (regSelect && override) {
                        regSelect.value = override;
                    }
                    
                    // 1. 실시간 캔들 병합 알고리즘
                    mergeRealtimePriceToCandle(ticker, price, msg.data.timestamp);
                    
                    // 2. 가격 판 갱신 및 플래시 이펙트
                    if (priceEl) {
                        const formattedPrice = ['DOGE', 'SAND'].includes(ticker) ? price.toFixed(2) : price.toLocaleString();
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
                            riskDesc = `🛡️🛡️ [자산 배분 차단] 한 종목당 최대 투자 한도(50%) 초과로 주문 차단! (${riskReason.split('(')[1] ? riskReason.split('(')[1].replace(')', '') : ''})`;
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
                addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', '브로커 연결 종료. 3초 후 재연결 시도...');
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
                        addLog(new Date().toLocaleTimeString(), ticker, 'SYSTEM', '자동매매 가동 시작.');
                    } else {
                        ind.classList.remove('active');
                        addLog(new Date().toLocaleTimeString(), ticker, 'SYSTEM', '자동매매 일시정지.');
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
                    addLog(new Date().toLocaleTimeString(), ticker, 'SYSTEM', '종목이 완전히 삭제되었습니다.');
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
            if (!confirm(`백그라운드에서 실시간 9년 데이터 최적화를 즉시 시작하시겠습니까?
(약 5~10분 정도 소요되며 거래 엔진은 중단 없이 계속 동작합니다)`)) return;
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
        async function loadBacktestCompareData(ticker) {
            try {
                const response = await fetch('/api/backtest/results');
                const resData = await response.json();
                
                const comparePanel = document.getElementById('bear-compare-panel');
                if (!comparePanel) return;
                
                if (resData.status === 'ok' && resData.data) {
                    const market = `KRW-${ticker}`;
                    const optData = resData.data[market];
                    if (optData && optData.BEAR) {
                        const bearData = optData.BEAR;
                        
                        if (bearData.mixed_strategy && bearData.custom_bear_strategy) {
                            comparePanel.style.display = 'block';
                            
                            const mixed = bearData.mixed_strategy;
                            const custom = bearData.custom_bear_strategy;
                            
                            const mixedProfits = mixed.period_expected_profits || { "1w": 0, "1m": 0, "3m": 0, "6m": 0 };
                            const customProfits = custom.period_expected_profits || { "1w": 0, "1m": 0, "3m": 0, "6m": 0 };
                            
                            document.getElementById('mixed-profit-1w').innerText = `${mixedProfits["1w"] >= 0 ? '+' : ''}${mixedProfits["1w"]}%`;
                            document.getElementById('mixed-profit-1m').innerText = `${mixedProfits["1m"] >= 0 ? '+' : ''}${mixedProfits["1m"]}%`;
                            document.getElementById('mixed-profit-3m').innerText = `${mixedProfits["3m"] >= 0 ? '+' : ''}${mixedProfits["3m"]}%`;
                            document.getElementById('mixed-profit-6m').innerText = `${mixedProfits["6m"] >= 0 ? '+' : ''}${mixedProfits["6m"]}%`;
                            
                            document.getElementById('custom-profit-1w').innerText = `${customProfits["1w"] >= 0 ? '+' : ''}${customProfits["1w"]}%`;
                            document.getElementById('custom-profit-1m').innerText = `${customProfits["1m"] >= 0 ? '+' : ''}${customProfits["1m"]}%`;
                            document.getElementById('custom-profit-3m').innerText = `${customProfits["3m"] >= 0 ? '+' : ''}${customProfits["3m"]}%`;
                            document.getElementById('custom-profit-6m').innerText = `${customProfits["6m"] >= 0 ? '+' : ''}${customProfits["6m"]}%`;
                            
                            document.getElementById('compare-mixed-ret').innerText = `${mixed.backtest.total_return_pct >= 0 ? '+' : ''}${mixed.backtest.total_return_pct.toFixed(2)}%`;
                            document.getElementById('compare-custom-ret').innerText = `${custom.backtest.total_return_pct >= 0 ? '+' : ''}${custom.backtest.total_return_pct.toFixed(2)}%`;
                            
                            document.getElementById('compare-mixed-sharpe').innerText = mixed.backtest.sharpe_ratio.toFixed(4);
                            document.getElementById('compare-custom-sharpe').innerText = custom.backtest.sharpe_ratio.toFixed(4);
                            
                            document.getElementById('compare-mixed-mdd').innerText = `-${mixed.backtest.mdd_pct.toFixed(1)}%`;
                            document.getElementById('compare-custom-mdd').innerText = `-${custom.backtest.mdd_pct.toFixed(1)}%`;
                            
                            document.getElementById('compare-mixed-trades').innerText = `${mixed.backtest.trade_count}회 / ${mixed.backtest.win_rate_pct.toFixed(1)}%`;
                            document.getElementById('compare-custom-trades').innerText = `${custom.backtest.trade_count}회 / ${custom.backtest.win_rate_pct.toFixed(1)}%`;
                            
                            return;
                        }
                    }
                }
                comparePanel.style.display = 'none';
            } catch (e) {
                console.error("Failed to load backtest compare data:", e);
            }
        }

        function syncBearStrategyUI(ticker) {
            const config = tickerConfigs[ticker];
            if (!config) return;
            
            const selected = config.selected_bear_strategy || 'custom_bear';
            
            const cardMixed = document.getElementById('bear-card-mixed');
            const cardCustom = document.getElementById('bear-card-custom');
            const badgeMixed = document.getElementById('badge-mixed');
            const badgeCustom = document.getElementById('badge-custom');
            
            if (selected === 'mixed') {
                if (cardMixed) cardMixed.className = 'bear-strategy-card selected';
                if (cardCustom) cardCustom.className = 'bear-strategy-card';
                if (badgeMixed) badgeMixed.style.display = 'inline-block';
                if (badgeCustom) badgeCustom.style.display = 'none';
            } else {
                if (cardMixed) cardMixed.className = 'bear-strategy-card';
                if (cardCustom) cardCustom.className = 'bear-strategy-card selected';
                if (badgeMixed) badgeMixed.style.display = 'none';
                if (badgeCustom) badgeCustom.style.display = 'inline-block';
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
                    }
                    syncBearStrategyUI(activeTicker);
                    // 핫스왑된 세부 UI 로드
                    loadConfigForTicker(activeTicker);
                    addLog(new Date().toLocaleTimeString(), activeTicker, 'SYSTEM', `${activeTicker} 하락장 전략을 ${strategy === 'custom_bear' ? 'CustomBear(나만의기법)' : 'Mixed(믹스기법)'}으로 변경 적용했습니다.`);
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
    
    defaults = {
        "BTC": fallback_defaults,
        "ETH": fallback_defaults,
        "XRP": fallback_defaults
    }
    
    if results:
        for market in ["KRW-BTC", "KRW-ETH", "KRW-XRP"]:
            ticker = market.replace("KRW-", "")
            if market in results:
                ticker_data = {}
                for regime in ["BULL", "BEAR", "RANGE"]:
                    reg_data = results[market].get(regime)
                    if reg_data:
                        if regime == "BEAR":
                            selected = active_ticker_configs.get(ticker, {}).get("selected_bear_strategy", "custom_bear")
                            if selected == "custom_bear" and "custom_bear_strategy" in reg_data:
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
        print(f"[DailyOptimizer] 최적화 중 오류: {e}")
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

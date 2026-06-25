import os
import sys
import json

# .env 파일 로드 로직 추가 (환경변수 자동 로딩)
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()
import uuid
import asyncio
import queue
import threading
import time
import websocket
import requests
from typing import List, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

# 템플릿 경로 로딩
TEMPLATE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 
    "../.agents/skills/auto-trading-builder/templates"
))
sys.path.append(TEMPLATE_PATH)

from core_engine import TradingEngine, AccountManager, BaseStrategy, BaseRiskManager, TickerWorker, CompositeStrategy

# 글로벌 설정 및 엔진 인스턴스 홀더
active_ticker_configs = {
    "BTC": {
        "active": True,
        "logic": "AND",
        "threshold": 0.5,
        "strategies": [
            {"name": "RSI", "enabled": True, "weight": 1.0, "params": {"period": 5, "oversold": 35, "overbought": 65}},
            {"name": "Bollinger", "enabled": True, "weight": 1.0, "params": {"period": 5, "std_dev": 1.5}},
            {"name": "MACD", "enabled": False, "weight": 1.0, "params": {"fast": 5, "slow": 10, "signal_period": 3}}
        ]
    },
    "ETH": {
        "active": True,
        "logic": "OR",
        "threshold": 0.5,
        "strategies": [
            {"name": "RSI", "enabled": True, "weight": 1.0, "params": {"period": 5, "oversold": 35, "overbought": 65}},
            {"name": "Bollinger", "enabled": False, "weight": 1.0, "params": {"period": 5, "std_dev": 1.5}},
            {"name": "MACD", "enabled": True, "weight": 1.0, "params": {"fast": 5, "slow": 10, "signal_period": 3}}
        ]
    },
    "XRP": {
        "active": True,
        "logic": "VOTE",
        "threshold": 0.5,
        "strategies": [
            {"name": "RSI", "enabled": True, "weight": 1.0, "params": {"period": 5, "oversold": 35, "overbought": 65}},
            {"name": "Bollinger", "enabled": True, "weight": 1.0, "params": {"period": 5, "std_dev": 1.5}},
            {"name": "MACD", "enabled": True, "weight": 1.0, "params": {"fast": 5, "slow": 10, "signal_period": 3}}
        ]
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
                active_ticker_configs.update(saved_configs)
                print(f"[Storage] 설정을 {CONFIG_FILE_PATH}에서 성공적으로 불러왔습니다.")
        except Exception as e:
            print(f"[Storage] 설정 파일 읽기 실패: {e}")
    else:
        save_persisted_configs()

def save_persisted_configs():
    try:
        with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(active_ticker_configs, f, indent=4, ensure_ascii=False)
            print(f"[Storage] 설정을 {CONFIG_FILE_PATH}에 백업 저장했습니다.")
    except Exception as e:
        print(f"[Storage] 설정 파일 저장 실패: {e}")

# 초기 구동 시 저장된 설정 로드 적용
load_persisted_configs()

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

    def __init__(self, params=None):
        super().__init__(params)
        self.prices = []

    def generate_signal(self, data: dict) -> str:
        price = data.get("price")
        if price is None: return "HOLD"
        
        self.prices.append(price)
        if len(self.prices) > 50: self.prices.pop(0)

        period = self.params["period"]
        if len(self.prices) <= period: return "HOLD"

        delta = pd.Series(self.prices).diff()
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

    def __init__(self, params=None):
        super().__init__(params)
        self.prices = []

    def generate_signal(self, data: dict) -> str:
        price = data.get("price")
        if price is None: return "HOLD"
        
        self.prices.append(price)
        if len(self.prices) > 50: self.prices.pop(0)

        slow = self.params["slow"]
        if len(self.prices) <= slow: return "HOLD"

        df = pd.Series(self.prices)
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

    def __init__(self, params=None):
        super().__init__(params)
        self.prices = []

    def generate_signal(self, data: dict) -> str:
        price = data.get("price")
        if price is None: return "HOLD"
        
        self.prices.append(price)
        if len(self.prices) > 50: self.prices.pop(0)

        period = self.params["period"]
        if len(self.prices) <= period: return "HOLD"

        df = pd.Series(self.prices)
        ma = df.rolling(window=period).mean()
        std = df.rolling(window=period).std()
        upper = ma + self.params["std_dev"] * std
        lower = ma - self.params["std_dev"] * std

        curr_upper = upper.iloc[-1]
        curr_lower = lower.iloc[-1]

        data["bb_val"] = f"U:{curr_upper:.1f}/L:{curr_lower:.1f}"

        if price < curr_lower: return "BUY"
        if price > curr_upper: return "SELL"
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
            elif all(s == "SELL" for s in sig_list): final_signal = "SELL"
        elif self.logic == "OR":
            if any(s == "BUY" for s in sig_list): final_signal = "BUY"
            elif any(s == "SELL" for s in sig_list): final_signal = "SELL"
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

        data["composite_details"] = {
            "logic": self.logic,
            "sub_signals": signals,
            "indicators": {
                "RSI": data.get("rsi_val", "-"),
                "MACD": data.get("macd_val", "-"),
                "Bollinger": data.get("bb_val", "-")
            },
            "weights": self.weights,
            "threshold": self.threshold,
            "final": final_signal
        }
        return final_signal


class AllowAllRiskManager(BaseRiskManager):
    def is_allowed(self, signal: str, position: dict) -> bool:
        return True


# ══════════════════════════════════════════════════════════════
# 2. 커스텀 워커 스레드 (실시간 체결 유입을 UI 큐로 전달)
# ══════════════════════════════════════════════════════════════
class UITickerWorker(TickerWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trading_active = True

    def _process(self, data: dict) -> None:
        self.position.current_price = data.get("price", self.position.current_price)
        timestamp = data.get("timestamp", int(time.time() * 1000))

        # 복합 전략 연산 수행
        final_signal = self.strategy.generate_signal(data)
        composite_details = data.get("composite_details", {
            "logic": "UNKNOWN", "sub_signals": {}, "indicators": {}, "final": final_signal
        })

        # 평가 금액 및 수익률 계산
        krw_value = self.position.quantity * self.position.current_price
        pnl_pct = self.position.pnl_pct

        # 실시간 체결 정보 UI 큐 적재 (동기식 큐로 병목 없음)
        ui_event = {
            "type": "trade",
            "data": {
                "ticker": self.ticker,
                "price": self.position.current_price,
                "volume": data.get("volume", 0),
                "composite": composite_details,
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

        # 비활성화(정지) 상태면 실제 매매 주문 생성 건너뜀
        if not self.trading_active:
            return

        # 주문 처리
        if final_signal == "HOLD":
            return

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
        print("[WS-UI-TimeframeCheck] 빗썸 소켓 연결 완료")
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
# 4. FastAPI 및 WebSocket 브로드캐스트 브로커
# ══════════════════════════════════════════════════════════════
app = FastAPI(title="Bithumb Multi-Timeframe PoC")

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
    def register_ticker(self, ticker: str, strategy: BaseStrategy, risk_manager: BaseRiskManager) -> None:
        worker = UITickerWorker(
            ticker=ticker,
            strategy=strategy,
            risk_manager=risk_manager,
            order_queue=self.order_queue,
            account_manager=self.account
        )
        self._workers[ticker] = worker
        self.dispatcher.register(ticker, worker)

    def _execute_order(self, order: dict):
        super()._execute_order(order)
        ticker = order["ticker"]
        signal = order["signal"]
        price = order["price"]
        
        worker = self._workers.get(ticker)
        if worker:
            pos = worker.position
            if signal == "BUY":
                # 매수 시뮬레이션: 1회당 1개 매수
                buy_qty = 1.0
                total_cost = (pos.quantity * pos.avg_price) + (buy_qty * price)
                pos.quantity += buy_qty
                pos.avg_price = total_cost / pos.quantity
            elif signal == "SELL":
                # 매도 시뮬레이션: 보유 수량 전량 혹은 1개 단위 매도
                sell_qty = min(1.0, pos.quantity)
                if sell_qty > 0:
                    pos.quantity -= sell_qty
                    self.account.release(sell_qty * price)
                    if pos.quantity == 0:
                        pos.avg_price = 0.0

        ui_event = {
            "type": "order",
            "data": {
                "ticker": ticker,
                "signal": signal,
                "price": price,
                "timestamp": int(time.time() * 1000)
            }
        }
        ui_event_queue.put(ui_event)

    def update_ticker_config(self, ticker: str, config: dict) -> bool:
        worker = self._workers.get(ticker)
        if not worker: return False
        
        strategies = []
        weights = {}
        for s_cfg in config.get("strategies", []):
            if not s_cfg.get("enabled", False):
                continue
            name = s_cfg["name"]
            weight = s_cfg.get("weight", 1.0)
            params = s_cfg.get("params", {})
            weights[name] = weight
            
            if name == "RSI":
                strategies.append(BithumbRsiStrategy(params))
            elif name == "MACD":
                strategies.append(BithumbMacdStrategy(params))
            elif name == "Bollinger":
                strategies.append(BithumbBollingerStrategy(params))
                
        logic = config.get("logic", "AND")
        threshold = config.get("threshold", 0.5)
        
        worker.strategy = VerboseCompositeStrategy(
            strategies=strategies,
            logic=logic,
            weights=weights,
            threshold=threshold
        )
        print(f"[Engine] {ticker} 전략 설정 변경: {logic} (Threshold: {threshold}, 가중치: {weights})")
        return True

    def add_new_ticker(self, ticker: str) -> bool:
        if ticker in self._workers:
            return False
            
        if ticker not in active_ticker_configs:
            active_ticker_configs[ticker] = {
                "active": True,
                "logic": "AND",
                "threshold": 0.5,
                "strategies": [
                    {"name": "RSI", "enabled": True, "weight": 1.0, "params": {"period": 5, "oversold": 35, "overbought": 65}},
                    {"name": "Bollinger", "enabled": True, "weight": 1.0, "params": {"period": 5, "std_dev": 1.5}},
                    {"name": "MACD", "enabled": False, "weight": 1.0, "params": {"fast": 5, "slow": 10, "signal_period": 3}}
                ]
            }
            
        config = active_ticker_configs[ticker]
        strategies = []
        weights = {}
        for s_cfg in config["strategies"]:
            if s_cfg["enabled"]:
                name = s_cfg["name"]
                weights[name] = s_cfg.get("weight", 1.0)
                params = s_cfg.get("params", {})
                if name == "RSI":
                    strategies.append(BithumbRsiStrategy(params))
                elif name == "MACD":
                    strategies.append(BithumbMacdStrategy(params))
                elif name == "Bollinger":
                    strategies.append(BithumbBollingerStrategy(params))
                    
        default_strategy = VerboseCompositeStrategy(
            strategies=strategies,
            logic=config["logic"],
            weights=weights,
            threshold=config["threshold"]
        )
        
        self.register_ticker(ticker, default_strategy, AllowAllRiskManager())
        
        # 워커 스레드 구동
        worker = self._workers[ticker]
        worker.trading_active = config.get("active", True)
        worker.start()
        
        # 웹소켓 리스너 구독 갱신
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
    """
    빗썸 REST API로부터 과거 캔들을 가져와 TradingView 규격에 맞춰 정렬 및 포맷팅해 반환한다.
    """
    base_url = "https://api.bithumb.com/v1"
    url = ""
    params = {"market": market, "count": 120}  # 기본 120개 조회
    
    # 타임프레임별 빗썸 API 분기
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
        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            return {"error": f"Bithumb API Error: {resp.text}"}
        
        raw_candles = resp.json()
        formatted_candles = []
        
        # 빗썸 응답은 최신순(Index 0)이므로 오름차순(과거 -> 최신)으로 변경하기 위해 역순 처리
        for c in reversed(raw_candles):
            if (timeframe in ["D", "W", "M"]):
                # 일/주/월 봉: Unix Timestamp (초 단위 정수)
                from datetime import datetime, timezone
                dt = datetime.strptime(c["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
                t_val = int(dt.replace(tzinfo=timezone.utc).timestamp())
            else:
                # 분/시간 봉: Unix Timestamp (초 단위 정수)
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


@app.get("/api/config")
def get_configs():
    return active_ticker_configs

@app.post("/api/config/update")
def update_config(payload: dict):
    ticker = payload.get("ticker")
    if not ticker:
        return {"success": False, "error": "Ticker is missing"}
    
    if ticker not in active_ticker_configs:
        return {"success": False, "error": "Ticker not found"}
        
    # 기존 active 플래그 보전
    is_active = active_ticker_configs[ticker].get("active", True)
    active_ticker_configs[ticker] = {
        "active": is_active,
        "logic": payload.get("logic", "AND"),
        "threshold": payload.get("threshold", 0.5),
        "strategies": payload.get("strategies", [])
    }
    
    # 설정 파일에 즉시 영구 저장
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
        # 종목이 정상 추가되면 그 설정도 영구 저장
        if success:
            save_persisted_configs()
        return {"success": success}
    return {"success": False, "error": "Engine not running"}

@app.post("/api/tickers/control")
def control_ticker(payload: dict):
    ticker = payload.get("ticker")
    action = payload.get("action")  # "start" or "stop"
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

        .main-layout {
            display: grid;
            grid-template-columns: 320px 1fr 360px;
            gap: 20px;
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
            min-height: 560px;
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
            height: 380px;
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
    </style>
</head>
<body>
    <div class="container">
        <!-- 상단 헤더 -->
        <header>
            <div class="logo-area">
                <div class="logo-icon"></div>
                <div class="logo-text">NEXUS MULTI-TIMEFRAME POC</div>
            </div>
            <div style="display:flex; align-items:center; gap:16px;">
                <div class="balance-display" id="total-balance">예수금: 10,000,000 KRW</div>
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
                <div class="strategy-header">
                    <span class="strategy-panel-title">STRATEGY CONFIGURATION</span>
                    <span class="refresh-icon" onclick="resetStrategyUI()" style="cursor:pointer; opacity:0.7;" title="원래대로 설정 초기화">&#8635;</span>
                </div>
                
                <div class="strategy-tabs">
                    <div class="strategy-tab active" id="tab-strategies" onclick="switchStrategyTab('strategies')">Strategies</div>
                    <div class="strategy-tab" id="tab-template" onclick="switchStrategyTab('template')">Logic & Weights</div>
                </div>
                
                <div id="strategies-tab-content" class="tab-content">
                    <!-- RSI 전략 -->
                    <div class="strategy-item-card">
                        <div class="strategy-item-header">
                            <div class="strategy-icon-title">
                                <span class="strategy-icon icon-rsi">RSI</span>
                                <div class="strategy-label-desc">
                                    <span class="strategy-item-name">RSI</span>
                                    <span class="strategy-item-desc">Relative Strength Index</span>
                                </div>
                            </div>
                            <label class="toggle-switch">
                                <input type="checkbox" id="toggle-rsi" onchange="toggleStrategy('RSI')">
                                <span class="slider-round"></span>
                            </label>
                        </div>
                        <div class="strategy-item-controls" id="controls-rsi">
                            <div class="control-row">
                                <span class="control-label">Time Period</span>
                                <input type="range" class="param-slider" id="rsi-period" min="2" max="25" value="5" oninput="updateParamValue('rsi-period-val', this.value)">
                                <span class="param-val" id="rsi-period-val">5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">Overbought</span>
                                <input type="range" class="param-slider" id="rsi-overbought" min="50" max="95" value="65" oninput="updateParamValue('rsi-overbought-val', this.value)">
                                <span class="param-val" id="rsi-overbought-val">65</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">Oversold</span>
                                <input type="range" class="param-slider" id="rsi-oversold" min="5" max="50" value="35" oninput="updateParamValue('rsi-oversold-val', this.value)">
                                <span class="param-val" id="rsi-oversold-val">35</span>
                            </div>
                            <div class="control-row weight-row">
                                <span class="control-label" style="color:var(--accent-blue); font-weight:700;">Strategy Weight</span>
                                <input type="range" class="param-slider weight-slider" id="rsi-weight" min="0.1" max="3.0" step="0.1" value="1.0" oninput="updateParamValue('rsi-weight-val', this.value)">
                                <span class="param-val" id="rsi-weight-val">1.0</span>
                            </div>
                        </div>
                    </div>

                    <!-- Bollinger Bands 전략 -->
                    <div class="strategy-item-card">
                        <div class="strategy-item-header">
                            <div class="strategy-icon-title">
                                <span class="strategy-icon icon-bb">BB</span>
                                <div class="strategy-label-desc">
                                    <span class="strategy-item-name">Bollinger Bands</span>
                                    <span class="strategy-item-desc">Volatility Bandwidth</span>
                                </div>
                            </div>
                            <label class="toggle-switch">
                                <input type="checkbox" id="toggle-bb" onchange="toggleStrategy('Bollinger')">
                                <span class="slider-round"></span>
                            </label>
                        </div>
                        <div class="strategy-item-controls" id="controls-bb">
                            <div class="control-row">
                                <span class="control-label">Time Period</span>
                                <input type="range" class="param-slider" id="bb-period" min="2" max="25" value="5" oninput="updateParamValue('bb-period-val', this.value)">
                                <span class="param-val" id="bb-period-val">5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">Std Dev</span>
                                <input type="range" class="param-slider" id="bb-stddev" min="0.5" max="3.5" step="0.1" value="1.5" oninput="updateParamValue('bb-stddev-val', this.value)">
                                <span class="param-val" id="bb-stddev-val">1.5</span>
                            </div>
                            <div class="control-row weight-row">
                                <span class="control-label" style="color:var(--accent-blue); font-weight:700;">Strategy Weight</span>
                                <input type="range" class="param-slider weight-slider" id="bb-weight" min="0.1" max="3.0" step="0.1" value="1.0" oninput="updateParamValue('bb-weight-val', this.value)">
                                <span class="param-val" id="bb-weight-val">1.0</span>
                            </div>
                        </div>
                    </div>

                    <!-- MACD 전략 -->
                    <div class="strategy-item-card">
                        <div class="strategy-item-header">
                            <div class="strategy-icon-title">
                                <span class="strategy-icon icon-macd">MACD</span>
                                <div class="strategy-label-desc">
                                    <span class="strategy-item-name">MACD</span>
                                    <span class="strategy-item-desc">Trend Momentum</span>
                                </div>
                            </div>
                            <label class="toggle-switch">
                                <input type="checkbox" id="toggle-macd" onchange="toggleStrategy('MACD')">
                                <span class="slider-round"></span>
                            </label>
                        </div>
                        <div class="strategy-item-controls" id="controls-macd">
                            <div class="control-row">
                                <span class="control-label">Fast Period</span>
                                <input type="range" class="param-slider" id="macd-fast" min="2" max="15" value="5" oninput="updateParamValue('macd-fast-val', this.value)">
                                <span class="param-val" id="macd-fast-val">5</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">Slow Period</span>
                                <input type="range" class="param-slider" id="macd-slow" min="5" max="30" value="10" oninput="updateParamValue('macd-slow-val', this.value)">
                                <span class="param-val" id="macd-slow-val">10</span>
                            </div>
                            <div class="control-row">
                                <span class="control-label">Signal Period</span>
                                <input type="range" class="param-slider" id="macd-signal" min="2" max="10" value="3" oninput="updateParamValue('macd-signal-val', this.value)">
                                <span class="param-val" id="macd-signal-val">3</span>
                            </div>
                            <div class="control-row weight-row">
                                <span class="control-label" style="color:var(--accent-blue); font-weight:700;">Strategy Weight</span>
                                <input type="range" class="param-slider weight-slider" id="macd-weight" min="0.1" max="3.0" step="0.1" value="1.0" oninput="updateParamValue('macd-weight-val', this.value)">
                                <span class="param-val" id="macd-weight-val">1.0</span>
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
                
                <button class="apply-btn" onclick="applySettingsToEngine()">Apply Settings to Engine</button>
            </div>
        </div>

        <!-- 하단: 로그 -->
        <div class="log-section">
            <div class="log-header">
                <h2>종합 실시간 트레이딩 시스템 로그</h2>
                <button class="tf-btn" onclick="clearLogs()" style="padding: 2px 8px;">지우기</button>
            </div>
            <div class="log-box" id="log-container">
                <div class="log-entry"><span class="log-time">[System]</span> 빗썸 다중 시세 트레이딩 엔진 가동 준비 중...</div>
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
        const lastPrices = { BTC: 0, ETH: 0, XRP: 0 };
        let tickerConfigs = {};
        
        // 타임프레임별 초단위 매핑
        const timeframeIntervals = {
            "1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800,
            "1h": 3600, "4h": 14400
        };

        // ══════════════════════════════════════════════════════════════
        // 로그 출력 헬퍼
        // ══════════════════════════════════════════════════════════════
        function addLog(time, ticker, type, message) {
            const entry = document.createElement('div');
            entry.className = 'log-entry';
            
            let tickerBadge = '';
            if (ticker) {
                const isSystemTicker = ['BTC', 'ETH', 'XRP'].includes(ticker);
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
            
            while (logContainer.childElementCount > 120) {
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
                        <span class="delete-btn" onclick="deleteTicker('${ticker}')" style="color:var(--accent-red); font-size:14px; font-weight:bold; cursor:pointer; margin-left:4px; padding:2px 4px;" title="종목 삭제">&#10006;</span>
                    </div>
                </div>
                <div class="ticker-info" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                    <span class="strategy-badge ${badgeClass}" id="badge-text-${ticker}">${logic} 결합</span>
                </div>
                <div class="price-display" id="price-${ticker}">- KRW</div>
                
                <!-- 실시간 보유 수량 및 수익률 원화 영역 -->
                <div class="position-info-box" style="background:rgba(0,0,0,0.25); border-radius:10px; padding:8px 12px; margin-top:8px; margin-bottom:8px; font-size:11px; display:flex; flex-direction:column; gap:4px; border: 1px solid rgba(255,255,255,0.03);">
                    <div style="display:flex; justify-content:space-between;">
                        <span style="color:var(--text-secondary);">수량</span>
                        <span id="pos-qty-${ticker}" style="font-family:'JetBrains Mono', monospace; font-weight:600;">0.0000 ${ticker}</span>
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
            
            document.getElementById(`card-${activeTicker}`).classList.remove('active');
            document.getElementById(`card-${ticker}`).classList.add('active');
            
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
            document.getElementById('strategies-tab-content').style.display = 'none';
            document.getElementById('template-tab-content').style.display = 'none';
            
            document.getElementById(`tab-${tab}`).classList.add('active');
            document.getElementById(`${tab}-tab-content`).style.display = 'flex';
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

        async function loadAllConfigs() {
            try {
                const response = await fetch('/api/config');
                tickerConfigs = await response.json();
                
                Object.entries(tickerConfigs).forEach(([ticker, config]) => {
                    createTickerCardIfNotExist(ticker, config.logic, config.active !== false);
                    updateStrategyDetailsTable(ticker, config);
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
                config.strategies.forEach(s => {
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
            
            document.getElementById('logic-select').value = config.logic;
            toggleThresholdDisplay(config.logic);
            
            document.getElementById('weighted-threshold').value = config.threshold || 0.5;
            document.getElementById('threshold-val').innerText = config.threshold || 0.5;
            
            config.strategies.forEach(s => {
                const prefix = s.name.toLowerCase();
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
                            inputEl.value = paramVal;
                            const valEl = document.getElementById(`${prefix}-${htmlParamName}-val`);
                            if (valEl) valEl.innerText = paramVal;
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
        }

        function resetStrategyUI() {
            loadConfigForTicker(activeTicker);
            addLog(new Date().toLocaleTimeString(), activeTicker, 'SYSTEM', '전략 UI 설정이 엔진 저장 기준값으로 복구되었습니다.');
        }

        async function applySettingsToEngine() {
            const logic = document.getElementById('logic-select').value;
            const threshold = parseFloat(document.getElementById('weighted-threshold').value);
            
            const strategies = [];
            
            // RSI
            strategies.push({
                name: 'RSI',
                enabled: document.getElementById('toggle-rsi').checked,
                weight: parseFloat(document.getElementById('rsi-weight').value),
                params: {
                    period: parseInt(document.getElementById('rsi-period').value),
                    oversold: parseInt(document.getElementById('rsi-oversold').value),
                    overbought: parseInt(document.getElementById('rsi-overbought').value)
                }
            });
            
            // Bollinger
            strategies.push({
                name: 'Bollinger',
                enabled: document.getElementById('toggle-bb').checked,
                weight: parseFloat(document.getElementById('bb-weight').value),
                params: {
                    period: parseInt(document.getElementById('bb-period').value),
                    std_dev: parseFloat(document.getElementById('bb-stddev').value)
                }
            });
            
            // MACD
            strategies.push({
                name: 'MACD',
                enabled: document.getElementById('toggle-macd').checked,
                weight: parseFloat(document.getElementById('macd-weight').value),
                params: {
                    fast: parseInt(document.getElementById('macd-fast').value),
                    slow: parseInt(document.getElementById('macd-slow').value),
                    signal_period: parseInt(document.getElementById('macd-signal').value)
                }
            });
            
            const newConfig = {
                ticker: activeTicker,
                logic: logic,
                threshold: threshold,
                strategies: strategies
            };
            
            try {
                const response = await fetch('/api/config/update', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(newConfig)
                });
                const res = await response.json();
                if (res.success) {
                    tickerConfigs[activeTicker] = newConfig;
                    const badgeEl = document.getElementById(`badge-text-${activeTicker}`);
                    if (badgeEl) badgeEl.innerText = `${logic} 결합`;
                    addLog(new Date().toLocaleTimeString(), activeTicker, 'SYSTEM', `복합 전략 설정이 런타임 엔진에 실시간 적용되었습니다. (${logic})`);
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
        
        function connectWebSocket() {
            const loc = window.location;
            const wsUri = (loc.protocol === "https:" ? "wss:" : "ws:") + "//" + loc.host + "/ws/trading-status";
            
            ws = new WebSocket(wsUri);
            
            ws.onopen = () => {
                wsStatusEl.className = 'status-badge connected';
                wsStatusText.innerText = 'Connected';
                addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', '다중 타임프레임 PoC 웹소켓 브로커에 접속 성공.');
                
                // 엔진 설정을 조회하여 UI와 동기화
                loadAllConfigs();
                
                // 첫 진입 시 기본 종목 캐시 준비 및 로드
                loadHistoricalCandles('BTC', activeTimeframe);
                loadHistoricalCandles('ETH', activeTimeframe);
                loadHistoricalCandles('XRP', activeTimeframe);
            };

            ws.onmessage = (event) => {
                const msg = jsonParseSafe(event.data);
                if (!msg) return;
                
                const timeStr = new Date(msg.data.timestamp).toLocaleTimeString();
                const ticker = msg.data.ticker;
                
                if (msg.type === 'trade') {
                    const price = msg.data.price;
                    const priceEl = document.getElementById(`price-${ticker}`);
                    const comp = msg.data.composite;
                    
                    // 1. 실시간 캔들 병합 알고리즘
                    mergeRealtimePriceToCandle(ticker, price, msg.data.timestamp);
                    
                    // 2. 가격 판 갱신 및 플래시 이펙트
                    if (priceEl) {
                        const formattedPrice = ['XRP', 'DOGE', 'SAND'].includes(ticker) ? price.toFixed(2) : price.toLocaleString();
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
                    }
                    
                    // 3. 포지션 세부 내역 실시간 바인딩
                    const pos = msg.data.position;
                    if (pos) {
                        const qtyEl = document.getElementById(`pos-qty-${ticker}`);
                        const avgEl = document.getElementById(`pos-avg-${ticker}`);
                        const valEl = document.getElementById(`pos-val-${ticker}`);
                        const pnlEl = document.getElementById(`pos-pnl-${ticker}`);
                        
                        if (qtyEl) qtyEl.innerText = `${pos.quantity.toFixed(4)} ${ticker}`;
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
                    addLog(
                        timeStr, 
                        ticker, 
                        signal, 
                        `★★ [자동매매 주문발송] 복합 결합 조건 충족! 신호: ${signal} @ ${price.toLocaleString()} KRW`
                    );
                }
            };

            ws.onclose = () => {
                wsStatusEl.className = 'status-badge';
                wsStatusText.innerText = 'Disconnected';
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
        connectWebSocket();
    </script>
</body>
</html>
"""

@app.get("/")
async def get_dashboard():
    return HTMLResponse(HTML_CONTENT)

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
    account = AccountManager(initial_balance=10_000_000)
    engine = MultiTickerUIEngine(account)
    
    # active_ticker_configs 기반으로 초기 종목 및 전략 동적 등록
    for ticker, config in active_ticker_configs.items():
        strategies = []
        weights = {}
        for s_cfg in config["strategies"]:
            if s_cfg["enabled"]:
                name = s_cfg["name"]
                weights[name] = s_cfg.get("weight", 1.0)
                params = s_cfg.get("params", {})
                if name == "RSI":
                    strategies.append(BithumbRsiStrategy(params))
                elif name == "MACD":
                    strategies.append(BithumbMacdStrategy(params))
                elif name == "Bollinger":
                    strategies.append(BithumbBollingerStrategy(params))
                    
        strategy = VerboseCompositeStrategy(
            strategies=strategies,
            logic=config["logic"],
            weights=weights,
            threshold=config["threshold"]
        )
        engine.register_ticker(ticker, strategy, AllowAllRiskManager())
        
    # 엔진 스레드 구동
    engine.start()
    print("[UI Server] 다중 타임프레임 지원형 복합 전략 엔진 가동.")
    
    # 빗썸 다중 웹소켓 연결
    markets = [f"KRW-{t}" for t in active_ticker_configs.keys()]
    ws_listener = MultiTickerWebSocketListener(engine.dispatcher, markets=markets)
    ws_listener.start()
    engine.ws_listener = ws_listener # 엔진 객체에 바인딩하여 dynamic subscribe 지원
    print("[UI Server] 빗썸 실시간 다중 마켓 피드 가동.")
    
    engine_instance = engine


@app.on_event("startup")
def startup_event():
    asyncio.create_task(ui_event_broadcaster())
    start_composite_engine()

if __name__ == "__main__":
    import uvicorn
    # 검증을 위한 로컬 서버 가동 (127.0.0.1:8006)
    uvicorn.run(app, host="127.0.0.1", port=8006)

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

        data["composite_details"] = {
            "logic": self.logic,
            "sub_signals": signals,
            "indicators": {
                "RSI": data.get("rsi_val", "-"),
                "MACD": data.get("macd_val", "-"),
                "Bollinger": data.get("bb_val", "-")
            },
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
    def _process(self, data: dict) -> None:
        self.position.current_price = data.get("price", self.position.current_price)
        timestamp = data.get("timestamp", int(time.time() * 1000))

        # 복합 전략 연산 수행
        final_signal = self.strategy.generate_signal(data)
        composite_details = data.get("composite_details", {
            "logic": "UNKNOWN", "sub_signals": {}, "indicators": {}, "final": final_signal
        })

        # 실시간 체결 정보 UI 큐 적재 (동기식 큐로 병목 없음)
        ui_event = {
            "type": "trade",
            "data": {
                "ticker": self.ticker,
                "price": self.position.current_price,
                "volume": data.get("volume", 0),
                "composite": composite_details,
                "timestamp": timestamp
            }
        }
        ui_event_queue.put(ui_event)

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
        self._thread = threading.Thread(target=self._run_loop, name="WS-UI-MultiTimeframe", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
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
        print("[WS-UI-MultiTimeframe] 빗썸 소켓 연결 완료")
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
app = FastAPI()

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
        ui_event = {
            "type": "order",
            "data": {
                "ticker": order["ticker"],
                "signal": order["signal"],
                "price": order["price"],
                "timestamp": int(time.time() * 1000)
            }
        }
        ui_event_queue.put(ui_event)


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
            # 시간 형식 규격화
            # 일/주/월 봉: 'YYYY-MM-DD' 문자열
            # 분/시간 봉: Unix Timestamp (초 단위 정수)
            if timeframe in ["D", "W", "M"]:
                # candle_date_time_kst: "2026-06-08T09:00:00" -> 날짜 부분만 분리
                t_val = c["candle_date_time_kst"][:10]
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
            await asyncio.sleep(1)


# ══════════════════════════════════════════════════════════════
# 7. 프리미엄 트레이딩뷰 대시보드 마크업 (lightweight-charts 연동)
# ══════════════════════════════════════════════════════════════
HTML_CONTENT = """
<!DOCTYPE html>
<html>
<head>
    <title>NEXUS ALGO TRADER - Multi-Timeframe Charts</title>
    <meta charset="utf-8">
    <!-- TradingView Lightweight Charts CDN 로드 (더 안정적인 jsdelivr 사용) -->
    <script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body {
            background-color: #030712;
            color: #f3f4f6;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0;
            padding: 24px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #1f2937;
            padding-bottom: 16px;
            margin-bottom: 20px;
        }
        .logo {
            font-size: 22px;
            font-weight: 800;
            color: #3b82f6;
            letter-spacing: -0.5px;
        }
        .status {
            background-color: #7f1d1d;
            color: #fca5a5;
            padding: 5px 12px;
            border-radius: 9999px;
            font-size: 12px;
            font-weight: 600;
            box-shadow: 0 0 10px rgba(239, 68, 68, 0.2);
            transition: all 0.3s ease;
        }
        .main-layout {
            display: grid;
            grid-template-columns: 320px 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }
        .sidebar {
            display: flex;
            flex-direction: column;
            gap: 20px;
        }
        .ticker-card {
            background: rgba(11, 19, 41, 0.7);
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 16px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.3);
            cursor: pointer;
            transition: all 0.2s ease;
        }
        .ticker-card:hover, .ticker-card.active {
            border-color: #3b82f6;
            background: rgba(59, 130, 246, 0.05);
        }
        .ticker-header {
            display: flex;
            justify-content: space-between;
            font-size: 14px;
            color: #9ca3af;
            margin-bottom: 8px;
        }
        .ticker-name {
            font-weight: 700;
            color: #ffffff;
        }
        .ticker-price {
            font-size: 24px;
            font-weight: 800;
            color: #10b981;
            margin-bottom: 8px;
        }
        .ticker-strategy {
            font-size: 11px;
            font-family: monospace;
            background: #090d16;
            padding: 6px;
            border-radius: 4px;
            border: 1px solid #1f2937;
            color: #9ca3af;
        }
        .chart-container-card {
            background: #0b1329;
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 20px;
            position: relative;
            display: flex;
            flex-direction: column;
            height: 520px;
        }
        .chart-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }
        .chart-title {
            font-size: 18px;
            font-weight: 700;
        }
        .timeframe-bar {
            display: flex;
            gap: 6px;
            background: #090d16;
            padding: 4px;
            border-radius: 8px;
            border: 1px solid #1e293b;
        }
        .tf-btn {
            background: transparent;
            border: none;
            color: #9ca3af;
            padding: 6px 12px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 12px;
            font-weight: 600;
            transition: all 0.2s ease;
        }
        .tf-btn:hover, .tf-btn.active {
            background: #3b82f6;
            color: #ffffff;
        }
        .chart-area {
            flex-grow: 1;
            border: 1px solid #1f2937;
            border-radius: 6px;
            overflow: hidden;
            background-color: #040814;
        }
        .bottom-section {
            display: grid;
            grid-template-columns: 1fr;
            gap: 20px;
        }
        .log-card {
            background-color: #0b1329;
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 20px;
        }
        .log-card h2 {
            margin-top: 0;
            border-bottom: 1px solid #1f2937;
            padding-bottom: 10px;
            color: #9ca3af;
            font-size: 15px;
        }
        .log-box {
            background-color: #020617;
            border: 1px solid #1e293b;
            border-radius: 6px;
            height: 200px;
            overflow-y: auto;
            padding: 12px;
            font-family: 'Fira Code', monospace;
            font-size: 12px;
        }
        .log-entry {
            margin-bottom: 6px;
            border-bottom: 1px solid #0f172a;
            padding-bottom: 4px;
        }
        .log-time { color: #6b7280; }
        .log-ticker {
            font-weight: 700;
            padding: 1px 4px;
            border-radius: 3px;
            font-size: 10px;
            margin-right: 6px;
        }
        .ticker-badge-btc { background: rgba(245, 158, 11, 0.15); color: #f59e0b; }
        .ticker-badge-eth { background: rgba(99, 102, 241, 0.15); color: #6366f1; }
        .ticker-badge-xrp { background: rgba(59, 130, 246, 0.15); color: #3b82f6; }
        .log-buy { color: #10b981; font-weight: bold; }
        .log-sell { color: #ef4444; font-weight: bold; }
        .log-trade { color: #9ca3af; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo">NEXUS ALGO TRADER (Multi-Timeframe Integration)</div>
            <div class="status" id="ws-status">WebSocket: Connecting...</div>
        </header>
        
        <div class="main-layout">
            <!-- 좌측 사이드바: 종목 선택 -->
            <div class="sidebar">
                <!-- BTC -->
                <div class="ticker-card active" id="card-BTC" onclick="selectTicker('BTC')">
                    <div class="ticker-header">
                        <div class="ticker-name">Bitcoin (BTC)</div>
                        <div style="font-weight:600; color:#ef4444;">AND 전략</div>
                    </div>
                    <div class="ticker-price" id="price-BTC">- KRW</div>
                    <div class="ticker-strategy" id="strategy-BTC">지표: 계산 대기 중...</div>
                </div>
                <!-- ETH -->
                <div class="ticker-card" id="card-ETH" onclick="selectTicker('ETH')">
                    <div class="ticker-header">
                        <div class="ticker-name">Ethereum (ETH)</div>
                        <div style="font-weight:600; color:#10b981;">OR 전략</div>
                    </div>
                    <div class="ticker-price" id="price-ETH">- KRW</div>
                    <div class="ticker-strategy" id="strategy-ETH">지표: 계산 대기 중...</div>
                </div>
                <!-- XRP -->
                <div class="ticker-card" id="card-XRP" onclick="selectTicker('XRP')">
                    <div class="ticker-header">
                        <div class="ticker-name">Ripple (XRP)</div>
                        <div style="font-weight:600; color:#60a5fa;">VOTE 전략</div>
                    </div>
                    <div class="ticker-price" id="price-XRP">- KRW</div>
                    <div class="ticker-strategy" id="strategy-XRP">지표: 계산 대기 중...</div>
                </div>
            </div>
            
            <!-- 중앙: TradingView Charts + Timeframe Bar -->
            <div class="chart-container-card">
                <div class="chart-header">
                    <div class="chart-title" id="chart-title-text">BTC/KRW 실시간 1분 봉 차트</div>
                    
                    <!-- 타임프레임 선택 버튼 바 -->
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
                <div class="chart-area" id="tv-chart-element"></div>
            </div>
        </div>
        
        <!-- 하단: 로그 영역 -->
        <div class="bottom-section">
            <div class="log-card">
                <h2>실시간 매매 엔진 로그 및 결합 시그널</h2>
                <div class="log-box" id="log-container">
                    <div class="log-entry"><span class="log-time">[System]</span> 빗썸 다중 시세 연결 대기 중...</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const wsStatusEl = document.getElementById('ws-status');
        const logContainer = document.getElementById('log-container');
        const chartTitleEl = document.getElementById('chart-title-text');
        
        let activeTicker = 'BTC';
        let activeTimeframe = '1m';  // 기본 1분봉
        
        // 종목별-타임프레임별 로컬 캔들스틱 데이터 저장소 (프론트엔드 캐싱)
        const candleCache = {};
        
        // 타임프레임별 초단위 간격 매핑
        const timeframeIntervals = {
            "1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800,
            "1h": 3600, "4h": 14400
        };

        // ══════════════════════════════════════════════════════════════
        // TradingView Lightweight Charts 초기화 로직 (방어적 예외 처리)
        // ══════════════════════════════════════════════════════════════
        const chartContainer = document.getElementById('tv-chart-element');
        let chart = null;
        let candlestickSeries = null;
        
        try {
            if (typeof LightweightCharts !== 'undefined') {
                chart = LightweightCharts.createChart(chartContainer, {
                    width: chartContainer.clientWidth,
                    height: chartContainer.clientHeight,
                    layout: {
                        background: { type: 'solid', color: '#040814' },
                        textColor: '#9ca3af',
                    },
                    grid: {
                        vertLines: { color: 'rgba(31, 41, 55, 0.4)' },
                        horzLines: { color: 'rgba(31, 41, 55, 0.4)' },
                    },
                    timeScale: {
                        timeVisible: true,
                        secondsVisible: false,
                    },
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
                    if (chart) chart.resize(chartContainer.clientWidth, chartContainer.clientHeight);
                });
            } else {
                throw new Error("LightweightCharts is not loaded from CDN");
            }
        } catch (e) {
            console.error("TradingView 차트 초기화 오류:", e);
            chartContainer.innerHTML = `<div style="display:flex; justify-content:center; align-items:center; height:100%; color:#ef4444; font-size:13px; font-weight:600; padding:20px; text-align:center;">차트 라이브러리 로드 대기/실패 (네트워크 확인 요망) - 실시간 시세 및 주문 피드는 정상 작동합니다.</div>`;
        }

        // ══════════════════════════════════════════════════════════════
        // REST API 호출: 특정 종목 및 타임프레임 과거 데이터 로드
        // ══════════════════════════════════════════════════════════════
        async def loadHistoricalCandles(ticker, timeframe) {
            const cacheKey = `${ticker}_${timeframe}`;
            addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `${ticker} ${timeframe} 과거 캔들 데이터 로딩 중...`);
            
            try {
                const response = await fetch(`/api/candles?market=KRW-${ticker}&timeframe=${timeframe}`);
                const data = await response.json();
                
                if (data.error) {
                    addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `에러: ${data.error}`);
                    return;
                }
                
                candleCache[cacheKey] = data;
                
                // 현재 활성화된 화면이면 차트에 주입
                if (ticker === activeTicker && timeframe === activeTimeframe && candlestickSeries) {
                    candlestickSeries.setData(data);
                    chart.timeScale().fitContent();
                    addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `${ticker} ${timeframe} 과거 봉 ${data.length}개 렌더링 완료.`);
                }
            } catch (e) {
                console.error("Error loading candles:", e);
                addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', `과거 데이터 호출 오류`);
            }
        }

        // 종목 스위칭
        function selectTicker(ticker) {
            if (activeTicker === ticker) return;
            
            document.getElementById(`card-${activeTicker}`).classList.remove('active');
            document.getElementById(`card-${ticker}`).classList.add('active');
            
            activeTicker = ticker;
            updateChartHeader();
            
            // 데이터 로드 호출
            const cacheKey = `${ticker}_${activeTimeframe}`;
            if (candleCache[cacheKey]) {
                candlestickSeries.setData(candleCache[cacheKey]);
                chart.timeScale().fitContent();
            } else {
                loadHistoricalCandles(activeTicker, activeTimeframe);
            }
        }

        // 타임프레임 변경
        function changeTimeframe(timeframe) {
            if (activeTimeframe === timeframe) return;
            
            document.getElementById(`tf-${activeTimeframe}`).classList.remove('active');
            document.getElementById(`tf-${timeframe}`).classList.add('active');
            
            activeTimeframe = timeframe;
            updateChartHeader();
            
            // 일/주/월 봉 변경에 따라 시간축 포맷 조절
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

            const cacheKey = `${activeTicker}_${timeframe}`;
            if (candleCache[cacheKey]) {
                candlestickSeries.setData(candleCache[cacheKey]);
                chart.timeScale().fitContent();
            } else {
                loadHistoricalCandles(activeTicker, activeTimeframe);
            }
        }

        function updateChartHeader() {
            const tfText = document.getElementById(`tf-${activeTimeframe}`).innerText;
            chartTitleEl.innerText = `${activeTicker}/KRW 실시간 ${tfText} 차트`;
        }

        // ══════════════════════════════════════════════════════════════
        // WebSocket 실시간 시세를 캔들 데이터에 실시간 병합(Merge)하는 알고리즘
        // ══════════════════════════════════════════════════════════════
        function mergeRealtimePriceToCandle(ticker, price, timestampMs) {
            const cacheKey = `${ticker}_${activeTimeframe}`;
            const cache = candleCache[cacheKey];
            if (!cache || cache.length === 0) return;
            
            let candleTime;
            
            // 일/주/월 봉일 경우 YYYY-MM-DD 포맷 시간값 계산
            if (["D", "W", "M"].includes(activeTimeframe)) {
                const dateObj = new Date(timestampMs);
                const year = dateObj.getFullYear();
                const month = String(dateObj.getMonth() + 1).padStart(2, '0');
                const day = String(dateObj.getDate()).padStart(2, '0');
                
                if (activeTimeframe === 'D') {
                    candleTime = `${year}-${month}-${day}`;
                } else if (activeTimeframe === 'W') {
                    // 주봉 계산 (해당 주의 월요일 구하기)
                    const dayOfWeek = dateObj.getDay(); // 0(일요일) ~ 6(토요일)
                    const diffToMon = dateObj.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1);
                    const monDate = new Date(dateObj.setDate(diffToMon));
                    candleTime = `${monDate.getFullYear()}-${String(monDate.getMonth() + 1).padStart(2, '0')}-${String(monDate.getDate()).padStart(2, '0')}`;
                } else {
                    // 월봉 계산 (해당 월의 1일 구하기)
                    candleTime = `${year}-${month}-01`;
                }
            } else {
                // 분/시간 봉일 경우 초 단위 Unix Timestamp 정렬
                const interval = timeframeIntervals[activeTimeframe] || 60;
                const timestampSec = Math.floor(timestampMs / 1000);
                candleTime = Math.floor(timestampSec / interval) * interval;
            }
            
            const lastCandle = cache[cache.length - 1];
            let targetCandle;
            
            if (lastCandle.time === candleTime) {
                // 기존 캔들 업데이트
                lastCandle.high = Math.max(lastCandle.high, price);
                lastCandle.low = Math.min(lastCandle.low, price);
                lastCandle.close = price;
                targetCandle = lastCandle;
            } else {
                // 신규 캔들 생성
                const newCandle = {
                    time: candleTime,
                    open: price,
                    high: price,
                    low: price,
                    close: price
                };
                cache.push(newCandle);
                if (cache.length > 200) cache.shift();
                targetCandle = newCandle;
            }
            
            // 현재 활성화된 종목의 차트이면 실시간 렌더링
            if (ticker === activeTicker && candlestickSeries) {
                candlestickSeries.update(targetCandle);
            }
        }

        // ══════════════════════════════════════════════════════════════
        // WebSocket 연결 및 로그 수집
        // ══════════════════════════════════════════════════════════════
        const lastPrices = { BTC: 0, ETH: 0, XRP: 0 };

        const ws = new WebSocket(`ws://${window.location.host}/ws/trading-status`);

        ws.onopen = () => {
            wsStatusEl.innerText = 'WebSocket: Connected';
            wsStatusEl.style.backgroundColor = '#064e3b';
            wsStatusEl.style.color = '#34d399';
            wsStatusEl.style.boxShadow = '0 0 10px rgba(52, 211, 153, 0.2)';
            addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', '다중 타임프레임 차트 브로커 연결 성공');
            
            // 첫 진입 시 BTC 1분봉 과거 데이터 즉각 로드
            loadHistoricalCandles('BTC', '1m');
            loadHistoricalCandles('ETH', '1m');
            loadHistoricalCandles('XRP', '1m');
        };

        ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            const timeStr = new Date(msg.data.timestamp).toLocaleTimeString();
            const ticker = msg.data.ticker;
            
            if (msg.type === 'trade') {
                const price = msg.data.price;
                const priceEl = document.getElementById(`price-${ticker}`);
                const strategyEl = document.getElementById(`strategy-${ticker}`);
                const comp = msg.data.composite;
                
                // 1. 실시간 캔들 머지 업데이트
                mergeRealtimePriceToCandle(ticker, price, msg.data.timestamp);
                
                if (priceEl) {
                    const formattedPrice = ticker === 'XRP' ? price.toFixed(2) : price.toLocaleString();
                    priceEl.innerText = formattedPrice + ' KRW';
                    
                    if (lastPrices[ticker] !== 0) {
                        if (price > lastPrices[ticker]) {
                            priceEl.className = 'price-display flash-up';
                        } else if (price < lastPrices[ticker]) {
                            priceEl.className = 'price-display flash-down';
                        }
                        setTimeout(() => { priceEl.className = 'price-display'; }, 150);
                    }
                    lastPrices[ticker] = price;
                }
                
                if (strategyEl && comp && comp.indicators) {
                    const currentStrategyVal = ticker === 'BTC' ? `RSI: ${comp.indicators.RSI}` : 
                                               ticker === 'ETH' ? `MACD: ${comp.indicators.MACD}` :
                                               `BB: ${comp.indicators.Bollinger}`;
                    strategyEl.innerText = `지표: ${currentStrategyVal} (결정: ${comp.final})`;
                }

                // 15% Throttling 로깅
                if (comp && Math.random() < 0.15) {
                    const subSigStr = Object.entries(comp.sub_signals)
                        .map(([name, s]) => `${name}(${s})`)
                        .join(' + ');
                    addLog(
                        timeStr, 
                        ticker, 
                        'TRADE', 
                        `[${comp.logic} 결합] ${subSigStr} -> 종합결정: ${comp.final} (현재가: ${price.toLocaleString()} KRW)`
                    );
                }
            } else if (msg.type === 'order') {
                const signal = msg.data.signal;
                const price = msg.data.price;
                addLog(
                    timeStr, 
                    ticker, 
                    signal, 
                    `★★ [주문 발송] 복합 전략이 최종 ${signal} 합의 도달! 체결 가격 @ ${price.toLocaleString()} KRW`
                );
            }
        };

        ws.onclose = () => {
            wsStatusEl.innerText = 'WebSocket: Disconnected';
            wsStatusEl.style.backgroundColor = '#7f1d1d';
            wsStatusEl.style.color = '#fca5a5';
            wsStatusEl.style.boxShadow = '0 0 10px rgba(239, 68, 68, 0.2)';
            addLog(new Date().toLocaleTimeString(), null, 'SYSTEM', '다중 타임프레임 차트 브로커 연결 해제');
        };
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
    account = AccountManager(initial_balance=10_000_000)
    engine = MultiTickerUIEngine(account)
    
    # 1. BTC: AND 결합 (RSI + Bollinger)
    btc_strategy = VerboseCompositeStrategy(
        strategies=[BithumbRsiStrategy(), BithumbBollingerStrategy()],
        logic="AND"
    )
    engine.register_ticker("BTC", btc_strategy, AllowAllRiskManager())
    
    # 2. ETH: OR 결합 (RSI + MACD)
    eth_strategy = VerboseCompositeStrategy(
        strategies=[BithumbRsiStrategy(), BithumbMacdStrategy()],
        logic="OR"
    )
    engine.register_ticker("ETH", eth_strategy, AllowAllRiskManager())
    
    # 3. XRP: VOTE 결합 (RSI + MACD + Bollinger)
    xrp_strategy = VerboseCompositeStrategy(
        strategies=[BithumbRsiStrategy(), BithumbMacdStrategy(), BithumbBollingerStrategy()],
        logic="VOTE"
    )
    engine.register_ticker("XRP", xrp_strategy, AllowAllRiskManager())
    
    # 엔진 스레드 구동
    engine.start()
    print("[UI Server] 다중 타임프레임 지원형 복합 전략 엔진 가동.")
    
    # 빗썸 다중 웹소켓 연결
    ws_listener = MultiTickerWebSocketListener(engine.dispatcher, markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"])
    ws_listener.start()
    print("[UI Server] 빗썸 실시간 다중 마켓 피드 가동.")


@app.on_event("startup")
def startup_event():
    # 1. 큐 모니터링 브로드캐스터 비동기 태스크 시작
    asyncio.create_task(ui_event_broadcaster())
    # 2. 복합 엔진 구동
    start_composite_engine()

if __name__ == "__main__":
    import uvicorn
    # 검증을 위한 로컬 서버 가동 (127.0.0.1:8005)
    uvicorn.run(app, host="127.0.0.1", port=8005)

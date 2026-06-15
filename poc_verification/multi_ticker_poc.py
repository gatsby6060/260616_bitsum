import sys
import os
import json
import time
import queue
import threading
import uuid
import websocket
import pandas as pd
import numpy as np

# 템플릿 경로 추가
TEMPLATE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 
    "../.agents/skills/auto-trading-builder/templates"
))
sys.path.append(TEMPLATE_PATH)

from core_engine import BaseStrategy, BaseRiskManager, TradingEngine, AccountManager

# ══════════════════════════════════════════════════════════════
# 1. 종목별 실시간 동적 기술 지표 전략 구현
# ══════════════════════════════════════════════════════════════

class BithumbRsiStrategy(BaseStrategy):
    """BTC용 실시간 RSI 전략 (Fast 검증을 위해 period=5 적용)"""
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

        # RSI 계산
        delta = pd.Series(self.prices).diff()
        gain = delta.clip(lower=0).rolling(window=period).mean()
        loss = (-delta.clip(upper=0)).rolling(window=period).mean()
        rs = gain / (loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))
        curr_rsi = rsi.iloc[-1]

        print(f"[Worker-BTC] 가격: {price:,.0f} | 데이터수: {len(self.prices)} | RSI: {curr_rsi:.2f}")

        if curr_rsi < self.params["oversold"]: return "BUY"
        if curr_rsi > self.params["overbought"]: return "SELL"
        return "HOLD"


class BithumbMacdStrategy(BaseStrategy):
    """ETH용 실시간 MACD 전략 (Fast 검증을 위해 fast=5, slow=10, signal=3 적용)"""
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

        # EMA 및 MACD 계산
        df = pd.Series(self.prices)
        ema_fast = df.ewm(span=self.params["fast"], adjust=False).mean()
        ema_slow = df.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=self.params["signal_period"], adjust=False).mean()

        curr_macd = macd.iloc[-1]
        curr_sig = macd_signal.iloc[-1]
        diff = curr_macd - curr_sig

        print(f"[Worker-ETH] 가격: {price:,.0f} | 데이터수: {len(self.prices)} | MACD: {curr_macd:.4f} | Sig: {curr_sig:.4f}")

        # 골든크로스 / 데드크로스 검출
        if len(macd) >= 2:
            prev_diff = macd.iloc[-2] - macd_signal.iloc[-2]
            if prev_diff <= 0 and diff > 0:
                return "BUY"
            if prev_diff >= 0 and diff < 0:
                return "SELL"
        return "HOLD"


class BithumbBollingerStrategy(BaseStrategy):
    """XRP용 실시간 볼린저 밴드 전략 (Fast 검증을 위해 period=5, std_dev=1.5 적용)"""
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

        # 볼린저 밴드 계산
        df = pd.Series(self.prices)
        ma = df.rolling(window=period).mean()
        std = df.rolling(window=period).std()
        upper = ma + self.params["std_dev"] * std
        lower = ma - self.params["std_dev"] * std

        curr_upper = upper.iloc[-1]
        curr_lower = lower.iloc[-1]

        # 밴드 폭 대비 위치 출력
        print(f"[Worker-XRP] 가격: {price:,.4f} | 데이터수: {len(self.prices)} | BB-Upper: {curr_upper:.4f} | BB-Lower: {curr_lower:.4f}")

        if price < curr_lower: return "BUY"
        if price > curr_upper: return "SELL"
        return "HOLD"


class AllowAllRiskManager(BaseRiskManager):
    def is_allowed(self, signal: str, position: dict) -> bool:
        return True


# ══════════════════════════════════════════════════════════════
# 2. 다중 종목 구독 지원 Bithumb 웹소켓 리스너
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
        self._thread = threading.Thread(target=self._run_loop, name="MultiBithumbWS", daemon=True)
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
            except Exception as e:
                print(f"[WS-Multi] 연결 실패/종료 예외: {e}")
            if self._running:
                time.sleep(3)

    def _on_open(self, ws):
        print("[WS-Multi] 빗썸 서버와 다중 종목 연결 수립 완료")
        sub_msg = [
            {"ticket": str(uuid.uuid4())},
            {"type": "trade", "codes": self.markets}
        ]
        ws.send(json.dumps(sub_msg))
        print(f"[WS-Multi] 다중 종목 구독 메시지 전송: {self.markets}")

    def _on_message(self, ws, message):
        if message == "PING":
            ws.send("PONG")
            return

        try:
            msg = json.loads(message)
            if msg.get("type") == "trade":
                market_code = msg.get("code", "")
                ticker = market_code.replace("KRW-", "")
                
                # 공통 규격 변환
                data = {
                    "ticker": ticker,
                    "price": float(msg["trade_price"]),
                    "volume": float(msg["trade_volume"]),
                    "timestamp": msg.get("timestamp", int(time.time() * 1000))
                }
                
                # 라우팅
                self.dispatcher.dispatch(ticker, data)
        except Exception as e:
            print(f"[WS-Multi] 데이터 분배 중 에러: {e}")

    def _on_error(self, ws, error):
        print(f"[WS-Multi] 에러: {error}")

    def _on_close(self, ws, close_code, close_msg):
        print(f"[WS-Multi] 소켓 종료")


# ══════════════════════════════════════════════════════════════
# 3. 통합 가동 및 멀티스레드 검증
# ══════════════════════════════════════════════════════════════
def verify_multithreading():
    print("=== [4] 다중 종목(BTC, ETH, XRP) 멀티스레딩 및 동적 전략 실시간 검증 ===")
    
    # 1. 천만 원 가상 계좌 관리자
    account = AccountManager(initial_balance=10_000_000)
    
    # 2. 거래 엔진
    engine = TradingEngine(account)
    
    # 3. 각 종목별 동적 전략 & 리스크 관리 등록
    # BTC -> RSI, ETH -> MACD, XRP -> Bollinger Bands
    engine.register_ticker("BTC", BithumbRsiStrategy(), AllowAllRiskManager())
    engine.register_ticker("ETH", BithumbMacdStrategy(), AllowAllRiskManager())
    engine.register_ticker("XRP", BithumbBollingerStrategy(), AllowAllRiskManager())
    
    print("[Engine] 3개 종목 등록 완료 (BTC-RSI / ETH-MACD / XRP-Bollinger)")

    # 4. 엔진 시작 (워커 스레드 기동)
    engine.start()
    print("[Engine] 엔진 구동 완료 (각 종목 워커 스레드 구동 시작)")

    # 5. 다중 종목 웹소켓 연결
    ws_listener = MultiTickerWebSocketListener(engine.dispatcher, markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"])
    ws_listener.start()

    # 6. 30초 동안 병렬 스레드 처리 로깅 관찰
    print("[Verification] 30초 동안 3개 스레드(BTC, ETH, XRP)의 병렬 데이터 연산 상태를 로깅합니다...\n")
    time.sleep(30)

    # 7. 종료 및 회수
    print("\n[Verification] 실시간 검증 종료. 모든 스레드 회수 진행...")
    ws_listener.stop()
    engine.stop()
    print("[Verification] 모든 자원과 스레드가 안전하게 종료되었습니다.")

if __name__ == "__main__":
    verify_multithreading()

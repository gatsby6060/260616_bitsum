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

# auto-trading-builder 템플릿 디렉토리를 path에 추가하여 core_engine 가져오기
TEMPLATE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 
    "../.agents/skills/auto-trading-builder/templates"
))
sys.path.append(TEMPLATE_PATH)

from core_engine import BaseStrategy, BaseRiskManager, TradingEngine, AccountManager

# ══════════════════════════════════════════════════════════════
# 1. 빗썸 맞춤형 단순 실시간 전략 구현 (검증용)
# ══════════════════════════════════════════════════════════════
class BithumbRsiStrategy(BaseStrategy):
    """
    실시간 수신 데이터를 누적하여 RSI를 계산하고 신호를 내보내는 검증용 전략.
    실제 pandas-ta 또는 단순 계산 방식을 적용.
    """
    NAME = "BithumbRsi"
    PARAMS = {"period": 14, "buy_threshold": 30, "sell_threshold": 70}

    def __init__(self, params=None):
        super().__init__(params)
        self.prices = []
        self.max_len = 100

    def generate_signal(self, data: dict) -> str:
        price = data.get("price")
        if price is None:
            return "HOLD"
        
        self.prices.append(price)
        if len(self.prices) > self.max_len:
            self.prices.pop(0)

        # RSI를 계산하기 위한 최소 데이터 수집
        period = self.params["period"]
        if len(self.prices) <= period:
            return "HOLD"

        # RSI 계산 (단순 구현)
        df = pd.DataFrame(self.prices, columns=["close"])
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / (loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]

        print(f"[Strategy-BTC] 현재가: {price:,.0f} KRW | 데이터 수: {len(self.prices)} | RSI: {current_rsi:.2f}")

        if current_rsi < self.params["buy_threshold"]:
            return "BUY"
        elif current_rsi > self.params["sell_threshold"]:
            return "SELL"
        
        return "HOLD"


class BithumbStopLossRiskManager(BaseRiskManager):
    """실시간 손절/익절 리스크 검증용"""
    def __init__(self, stop_loss_pct=0.02):
        self.stop_loss_pct = stop_loss_pct

    def is_allowed(self, signal: str, position: dict) -> bool:
        # 손절 비율 체크 예시
        if position.get("pnl_pct", 0) <= -self.stop_loss_pct:
            print(f"[RiskManager] 손절 비율 도달! PnL: {position['pnl_pct']*100:.2f}% (허용치: -{self.stop_loss_pct*100}%)")
            return False
        return True


# ══════════════════════════════════════════════════════════════
# 2. 빗썸 전용 WebSocket Listener 구현
# ══════════════════════════════════════════════════════════════
class BithumbWebSocketListener:
    """
    빗썸 Public WebSocket 규격을 맞춘 전용 리스너.
    - PING-PONG 자동 대응
    - trade 수신 데이터 파싱 후 DataDispatcher 라우팅
    """
    def __init__(self, dispatcher, markets=["KRW-BTC"]):
        self.dispatcher = dispatcher
        self.markets = markets
        self.url = "wss://ws-api.bithumb.com/websocket/v1"
        self._ws = None
        self._running = False

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, name="BithumbWS", daemon=True)
        self._thread.start()
        print("[WebSocket] 빗썸 웹소켓 스레드 시작됨.")

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
                print(f"[WebSocket] 연결 루프 예외 발생: {e}")
            if self._running:
                print("[WebSocket] 3초 후 재연결 시도...")
                time.sleep(3)

    def _on_open(self, ws):
        print("[WebSocket] 빗썸 서버와 소켓 연결 성공")
        # 빗썸 구독 형식 전송
        sub_msg = [
            {"ticket": str(uuid.uuid4())},
            {"type": "trade", "codes": self.markets}
        ]
        ws.send(json.dumps(sub_msg))
        print(f"[WebSocket] 구독 메시지 발송 완료: {sub_msg}")

    def _on_message(self, ws, message):
        # 빗썸 서버의 PING 수신 처리 (매우 중요)
        if message == "PING":
            ws.send("PONG")
            print("[WebSocket] PING 수신 -> PONG 전송 완료 (연결 유지)")
            return

        try:
            msg = json.loads(message)
            msg_type = msg.get("type")
            
            # 실시간 체결 처리
            if msg_type == "trade":
                ticker = msg.get("code", "").replace("KRW-", "")
                trade_price = float(msg.get("trade_price", 0))
                volume = float(msg.get("trade_volume", 0))
                
                # 공통 포맷화하여 Dispatcher에 전송
                data = {
                    "ticker": ticker,
                    "price": trade_price,
                    "volume": volume,
                    "timestamp": msg.get("timestamp", 0)
                }
                # print(f"[WebSocket Received] {ticker} | Price: {trade_price:,}")
                self.dispatcher.dispatch(ticker, data)
                
        except Exception as e:
            print(f"[WebSocket] 메시지 파싱 에러: {e} | Message: {message}")

    def _on_error(self, ws, error):
        print(f"[WebSocket] 에러 발생: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print(f"[WebSocket] 소켓 닫힘 (코드: {close_status_code}, 메시지: {close_msg})")


# ══════════════════════════════════════════════════════════════
# 3. 전체 조립 및 실시간 동작 검증 메인 루프
# ══════════════════════════════════════════════════════════════
def run_integration_verification():
    print("=== [3] 빗썸 WebSocket 실시간 시세 + 거래 엔진 통합 검증 ===")
    
    # 1. Thread-safe 계좌 매니저 생성 (천만 원 가상 예수금)
    account = AccountManager(initial_balance=10_000_000)
    
    # 2. 거래 엔진 구축
    engine = TradingEngine(account)
    
    # 3. 종목 등록 (BTC에 대해 RSI 전략 및 손절 리스크 매니저 등록)
    strategy = BithumbRsiStrategy(params={"period": 5, "buy_threshold": 40, "sell_threshold": 60}) # 빠른 검증 위해 기간 5로 셋팅
    risk_manager = BithumbStopLossRiskManager(stop_loss_pct=0.03)
    
    engine.register_ticker(
        ticker="BTC",
        strategy=strategy,
        risk_manager=risk_manager
    )
    
    # 4. 트레이딩 엔진 스레드 가동
    engine.start()
    print("[Engine] 거래 엔진 가동 완료 (워커 스레드 및 주문 처리 스레드 실행 중)")

    # 5. 웹소켓 리스너 조립 및 시작 (실제 빗썸 데이터 피딩 시작)
    ws_listener = BithumbWebSocketListener(dispatcher=engine.dispatcher, markets=["KRW-BTC"])
    ws_listener.start()

    # 6. 실시간 수신 및 엔진 작동을 20초간 지켜보기
    print("[Verification] 20초 동안 실시간 데이터 흐름 및 전략 시그널 생성을 감시합니다...\n")
    time.sleep(20)
    
    # 7. 종료
    print("\n[Verification] 실시간 검증 완료. 자원 회수 진행...")
    ws_listener.stop()
    engine.stop()
    print("[Verification] 모든 스레드 안전하게 종료되었습니다.")

if __name__ == "__main__":
    run_integration_verification()

"""
WebSocket 리스너 템플릿
────────────────────────
거래소 WebSocket에서 실시간 체결가를 수신하여
DataDispatcher를 통해 종목별 TickerWorker 큐로 전달한다.

주의:
  - on_message 콜백은 최대한 가볍게 유지한다 (큐 put 후 즉시 반환).
  - 거래소마다 메시지 형식이 다르므로 _parse() 메서드를 수정한다.
  - 연결 끊김 시 지수 백오프(Exponential Backoff) 재연결을 사용한다.
"""

import json
import threading
import time
from typing import Any, Dict, Optional

try:
    import websocket  # pip install websocket-client
except ImportError:
    raise ImportError("websocket-client 패키지가 필요합니다: pip install websocket-client")

from core_engine import DataDispatcher


class WebSocketListener:
    """
    거래소 WebSocket 연결을 유지하며 실시간 데이터를 수신한다.

    사용 예시:
        listener = WebSocketListener(
            dispatcher=engine.dispatcher,
            url="wss://api.upbit.com/websocket/v1",
            subscribe_msg=[{"ticket": "test"}, {"type": "trade", "codes": ["KRW-BTC", "KRW-ETH"]}],
        )
        listener.start()
    """

    MAX_RETRY_DELAY = 60  # 최대 재연결 대기 시간 (초)

    def __init__(
        self,
        dispatcher: DataDispatcher,
        url: str,
        subscribe_msg: Any,
    ):
        self.dispatcher = dispatcher
        self.url = url
        self.subscribe_msg = subscribe_msg
        self._ws: Optional[websocket.WebSocketApp] = None
        self._retry_delay = 1
        self._running = True

    def start(self) -> None:
        """별도 스레드에서 WebSocket 연결 루프 시작."""
        t = threading.Thread(target=self._connect_loop, name="WebSocketListener", daemon=True)
        t.start()

    def stop(self) -> None:
        self._running = False
        if self._ws:
            self._ws.close()

    # ── 재연결 루프 ──────────────────────────────────────────────
    def _connect_loop(self) -> None:
        while self._running:
            self._ws = websocket.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws.run_forever()
            if self._running:
                print(f"[WebSocket] {self._retry_delay}초 후 재연결 시도...")
                time.sleep(self._retry_delay)
                self._retry_delay = min(self._retry_delay * 2, self.MAX_RETRY_DELAY)

    # ── WebSocket 콜백 ───────────────────────────────────────────
    def _on_open(self, ws) -> None:
        ws.send(json.dumps(self.subscribe_msg))
        self._retry_delay = 1  # 연결 성공 시 재연결 딜레이 초기화
        print("[WebSocket] 연결 성공")

    def _on_message(self, ws, raw: str) -> None:
        """
        수신 즉시 파싱 후 dispatcher에 전달. 이 함수는 최대한 가볍게 유지한다.
        복잡한 계산은 절대 여기서 하지 않는다.
        """
        try:
            data = self._parse(raw)
            if data and "ticker" in data:
                self.dispatcher.dispatch(data["ticker"], data)
        except Exception as e:
            print(f"[WebSocket] 메시지 처리 오류: {e}")

    def _on_error(self, ws, error) -> None:
        print(f"[WebSocket] 오류: {error}")

    def _on_close(self, ws, *args) -> None:
        print("[WebSocket] 연결 종료")

    # ── 거래소별 메시지 파싱 (수정 필요) ────────────────────────
    def _parse(self, raw: str) -> Optional[Dict[str, Any]]:
        """
        거래소 메시지를 공통 포맷으로 변환한다.
        반환 포맷: {"ticker": "BTC", "price": 100000000, ...}

        ── 업비트(Upbit) 예시 ──
        raw = {"type":"trade","code":"KRW-BTC","trade_price":100000000,...}
        → {"ticker": "BTC", "price": 100000000}

        ── 바이낸스(Binance) 예시 ──
        raw = {"s":"BTCUSDT","p":"65000.00",...}
        → {"ticker": "BTC", "price": 65000.0}
        """
        msg = json.loads(raw)

        # ── 업비트 형식 ─────────────────────────────────────────
        if "code" in msg and "trade_price" in msg:
            ticker = msg["code"].replace("KRW-", "")
            return {
                "ticker": ticker,
                "price": float(msg["trade_price"]),
                "volume": float(msg.get("trade_volume", 0)),
                "timestamp": msg.get("timestamp", 0),
            }

        # ── 바이낸스 형식 ────────────────────────────────────────
        if "s" in msg and "p" in msg:
            ticker = msg["s"].replace("USDT", "")
            return {
                "ticker": ticker,
                "price": float(msg["p"]),
                "volume": float(msg.get("q", 0)),
            }

        # ── 커스텀 형식 (직접 구현) ──────────────────────────────
        return None

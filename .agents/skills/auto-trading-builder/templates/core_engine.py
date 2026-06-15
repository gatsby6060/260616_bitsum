"""
자동매매 시스템 핵심 엔진 템플릿
─────────────────────────────────
포함 모듈:
  - BaseStrategy / BaseRiskManager  : 전략·리스크 인터페이스
  - CompositeStrategy               : 다중 전략 신호 통합 (AND / OR / VOTE)
  - TickerWorker                    : 종목별 독립 스레드
  - DataDispatcher                  : WebSocket → 종목 큐 라우팅
  - AccountManager                  : Thread-Safe 잔고 관리
  - TradingEngine                   : 전체 조립 및 실행
"""

import abc
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ══════════════════════════════════════════════════════════════
# 1. 인터페이스 (ABC)
# ══════════════════════════════════════════════════════════════

class BaseStrategy(abc.ABC):
    """모든 전략 파일은 이 클래스를 상속받아 구현한다."""

    # UI 표시 이름 및 기본 파라미터 — 서브클래스에서 반드시 재정의
    NAME: str = "Unnamed"
    PARAMS: Dict[str, Any] = {}

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = {**self.PARAMS, **(params or {})}

    @abc.abstractmethod
    def generate_signal(self, data: Dict[str, Any]) -> str:
        """
        매매 신호 생성.
        반환값: 'BUY' | 'SELL' | 'HOLD'
        """
        ...


class BaseRiskManager(abc.ABC):
    """모든 리스크 관리자는 이 클래스를 상속받아 구현한다."""

    @abc.abstractmethod
    def is_allowed(self, signal: str, position: Dict[str, Any]) -> bool:
        """
        주문 허용 여부 판단.
        True: 주문 허용 / False: 주문 차단
        """
        ...


# ══════════════════════════════════════════════════════════════
# 2. 복합 전략 (Composite Pattern)
# ══════════════════════════════════════════════════════════════

class CompositeStrategy(BaseStrategy):
    """
    여러 전략의 신호를 하나로 통합한다.

    logic 옵션:
      'AND'  — 모든 전략이 BUY여야 BUY (보수적)
      'OR'   — 하나라도 BUY면 BUY (공격적)
      'VOTE' — 과반수 득표 전략 채택
    """

    NAME = "Composite"

    def __init__(self, strategies: List[BaseStrategy], logic: str = "AND"):
        super().__init__()
        self.strategies = strategies
        self.logic = logic.upper()

    def generate_signal(self, data: Dict[str, Any]) -> str:
        signals = [s.generate_signal(data) for s in self.strategies]

        if self.logic == "AND":
            if all(s == "BUY" for s in signals):
                return "BUY"
            if any(s == "SELL" for s in signals):
                return "SELL"

        elif self.logic == "OR":
            if any(s == "BUY" for s in signals):
                return "BUY"
            if all(s == "SELL" for s in signals):
                return "SELL"

        elif self.logic == "VOTE":
            majority = len(signals) // 2 + 1
            if signals.count("BUY") >= majority:
                return "BUY"
            if signals.count("SELL") >= majority:
                return "SELL"

        return "HOLD"


# ══════════════════════════════════════════════════════════════
# 3. 포지션 상태 (Dataclass)
# ══════════════════════════════════════════════════════════════

@dataclass
class Position:
    """종목별 포지션 상태 — TickerWorker 내부에서만 접근한다."""
    ticker: str
    quantity: float = 0.0
    avg_price: float = 0.0
    current_price: float = 0.0

    @property
    def pnl_pct(self) -> float:
        if self.avg_price == 0:
            return 0.0
        return (self.current_price - self.avg_price) / self.avg_price


# ══════════════════════════════════════════════════════════════
# 4. 종목별 독립 워커 스레드
# ══════════════════════════════════════════════════════════════

class TickerWorker(threading.Thread):
    """
    종목 하나를 전담하는 독립 워커 스레드.

    - 자체 Queue(maxsize=100)로 데이터를 수신
    - 자체 Strategy + RiskManager 보유
    - 주문 신호는 공유 order_queue로 전달
    """

    def __init__(
        self,
        ticker: str,
        strategy: BaseStrategy,
        risk_manager: BaseRiskManager,
        order_queue: queue.Queue,
        account_manager: "AccountManager",
    ):
        super().__init__(name=f"Worker-{ticker}", daemon=True)
        self.ticker = ticker
        self.strategy = strategy
        self.risk_manager = risk_manager
        self.order_queue = order_queue
        self.account = account_manager
        self.data_queue: queue.Queue = queue.Queue(maxsize=100)
        self.position = Position(ticker=ticker)
        self._stop_event = threading.Event()

    # ── 외부(DataDispatcher)에서 호출 ───────────────────────────
    def push_data(self, data: Dict[str, Any]) -> None:
        """큐가 가득 찼을 때 가장 오래된 데이터를 드롭하고 재삽입."""
        try:
            self.data_queue.put_nowait(data)
        except queue.Full:
            try:
                self.data_queue.get_nowait()
            except queue.Empty:
                pass
            self.data_queue.put_nowait(data)

    # ── 스레드 메인 루프 ─────────────────────────────────────────
    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                data = self.data_queue.get(timeout=1.0)
                self._process(data)
                self.data_queue.task_done()
            except queue.Empty:
                continue

    def _process(self, data: Dict[str, Any]) -> None:
        # 1. 현재가 업데이트
        self.position.current_price = data.get("price", self.position.current_price)

        # 2. 전략 신호 생성
        signal = self.strategy.generate_signal(data)
        if signal == "HOLD":
            return

        # 3. 리스크 관리 검증
        position_info = {
            "avg_price": self.position.avg_price,
            "current_price": self.position.current_price,
            "quantity": self.position.quantity,
            "pnl_pct": self.position.pnl_pct,
        }
        if not self.risk_manager.is_allowed(signal, position_info):
            return

        # 4. 잔고 확인 (매수 시)
        if signal == "BUY":
            required = self.position.current_price  # 1주/1개 기준
            if not self.account.reserve(required):
                return  # 잔고 부족

        # 5. 주문 큐에 전달
        self.order_queue.put({
            "ticker": self.ticker,
            "signal": signal,
            "price": self.position.current_price,
        })

    def stop(self) -> None:
        self._stop_event.set()


# ══════════════════════════════════════════════════════════════
# 5. 데이터 분배기
# ══════════════════════════════════════════════════════════════

class DataDispatcher:
    """
    WebSocket 수신 데이터를 종목별 TickerWorker 큐로 라우팅한다.
    on_message 콜백 내에서 호출되므로 최대한 빠르게 반환해야 한다.
    """

    def __init__(self):
        self._workers: Dict[str, TickerWorker] = {}

    def register(self, ticker: str, worker: TickerWorker) -> None:
        self._workers[ticker] = worker

    def dispatch(self, ticker: str, data: Dict[str, Any]) -> None:
        worker = self._workers.get(ticker)
        if worker:
            worker.push_data(data)


# ══════════════════════════════════════════════════════════════
# 6. Thread-Safe 계좌 관리자
# ══════════════════════════════════════════════════════════════

class AccountManager:
    """
    공유 잔고를 Thread-Safe하게 관리한다.
    여러 TickerWorker가 동시에 접근하므로 Lock 필수.
    """

    def __init__(self, initial_balance: float):
        self._balance = initial_balance
        self._lock = threading.Lock()

    def get_balance(self) -> float:
        with self._lock:
            return self._balance

    def reserve(self, amount: float) -> bool:
        """매수 전 잔고 예약. 잔고 부족 시 False 반환."""
        with self._lock:
            if self._balance >= amount:
                self._balance -= amount
                return True
            return False

    def release(self, amount: float) -> None:
        """매도 후 잔고 반환."""
        with self._lock:
            self._balance += amount

    def sync(self, new_balance: float) -> None:
        """거래소 REST API 잔고 동기화 시 호출."""
        with self._lock:
            self._balance = new_balance


# ══════════════════════════════════════════════════════════════
# 7. 트레이딩 엔진 (최상위 조립체)
# ══════════════════════════════════════════════════════════════

class TradingEngine:
    """
    모든 모듈을 조립하고 데이터 흐름을 제어하는 중심 엔진.

    사용 예시:
        engine = TradingEngine(AccountManager(10_000_000))
        engine.register_ticker(
            ticker="BTC",
            strategy=CompositeStrategy([RsiStrategy(), MacdStrategy()], "AND"),
            risk_manager=StopLossRiskManager(0.03, 0.06),
        )
        engine.start()
        # WebSocket 콜백에서:
        engine.on_market_data("BTC", {"price": 100_000_000, "rsi": 28})
    """

    def __init__(self, account: AccountManager):
        self.account = account
        self.order_queue: queue.Queue = queue.Queue()
        self.dispatcher = DataDispatcher()
        self._workers: Dict[str, TickerWorker] = {}

    def register_ticker(
        self,
        ticker: str,
        strategy: BaseStrategy,
        risk_manager: BaseRiskManager,
    ) -> None:
        """종목 등록 — 전용 TickerWorker 생성 및 Dispatcher에 등록."""
        worker = TickerWorker(
            ticker=ticker,
            strategy=strategy,
            risk_manager=risk_manager,
            order_queue=self.order_queue,
            account_manager=self.account,
        )
        self._workers[ticker] = worker
        self.dispatcher.register(ticker, worker)

    def on_market_data(self, ticker: str, data: Dict[str, Any]) -> None:
        """WebSocket on_message 콜백에서 호출한다."""
        self.dispatcher.dispatch(ticker, data)

    def start(self) -> None:
        """모든 워커 스레드 및 주문 처리 스레드 시작."""
        for worker in self._workers.values():
            worker.start()
        threading.Thread(
            target=self._order_loop, name="OrderExecutor", daemon=True
        ).start()

    def stop(self) -> None:
        """모든 워커 스레드 안전 종료."""
        for worker in self._workers.values():
            worker.stop()

    def _order_loop(self) -> None:
        """공유 주문 큐를 소비하여 거래소 API 호출."""
        while True:
            order = self.order_queue.get()
            self._execute_order(order)
            self.order_queue.task_done()

    def _execute_order(self, order: Dict[str, Any]) -> None:
        """
        실제 거래소 API 호출 로직을 여기에 구현한다.
        예: upbit_api.order(order['ticker'], order['signal'], order['price'])
        """
        print(
            f"[ORDER] {order['ticker']} | {order['signal']} "
            f"@ {order['price']:,.0f}"
        )

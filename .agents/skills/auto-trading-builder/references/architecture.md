# 자동매매 시스템 아키텍처 참조

## 5계층 구조

```
① WebSocket Listener  — 거래소 실시간 체결가 수신 (단일 스레드)
        ↓
② DataDispatcher      — 종목 코드 기준으로 워커 큐에 라우팅
        ↓
③ TickerWorker × N    — 종목별 독립 스레드 (각자 Queue 보유)
   ├─ CompositeStrategy  → 다중 전략 신호 통합 (AND/OR/VOTE)
   └─ RiskManager        → 손절/익절 검증
        ↓
④ 공유 주문 큐 → OrderExecutor  — 거래소 API 순차 호출 (단일 스레드)
        ↕  (Lock)
⑤ AccountManager      — Thread-Safe 잔고 동기화
```

## 스레드 수 계산

| 역할 | 스레드 수 |
|---|---|
| WebSocket Listener | 1 |
| TickerWorker | 종목 수 N |
| OrderExecutor | 1 |
| AccountSync (REST 주기 동기화) | 1 |
| **합계** | **N + 3** |

## 전략 플러그인 구조

```
strategies/
  rsi_strategy.py          ← RsiStrategy(BaseStrategy)
  macd_strategy.py         ← MacdStrategy(BaseStrategy)
  disparity_strategy.py    ← DisparityStrategy(BaseStrategy)
  bollinger_strategy.py    ← BollingerStrategy(BaseStrategy)
  (추가 파일 → 자동 인식)
```

StrategyLoader.scan()이 importlib로 디렉토리를 스캔하여
BaseStrategy 서브클래스를 자동 탐색한다.

## 동시성 규칙

- TickerWorker 내부 상태(Position)는 해당 스레드만 접근 → Lock 불필요
- AccountManager._balance는 여러 스레드가 접근 → threading.Lock 필수
- 주문 큐(order_queue)는 queue.Queue → 자체적으로 Thread-Safe
- WebSocket on_message 콜백 내에서 계산 금지 → 큐 put 후 즉시 반환

## 백테스트 전환 방법

실전 매매와 백테스트는 동일한 Strategy + RiskManager를 공유한다.
교체 대상만 다르다:

| 구성요소 | 실전 매매 | 백테스트 |
|---|---|---|
| 데이터 소스 | WebSocketListener | CsvDataFeeder |
| 주문 실행 | 거래소 API | VirtualOrderExecutor |
| Strategy/Risk | 동일 | 동일 |

## 성능 확장 가이드

| 종목 수 | 권장 방식 |
|---|---|
| ~20개 | threading (현재 구조) |
| 20~100개 | asyncio + async queue |
| 100개 이상 | multiprocessing (GIL 우회) |

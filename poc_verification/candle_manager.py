import time
import os
import requests
from datetime import datetime, timezone
import threading

try:
    from timeframe_aggregate import (
        daily_ohlcv_from_bars,
        weekly_ohlcv_from_daily,
        monthly_ohlcv_from_daily,
        load_15m_bars_from_cache,
        regime_series_from_15m_bars,
    )
    HAS_AGGREGATE = True
except ImportError:
    HAS_AGGREGATE = False

# API로 직접 받지 않고 15분봉(또는 일봉)에서 파생하는 타임프레임
DERIVED_FROM_INTRADAY = frozenset({"D"})
DERIVED_FROM_DAILY = frozenset({"W", "M"})

class CandleManager:
    """
    매매 워커용 캔들 관리.
    - 전략용 분봉(1m~1h): 빗썸 API warm-up + 실시간 틱 갱신
    - 장세·하락패턴 분석용 일/주/월: 15분 캐시 집계 (get_regime_series)
    UI 차트는 /api/candles 로 빗썸 API 직접 조회 — 이 클래스와 무관
    """
    
    TIMEFRAME_MAP = {
        "1m": "minutes/1",
        "3m": "minutes/3",
        "5m": "minutes/5",
        "10m": "minutes/10",
        "15m": "minutes/15",
        "30m": "minutes/30",
        "1h": "minutes/60",
        "4h": "minutes/240",
        "D": "days",
        "W": "weeks",
        "M": "months"
    }

    TIMEFRAME_INTERVALS = {
        "1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400
    }

    def __init__(self, ticker: str, timeframes=None, max_candles=4500):
        self.ticker = ticker
        self.market = f"KRW-{ticker}"
        self.timeframes = timeframes or ["1m", "5m", "1h", "D"]
        self.max_candles = max_candles
        self.candles = {tf: [] for tf in self.timeframes}
        self.lock = threading.Lock()
        self._regime_series = None  # 15m 캐시 기반 장기 일·주·월 (별도 API 없음)
        self._last_derived_rebuild = 0.0

    def _rebuild_derived_from_15m(self):
        """메모리 15m + 캐시 15m → 일·주·월봉 파생."""
        if not HAS_AGGREGATE:
            return
        with self.lock:
            live_15m = list(self.candles.get("15m", []))
        cache_15m = load_15m_bars_from_cache(self.ticker)
        merged = self._merge_15m_series(cache_15m, live_15m)
        if not merged:
            return
        series = regime_series_from_15m_bars(merged)
        self._regime_series = series
        with self.lock:
            self.candles["D"] = series["daily_candles"][-self.max_candles:]
            self.candles["W"] = series["weekly_candles"]
            self.candles["M"] = series["monthly_candles"]

    @staticmethod
    def _merge_15m_series(cache_bars: list, live_candles: list) -> list:
        """캐시(15m ts) + 실시간(15m time) 병합 — ts 기준 중복 제거."""
        by_key = {}
        for bar in cache_bars or []:
            ts = bar.get("ts")
            if ts:
                by_key[ts] = {
                    "ts": ts,
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"]),
                    "volume": float(bar.get("volume", 0)),
                }
        kst_offset = 9 * 3600
        for c in live_candles or []:
            dt = datetime.fromtimestamp(c["time"] + kst_offset, tz=timezone.utc)
            ts = dt.strftime("%Y-%m-%dT%H:%M:%S+09:00")
            by_key[ts] = {
                "ts": ts,
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c.get("volume", 0)),
            }
        return [by_key[k] for k in sorted(by_key.keys())]

    def _maybe_rebuild_derived(self, min_interval_sec: float = 300.0):
        """15m 신규 봉 시 일·주·월 재파생 (과도한 캐시 스캔 방지)."""
        now = time.time()
        if now - self._last_derived_rebuild < min_interval_sec:
            return
        self._rebuild_derived_from_15m()
        self._last_derived_rebuild = now

    def get_regime_series(self) -> dict:
        """장세 분석용 일·주·월 (15m 집계). 없으면 빌드 시도."""
        if self._regime_series is None:
            self._rebuild_derived_from_15m()
        return self._regime_series or {}

    def warmup(self):
        """
        프로그램 기동 시 각 타임프레임별 과거 캔들을 조회하여 Warm-up합니다.
        """
        print(f"[CandleManager] {self.ticker} 과거 캔들 데이터 로딩(Warm-up)을 시작합니다. 대상: {self.timeframes}")
        base_url = "https://api.bithumb.com/v1"
        
        for tf in self.timeframes:
            if tf in DERIVED_FROM_INTRADAY or tf in DERIVED_FROM_DAILY:
                continue
            sub_path = self.TIMEFRAME_MAP.get(tf)
            if not sub_path:
                print(f"[CandleManager] 지원하지 않는 타임프레임: {tf}")
                continue
                
            url = f"{base_url}/candles/{sub_path}"
            count = 200 if tf == "15m" else 150
            
            success = False
            for attempt in range(3):
                try:
                    raw_candles = []
                    to_time = None
                    remaining = count
                    api_success = True
                    
                    while remaining > 0:
                        req_count = min(remaining, 200)
                        params = {"market": self.market, "count": req_count}
                        if to_time:
                            params["to"] = to_time
                            
                        resp = requests.get(url, params=params, timeout=5)
                        if resp.status_code == 200:
                            chunk = resp.json()
                            if not chunk:
                                break
                            raw_candles.extend(chunk)
                            remaining -= len(chunk)
                            if len(chunk) < req_count:
                                # 더 이상 가져올 데이터가 없음
                                break
                            # 다음 요청을 위해 가장 오래된 캔들의 시간 설정 (KST)
                            to_time = chunk[-1]["candle_date_time_kst"]
                        else:
                            print(f"[CandleManager] {self.ticker} {tf} 로드 실패 (상태 코드: {resp.status_code}): {resp.text}")
                            api_success = False
                            break
                        time.sleep(0.1) # API Rate Limit 방지를 위해 짧은 대기
                    
                    if api_success and raw_candles:
                        formatted = []
                        # 빗썸 응답은 최신순(0번 인덱스가 최신)이므로 역순 정렬하여 과거 -> 최신 순서로 삽입
                        for c in reversed(raw_candles):
                            if tf in ["D", "W", "M"]:
                                dt = datetime.strptime(c["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
                                t_val = int(dt.replace(tzinfo=timezone.utc).timestamp())
                            else:
                                t_val = c["timestamp"] // 1000
                                
                            formatted.append({
                                "time": t_val,
                                "open": float(c["opening_price"]),
                                "high": float(c["high_price"]),
                                "low": float(c["low_price"]),
                                "close": float(c["trade_price"]),
                                "volume": float(c["candle_acc_trade_volume"])
                            })
                        
                        with self.lock:
                            self.candles[tf] = formatted
                        print(f"[CandleManager] {self.ticker} {tf} 캔들 {len(formatted)}개 로드 완료.")
                        success = True
                        break
                    else:
                        print(f"[CandleManager] {self.ticker} {tf} 데이터 로드 실패 또는 데이터 없음.")
                except Exception as e:
                    print(f"[CandleManager] {self.ticker} {tf} API 요청 중 예외 발생 (시도 {attempt+1}/3): {e}")
                time.sleep(0.5)
            
            if not success:
                print(f"[CandleManager] {self.ticker} {tf} 캔들 데이터 로드에 최종 실패했습니다. 빈 데이터로 시작합니다.")

        # D/W/M: 15m 캐시+실시간에서 파생 (월봉·주봉 API 별도 호출 없음)
        if HAS_AGGREGATE and ("D" in self.timeframes or "W" in self.timeframes or "M" in self.timeframes):
            self._rebuild_derived_from_15m()
            rs = self._regime_series or {}
            print(
                f"[CandleManager] {self.ticker} 15m→일·주·월 파생: "
                f"일봉 {len(rs.get('daily_prices', []))} / "
                f"주봉 {len(rs.get('weekly_prices', []))} / "
                f"월봉 {len(rs.get('monthly_prices', []))}"
            )

    def update(self, tick: dict):
        """
        실시간 틱 데이터를 입력받아 모든 타임프레임의 캔들을 업데이트합니다.
        tick = {"price": float, "volume": float, "timestamp": int(ms)}
        """
        price = tick.get("price")
        volume = tick.get("volume", 0.0)
        timestamp_ms = tick.get("timestamp", int(time.time() * 1000))
        timestamp_sec = timestamp_ms // 1000
        new_15m_bar = False

        with self.lock:
            for tf in self.timeframes:
                if tf in DERIVED_FROM_INTRADAY or tf in DERIVED_FROM_DAILY:
                    continue
                cache = self.candles[tf]

                interval = self.TIMEFRAME_INTERVALS.get(tf, 60)
                kst_offset_sec = 9 * 3600
                candle_time = ((timestamp_sec + kst_offset_sec) // interval) * interval - kst_offset_sec

                if not cache:
                    cache.append({
                        "time": candle_time,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": volume
                    })
                    if tf == "15m":
                        new_15m_bar = True
                    continue

                last_candle = cache[-1]
                if last_candle["time"] == candle_time:
                    last_candle["high"] = max(last_candle["high"], price)
                    last_candle["low"] = min(last_candle["low"], price)
                    last_candle["close"] = price
                    last_candle["volume"] += volume
                else:
                    new_candle = {
                        "time": candle_time,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": volume
                    }
                    cache.append(new_candle)
                    if len(cache) > self.max_candles:
                        cache.pop(0)
                    if tf == "15m":
                        new_15m_bar = True

        if new_15m_bar and HAS_AGGREGATE and {"D", "W", "M"} & set(self.timeframes):
            self._maybe_rebuild_derived()

    def get_prices(self, timeframe: str) -> list:
        """
        특정 타임프레임의 종가(close) 리스트를 반환합니다.
        """
        with self.lock:
            cache = self.candles.get(timeframe, [])
            return [c["close"] for c in cache]

    def get_candles(self, timeframe: str) -> list:
        """
        특정 타임프레임의 전체 캔들 복사본을 반환합니다.
        """
        with self.lock:
            return list(self.candles.get(timeframe, []))


if __name__ == "__main__":
    # 간단한 단독 검증 테스트
    manager = CandleManager("BTC", timeframes=["1m", "D"])
    manager.warmup()
    print("로드된 1분봉 개수:", len(manager.get_candles("1m")))
    print("로드된 일봉 개수:", len(manager.get_candles("D")))
    if manager.get_prices("D"):
        print("최신 일봉 종가:", manager.get_prices("D")[-1])

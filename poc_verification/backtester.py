"""
backtester.py — 9년 실 데이터 기반 자동 파라미터 최적화 모듈
=====================================================================
■ 데이터:    빗썸 30분봉 캔들, 하루 12개 포인트 (00:00, 02:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00 KST)
■ 최초 수집: 9년치 전체 (약 39,420개 × 3종목), 이후 매일 12개만 추가(증분)
■ 최적화:   BULL/BEAR/RANGE 장세별 독립 그리드 서치 (지표 7조합 × OR/AND/VOTE/WEIGHTED_VOTE)
■ 기준:     Sharpe Ratio 최대 + MDD ≤ 50% 필터
■ 수수료:   빗썸 실측 편도 0.25% (왕복 0.50%)
"""

import os
import json
import time
import math
import logging
import requests
import sys
import io
from datetime import datetime, timedelta, timezone
from typing import Optional

# Windows 터미널 인코딩 오류 방지 (UTF-8 강제 지정)
if sys.platform.startswith('win'):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    except Exception:
        pass

try:
    import numpy as np
    import pandas as pd
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

logger = logging.getLogger("Backtester")

# ─────────────────────────────────────────────────────────────────────────────
# 상수
# ─────────────────────────────────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))
BITHUMB_BASE = "https://api.bithumb.com"

# 하루 중 수집할 KST 시각 (시, 분) - 2시간 간격 (하루 12개)
TARGET_TIMES_KST = [(0, 0), (2, 0), (4, 0), (6, 0), (8, 0), (10, 0), (12, 0), (14, 0), (16, 0), (18, 0), (20, 0), (22, 0)]

# 수수료: 빗썸 실거래 체결 기준 편도 약 0.25% (왕복 0.50%)
FEE_RATE = 0.0025          # 편도 1회 수수료
ROUND_TRIP_FEE = FEE_RATE * 2  # 왕복 0.005

# 캐시 파일 경로 (backtester.py 와 같은 폴더)
_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE    = os.path.join(_DIR, "historical_data_cache.json")
RESULT_FILE   = os.path.join(_DIR, "optimized_params.json")
PROGRESS_FILE = os.path.join(_DIR, "backtest_progress.json")

MARKETS = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]

# 그리드 서치 파라미터 공간
GRID = {
    "rsi_period":     [5, 7, 10, 14],
    "rsi_oversold":   [20, 25, 30, 35],
    "rsi_overbought": [60, 65, 70, 75],
    "bb_period":      [5, 10, 15, 20],
    "bb_std":         [1.5, 2.0, 2.5],
    "macd_fast":      [5, 8, 12],
    "macd_slow":      [10, 15, 26],
    "macd_signal":    [3, 5, 9],
}

# 허용할 전략 조합 (at least 1 active)
STRATEGY_COMBOS = [
    {"rsi": True,  "bb": False, "macd": False},
    {"rsi": False, "bb": True,  "macd": False},
    {"rsi": False, "bb": False, "macd": True},
    {"rsi": True,  "bb": True,  "macd": False},
    {"rsi": True,  "bb": False, "macd": True},
    {"rsi": False, "bb": True,  "macd": True},
    {"rsi": True,  "bb": True,  "macd": True},
]

# 지표 2개 이상일 때 검색할 합의 Logic (1개만 켠 조합은 OR과 동일하므로 OR만 사용)
LOGIC_TYPES = ["OR", "AND", "VOTE", "WEIGHTED_VOTE"]
DEFAULT_WEIGHTED_THRESHOLD = 0.5

MDD_LIMIT = 0.50  # MDD 허용 한계 50%


def _active_indicator_count(combo: dict) -> int:
    return sum(1 for k in ("rsi", "bb", "macd") if combo.get(k))


def _logics_for_combo(combo: dict) -> list:
    """활성 지표가 1개면 OR만, 2개 이상이면 OR/AND/VOTE/WEIGHTED_VOTE 전부 검색."""
    if _active_indicator_count(combo) <= 1:
        return ["OR"]
    return LOGIC_TYPES


def _resolve_composite_signals(
    buy_signals: list, sell_signals: list, logic: str, threshold: float = DEFAULT_WEIGHTED_THRESHOLD
) -> tuple:
    """실거래 VerboseCompositeStrategy와 동일한 AND/OR/VOTE/WEIGHTED_VOTE 규칙."""
    n = len(buy_signals)
    if n == 0:
        return False, False

    if logic == "AND":
        return all(buy_signals), all(sell_signals)
    if logic == "OR":
        return any(buy_signals), any(sell_signals)
    if logic == "VOTE":
        majority = n // 2 + 1
        return sum(buy_signals) >= majority, sum(sell_signals) >= majority
    if logic == "WEIGHTED_VOTE":
        buy_ratio = sum(1 for x in buy_signals if x) / n
        sell_ratio = sum(1 for x in sell_signals if x) / n
        return buy_ratio >= threshold, sell_ratio >= threshold
    return any(buy_signals), any(sell_signals)


# ─────────────────────────────────────────────────────────────────────────────
# 진행 상태 업데이트 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def _save_progress(stage: str, pct: float, msg: str = ""):
    for attempt in range(10):
        try:
            with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "stage": stage,
                    "percent": round(pct, 1),
                    "message": msg,
                    "updated_at": datetime.now(KST).isoformat()
                }, f, ensure_ascii=False)
            break
        except Exception:
            time.sleep(0.05)


# ─────────────────────────────────────────────────────────────────────────────
# 1. 데이터 수집
# ─────────────────────────────────────────────────────────────────────────────
class HistoricalDataFetcher:
    """
    빗썸 30분봉 캔들을 페이지네이션하여 수집, JSON 캐시에 저장.
    증분 업데이트: 이미 캐시된 날짜 이후 데이터만 추가.
    """

    CANDLE_URL = f"{BITHUMB_BASE}/v1/candles/minutes/30"
    MAX_PER_CALL = 200
    CALL_DELAY = 0.25  # Rate-limit 준수

    def _fetch_page(self, market: str, to_dt: datetime) -> list:
        """지정 시각(to_dt) 이전 30분봉 최대 200개 반환."""
        to_str = to_dt.strftime("%Y-%m-%dT%H:%M:%S")
        url = f"{self.CANDLE_URL}?market={market}&count={self.MAX_PER_CALL}&to={to_str}"
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    return r.json()
                logger.warning(f"[Fetcher] HTTP {r.status_code} — 재시도 {attempt+1}")
            except Exception as e:
                logger.warning(f"[Fetcher] 요청 오류: {e} — 재시도 {attempt+1}")
            time.sleep(1)
        return []

    def _is_target_time(self, dt_kst: datetime) -> bool:
        """KST 기준으로 TARGET_TIMES_KST에 해당하는 시각인지 확인."""
        return (dt_kst.hour, dt_kst.minute) in TARGET_TIMES_KST

    def fetch_full_history(self, market: str, years: int = 9, m_idx: int = 0, num_markets: int = 3) -> list:
        """
        최초 1회 전체 히스토리 수집.
        반환: [{"ts": "2017-09-01T01:00:00+09:00", "open": ..., "high": ..., "low": ..., "close": 3000000.0, "volume": 12.3}, ...]
        """
        cutoff = datetime.now(KST) - timedelta(days=years * 365)
        collected = []
        cursor = datetime.now(KST)
        call_count = 0

        logger.info(f"[Fetcher] {market} 전체 히스토리 수집 시작 ({years}년 / 목표 {years*365*8}개)")

        while cursor > cutoff:
            page = self._fetch_page(market, cursor)
            if not page:
                break

            added = 0
            for c in page:
                # KST 파싱
                dt_kst = datetime.fromisoformat(c["candle_date_time_kst"].replace("T", "T"))
                dt_kst = dt_kst.replace(tzinfo=KST)

                if dt_kst <= cutoff:
                    cursor = cutoff  # 종료 신호
                    break

                if self._is_target_time(dt_kst):
                    collected.append({
                        "ts":     dt_kst.isoformat(),
                        "open":   float(c["opening_price"]),
                        "high":   float(c["high_price"]),
                        "low":    float(c["low_price"]),
                        "close":  float(c["trade_price"]),
                        "volume": float(c.get("candle_acc_trade_volume", 0))
                    })
                    added += 1

            call_count += 1
            # 가장 오래된 시각으로 cursor 이동
            oldest_str = page[-1]["candle_date_time_kst"].replace("T", "T")
            oldest_kst = datetime.fromisoformat(oldest_str).replace(tzinfo=KST)
            cursor = oldest_kst

            if call_count % 20 == 0:
                market_weight = 100.0 / num_markets
                collect_weight = market_weight * 0.2
                base_pct = m_idx * market_weight
                pct = base_pct + (1 - (cursor - cutoff).days / (years * 365)) * collect_weight
                pct = min(base_pct + collect_weight - 0.1, pct)
                _save_progress("collecting", pct, f"{market}: {len(collected)}개 수집중")
                logger.info(f"[Fetcher] {market} {len(collected)}개 수집 (API 호출 {call_count}회, 현재: {cursor.date()})")

            time.sleep(self.CALL_DELAY)

        # 시간순 정렬 (오래된 것 먼저)
        collected.sort(key=lambda x: x["ts"])
        logger.info(f"[Fetcher] {market} 수집 완료: {len(collected)}개")
        return collected

    def fetch_incremental(self, market: str, last_ts: str) -> list:
        """
        last_ts 이후 누락된 데이터만 수집 (매일 8개 내외).
        """
        last_dt = datetime.fromisoformat(last_ts)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=KST)

        new_data = []
        cursor = datetime.now(KST) + timedelta(hours=1)
        stop = False

        while not stop:
            page = self._fetch_page(market, cursor)
            if not page:
                break

            for c in page:
                dt_kst = datetime.fromisoformat(
                    c["candle_date_time_kst"].replace("T", "T")
                ).replace(tzinfo=KST)

                if dt_kst <= last_dt:
                    stop = True
                    break

                if self._is_target_time(dt_kst):
                    new_data.append({
                        "ts":     dt_kst.isoformat(),
                        "open":   float(c["opening_price"]),
                        "high":   float(c["high_price"]),
                        "low":    float(c["low_price"]),
                        "close":  float(c["trade_price"]),
                        "volume": float(c.get("candle_acc_trade_volume", 0))
                    })

            if not stop:
                oldest_kst = datetime.fromisoformat(
                    page[-1]["candle_date_time_kst"].replace("T", "T")
                ).replace(tzinfo=KST)
                if oldest_kst <= last_dt:
                    break
                cursor = oldest_kst
                time.sleep(self.CALL_DELAY)

        new_data.sort(key=lambda x: x["ts"])
        logger.info(f"[Fetcher] {market} 증분 수집: {len(new_data)}개 추가")
        return new_data


# ─────────────────────────────────────────────────────────────────────────────
# 2. 캐시 관리
# ─────────────────────────────────────────────────────────────────────────────
class DataCache:
    def load(self) -> dict:
        if os.path.exists(CACHE_FILE):
            try:
                is_invalid = False
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                    if self._check_cache_invalid(cache):
                        is_invalid = True
                
                if is_invalid:
                    logger.warning("[Cache] 수집 시각 설정(TARGET_TIMES_KST) 또는 데이터 스키마가 변경된 것으로 감지되었습니다. 기존 캐시를 백업하고 새로 수집합니다.")
                    bak_file = CACHE_FILE + ".bak"
                    try:
                        if os.path.exists(bak_file):
                            os.remove(bak_file)
                        os.rename(CACHE_FILE, bak_file)
                    except Exception as e:
                        logger.error(f"[Cache] 백업 중 오류: {e}")
                    return {}
                return cache
            except Exception:
                pass
        return {}

    def _check_cache_invalid(self, cache: dict) -> bool:
        """기존 캐시가 현재 TARGET_TIMES_KST 설정과 호환되지 않거나 open/high/low 데이터가 누락되었는지 검사."""
        for market, m_data in cache.items():
            rows = m_data.get("data", [])
            if not rows:
                continue
            for r in rows[:10]:
                try:
                    if not all(k in r for k in ["open", "high", "low", "close", "volume"]):
                        return True
                    dt = datetime.fromisoformat(r["ts"])
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=KST)
                    else:
                        dt = dt.astimezone(KST)
                    if (dt.hour, dt.minute) not in TARGET_TIMES_KST:
                        return True
                except Exception:
                    return True
        return False

    def save(self, cache: dict):
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)

    def get_last_ts(self, cache: dict, market: str) -> Optional[str]:
        data = cache.get(market, {}).get("data", [])
        return data[-1]["ts"] if data else None

    def append(self, cache: dict, market: str, new_rows: list) -> dict:
        if market not in cache:
            cache[market] = {"data": []}
        existing_ts = {r["ts"] for r in cache[market]["data"]}
        for r in new_rows:
            if r["ts"] not in existing_ts:
                cache[market]["data"].append(r)
                existing_ts.add(r["ts"])
        cache[market]["data"].sort(key=lambda x: x["ts"])
        cache[market]["last_updated"] = datetime.now(KST).isoformat()
        return cache


# ─────────────────────────────────────────────────────────────────────────────
# 3. 기술 지표 계산 (numpy 기반)
# ─────────────────────────────────────────────────────────────────────────────
class Indicators:
    _rsi_cache = {}
    _bb_cache = {}
    _macd_cache = {}

    @staticmethod
    def clear_cache():
        Indicators._rsi_cache.clear()
        Indicators._bb_cache.clear()
        Indicators._macd_cache.clear()

    @staticmethod
    def rsi(closes: list, period: int) -> list:
        if not closes:
            return []
        key = (len(closes), closes[0], closes[-1], period)
        if key in Indicators._rsi_cache:
            return Indicators._rsi_cache[key]

        if not HAS_NUMPY or len(closes) < period + 1:
            return [50.0] * len(closes)
        arr = np.array(closes, dtype=float)
        delta = np.diff(arr)
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)
        result = [float('nan')] * len(closes)
        avg_gain = np.mean(gain[:period])
        avg_loss = np.mean(loss[:period])
        for i in range(period, len(closes) - 1):
            if avg_loss == 0:
                result[i + 1] = 100.0
            else:
                rs = avg_gain / avg_loss
                result[i + 1] = 100 - (100 / (1 + rs))
            avg_gain = (avg_gain * (period - 1) + gain[i]) / period
            avg_loss = (avg_loss * (period - 1) + loss[i]) / period
        # nan 채우기
        filled = [50.0] * len(result)
        for i, v in enumerate(result):
            if not math.isnan(v):
                filled[i] = v
        
        Indicators._rsi_cache[key] = filled
        return filled

    @staticmethod
    def bollinger(closes: list, period: int, std_dev: float):
        if not closes:
            return [], [], []
        key = (len(closes), closes[0], closes[-1], period, std_dev)
        if key in Indicators._bb_cache:
            return Indicators._bb_cache[key]

        if not HAS_NUMPY or len(closes) < period:
            return [0.0]*len(closes), [c for c in closes], [c*2 for c in closes]
        arr = np.array(closes, dtype=float)
        mid, upper, lower = [], [], []
        for i in range(len(arr)):
            if i < period - 1:
                mid.append(arr[i]); upper.append(arr[i]); lower.append(arr[i])
            else:
                window = arr[i - period + 1: i + 1]
                m = float(np.mean(window))
                s = float(np.std(window, ddof=1))
                mid.append(m)
                upper.append(m + std_dev * s)
                lower.append(m - std_dev * s)
        
        res = (mid, upper, lower)
        Indicators._bb_cache[key] = res
        return res

    @staticmethod
    def macd(closes: list, fast: int, slow: int, signal_period: int):
        if not closes:
            return [], [], []
        key = (len(closes), closes[0], closes[-1], fast, slow, signal_period)
        if key in Indicators._macd_cache:
            return Indicators._macd_cache[key]

        if not HAS_NUMPY or len(closes) < slow:
            n = len(closes)
            return [0.0]*n, [0.0]*n, [0.0]*n
        arr = np.array(closes, dtype=float)

        def ema(data, span):
            k = 2 / (span + 1)
            e = [data[0]]
            for v in data[1:]:
                e.append(v * k + e[-1] * (1 - k))
            return np.array(e)

        ema_fast = ema(arr, fast)
        ema_slow = ema(arr, slow)
        macd_line = ema_fast - ema_slow
        signal_line = ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        
        res = (macd_line.tolist(), signal_line.tolist(), histogram.tolist())
        Indicators._macd_cache[key] = res
        return res


# ─────────────────────────────────────────────────────────────────────────────
# 4. 장세 라벨링 (RegimeDetector 동일 알고리즘)
# ─────────────────────────────────────────────────────────────────────────────
class RegimeLabeler:
    def label(self, data: list) -> list:
        """
        각 데이터 포인트에 regime (BULL/BEAR/RANGE) 태그 추가.
        실시간 엔진의 MarketRegimeDetector와 100% 동일하게 일봉 기준 다중 선형 회귀 및 기울기 분석 동기화.
        """
        if not data:
            return data

        # 1. 일별 종가 추출 (KST 일자 기준 마지막 캔들)
        daily_map = {}
        for row in data:
            ts_str = row["ts"]
            # ts_str 예시: "2017-09-01T01:00:00+09:00" -> "2017-09-01"
            date_str = ts_str.split("T")[0]
            daily_map[date_str] = row["close"]

        sorted_dates = sorted(daily_map.keys())
        daily_prices = [daily_map[d] for d in sorted_dates]
        
        # 2. 각 일자별로 MarketRegimeDetector와 동일한 로직 적용
        daily_regimes = {}
        n_daily = len(daily_prices)
        long_term_period = 210
        
        for idx, date_str in enumerate(sorted_dates):
            avail = idx + 1
            actual_period = min(long_term_period, avail)
            
            min_required = min(300, long_term_period)
            if actual_period < min_required:
                daily_regimes[date_str] = "RANGE"
                continue
                
            p_long = daily_prices[idx - actual_period + 1 : idx + 1]
            x_long = np.arange(actual_period)
            slope_long, intercept_long = np.polyfit(x_long, p_long, 1)
            
            fair_value = slope_long * (actual_period - 1) + intercept_long
            price_now = float(daily_prices[idx])
            dev_pct = (price_now - fair_value) / fair_value * 100
            
            # 최근 90일, 30일 선형 회귀 기울기 계산 (% / 일)
            p_90 = daily_prices[max(0, idx - 90 + 1) : idx + 1]
            slope_90, _ = np.polyfit(np.arange(len(p_90)), p_90, 1) if len(p_90) >= 2 else (0.0, 0.0)
            slope_rate_90 = (slope_90 / price_now) * 100
            
            p_30 = daily_prices[max(0, idx - 30 + 1) : idx + 1]
            slope_30, _ = np.polyfit(np.arange(len(p_30)), p_30, 1) if len(p_30) >= 2 else (0.0, 0.0)
            slope_rate_30 = (slope_30 / price_now) * 100
            
            is_bottoming = (slope_rate_90 >= -0.05) or (slope_rate_30 >= 0.0)
            
            # 종합 조건 판정 (MarketRegimeDetector와 100% 동일)
            if dev_pct <= -25.0:
                if slope_rate_30 >= 0.1:
                    reg = "BULL"
                elif is_bottoming:
                    reg = "RANGE"
                else:
                    reg = "BEAR"
            elif dev_pct >= -10.0 and slope_rate_90 > 0 and slope_rate_30 > 0:
                reg = "BULL"
            elif dev_pct < -10.0 and (slope_rate_90 < -0.05 or slope_rate_30 < -0.1):
                reg = "BEAR"
            else:
                reg = "RANGE"
                
            daily_regimes[date_str] = reg

        # 3. 30분봉 데이터에 일자별 장세 역매핑
        labeled = []
        for row in data:
            date_str = row["ts"].split("T")[0]
            reg = daily_regimes.get(date_str, "RANGE")
            labeled.append({**row, "regime": reg})
            
        return labeled


# ─────────────────────────────────────────────────────────────────────────────
# 5. 전략 시뮬레이터
# ─────────────────────────────────────────────────────────────────────────────
class StrategySimulator:
    """
    주어진 파라미터 조합으로 장세별 수익률 시뮬레이션.
    수수료/슬리피지 반영, Sharpe Ratio / MDD 반환.
    """

    def simulate(self, data: list, params: dict) -> dict:
        """
        data: RegimeLabeler.label() 결과 (regime 포함)
        params: {
          "rsi": bool, "rsi_period": int, "rsi_oversold": int, "rsi_overbought": int,
          "bb": bool, "bb_period": int, "bb_std": float,
          "macd": bool, "macd_fast": int, "macd_slow": int, "macd_signal": int,
          "custom_bear": bool, "lookback": int, "drop_pct": float, "volume_ratio": float,
          "trail_pct": float, "stop_loss": float, "time_cut": int,
          "target_regime": "BULL"|"BEAR"|"RANGE"
        }
        """
        target = params["target_regime"]
        regime_data = [d for d in data if d["regime"] == target]

        if len(regime_data) < 30:
            return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0}

        # 1. CustomBear 전용 분기 (하락장 롱테일 극복 + 트레일링 스톱 결합)
        if params.get("custom_bear", False):
            opens = [d["open"] for d in regime_data]
            closes = [d["close"] for d in regime_data]
            highs = [d.get("high", d["close"]) for d in regime_data]
            volumes = [d["volume"] for d in regime_data]

            capital = 1_000_000.0
            position = 0.0
            entry_price = 0.0
            peak_price = 0.0
            entry_idx = 0
            equity_curve = [capital]
            wins = 0
            total_trades = 0

            lookback = params["lookback"]
            drop_pct = params["drop_pct"]
            volume_ratio = params["volume_ratio"]
            trail_pct = params["trail_pct"]
            stop_loss = params["stop_loss"]
            time_cut = params["time_cut"]

            warmup = max(lookback, 20)

            for i in range(warmup, len(closes)):
                price = closes[i]

                if position == 0:
                    # lookback 기간의 최고가 대비 하락폭 감시
                    window_closes = closes[i - lookback: i]
                    highest = max(window_closes) if window_closes else price
                    drop = (highest - price) / highest if highest > 0 else 0

                    # 거래량 10평균 대비 급증 감시
                    vol_window = volumes[i - 10: i]
                    avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1.0
                    curr_vol = volumes[i]

                    # 반등 양봉 조건 (시가 대비 종가 상승)
                    is_bullish = closes[i] > opens[i]

                    buy = (drop >= drop_pct) and (curr_vol >= avg_vol * volume_ratio) and is_bullish

                    if buy:
                        cost = capital * FEE_RATE
                        position = (capital - cost) / price
                        entry_price = price
                        peak_price = price
                        entry_idx = i
                        capital = 0.0
                        total_trades += 1
                else:
                    # peak_price 실시간 최고가 업데이트
                    peak_price = max(peak_price, highs[i])
                    
                    # 청산 조건 (트레일링 스톱, 손절, 시간청산)
                    pnl = (price - entry_price) / entry_price
                    drawdown = (peak_price - price) / peak_price
                    
                    exit_ts = drawdown >= trail_pct
                    exit_sl = pnl <= -stop_loss
                    exit_tc = (i - entry_idx) >= time_cut

                    if exit_ts or exit_sl or exit_tc:
                        gross = position * price
                        fee = gross * FEE_RATE
                        capital = gross - fee
                        if price > entry_price:
                            wins += 1
                        position = 0.0
                        peak_price = 0.0
                        equity_curve.append(capital)

            if position > 0:
                capital = position * closes[-1] * (1 - FEE_RATE)
                equity_curve.append(capital)

            if len(equity_curve) < 2 or total_trades == 0:
                return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0}

            total_return = (equity_curve[-1] / equity_curve[0]) - 1.0

            peak = equity_curve[0]
            mdd = 0.0
            for v in equity_curve:
                if v > peak:
                    peak = v
                dd = (peak - v) / peak
                if dd > mdd:
                    mdd = dd

            returns = [(equity_curve[j] / equity_curve[j-1]) - 1 for j in range(1, len(equity_curve))]
            if len(returns) > 1 and HAS_NUMPY:
                arr = np.array(returns)
                std = float(np.std(arr, ddof=1))
                sharpe = float(np.mean(arr)) / std if std > 0 else 0.0
            else:
                sharpe = 0.0

            return {
                "sharpe":       round(sharpe, 4),
                "mdd":          round(mdd, 4),
                "total_return": round(total_return * 100, 2),
                "win_rate":     round(wins / total_trades * 100, 2) if total_trades > 0 else 0.0,
                "trades":       total_trades
            }

        # 2. 기존 보조지표 믹스 분기
        closes = [d["close"] for d in regime_data]
        ind = Indicators()

        # 지표 계산
        rsi_vals = ind.rsi(closes, params["rsi_period"]) if params["rsi"] else None
        bb_mid, bb_upper, bb_lower = ind.bollinger(closes, params["bb_period"], params["bb_std"]) if params["bb"] else (None, None, None)
        macd_line, signal_line, _ = ind.macd(closes, params["macd_fast"], params["macd_slow"], params["macd_signal"]) if params["macd"] else (None, None, None)

        # 매수/매도 시뮬레이션
        capital = 1_000_000.0
        position = 0.0
        entry_price = 0.0
        equity_curve = [capital]
        wins = 0
        total_trades = 0
        warmup = max(params.get("rsi_period", 14),
                     params.get("bb_period", 20),
                     params.get("macd_slow", 26)) + 5

        for i in range(warmup, len(closes)):
            price = closes[i]
            buy_signals = []
            sell_signals = []

            # RSI 신호
            if params["rsi"] and rsi_vals:
                rv = rsi_vals[i]
                buy_signals.append(rv < params["rsi_oversold"])
                sell_signals.append(rv > params["rsi_overbought"])

            # 볼린저 신호
            if params["bb"] and bb_lower:
                buy_signals.append(price < bb_lower[i])
                sell_signals.append(price > bb_upper[i])

            # MACD 신호
            if params["macd"] and macd_line:
                prev_diff = macd_line[i-1] - signal_line[i-1]
                curr_diff = macd_line[i] - signal_line[i]
                buy_signals.append(prev_diff < 0 and curr_diff >= 0)   # 골든크로스
                sell_signals.append(prev_diff > 0 and curr_diff <= 0)  # 데드크로스

            buy, sell = _resolve_composite_signals(
                buy_signals, sell_signals,
                params.get("logic", "OR"),
                params.get("threshold", DEFAULT_WEIGHTED_THRESHOLD),
            )

            if position == 0 and buy:
                # 매수
                cost = capital * FEE_RATE
                position = (capital - cost) / price
                entry_price = price
                capital = 0.0
                total_trades += 1

            elif position > 0 and sell:
                # 매도
                gross = position * price
                fee = gross * FEE_RATE
                capital = gross - fee
                if price > entry_price:
                    wins += 1
                position = 0.0
                equity_curve.append(capital)

        # 포지션 강제 청산
        if position > 0:
            capital = position * closes[-1] * (1 - FEE_RATE)
            equity_curve.append(capital)

        if len(equity_curve) < 2 or total_trades == 0:
            return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0}

        total_return = (equity_curve[-1] / equity_curve[0]) - 1.0

        # MDD
        peak = equity_curve[0]
        mdd = 0.0
        for v in equity_curve:
            if v > peak:
                peak = v
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd

        # Sharpe Ratio (거래 단위 수익률 기반)
        returns = [(equity_curve[idx] / equity_curve[idx-1]) - 1
                   for idx in range(1, len(equity_curve))]
        if len(returns) > 1 and HAS_NUMPY:
            arr = np.array(returns)
            std = float(np.std(arr, ddof=1))
            sharpe = float(np.mean(arr)) / std if std > 0 else 0.0
        else:
            sharpe = 0.0

        win_rate = wins / total_trades if total_trades > 0 else 0.0

        return {
            "sharpe":       round(sharpe, 4),
            "mdd":          round(mdd, 4),
            "total_return": round(total_return * 100, 2),  # %
            "win_rate":     round(win_rate * 100, 2),       # %
            "trades":       total_trades
        }


# ─────────────────────────────────────────────────────────────────────────────
# 6. 그리드 서치 최적화
# ─────────────────────────────────────────────────────────────────────────────
def _calculate_expected_profits(total_return_pct: float, regime_data: list) -> dict:
    """전체 백테스트 기간 기반 1주/1달/3개월/6개월 기대 수익률(복리) 계산."""
    if len(regime_data) >= 2:
        try:
            t1 = datetime.fromisoformat(regime_data[0]["ts"])
            t2 = datetime.fromisoformat(regime_data[-1]["ts"])
            days = max((t2 - t1).days, 1)
        except Exception:
            days = 365
    else:
        days = 365
        
    daily_rate = (1 + total_return_pct / 100.0) ** (1.0 / days) - 1.0
    
    w1 = ((1 + daily_rate) ** 7 - 1.0) * 100.0
    m1 = ((1 + daily_rate) ** 30 - 1.0) * 100.0
    m3 = ((1 + daily_rate) ** 90 - 1.0) * 100.0
    m6 = ((1 + daily_rate) ** 180 - 1.0) * 100.0
    
    return {
        "1w": round(w1, 2),
        "1m": round(m1, 2),
        "3m": round(m3, 2),
        "6m": round(m6, 2)
    }


# ─────────────────────────────────────────────────────────────────────────────
# 6. 그리드 서치 최적화
# ─────────────────────────────────────────────────────────────────────────────
class GridSearchOptimizer:
    def __init__(self):
        self.sim = StrategySimulator()

    def _iter_params(self):
        """모든 파라미터 조합 생성기 (지표 7조합 × Logic 최대 4종 × 파라미터 그리드)."""
        for combo in STRATEGY_COMBOS:
            for logic in _logics_for_combo(combo):
                rsi_periods   = GRID["rsi_period"]   if combo["rsi"]  else [14]
                rsi_oversolds = GRID["rsi_oversold"]  if combo["rsi"]  else [30]
                rsi_overs     = GRID["rsi_overbought"] if combo["rsi"] else [70]
                bb_periods    = GRID["bb_period"]     if combo["bb"]   else [20]
                bb_stds       = GRID["bb_std"]        if combo["bb"]   else [2.0]
                macd_fasts    = GRID["macd_fast"]     if combo["macd"] else [12]
                macd_slows    = GRID["macd_slow"]     if combo["macd"] else [26]
                macd_sigs     = GRID["macd_signal"]   if combo["macd"] else [9]

                for rp in rsi_periods:
                  for ro in rsi_oversolds:
                    for rob in rsi_overs:
                      for bp in bb_periods:
                        for bs in bb_stds:
                          for mf in macd_fasts:
                            for ms in macd_slows:
                              if mf >= ms:
                                  continue  # fast < slow 강제
                              for msig in macd_sigs:
                                yield {
                                    "custom_bear": False,
                                    **combo,
                                    "logic": logic,
                                    "threshold": DEFAULT_WEIGHTED_THRESHOLD,
                                    "rsi_period": rp, "rsi_oversold": ro, "rsi_overbought": rob,
                                    "bb_period": bp, "bb_std": bs,
                                    "macd_fast": mf, "macd_slow": ms, "macd_signal": msig,
                                }

    def _iter_custom_bear_params(self):
        """CustomBear 전략용 파라미터 조합 생성기."""
        lookbacks     = [4, 6, 8, 12, 16, 24]
        drop_pcts     = [0.01, 0.02, 0.03, 0.05, 0.08]
        volume_ratios = [1.2, 1.5, 2.0, 2.5, 3.0]
        trail_pcts    = [0.01, 0.02, 0.03, 0.04, 0.05]
        stop_losses   = [0.01, 0.02, 0.03, 0.04, 0.05]
        time_cuts     = [12, 24, 36, 48]
        
        for lb in lookbacks:
            for dp in drop_pcts:
                for vr in volume_ratios:
                    for trail in trail_pcts:
                        for sl in stop_losses:
                            for tc in time_cuts:
                                yield {
                                    "custom_bear": True,
                                    "rsi": False, "bb": False, "macd": False,
                                    "lookback": lb,
                                    "drop_pct": dp,
                                    "volume_ratio": vr,
                                    "trail_pct": trail,
                                    "stop_loss": sl,
                                    "time_cut": tc
                                }

    def optimize_regime(self, data: list, regime: str, market: str, base_pct: float = 20.0, span_pct: float = 60.0) -> dict:
        """
        지정 장세에 대해 그리드 서치 실행.
        BEAR 장세는 믹스 전략과 CustomBear 전략을 각각 돌려 비교 결과 출력.
        """
        regime_data = [d for d in data if d["regime"] == regime]
        
        def get_profits(ret_pct):
            return _calculate_expected_profits(ret_pct, regime_data)

        if regime != "BEAR":
            # BULL, RANGE는 기존 믹스 전략만 수행
            best_params, best_result = self._optimize_mixed(data, regime, market, base_pct, span_pct)
            if best_params is None:
                return _default_regime_params(regime)
            return _build_output(best_params, best_result, get_profits(best_result["total_return"]))
        else:
            # BEAR 장세: 믹스 전략(50%) + CustomBear 전략(50%) 수행
            mixed_span = span_pct * 0.5
            custom_span = span_pct * 0.5
            
            mixed_params, mixed_result = self._optimize_mixed(data, regime, market, base_pct, mixed_span)
            custom_params, custom_result = self._optimize_custom_bear(data, regime, market, base_pct + mixed_span, custom_span)
            
            if mixed_params is None:
                mixed_out = _default_regime_params(regime)
                mixed_out["period_expected_profits"] = {"1w": 0, "1m": 0, "3m": 0, "6m": 0}
            else:
                mixed_out = _build_output(mixed_params, mixed_result, get_profits(mixed_result["total_return"]))
                
            if custom_params is None:
                custom_out = _default_custom_bear_params(regime)
            else:
                custom_out = _build_custom_bear_output(custom_params, custom_result, get_profits(custom_result["total_return"]))
                
            return {
                "mixed_strategy": mixed_out,
                "custom_bear_strategy": custom_out,
                **mixed_out  # 하위 호환 병합
            }

    def _optimize_mixed(self, data: list, regime: str, market: str, base_pct: float, span_pct: float):
        best_sharpe = -999
        best_result = None
        best_params = None
        total = 0
        valid = 0

        total_combos = sum(1 for _ in self._iter_params())
        last_update_time = time.time()

        first_span = span_pct * 0.8
        fallback_span = span_pct * 0.2

        for params in self._iter_params():
            params["target_regime"] = regime
            result = self.sim.simulate(data, params)
            total += 1

            if result["mdd"] > MDD_LIMIT:
                continue
            if result["trades"] < 5:
                continue

            valid += 1
            if result["sharpe"] > best_sharpe:
                best_sharpe = result["sharpe"]
                best_result = result
                best_params = dict(params)

            current_time = time.time()
            if total % 200 == 0 or (current_time - last_update_time) > 4.0:
                combo_pct = (total / total_combos) * first_span
                pct = base_pct + combo_pct
                _save_progress("optimizing", pct, f"{market} {regime} 믹스전략 최적화 중 ({total}/{total_combos})")
                last_update_time = current_time

        logger.info(f"[Optimizer] {market}/{regime} Mixed: 총 {total}조합 검색, {valid}개 통과, 최적 Sharpe={best_sharpe:.4f}")

        if best_params is None:
            logger.warning(f"[Optimizer] {market}/{regime} Mixed: MDD 필터 통과 없음 → 차선책 선택")
            fb_total = 0
            last_update_time = time.time()
            for params in self._iter_params():
                params["target_regime"] = regime
                result = self.sim.simulate(data, params)
                fb_total += 1
                
                if result["sharpe"] > best_sharpe and result["trades"] >= 3:
                    best_sharpe = result["sharpe"]
                    best_result = result
                    best_params = dict(params)
                    
                current_time = time.time()
                if fb_total % 200 == 0 or (current_time - last_update_time) > 4.0:
                    fb_pct = (fb_total / total_combos) * fallback_span
                    pct = base_pct + first_span + fb_pct
                    _save_progress("optimizing", pct, f"{market} {regime} 믹스 차선책 최적화 중 ({fb_total}/{total_combos})")
                    last_update_time = current_time

        _save_progress("optimizing", base_pct + span_pct, f"{market} {regime} 믹스전략 최적화 완료")
        return best_params, best_result

    def _optimize_custom_bear(self, data: list, regime: str, market: str, base_pct: float, span_pct: float):
        best_sharpe = -999
        best_result = None
        best_params = None
        total = 0
        valid = 0

        total_combos = sum(1 for _ in self._iter_custom_bear_params())
        last_update_time = time.time()

        first_span = span_pct * 0.8
        fallback_span = span_pct * 0.2

        for params in self._iter_custom_bear_params():
            params["target_regime"] = regime
            result = self.sim.simulate(data, params)
            total += 1

            if result["mdd"] > MDD_LIMIT:
                continue
            if result["trades"] < 3:
                continue

            valid += 1
            if result["sharpe"] > best_sharpe:
                best_sharpe = result["sharpe"]
                best_result = result
                best_params = dict(params)

            current_time = time.time()
            if total % 50 == 0 or (current_time - last_update_time) > 4.0:
                combo_pct = (total / total_combos) * first_span
                pct = base_pct + combo_pct
                _save_progress("optimizing", pct, f"{market} {regime} CustomBear 최적화 중 ({total}/{total_combos})")
                last_update_time = current_time

        logger.info(f"[Optimizer] {market}/{regime} CustomBear: 총 {total}조합 검색, {valid}개 통과, 최적 Sharpe={best_sharpe:.4f}")

        if best_params is None:
            logger.warning(f"[Optimizer] {market}/{regime} CustomBear: 필터 통과 없음 → 차선책 선택")
            fb_total = 0
            last_update_time = time.time()
            for params in self._iter_custom_bear_params():
                params["target_regime"] = regime
                result = self.sim.simulate(data, params)
                fb_total += 1
                
                if result["sharpe"] > best_sharpe and result["trades"] >= 1:
                    best_sharpe = result["sharpe"]
                    best_result = result
                    best_params = dict(params)
                    
                current_time = time.time()
                if fb_total % 50 == 0 or (current_time - last_update_time) > 4.0:
                    fb_pct = (fb_total / total_combos) * fallback_span
                    pct = base_pct + first_span + fb_pct
                    _save_progress("optimizing", pct, f"{market} {regime} CustomBear 차선책 최적화 중 ({fb_total}/{total_combos})")
                    last_update_time = current_time

        _save_progress("optimizing", base_pct + span_pct, f"{market} {regime} CustomBear 최적화 완료")
        return best_params, best_result

    def run_full_optimization(self, market: str, data: list, m_idx: int = 0, num_markets: int = 3) -> dict:
        """종목 전체 장세 최적화."""
        labeler = RegimeLabeler()
        labeled = labeler.label(data)
        output = {}
        regimes = ["BULL", "BEAR", "RANGE"]
        
        market_weight = 100.0 / num_markets
        optimize_weight = market_weight * 0.8
        regime_weight = optimize_weight / len(regimes)
        collect_weight = market_weight * 0.2
        
        for r_idx, regime in enumerate(regimes):
            base_pct = (m_idx * market_weight) + collect_weight + (r_idx * regime_weight)
            output[regime] = self.optimize_regime(
                labeled, regime, market, 
                base_pct=base_pct, 
                span_pct=regime_weight
            )
        return output


# ─────────────────────────────────────────────────────────────────────────────
# 7. 출력 포맷 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def _build_output(params: dict, result: dict, expected_profits: dict = None) -> dict:
    out = {
        "strategies": {
            "RSI": {
                "enabled":    params["rsi"],
                "weight":     1.0,
                "period":     params["rsi_period"],
                "oversold":   params["rsi_oversold"],
                "overbought": params["rsi_overbought"]
            },
            "Bollinger": {
                "enabled": params["bb"],
                "weight":  1.0,
                "period":  params["bb_period"],
                "std_dev": params["bb_std"]
            },
            "MACD": {
                "enabled":       params["macd"],
                "weight":        1.0,
                "fast":          params["macd_fast"],
                "slow":          params["macd_slow"],
                "signal_period": params["macd_signal"]
            }
        },
        "logic":       params.get("logic", "OR"),
        "threshold":   params.get("threshold", DEFAULT_WEIGHTED_THRESHOLD),
        "risk": {
            "type":           "StopLoss" if params["target_regime"] != "BULL" else "None",
            "stop_loss_pct":  0.02,
            "take_profit_pct": 0.03
        },
        "backtest": {
            "total_return_pct": result["total_return"],
            "sharpe_ratio":     result["sharpe"],
            "mdd_pct":          round(result["mdd"] * 100, 2),
            "win_rate_pct":     result["win_rate"],
            "trade_count":      result["trades"]
        }
    }
    if expected_profits:
        out["period_expected_profits"] = expected_profits
    return out


def _build_custom_bear_output(params: dict, result: dict, expected_profits: dict) -> dict:
    return {
        "strategies": {
            "CustomBear": {
                "enabled":      True,
                "lookback":     params["lookback"],
                "drop_pct":     params["drop_pct"],
                "volume_ratio": params["volume_ratio"],
                "trail_pct":    params["trail_pct"],
                "stop_loss":    params["stop_loss"],
                "time_cut":     params["time_cut"]
            }
        },
        "logic": "CUSTOM_BEAR",
        "threshold": 0.5,
        "risk": {
            "type": "StopLoss",
            "stop_loss_pct": params["stop_loss"],
            "take_profit_pct": 9.9
        },
        "backtest": {
            "total_return_pct": result["total_return"],
            "sharpe_ratio":     result["sharpe"],
            "mdd_pct":          round(result["mdd"] * 100, 2),
            "win_rate_pct":     result["win_rate"],
            "trade_count":      result["trades"]
        },
        "period_expected_profits": expected_profits
    }


def _default_regime_params(regime: str) -> dict:
    """데이터 부족 시 기본값 반환."""
    return {
        "strategies": {
            "RSI": {"enabled": True, "weight": 1.0, "period": 14, "oversold": 30, "overbought": 70},
            "Bollinger": {"enabled": regime == "RANGE", "weight": 1.0, "period": 20, "std_dev": 2.0},
            "MACD": {"enabled": regime == "BULL", "weight": 1.0, "fast": 12, "slow": 26, "signal_period": 9}
        },
        "logic": "OR", "threshold": 0.5,
        "risk": {"type": "StopLoss" if regime != "BULL" else "None",
                 "stop_loss_pct": 0.02, "take_profit_pct": 0.03},
        "backtest": {"total_return_pct": 0, "sharpe_ratio": 0, "mdd_pct": 0, "win_rate_pct": 0, "trade_count": 0}
    }


def _default_custom_bear_params(regime: str) -> dict:
    return {
        "strategies": {
            "CustomBear": {
                "enabled": True,
                "lookback": 8,
                "drop_pct": 0.05,
                "volume_ratio": 2.0,
                "trail_pct": 0.015,
                "stop_loss": 0.015,
                "time_cut": 24
            }
        },
        "logic": "CUSTOM_BEAR",
        "threshold": 0.5,
        "risk": {
            "type": "StopLoss",
            "stop_loss_pct": 0.015,
            "take_profit_pct": 9.9
        },
        "backtest": {
            "total_return_pct": 0,
            "sharpe_ratio": 0,
            "mdd_pct": 0,
            "win_rate_pct": 0,
            "trade_count": 0
        },
        "period_expected_profits": {"1w": 0, "1m": 0, "3m": 0, "6m": 0}
    }


# ─────────────────────────────────────────────────────────────────────────────
# 8. 메인 진입점
# ─────────────────────────────────────────────────────────────────────────────
def run_optimization(markets: list = None) -> dict:
    """
    전체 파이프라인 실행:
    1. 캐시 로드 → 없으면 전체 수집, 있으면 증분 추가
    2. 종목별/장세별 그리드 서치
    3. optimized_params.json 저장
    반환: 최적화 결과 dict
    """
    if markets is None:
        markets = MARKETS

    _save_progress("start", 0, "백테스팅 최적화 시작...")
    fetcher = HistoricalDataFetcher()
    cache_mgr = DataCache()
    optimizer = GridSearchOptimizer()

    cache = cache_mgr.load()
    results = {
        "last_updated": datetime.now(KST).isoformat(),
        "source": f"빗썸 30분봉 ({len(TARGET_TIMES_KST)}포인트/일, "
                  f"KST {[f'{h:02d}:{m:02d}' for h,m in TARGET_TIMES_KST]})",
        "fee_assumption": "편도 0.25% (빗썸 실거래 체결 기준, 왕복 0.50%)",
        "optimization_criteria": (
            f"Sharpe Ratio 최대 + MDD <= {int(MDD_LIMIT * 100)}% "
            "(지표 7조합 × OR/AND/VOTE/WEIGHTED_VOTE)"
        ),
    }

    num_markets = len(markets)
    market_weight = 100.0 / num_markets
    collect_weight = market_weight * 0.2

    for m_idx, market in enumerate(markets):
        logger.info(f"\n{'='*60}")
        logger.info(f"[Pipeline] {market} 처리 시작")
        
        base_collect_pct = m_idx * market_weight
        _save_progress("collecting", base_collect_pct, f"{market} 데이터 준비중...")

        # 데이터 수집 (최초 or 증분)
        last_ts = cache_mgr.get_last_ts(cache, market)
        if last_ts is None:
            logger.info(f"[Pipeline] {market} 캐시 없음 -> 전체 9년 수집")
            new_data = fetcher.fetch_full_history(market, years=9, m_idx=m_idx, num_markets=num_markets)
        else:
            logger.info(f"[Pipeline] {market} 캐시 있음 (마지막: {last_ts}) -> 증분 수집")
            new_data = fetcher.fetch_incremental(market, last_ts)

        cache = cache_mgr.append(cache, market, new_data)
        cache_mgr.save(cache)

        # 최적화 실행
        data = cache[market]["data"]
        logger.info(f"[Pipeline] {market} 총 {len(data)}개 데이터로 최적화 시작")
        _save_progress("optimizing", base_collect_pct + collect_weight, f"{market} 그리드서치 실행중...")

        regime_results = optimizer.run_full_optimization(market, data, m_idx=m_idx, num_markets=num_markets)
        results[market] = regime_results

    # 결과 저장
    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    _save_progress("done", 100, f"최적화 완료! {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST")
    logger.info(f"[Pipeline] 최적화 완료 -> {RESULT_FILE}")
    return results


def get_latest_results() -> Optional[dict]:
    """저장된 최적화 결과 반환 (없으면 None)."""
    if os.path.exists(RESULT_FILE):
        try:
            with open(RESULT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def get_progress() -> dict:
    """현재 최적화 진행 상태 반환."""
    if os.path.exists(PROGRESS_FILE):
        for attempt in range(10):
            try:
                with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                time.sleep(0.05)
    return {"stage": "idle", "percent": 0, "message": "대기중"}


# ─────────────────────────────────────────────────────────────────────────────
# 단독 실행 (테스트)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S"
    )
    print("=" * 60)
    print("백테스팅 최적화 모듈 단독 실행")
    print(f"대상: {MARKETS}")
    print(f"수집 포인트: {[f'{h:02d}:{m:02d}' for h,m in TARGET_TIMES_KST]}")
    print("=" * 60)
    result = run_optimization()
    print("\n[SUCCESS] 최적화 완료!")
    for market in MARKETS:
        if market in result:
            for regime in ["BULL", "BEAR", "RANGE"]:
                r = result[market].get(regime, {})
                bt = r.get("backtest", {})
                strats = r.get("strategies", {})
                active = [k for k, v in strats.items() if v.get("enabled")]
                print(f"  {market}/{regime}: {active} -> "
                      f"수익 {bt.get('total_return_pct',0):.1f}%, "
                      f"Sharpe {bt.get('sharpe_ratio',0):.3f}, "
                      f"MDD {bt.get('mdd_pct',0):.1f}%")

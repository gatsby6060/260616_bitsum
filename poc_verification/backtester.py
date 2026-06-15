"""
backtester.py — 9년 실 데이터 기반 자동 파라미터 최적화 모듈
=====================================================================
■ 데이터:    빗썸 30분봉 캔들, 하루 8개 포인트 (01:00, 01:30, 07:00, 07:30, 14:00, 14:30, 23:00, 23:30 KST)
■ 최초 수집: 9년치 전체 (약 26,280개 × 2종목), 이후 매일 8개만 추가(증분)
■ 최적화:   BULL/BEAR/RANGE 장세별 독립 그리드 서치
■ 기준:     Sharpe Ratio 최대 + MDD ≤ 30% 필터
■ 수수료:   빗썸 0.04% + 슬리피지 0.05% → 왕복 총 0.18%
"""

import os
import json
import time
import math
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional

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

# 하루 중 수집할 KST 시각 (시, 분)
TARGET_TIMES_KST = [(1, 0), (1, 30), (7, 0), (7, 30),
                    (14, 0), (14, 30), (23, 0), (23, 30)]

# 수수료: maker 0.04% + taker 0.04% + 슬리피지 0.05% = 편도 0.09%, 왕복 0.18%
FEE_RATE = 0.0009          # 편도 1회 수수료+슬리피지
ROUND_TRIP_FEE = FEE_RATE * 2  # 왕복 0.0018

# 캐시 파일 경로 (backtester.py 와 같은 폴더)
_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE    = os.path.join(_DIR, "historical_data_cache.json")
RESULT_FILE   = os.path.join(_DIR, "optimized_params.json")
PROGRESS_FILE = os.path.join(_DIR, "backtest_progress.json")

MARKETS = ["KRW-BTC", "KRW-ETH"]

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

MDD_LIMIT = 0.30  # MDD 허용 한계 30%


# ─────────────────────────────────────────────────────────────────────────────
# 진행 상태 업데이트 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def _save_progress(stage: str, pct: float, msg: str = ""):
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "stage": stage,
                "percent": round(pct, 1),
                "message": msg,
                "updated_at": datetime.now(KST).isoformat()
            }, f, ensure_ascii=False)
    except Exception:
        pass


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

    def fetch_full_history(self, market: str, years: int = 9) -> list:
        """
        최초 1회 전체 히스토리 수집.
        반환: [{"ts": "2017-09-01T01:00:00+09:00", "close": 3000000.0, "volume": 12.3}, ...]
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
                pct = min(99, (1 - (cursor - cutoff).days / (years * 365)) * 100)
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
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

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
    @staticmethod
    def rsi(closes: list, period: int) -> list:
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
        return filled

    @staticmethod
    def bollinger(closes: list, period: int, std_dev: float):
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
        return mid, upper, lower

    @staticmethod
    def macd(closes: list, fast: int, slow: int, signal_period: int):
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
        return macd_line.tolist(), signal_line.tolist(), histogram.tolist()


# ─────────────────────────────────────────────────────────────────────────────
# 4. 장세 라벨링 (RegimeDetector 동일 알고리즘)
# ─────────────────────────────────────────────────────────────────────────────
class RegimeLabeler:
    FAIR_WINDOW = 4000   # 일봉 기준 약 4000일 이동평균 (8-포인트/일 기준: 4000 × 8 = 32000)
    TREND_WINDOW = 30    # 30일 추세 (8-포인트/일 기준: 240)
    BEAR_THRESHOLD = -0.15   # 공정가 대비 -15% 이하 → BEAR
    BULL_THRESHOLD = 0.10    # 공정가 대비 +10% 이상 → BULL

    def label(self, data: list) -> list:
        """
        각 데이터 포인트에 regime (BULL/BEAR/RANGE) 태그 추가.
        data: [{"ts":..., "close":...}, ...]
        반환: 동일 리스트에 "regime" 키 추가
        """
        if not data:
            return data

        closes = [d["close"] for d in data]
        n = len(closes)
        fw = min(self.FAIR_WINDOW * 8, n)   # 8포인트/일 환산
        tw = min(self.TREND_WINDOW * 8, n)

        labeled = []
        for i, row in enumerate(data):
            fair = sum(closes[max(0, i - fw): i + 1]) / min(i + 1, fw)
            dev = (closes[i] - fair) / fair if fair > 0 else 0

            if i >= tw:
                slope = (closes[i] - closes[i - tw]) / closes[i - tw] if closes[i - tw] > 0 else 0
            else:
                slope = 0

            if dev <= self.BEAR_THRESHOLD or slope < -0.005:
                regime = "BEAR"
            elif dev >= self.BULL_THRESHOLD and slope > 0.002:
                regime = "BULL"
            else:
                regime = "RANGE"

            labeled.append({**row, "regime": regime})

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
          "target_regime": "BULL"|"BEAR"|"RANGE"
        }
        """
        target = params["target_regime"]
        regime_data = [d for d in data if d["regime"] == target]

        if len(regime_data) < 30:
            return {"sharpe": -999, "mdd": 1.0, "total_return": 0, "win_rate": 0, "trades": 0}

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

            # OR 로직: 하나라도 신호
            buy  = any(buy_signals)  if buy_signals  else False
            sell = any(sell_signals) if sell_signals else False

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
        returns = [(equity_curve[i] / equity_curve[i-1]) - 1
                   for i in range(1, len(equity_curve))]
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
class GridSearchOptimizer:
    def __init__(self):
        self.sim = StrategySimulator()

    def _iter_params(self):
        """모든 파라미터 조합 생성기."""
        for combo in STRATEGY_COMBOS:
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
                                **combo,
                                "rsi_period": rp, "rsi_oversold": ro, "rsi_overbought": rob,
                                "bb_period": bp, "bb_std": bs,
                                "macd_fast": mf, "macd_slow": ms, "macd_signal": msig,
                            }

    def optimize_regime(self, data: list, regime: str, market: str) -> dict:
        """
        지정 장세에 대해 그리드 서치 실행.
        반환: 최적 파라미터 + 성과 지표
        """
        best_sharpe = -999
        best_result = None
        best_params = None
        total = 0
        valid = 0

        for params in self._iter_params():
            params["target_regime"] = regime
            result = self.sim.simulate(data, params)
            total += 1

            # MDD 필터
            if result["mdd"] > MDD_LIMIT:
                continue
            if result["trades"] < 5:
                continue

            valid += 1
            if result["sharpe"] > best_sharpe:
                best_sharpe = result["sharpe"]
                best_result = result
                best_params = dict(params)

        logger.info(f"[Optimizer] {market}/{regime}: 총 {total}조합 검색, {valid}개 통과, 최적 Sharpe={best_sharpe:.4f}")

        if best_params is None:
            # 필터 통과 없으면 MDD 무시하고 최고 Sharpe
            logger.warning(f"[Optimizer] {market}/{regime}: MDD 필터 통과 없음 → 차선책 선택")
            for params in self._iter_params():
                params["target_regime"] = regime
                result = self.sim.simulate(data, params)
                if result["sharpe"] > best_sharpe and result["trades"] >= 3:
                    best_sharpe = result["sharpe"]
                    best_result = result
                    best_params = dict(params)

        if best_params is None:
            return _default_regime_params(regime)

        return _build_output(best_params, best_result)

    def run_full_optimization(self, market: str, data: list) -> dict:
        """BTC 또는 ETH 전체 장세 최적화."""
        labeler = RegimeLabeler()
        labeled = labeler.label(data)
        output = {}
        regimes = ["BULL", "BEAR", "RANGE"]
        for idx, regime in enumerate(regimes):
            pct = 20 + (idx / len(regimes)) * 60
            _save_progress("optimizing", pct, f"{market} {regime} 장세 그리드서치 중...")
            output[regime] = self.optimize_regime(labeled, regime, market)
        return output


# ─────────────────────────────────────────────────────────────────────────────
# 7. 출력 포맷 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def _build_output(params: dict, result: dict) -> dict:
    return {
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
        "logic":       "OR",
        "threshold":   0.5,
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
        "fee_assumption": "편도 0.09% (수수료 0.04% + 슬리피지 0.05%)",
        "optimization_criteria": "Sharpe Ratio 최대 + MDD ≤ 30%"
    }

    for m_idx, market in enumerate(markets):
        logger.info(f"\n{'='*60}")
        logger.info(f"[Pipeline] {market} 처리 시작")
        _save_progress("collecting", m_idx * 50, f"{market} 데이터 준비중...")

        # 데이터 수집 (최초 or 증분)
        last_ts = cache_mgr.get_last_ts(cache, market)
        if last_ts is None:
            logger.info(f"[Pipeline] {market} 캐시 없음 → 전체 9년 수집")
            new_data = fetcher.fetch_full_history(market, years=9)
        else:
            logger.info(f"[Pipeline] {market} 캐시 있음 (마지막: {last_ts}) → 증분 수집")
            new_data = fetcher.fetch_incremental(market, last_ts)

        cache = cache_mgr.append(cache, market, new_data)
        cache_mgr.save(cache)

        # 최적화 실행
        data = cache[market]["data"]
        logger.info(f"[Pipeline] {market} 총 {len(data)}개 데이터로 최적화 시작")
        _save_progress("optimizing", m_idx * 50 + 5, f"{market} 그리드서치 실행중...")

        regime_results = optimizer.run_full_optimization(market, data)
        results[market] = regime_results

    # 결과 저장
    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    _save_progress("done", 100, f"최적화 완료! {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST")
    logger.info(f"[Pipeline] 최적화 완료 → {RESULT_FILE}")
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
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
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
    print("\n✅ 최적화 완료!")
    for market in MARKETS:
        if market in result:
            for regime in ["BULL", "BEAR", "RANGE"]:
                r = result[market].get(regime, {})
                bt = r.get("backtest", {})
                strats = r.get("strategies", {})
                active = [k for k, v in strats.items() if v.get("enabled")]
                print(f"  {market}/{regime}: {active} → "
                      f"수익 {bt.get('total_return_pct',0):.1f}%, "
                      f"Sharpe {bt.get('sharpe_ratio',0):.3f}, "
                      f"MDD {bt.get('mdd_pct',0):.1f}%")

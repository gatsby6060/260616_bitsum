"""
15분봉(또는 intraday) OHLCV → 일·주·월봉 집계.
별도 월봉/주봉 API 없이 동일 규칙으로 장세·하락 패턴 분석에 사용합니다.
"""

import json
import os
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
KST_OFFSET_SEC = 9 * 3600


def _bar_ts_key(bar: dict) -> int:
    if "time" in bar:
        return int(bar["time"])
    ts = str(bar.get("ts", ""))
    if not ts:
        return 0
    s = ts.replace("Z", "+00:00")
    if "+" not in s[10:] and "T" in s:
        s = s + "+09:00"
    return int(datetime.fromisoformat(s).timestamp())


def bar_date_str(bar: dict) -> str:
    if "ts" in bar:
        return str(bar["ts"]).split("T")[0]
    dt = datetime.fromtimestamp(_bar_ts_key(bar) + KST_OFFSET_SEC, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def bar_month_str(bar: dict) -> str:
    return bar_date_str(bar)[:7]


def daily_ohlcv_from_bars(bars: list) -> list:
    """15분봉 리스트 → 일봉 OHLCV (KST 일자 기준, 마지막 봉 종가=일 종가)."""
    if not bars:
        return []
    buckets = {}
    order = []
    for bar in bars:
        day = bar_date_str(bar)
        if day not in buckets:
            buckets[day] = {
                "time": _day_start_unix(day),
                "open": float(bar["open"]),
                "high": float(bar["high"]),
                "low": float(bar["low"]),
                "close": float(bar["close"]),
                "volume": float(bar.get("volume", 0)),
            }
            order.append(day)
            continue
        b = buckets[day]
        b["high"] = max(b["high"], float(bar["high"]))
        b["low"] = min(b["low"], float(bar["low"]))
        b["close"] = float(bar["close"])
        b["volume"] += float(bar.get("volume", 0))
    return [buckets[d] for d in sorted(order)]


def _day_start_unix(day: str) -> int:
    y, m, d = int(day[:4]), int(day[5:7]), int(day[8:10])
    return int(datetime(y, m, d, tzinfo=KST).timestamp()) - KST_OFFSET_SEC


def _week_start_key(day: str) -> str:
    dt = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=KST)
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")


def weekly_ohlcv_from_daily(daily: list) -> list:
    if not daily:
        return []
    buckets = {}
    order = []
    for bar in daily:
        day = bar_date_str(bar) if "ts" in bar else datetime.fromtimestamp(
            bar["time"] + KST_OFFSET_SEC, tz=timezone.utc
        ).strftime("%Y-%m-%d")
        wk = _week_start_key(day)
        if wk not in buckets:
            buckets[wk] = {
                "time": _day_start_unix(wk),
                "open": float(bar["open"]),
                "high": float(bar["high"]),
                "low": float(bar["low"]),
                "close": float(bar["close"]),
                "volume": float(bar.get("volume", 0)),
            }
            order.append(wk)
            continue
        b = buckets[wk]
        b["high"] = max(b["high"], float(bar["high"]))
        b["low"] = min(b["low"], float(bar["low"]))
        b["close"] = float(bar["close"])
        b["volume"] += float(bar.get("volume", 0))
    return [buckets[k] for k in sorted(order)]


def monthly_ohlcv_from_daily(daily: list) -> list:
    if not daily:
        return []
    buckets = {}
    order = []
    for bar in daily:
        if "ts" in bar:
            ym = str(bar["ts"])[:7]
            day = bar_date_str(bar)
        else:
            day = datetime.fromtimestamp(bar["time"] + KST_OFFSET_SEC, tz=timezone.utc).strftime("%Y-%m-%d")
            ym = day[:7]
        if ym not in buckets:
            buckets[ym] = {
                "time": _day_start_unix(f"{ym}-01"),
                "open": float(bar["open"]),
                "high": float(bar["high"]),
                "low": float(bar["low"]),
                "close": float(bar["close"]),
                "volume": float(bar.get("volume", 0)),
            }
            order.append(ym)
            continue
        b = buckets[ym]
        b["high"] = max(b["high"], float(bar["high"]))
        b["low"] = min(b["low"], float(bar["low"]))
        b["close"] = float(bar["close"])
        b["volume"] += float(bar.get("volume", 0))
    return [buckets[k] for k in sorted(order)]


def daily_closes_from_bars(bars: list) -> tuple:
    daily = daily_ohlcv_from_bars(bars)
    dates = []
    prices = []
    for bar in daily:
        if "ts" in bar:
            dates.append(bar_date_str(bar))
        else:
            dates.append(
                datetime.fromtimestamp(bar["time"] + KST_OFFSET_SEC, tz=timezone.utc).strftime("%Y-%m-%d")
            )
        prices.append(float(bar["close"]))
    return dates, prices


def monthly_closes_from_bars(bars: list) -> tuple:
    daily = daily_ohlcv_from_bars(bars)
    monthly = monthly_ohlcv_from_daily(daily)
    months = []
    prices = []
    for bar in monthly:
        dt = datetime.fromtimestamp(bar["time"] + KST_OFFSET_SEC, tz=timezone.utc)
        months.append(dt.strftime("%Y-%m"))
        prices.append(float(bar["close"]))
    return months, prices


def closes_from_ohlcv(candles: list) -> list:
    return [float(c["close"]) for c in candles]


def load_15m_bars_from_cache(ticker: str, cache_path: str = None) -> list:
    """백테스트 캐시(15m)에서 장기 시계열 로드."""
    if cache_path is None:
        cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "historical_data_cache.json")
    if not os.path.isfile(cache_path):
        return []
    try:
        with open(cache_path, encoding="utf-8") as f:
            cache = json.load(f)
        market = f"KRW-{ticker}"
        entry = cache.get(market, {})
        if isinstance(entry, dict):
            return list(entry.get("data") or [])
        return list(entry or [])
    except Exception:
        return []


def regime_series_from_15m_bars(bars: list) -> dict:
    """15분봉만으로 일·주·월 종가 시계열 생성."""
    daily = daily_ohlcv_from_bars(bars)
    weekly = weekly_ohlcv_from_daily(daily)
    monthly = monthly_ohlcv_from_daily(daily)
    d_dates, d_prices = daily_closes_from_bars(bars)
    m_months, m_prices = monthly_closes_from_bars(bars)
    return {
        "daily_candles": daily,
        "weekly_candles": weekly,
        "monthly_candles": monthly,
        "daily_prices": d_prices,
        "weekly_prices": closes_from_ohlcv(weekly),
        "monthly_prices": m_prices,
        "month_dates": [f"{m}-01" for m in m_months],
        "daily_dates": d_dates,
    }

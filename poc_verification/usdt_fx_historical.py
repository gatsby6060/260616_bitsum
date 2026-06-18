"""
백테스트용 USD/KRW 일별 환율 — 캔들 ts 날짜 기준 fair_krw 부여.
Frankfurter API (무료) + 로컬 캐시.
"""
import os
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger("UsdtFxHistorical")

_DIR = os.path.dirname(os.path.abspath(__file__))
FX_DAILY_CACHE_FILE = os.path.join(_DIR, "usd_krw_daily_cache.json")
FRANKFURTER_URL = "https://api.frankfurter.app"
DEFAULT_KRW = 1400.0
KST = timezone(timedelta(hours=9))


def _date_from_ts(ts: str) -> str:
    return ts.split("T")[0]


def _load_cache() -> dict:
    if os.path.exists(FX_DAILY_CACHE_FILE):
        try:
            with open(FX_DAILY_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"[UsdtFxHist] 캐시 읽기 실패: {e}")
    return {"rates": {}, "updated_at": None}


def _save_cache(cache: dict) -> None:
    try:
        with open(FX_DAILY_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"[UsdtFxHist] 캐시 저장 실패: {e}")


def _fetch_frankfurter_range(start: str, end: str) -> dict:
    """start~end 일별 USD→KRW 환율 dict {YYYY-MM-DD: rate}."""
    url = f"{FRANKFURTER_URL}/{start}..{end}?from=USD&to=KRW"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            logger.warning(f"[UsdtFxHist] API {r.status_code}: {url}")
            return {}
        body = r.json()
        out = {}
        for day, rates in (body.get("rates") or {}).items():
            krw = rates.get("KRW") if isinstance(rates, dict) else None
            if krw:
                out[day] = float(krw)
        return out
    except Exception as e:
        logger.warning(f"[UsdtFxHist] API 조회 실패 ({start}..{end}): {e}")
        return {}


def ensure_daily_fx_for_bars(bars: list) -> dict:
    """
    캔들 구간에 필요한 일별 USD/KRW 환율을 캐시/API로 채워 반환.
    Returns: {YYYY-MM-DD: krw_rate}
    """
    if not bars:
        return {}

    dates = sorted({_date_from_ts(b["ts"]) for b in bars if b.get("ts")})
    start = dates[0]
    end = dates[-1]
    start_dt = datetime.strptime(start, "%Y-%m-%d").date() - timedelta(days=14)
    fetch_start = start_dt.isoformat()

    cache = _load_cache()
    rates = dict(cache.get("rates") or {})

    need_fetch = any(d not in rates for d in dates) or end not in rates
    if need_fetch:
        fetched = _fetch_frankfurter_range(fetch_start, end)
        if fetched:
            rates.update(fetched)
            logger.info(f"[UsdtFxHist] API 갱신 {len(fetched)}일 ({fetch_start}~{end})")
        else:
            logger.warning(f"[UsdtFxHist] API 실패 — 캐시/기본값 사용 ({fetch_start}~{end})")

    cache["rates"] = rates
    cache["updated_at"] = datetime.now(KST).isoformat()
    _save_cache(cache)
    return rates


def _lookup_fair_krw(day: str, rates: dict, last_known: list) -> float:
    """해당 날짜 환율, 없으면 직전 영업일 환율 forward-fill."""
    if day in rates:
        last_known[0] = rates[day]
        return rates[day]
    if last_known[0] is not None:
        return last_known[0]
    prior = [d for d in rates if d < day]
    if prior:
        last_known[0] = rates[prior[-1]]
        return last_known[0]
    return DEFAULT_KRW


def enrich_bars_with_historical_fx(bars: list, rates: dict = None) -> list:
    """각 캔들에 fair_krw(당일 USD/KRW) 필드 추가."""
    if not bars:
        return bars
    if rates is None:
        rates = ensure_daily_fx_for_bars(bars)

    last_known = [None]
    enriched = []
    for bar in bars:
        day = _date_from_ts(bar["ts"])
        fair = _lookup_fair_krw(day, rates, last_known)
        enriched.append({**bar, "fair_krw": round(fair, 2)})
    return enriched


def usdt_fx_buy_allowed(
    usdt_krw: float,
    fair_krw: float,
    min_consider_gap_krw: float,
    min_target_profit_pct: float,
    fee_one_way: float = 0.0025,
) -> bool:
    """백테스트·실전 공통: 검토구역 + 수수료·목표 반영 매수 가능 여부."""
    if fair_krw <= 0 or usdt_krw <= 0:
        return False
    gap = fair_krw - usdt_krw
    if gap < min_consider_gap_krw:
        return False
    buy_fee = usdt_krw * fee_one_way
    sell_fee = fair_krw * fee_one_way
    min_profit = fair_krw * (min_target_profit_pct / 100.0)
    return gap > (buy_fee + sell_fee + min_profit)


def usdt_fx_net_pnl_pct(entry_price: float, exit_price: float, fee_one_way: float = 0.0025) -> float:
    if entry_price <= 0:
        return 0.0
    entry_cost = entry_price * (1 + fee_one_way)
    exit_val = exit_price * (1 - fee_one_way)
    return (exit_val - entry_cost) / entry_cost

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
FX_MIN_HISTORY_YEARS = 3       # USDT 학습 최소 환율 이력 (일별)
FX_FETCH_CHUNK_DAYS = 360      # Frankfurter 구간 조회 단위


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


def fx_cache_stats(rates: dict = None) -> dict:
    """캐시된 일별 환율 규모 요약."""
    if rates is None:
        rates = (_load_cache().get("rates") or {})
    dates = sorted(rates.keys())
    if not dates:
        return {"days": 0, "start": None, "end": None, "span_days": 0}
    start_d = datetime.strptime(dates[0], "%Y-%m-%d").date()
    end_d = datetime.strptime(dates[-1], "%Y-%m-%d").date()
    return {
        "days": len(dates),
        "start": dates[0],
        "end": dates[-1],
        "span_days": (end_d - start_d).days,
    }


def _fx_cache_covers_min_years(rates: dict, min_years: int) -> bool:
    stats = fx_cache_stats(rates)
    if stats["days"] < 200:
        return False
    today = datetime.now(KST).date()
    need_start = today - timedelta(days=min_years * 365)
    if not stats["start"]:
        return False
    cache_start = datetime.strptime(stats["start"], "%Y-%m-%d").date()
    cache_end = datetime.strptime(stats["end"], "%Y-%m-%d").date()
    recent_enough = cache_end >= today - timedelta(days=10)
    return cache_start <= need_start + timedelta(days=30) and recent_enough


def ensure_fx_history(min_years: int = FX_MIN_HISTORY_YEARS) -> dict:
    """
    최소 N년치 일별 USD/KRW 환율을 캐시에 선행 적재.
    (기존: USDT 15m 봉 구간만 요청 → 짧은 테스트 시 10일분만 쌓이는 문제 해결)
    """
    cache = _load_cache()
    rates = dict(cache.get("rates") or {})

    if _fx_cache_covers_min_years(rates, min_years):
        return rates

    today = datetime.now(KST).date()
    fetch_start = today - timedelta(days=min_years * 365 + 14)
    cursor = fetch_start

    logger.info(f"[UsdtFxHist] 환율 이력 백필 시작 (목표 {min_years}년+, {fetch_start}~{today})")

    while cursor <= today:
        chunk_end = min(cursor + timedelta(days=FX_FETCH_CHUNK_DAYS), today)
        fetched = _fetch_frankfurter_range(cursor.isoformat(), chunk_end.isoformat())
        if fetched:
            rates.update(fetched)
            logger.info(
                f"[UsdtFxHist] 백필 {cursor.isoformat()}~{chunk_end.isoformat()} "
                f"→ {len(fetched)}일"
            )
        else:
            logger.warning(
                f"[UsdtFxHist] 백필 실패 {cursor.isoformat()}~{chunk_end.isoformat()}"
            )
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.25)

    stats = fx_cache_stats(rates)
    cache["rates"] = rates
    cache["updated_at"] = datetime.now(KST).isoformat()
    cache["min_history_years"] = min_years
    cache["stats"] = stats
    _save_cache(cache)
    logger.info(
        f"[UsdtFxHist] 환율 캐시 완료 — {stats['days']}일 "
        f"({stats['start']} ~ {stats['end']})"
    )
    return rates


def ensure_daily_fx_for_bars(bars: list, min_years: int = FX_MIN_HISTORY_YEARS) -> dict:
    """
    캔들 구간에 필요한 일별 USD/KRW 환율을 캐시/API로 채워 반환.
    Returns: {YYYY-MM-DD: krw_rate}
    """
    rates = ensure_fx_history(min_years=min_years)

    if not bars:
        return rates

    dates = sorted({_date_from_ts(b["ts"]) for b in bars if b.get("ts")})
    start = dates[0]
    end = dates[-1]
    start_dt = datetime.strptime(start, "%Y-%m-%d").date() - timedelta(days=14)
    fetch_start = start_dt.isoformat()

    cache = _load_cache()
    rates = dict(cache.get("rates") or rates)

    need_fetch = any(d not in rates for d in dates) or end not in rates
    if need_fetch:
        fetched = _fetch_frankfurter_range(fetch_start, end)
        if fetched:
            rates.update(fetched)
            logger.info(f"[UsdtFxHist] 봉 구간 보강 {len(fetched)}일 ({fetch_start}~{end})")
        else:
            logger.warning(f"[UsdtFxHist] 봉 구간 API 실패 — 캐시 사용 ({fetch_start}~{end})")

    cache["rates"] = rates
    cache["updated_at"] = datetime.now(KST).isoformat()
    cache["stats"] = fx_cache_stats(rates)
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


# USDT 환율 차익 기본값 — 백테스트 학습 결과(usdt_optimal_defaults.json) 우선
USDT_OPTIMAL_DEFAULTS_FILE = os.path.join(_DIR, "usdt_optimal_defaults.json")
_BUILTIN_USDT_DEFAULTS = {
    "min_consider_gap_krw": 30,
    "exit_gap_krw": 10,
    "min_target_profit_pct": 3.0,
}
USDT_MIN_ENTRY_GAP_FLOOR = 30
USDT_MIN_EXIT_GAP_FLOOR = 10
USDT_MIN_EXIT_TP_FLOOR = 2.0
USDT_LIVE_GAP_SAFETY_OFFSET = 5  # 학습 최적값 대비 실매매 진입·탈출 gap 여유
_usdt_defaults_cache: dict = {}
USDT_OPTIMAL_ENTRY_GAP_KRW = _BUILTIN_USDT_DEFAULTS["min_consider_gap_krw"]
USDT_OPTIMAL_EXIT_GAP_KRW = _BUILTIN_USDT_DEFAULTS["exit_gap_krw"]


def _load_usdt_defaults_file() -> dict:
    if os.path.exists(USDT_OPTIMAL_DEFAULTS_FILE):
        try:
            with open(USDT_OPTIMAL_DEFAULTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("min_consider_gap_krw") is not None:
                return data
        except Exception as e:
            logger.warning(f"[UsdtDefaults] 파일 읽기 실패: {e}")
    return dict(_BUILTIN_USDT_DEFAULTS)


def _sync_usdt_default_globals(d: dict) -> None:
    global USDT_DEFAULT_ENTRY_GAP_KRW, USDT_DEFAULT_EXIT_GAP_KRW, USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT
    global USDT_OPTIMAL_ENTRY_GAP_KRW, USDT_OPTIMAL_EXIT_GAP_KRW
    USDT_OPTIMAL_ENTRY_GAP_KRW = int(d.get("min_consider_gap_krw", _BUILTIN_USDT_DEFAULTS["min_consider_gap_krw"]))
    USDT_OPTIMAL_EXIT_GAP_KRW = int(d.get("exit_gap_krw", _BUILTIN_USDT_DEFAULTS["exit_gap_krw"]))
    USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT = float(
        d.get("min_target_profit_pct", _BUILTIN_USDT_DEFAULTS["min_target_profit_pct"])
    )
    USDT_DEFAULT_ENTRY_GAP_KRW = USDT_OPTIMAL_ENTRY_GAP_KRW + USDT_LIVE_GAP_SAFETY_OFFSET
    USDT_DEFAULT_EXIT_GAP_KRW = USDT_OPTIMAL_EXIT_GAP_KRW + USDT_LIVE_GAP_SAFETY_OFFSET


def get_usdt_optimal_params() -> dict:
    """백테스트 학습 원값 (오프셋 없음)."""
    d = refresh_usdt_defaults()
    return {
        "min_consider_gap_krw": int(d.get("min_consider_gap_krw", _BUILTIN_USDT_DEFAULTS["min_consider_gap_krw"])),
        "exit_gap_krw": int(d.get("exit_gap_krw", _BUILTIN_USDT_DEFAULTS["exit_gap_krw"])),
        "min_target_profit_pct": float(d.get("min_target_profit_pct", _BUILTIN_USDT_DEFAULTS["min_target_profit_pct"])),
        "backtest_trades": d.get("backtest_trades"),
        "backtest_return_pct": d.get("backtest_return_pct"),
        "backtest_win_rate": d.get("backtest_win_rate"),
        "updated_at": d.get("updated_at"),
    }


def apply_usdt_live_gaps(params: dict) -> dict:
    """학습 최적 gap에 실매매 안전 여유(+5원) 적용."""
    entry = int(params.get("min_consider_gap_krw", _BUILTIN_USDT_DEFAULTS["min_consider_gap_krw"]))
    exit_g = int(params.get("exit_gap_krw", _BUILTIN_USDT_DEFAULTS["exit_gap_krw"]))
    return {
        **params,
        "min_consider_gap_krw": entry + USDT_LIVE_GAP_SAFETY_OFFSET,
        "exit_gap_krw": exit_g + USDT_LIVE_GAP_SAFETY_OFFSET,
        "min_target_profit_pct": float(
            params.get("min_target_profit_pct", _BUILTIN_USDT_DEFAULTS["min_target_profit_pct"])
        ),
        "optimal_min_consider_gap_krw": entry,
        "optimal_exit_gap_krw": exit_g,
        "live_gap_safety_offset": USDT_LIVE_GAP_SAFETY_OFFSET,
    }


def get_usdt_live_default_params() -> dict:
    """실매매·UI 기본값 (학습 최적 + gap 여유 5원)."""
    return apply_usdt_live_gaps(get_usdt_optimal_params())


def refresh_usdt_defaults(force: bool = False) -> dict:
    """메모리·모듈 상수를 usdt_optimal_defaults.json 과 동기화."""
    global _usdt_defaults_cache
    if force or not _usdt_defaults_cache:
        _usdt_defaults_cache = _load_usdt_defaults_file()
    _sync_usdt_default_globals(_usdt_defaults_cache)
    return dict(_usdt_defaults_cache)


def get_usdt_default_params() -> dict:
    return get_usdt_live_default_params()


def save_usdt_optimal_defaults(params: dict, result: dict = None) -> dict:
    """백테스트 최적 조합을 파일·메모리 기본값으로 저장 (학습 후 자동 호출)."""
    global _usdt_defaults_cache
    payload = {
        "min_consider_gap_krw": int(params.get("min_consider_gap_krw", _BUILTIN_USDT_DEFAULTS["min_consider_gap_krw"])),
        "exit_gap_krw": int(params.get("exit_gap_krw", _BUILTIN_USDT_DEFAULTS["exit_gap_krw"])),
        "min_target_profit_pct": round(float(params.get("min_target_profit_pct", _BUILTIN_USDT_DEFAULTS["min_target_profit_pct"])), 2),
        "updated_at": datetime.now(KST).isoformat(),
    }
    if result:
        payload["backtest_trades"] = int(result.get("trades", 0))
        payload["backtest_return_pct"] = float(result.get("total_return", 0))
        payload["backtest_win_rate"] = float(result.get("win_rate", 0))
    try:
        with open(USDT_OPTIMAL_DEFAULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"[UsdtDefaults] 저장 실패: {e}")
    _usdt_defaults_cache = payload
    _sync_usdt_default_globals(payload)
    logger.info(
        f"[UsdtDefaults] 최적 기본값 저장 — 진입 gap {payload['min_consider_gap_krw']}원 · "
        f"탈출 gap≤{payload['exit_gap_krw']}원 / TP {payload['min_target_profit_pct']}%"
    )
    return payload


refresh_usdt_defaults(force=True)


def usdt_fx_entry_allowed(
    usdt_krw: float,
    fair_krw: float,
    entry_gap_krw: float = USDT_DEFAULT_ENTRY_GAP_KRW,
    fee_one_way: float = 0.0025,
) -> bool:
    """진입: 환율−USDT gap ≥ entry_gap + 수수료(왕복) 확보."""
    return usdt_fx_buy_allowed(usdt_krw, fair_krw, entry_gap_krw, 0.0, fee_one_way)


def usdt_fx_buy_allowed(
    usdt_krw: float,
    fair_krw: float,
    min_consider_gap_krw: float,
    min_target_profit_pct: float,
    fee_one_way: float = 0.0025,
) -> bool:
    """검토구역(gap 하한) + 수수료·목표 반영 — USDT 학습믹스 필터 등."""
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


def calc_usdt_fx_convergence(
    entry_fair: float,
    entry_price: float,
    current_fair: float,
    current_price: float,
) -> dict:
    """
    매수 시점 환율 대비 할인(entry_gap)이 현재 환율까지 얼마나 수렴했는지.
    USDT 탈출 판단의 핵심 지표.
    """
    if entry_fair <= 0 or current_fair <= 0 or entry_price <= 0 or current_price <= 0:
        return {
            "entry_gap_krw": 0.0,
            "current_gap_krw": 0.0,
            "gap_closed_krw": 0.0,
            "convergence_pct": 0.0,
            "vs_fair_pct": 0.0,
        }

    entry_gap = max(entry_fair - entry_price, 0.0)
    current_gap = current_fair - current_price
    gap_closed = entry_gap - current_gap if entry_gap > 0 else 0.0
    if entry_gap > 0:
        convergence_pct = min(max(gap_closed / entry_gap * 100.0, 0.0), 100.0)
    else:
        convergence_pct = 100.0 if current_price >= current_fair else 0.0

    vs_fair_pct = (current_price - current_fair) / current_fair * 100.0

    return {
        "entry_gap_krw": round(entry_gap, 2),
        "current_gap_krw": round(current_gap, 2),
        "gap_closed_krw": round(gap_closed, 2),
        "convergence_pct": round(convergence_pct, 2),
        "vs_fair_pct": round(vs_fair_pct, 3),
    }


def usdt_fx_exit_fusion(
    entry_price: float,
    entry_fair: float,
    current_price: float,
    current_fair: float,
    exit_gap_krw: float = USDT_DEFAULT_EXIT_GAP_KRW,
    exit_take_profit_pct: float = USDT_DEFAULT_EXIT_TAKE_PROFIT_PCT,
    fee_one_way: float = 0.0025,
) -> dict:
    """
    USDT 융합 탈출 — 순손실 홀딩.
    1순위: 잔여 gap ≤ exit_gap_krw  2순위(백업): 순이익 ≥ exit_take_profit_pct
    """
    conv = calc_usdt_fx_convergence(entry_fair, entry_price, current_fair, current_price)
    net_pnl = usdt_fx_net_pnl_pct(entry_price, current_price, fee_one_way)
    curr_gap = conv["current_gap_krw"]

    if net_pnl <= 0:
        return {
            "allowed": False,
            "reason": "순손실 홀딩",
            "net_pnl_pct": round(net_pnl * 100, 3),
            **conv,
        }

    if curr_gap <= exit_gap_krw:
        return {
            "allowed": True,
            "reason": f"gap 수렴 ≤{exit_gap_krw:.0f}원 (잔여 {curr_gap:.0f}원)",
            "net_pnl_pct": round(net_pnl * 100, 3),
            **conv,
        }

    if net_pnl >= exit_take_profit_pct / 100.0:
        return {
            "allowed": True,
            "reason": f"목표 익절 {exit_take_profit_pct:.1f}% (순 {net_pnl * 100:.2f}%)",
            "net_pnl_pct": round(net_pnl * 100, 3),
            **conv,
        }

    return {
        "allowed": False,
        "reason": (
            f"수렴 대기 (gap {curr_gap:.0f}원 > {exit_gap_krw:.0f}원, "
            f"순 {net_pnl * 100:.2f}% < {exit_take_profit_pct:.1f}%)"
        ),
        "net_pnl_pct": round(net_pnl * 100, 3),
        **conv,
    }


def usdt_fx_exit_allowed(
    entry_price: float,
    entry_fair: float,
    current_price: float,
    current_fair: float,
    exit_gap_krw: float = 15.0,
    exit_convergence_pct: float = 90.0,
    fee_one_way: float = 0.0025,
) -> dict:
    """
    환율 수렴 기반 탈출(학습믹스용) — 순손실이면 절대 매도 불가.
    탈출: 현재가가 기준환율에 exit_gap 이내 접근 OR 할인 수렴률 달성 OR 기준환율 이상(순이익).
    """
    conv = calc_usdt_fx_convergence(entry_fair, entry_price, current_fair, current_price)
    net_pnl = usdt_fx_net_pnl_pct(entry_price, current_price, fee_one_way)

    if net_pnl <= 0:
        return {
            "allowed": False,
            "reason": "순손실 홀딩",
            "net_pnl_pct": round(net_pnl * 100, 3),
            **conv,
        }

    near_fair = conv["current_gap_krw"] <= exit_gap_krw
    converged = conv["convergence_pct"] >= exit_convergence_pct
    at_or_above_fair = current_price >= current_fair

    if at_or_above_fair:
        reason = f"기준환율 도달/초과 (vs_fair {conv['vs_fair_pct']:+.2f}%)"
        allowed = True
    elif converged:
        reason = f"할인 수렴 {conv['convergence_pct']:.0f}% ≥ {exit_convergence_pct:.0f}%"
        allowed = True
    elif near_fair:
        reason = f"환율 근접 (잔여 gap {conv['current_gap_krw']:.0f}원 ≤ {exit_gap_krw:.0f}원)"
        allowed = True
    else:
        reason = (
            f"수렴 대기 (수렴 {conv['convergence_pct']:.0f}% / "
            f"잔여 gap {conv['current_gap_krw']:.0f}원)"
        )
        allowed = False

    return {
        "allowed": allowed,
        "reason": reason,
        "net_pnl_pct": round(net_pnl * 100, 3),
        **conv,
    }

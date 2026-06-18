"""
USD/KRW 기준 환율 조회 — USDT 역프리미엄(할인) 매매용.
1 USDT ≒ 1 USD 이므로, 빗썸 KRW-USDT가 기준 환율보다 낮으면 역프(할인) 구간.
"""
import time
import logging
import requests

logger = logging.getLogger("UsdtFxReference")

FX_API_URL = "https://open.er-api.com/v6/latest/USD"
DEFAULT_KRW = 1400.0
CACHE_TTL_SEC = 3600

_cache = {"krw": 0.0, "updated_at": 0.0, "source": "default"}


def get_usd_krw_rate(manual_krw: float = 0.0, cache_ttl_sec: int = CACHE_TTL_SEC) -> tuple:
    """
    USD 1달러당 KRW 기준환율 반환.
    manual_krw > 0 이면 수동값 우선, 아니면 API 캐시(1시간) → 실패 시 DEFAULT_KRW.
    Returns: (rate, source_label)
    """
    if manual_krw and manual_krw > 0:
        return float(manual_krw), "manual"

    now = time.time()
    if _cache["krw"] > 0 and (now - _cache["updated_at"]) < cache_ttl_sec:
        return _cache["krw"], _cache["source"]

    try:
        r = requests.get(FX_API_URL, timeout=8)
        if r.status_code == 200:
            data = r.json()
            krw = float(data.get("rates", {}).get("KRW", 0))
            if krw > 0:
                _cache["krw"] = krw
                _cache["updated_at"] = now
                _cache["source"] = "api"
                logger.info(f"[UsdtFx] USD/KRW 기준환율 갱신: {krw:,.2f} (API)")
                return krw, "api"
    except Exception as e:
        logger.warning(f"[UsdtFx] 환율 API 조회 실패: {e}")

    if _cache["krw"] > 0:
        return _cache["krw"], _cache["source"] + "_stale"

    return DEFAULT_KRW, "default"


def calc_reverse_premium_pct(usdt_krw: float, fair_krw: float) -> dict:
    """
    역프리미엄(할인) % — 양수면 빗썸 USDT가 기준환율보다 쌈(매수 유리).
    premium_pct — 양수면 프리미엄(비쌈).
    """
    if fair_krw <= 0 or usdt_krw <= 0:
        return {
            "fair_krw": fair_krw,
            "usdt_krw": usdt_krw,
            "reverse_premium_pct": 0.0,
            "premium_pct": 0.0,
            "gap_krw": 0.0,
        }
    gap = fair_krw - usdt_krw
    reverse_pct = gap / fair_krw * 100.0
    premium_pct = -reverse_pct if reverse_pct < 0 else 0.0
    if reverse_pct < 0:
        reverse_pct = 0.0
        premium_pct = (usdt_krw - fair_krw) / fair_krw * 100.0
    return {
        "fair_krw": round(fair_krw, 2),
        "usdt_krw": round(usdt_krw, 2),
        "reverse_premium_pct": round(reverse_pct, 3),
        "premium_pct": round(premium_pct, 3),
        "gap_krw": round(gap, 2),
    }


def calc_usdt_fee_aware_edge(
    fair_krw: float,
    usdt_krw: float,
    fee_one_way: float = 0.0025,
    min_target_profit_pct: float = 0.2,
) -> dict:
    """
    (실제환율 - 국내매수가) > 매수수수료 + 매도수수료 + 최소목표수익 여부.
    정상화 매도 가정: 기준환율(fair)에서 매도.
    """
    if fair_krw <= 0 or usdt_krw <= 0:
        return {
            "spread_krw": 0.0,
            "buy_fee_krw": 0.0,
            "sell_fee_krw": 0.0,
            "min_profit_krw": 0.0,
            "required_spread_krw": 0.0,
            "net_edge_krw": 0.0,
            "net_edge_pct": 0.0,
            "is_buy_profitable": False,
        }

    spread = fair_krw - usdt_krw
    buy_fee = usdt_krw * fee_one_way
    sell_fee = fair_krw * fee_one_way
    min_profit = fair_krw * (min_target_profit_pct / 100.0)
    required = buy_fee + sell_fee + min_profit
    net_edge = spread - required

    return {
        "spread_krw": round(spread, 2),
        "buy_fee_krw": round(buy_fee, 2),
        "sell_fee_krw": round(sell_fee, 2),
        "min_profit_krw": round(min_profit, 2),
        "required_spread_krw": round(required, 2),
        "net_edge_krw": round(net_edge, 2),
        "net_edge_pct": round(net_edge / fair_krw * 100.0, 3),
        "is_buy_profitable": net_edge > 0,
    }


def calc_usdt_consideration_phase(
    gap_krw: float,
    min_consider_gap_krw: float,
    edge: dict,
) -> dict:
    """
    min_consider_gap_krw 이상일 때만 '매수 검토' 구역 진입 (즉시 매수 아님).
    phase: idle | consider | ready_fx
    """
    gap = float(gap_krw or 0)
    min_gap = float(min_consider_gap_krw or 0)
    if gap < min_gap:
        return {
            "phase": "idle",
            "in_consideration": False,
            "gap_krw": round(gap, 2),
            "min_consider_gap_krw": min_gap,
            "gap_to_consider_krw": round(max(0.0, min_gap - gap), 2),
        }
    if not edge.get("is_buy_profitable"):
        return {
            "phase": "consider",
            "in_consideration": True,
            "gap_krw": round(gap, 2),
            "min_consider_gap_krw": min_gap,
            "gap_to_consider_krw": 0.0,
        }
    return {
        "phase": "ready_fx",
        "in_consideration": True,
        "gap_krw": round(gap, 2),
        "min_consider_gap_krw": min_gap,
        "gap_to_consider_krw": 0.0,
    }

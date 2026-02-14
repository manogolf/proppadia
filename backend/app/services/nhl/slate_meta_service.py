from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.domains.nhl.repository import fetch_games_today, fetch_props_today, fetch_saves, fetch_sog

ET = ZoneInfo("America/New_York")
SLATE_META_CACHE_TTL_SECONDS = int(os.getenv("NHL_SLATE_META_CACHE_TTL_SECONDS", "300"))
_slate_meta_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _resolve_target_date(date_str: Optional[str]) -> str:
    if date_str:
        try:
            return datetime.fromisoformat(str(date_str)).date().isoformat()
        except Exception as e:
            raise ValueError("date must be YYYY-MM-DD") from e
    return datetime.now(ET).date().isoformat()


def _normalize_rows_result(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        if value.get("ok") is False:
            return {"ok": False, "count": 0, "error": value.get("error")}
        rows = value.get("rows")
        count = value.get("count")
        if isinstance(rows, list):
            return {"ok": True, "count": int(count) if isinstance(count, int) else len(rows), "error": None}
        return {"ok": False, "count": 0, "error": "unexpected payload"}
    if isinstance(value, list):
        return {"ok": True, "count": len(value), "error": None}
    return {"ok": False, "count": 0, "error": "unexpected payload"}


def _build_slate_meta(*, date_str: str, limit: int) -> Dict[str, Any]:
    games = _normalize_rows_result(fetch_games_today(date_str, limit=limit, offset=0))
    props = _normalize_rows_result(fetch_props_today(date_str, limit=limit, offset=0))
    sog = _normalize_rows_result(fetch_sog(date_str, limit=limit, offset=0))
    saves = _normalize_rows_result(fetch_saves(date_str, limit=limit, offset=0))
    all_ok = all(x.get("ok") is True for x in (games, props, sog, saves))
    return {
        "date": date_str,
        "limit": int(limit),
        "components": {
            "games_today": games,
            "props_today": props,
            "sog": sog,
            "saves": saves,
        },
        "all_ok": all_ok,
    }


def get_nhl_slate_meta(*, date: Optional[str], limit: int = 100) -> Dict[str, Any]:
    target_date = _resolve_target_date(date)
    key = f"{target_date}:{int(limit)}"
    now = time.time()
    cached = _slate_meta_cache.get(key)

    if cached and (now - cached[0]) < SLATE_META_CACHE_TTL_SECONDS:
        payload = dict(cached[1])
        payload.update(
            {
                "ok": True,
                "source": "cache",
                "stale": False,
                "cached_at": datetime.fromtimestamp(cached[0], tz=ET).isoformat(),
                "ttl_seconds": SLATE_META_CACHE_TTL_SECONDS,
            }
        )
        return payload

    try:
        payload = _build_slate_meta(date_str=target_date, limit=limit)
        ts = time.time()
        _slate_meta_cache[key] = (ts, payload)
        return {
            "ok": True,
            **payload,
            "source": "upstream",
            "stale": False,
            "cached_at": datetime.fromtimestamp(ts, tz=ET).isoformat(),
            "ttl_seconds": SLATE_META_CACHE_TTL_SECONDS,
        }
    except Exception as e:
        if cached:
            payload = dict(cached[1])
            return {
                "ok": True,
                **payload,
                "source": "stale_cache",
                "stale": True,
                "cached_at": datetime.fromtimestamp(cached[0], tz=ET).isoformat(),
                "ttl_seconds": SLATE_META_CACHE_TTL_SECONDS,
                "warning": f"upstream unavailable: {type(e).__name__}",
            }
        raise


from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import requests

STANDINGS_CACHE_TTL_SECONDS = int(os.getenv("NHL_STANDINGS_CACHE_TTL_SECONDS", "21600"))
_standings_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _cache_key(*, as_of: str) -> str:
    return str(as_of or "now")


def _to_iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def fetch_standings(*, as_of: str = "now") -> Dict[str, Any]:
    suffix = "now" if not as_of or str(as_of).strip().lower() == "now" else str(as_of).strip()
    url = f"https://api-web.nhle.com/v1/standings/{suffix}"
    resp = requests.get(url, timeout=20, headers={"User-Agent": "proppadia/1.0"})
    resp.raise_for_status()
    return resp.json()


def get_standings(*, as_of: str = "now", allow_stale_on_error: bool = True) -> Dict[str, Any]:
    key = _cache_key(as_of=as_of)
    now = time.time()
    cached = _standings_cache.get(key)
    if cached and (now - cached[0]) < STANDINGS_CACHE_TTL_SECONDS:
        payload = cached[1]
        return {
            "ok": True,
            "as_of": as_of,
            "source": "cache",
            "stale": False,
            "cached_at": _to_iso_utc(cached[0]),
            "standings": payload.get("standings", []),
        }

    try:
        payload = fetch_standings(as_of=as_of)
        fetched_at = time.time()
        _standings_cache[key] = (fetched_at, payload)
        return {
            "ok": True,
            "as_of": as_of,
            "source": "upstream",
            "stale": False,
            "cached_at": _to_iso_utc(fetched_at),
            "standings": payload.get("standings", []),
        }
    except Exception as e:
        if allow_stale_on_error and cached:
            payload = cached[1]
            return {
                "ok": True,
                "as_of": as_of,
                "source": "stale_cache",
                "stale": True,
                "cached_at": _to_iso_utc(cached[0]),
                "standings": payload.get("standings", []),
                "warning": f"upstream unavailable: {type(e).__name__}",
            }
        raise

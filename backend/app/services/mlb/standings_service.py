from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import requests

STANDINGS_CACHE_TTL_SECONDS = int(os.getenv("MLB_STANDINGS_CACHE_TTL_SECONDS", "21600"))
_standings_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _cache_key(*, season: int, league_ids: str) -> str:
    return f"{int(season)}::{str(league_ids)}"


def _to_iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def fetch_standings(*, season: int, league_ids: str = "103,104") -> Dict[str, Any]:
    url = (
        "https://statsapi.mlb.com/api/v1/standings"
        f"?leagueId={league_ids}"
        f"&season={int(season)}"
        "&standingsTypes=regularSeason"
    )
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()


def get_standings(
    *,
    season: int,
    league_ids: str = "103,104",
    allow_stale_on_error: bool = True,
) -> Dict[str, Any]:
    key = _cache_key(season=season, league_ids=league_ids)
    now = time.time()
    cached = _standings_cache.get(key)
    if cached and (now - cached[0]) < STANDINGS_CACHE_TTL_SECONDS:
        payload = cached[1]
        return {
            "ok": True,
            "season": int(season),
            "league_ids": str(league_ids),
            "source": "cache",
            "stale": False,
            "cached_at": _to_iso_utc(cached[0]),
            "records": payload.get("records", []),
        }

    try:
        payload = fetch_standings(season=int(season), league_ids=str(league_ids))
        fetched_at = time.time()
        _standings_cache[key] = (fetched_at, payload)
        return {
            "ok": True,
            "season": int(season),
            "league_ids": str(league_ids),
            "source": "upstream",
            "stale": False,
            "cached_at": _to_iso_utc(fetched_at),
            "records": payload.get("records", []),
        }
    except Exception as e:
        if allow_stale_on_error and cached:
            payload = cached[1]
            return {
                "ok": True,
                "season": int(season),
                "league_ids": str(league_ids),
                "source": "stale_cache",
                "stale": True,
                "cached_at": _to_iso_utc(cached[0]),
                "records": payload.get("records", []),
                "warning": f"upstream unavailable: {type(e).__name__}",
            }
        raise

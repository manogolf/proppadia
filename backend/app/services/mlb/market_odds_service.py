from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

from backend.domains.mlb.prop_workflow import normalize_prop_type

ET = ZoneInfo("America/New_York")
ODDS_BASE = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
SNAPSHOT_TTL_SECONDS = int(os.getenv("MLB_ODDS_CACHE_TTL_SECONDS", "21600"))

PROP_TO_ODDS_MARKET = {
    # Batter O/U props
    "hits": "batter_hits",
    "home_runs": "batter_home_runs",
    "rbis": "batter_rbis",
    "runs_scored": "batter_runs_scored",
    "hits_runs_rbis": "batter_hits_runs_rbis",
    "singles": "batter_singles",
    "doubles": "batter_doubles",
    "triples": "batter_triples",
    "walks": "batter_walks",
    "strikeouts_batting": "batter_strikeouts",
    "stolen_bases": "batter_stolen_bases",
    "total_bases": "batter_total_bases",

    # Pitcher O/U props
    "strikeouts_pitching": "pitcher_strikeouts",
    "hits_allowed": "pitcher_hits_allowed",
    "walks_allowed": "pitcher_walks",
    "earned_runs": "pitcher_earned_runs",
    "outs_recorded": "pitcher_outs",

    # Not mapped yet (non O/U or unsupported by current flow):
    # - batter_first_home_run (yes/no)
    # - pitcher_record_a_win (yes/no)
}

_snapshot_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


def _markets_query() -> str:
    # Fetch all supported O/U markets in one call to minimize credit burn.
    return ",".join(sorted(set(PROP_TO_ODDS_MARKET.values())))


def get_supported_market_map() -> Dict[str, str]:
    # Return a copy to keep module constants immutable to callers.
    return dict(PROP_TO_ODDS_MARKET)


def get_market_cache_status() -> Dict[str, Any]:
    now = time.time()
    entries = []
    for game_date, (cached_at, rows) in sorted(_snapshot_cache.items()):
        age_seconds = max(0, int(now - cached_at))
        entries.append(
            {
                "game_date": game_date,
                "age_seconds": age_seconds,
                "rows_cached": len(rows),
                "stale": age_seconds > SNAPSHOT_TTL_SECONDS,
            }
        )
    return {
        "ok": True,
        "ttl_seconds": SNAPSHOT_TTL_SECONDS,
        "entry_count": len(entries),
        "supported_prop_count": len(PROP_TO_ODDS_MARKET),
        "supported_market_count": len(set(PROP_TO_ODDS_MARKET.values())),
        "entries": entries,
    }


def _normalize_name(name: str) -> str:
    return "".join(ch.lower() for ch in str(name or "") if ch.isalnum() or ch.isspace()).strip()


def _american_to_implied_probability(price: Optional[float]) -> Optional[float]:
    if price is None:
        return None
    try:
        p = float(price)
    except Exception:
        return None
    if p == 0:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    return abs(p) / (abs(p) + 100.0)


def _event_date_et(event: Dict[str, Any]) -> Optional[str]:
    commence = event.get("commence_time")
    if not commence:
        return None
    try:
        dt = datetime.fromisoformat(str(commence).replace("Z", "+00:00"))
        return dt.astimezone(ET).date().isoformat()
    except Exception:
        return None


def _fetch_market_snapshot(*, game_date: str) -> List[Dict[str, Any]]:
    cache_key = game_date
    now = time.time()
    cached = _snapshot_cache.get(cache_key)
    if cached and (now - cached[0]) < SNAPSHOT_TTL_SECONDS:
        return cached[1]

    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ODDS_API_KEY missing")

    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": _markets_query(),
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    res = requests.get(ODDS_BASE, params=params, timeout=20)
    res.raise_for_status()
    payload = res.json()
    if not isinstance(payload, list):
        raise RuntimeError("unexpected OddsAPI payload shape")

    rows = [ev for ev in payload if _event_date_et(ev) == game_date]
    _snapshot_cache[cache_key] = (now, rows)
    return rows


def _extract_candidate_outcomes(
    *,
    events: List[Dict[str, Any]],
    market_key: str,
    player_name: str,
    over_under: str,
    line: Optional[float],
) -> List[Dict[str, Any]]:
    player_norm = _normalize_name(player_name)
    side_norm = (over_under or "over").strip().lower()
    out: List[Dict[str, Any]] = []

    for ev in events:
        for book in ev.get("bookmakers") or []:
            bookmaker_key = book.get("key")
            bookmaker_title = book.get("title")
            for market in book.get("markets") or []:
                if market.get("key") != market_key:
                    continue
                for outcome in market.get("outcomes") or []:
                    desc = str(outcome.get("description") or "").strip()
                    side = str(outcome.get("name") or "").strip().lower()
                    point = outcome.get("point")
                    price = outcome.get("price")
                    price_num = None
                    try:
                        price_num = float(price)
                    except Exception:
                        pass
                    if not desc or price_num is None:
                        continue

                    desc_norm = _normalize_name(desc)
                    if not desc_norm:
                        continue

                    # Player-name match scoring.
                    score = 0.0
                    if desc_norm == player_norm:
                        score += 3.0
                    elif player_norm in desc_norm or desc_norm in player_norm:
                        score += 2.0
                    else:
                        continue

                    if side and side == side_norm:
                        score += 1.0

                    point_num = None
                    try:
                        point_num = float(point) if point is not None else None
                    except Exception:
                        point_num = None
                    if line is not None and point_num is not None and abs(point_num - line) < 0.06:
                        score += 1.0
                    elif line is not None:
                        score -= 0.25

                    out.append(
                        {
                            "score": score,
                            "event_id": ev.get("id"),
                            "commence_time": ev.get("commence_time"),
                            "home_team": ev.get("home_team"),
                            "away_team": ev.get("away_team"),
                            "bookmaker_key": bookmaker_key,
                            "bookmaker_title": bookmaker_title,
                            "market_key": market_key,
                            "player_name": desc,
                            "side": side,
                            "line": point_num,
                            "price_american": int(round(price_num)),
                            "implied_probability": _american_to_implied_probability(price_num),
                        }
                    )
    out.sort(key=lambda r: r["score"], reverse=True)
    return out


def fetch_mlb_market_odds(
    *,
    player_name: str,
    prop_type: str,
    game_date: str,
    over_under: str = "over",
    line: Optional[float] = None,
) -> Dict[str, Any]:
    normalized_prop = normalize_prop_type(prop_type)
    market_key = PROP_TO_ODDS_MARKET.get(normalized_prop)
    if not market_key:
        return {
            "ok": True,
            "found": False,
            "reason": f"unsupported prop_type for OddsAPI mapping: {normalized_prop}",
            "market_key": None,
        }

    if not str(player_name or "").strip():
        raise ValueError("player_name is required")
    try:
        datetime.fromisoformat(game_date)
    except Exception as e:
        raise ValueError("game_date must be YYYY-MM-DD") from e

    try:
        events = _fetch_market_snapshot(game_date=game_date)
    except Exception as e:
        return {
            "ok": False,
            "found": False,
            "reason": f"{type(e).__name__}: {e}",
            "market_key": market_key,
        }

    candidates = _extract_candidate_outcomes(
        events=events,
        market_key=market_key,
        player_name=player_name,
        over_under=over_under,
        line=line,
    )
    if not candidates:
        return {
            "ok": True,
            "found": False,
            "reason": "no matching market outcome",
            "market_key": market_key,
            "events_considered": len(events),
        }

    best = candidates[0]
    return {
        "ok": True,
        "found": True,
        "market_key": market_key,
        "events_considered": len(events),
        "event_id": best.get("event_id"),
        "commence_time": best.get("commence_time"),
        "home_team": best.get("home_team"),
        "away_team": best.get("away_team"),
        "bookmaker": best.get("bookmaker_title") or best.get("bookmaker_key"),
        "player_name": best.get("player_name"),
        "side": best.get("side"),
        "line": best.get("line"),
        "price_american": best.get("price_american"),
        "implied_probability": best.get("implied_probability"),
    }

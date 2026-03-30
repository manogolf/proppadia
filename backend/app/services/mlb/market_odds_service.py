from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests

from backend.domains.mlb.prop_workflow import normalize_prop_type

ET = ZoneInfo("America/New_York")
ODDS_BASE = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
EVENTS_BASE = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events"
SNAPSHOT_TTL_SECONDS = int(os.getenv("MLB_ODDS_CACHE_TTL_SECONDS", "21600"))

BOOKMAKER_KEY_ALIASES = {
    "betonline.ag": "betonlineag",
    "mybookie.ag": "mybookieag",
    "betonline_ag": "betonlineag",
    "mybookie_ag": "mybookieag",
}

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

# Additional market-key aliases.
# - Aliases for props that do not have a primary stable market mapping are always
#   attempted via event-level fetch (422-safe).
# - Other aliases are enabled by MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED=1.
PROP_TO_ODDS_MARKET_ALIASES = {
    "runs_rbis": (
        "batter_runs_rbis",
        "batter_runs_rbi",
        "batter_r+rbi",
    ),
}

_snapshot_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_API_KEY_RE = re.compile(r"(apiKey=)([^&\s]+)")


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _parse_csv(raw: str) -> List[str]:
    vals = [str(x).strip() for x in str(raw or "").split(",")]
    return [v for v in vals if v]


def _normalize_bookmaker_key(value: str) -> str:
    key = str(value or "").strip().lower()
    if not key:
        return ""
    key = BOOKMAKER_KEY_ALIASES.get(key, key)
    return key


def _bookmakers_query_csv() -> str:
    toks = _parse_csv(str(os.getenv("MLB_ODDS_BOOKMAKERS", "") or ""))
    seen = set()
    out: List[str] = []
    for tok in toks:
        key = _normalize_bookmaker_key(tok)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return ",".join(out)


def _include_bet_limits_param() -> str:
    return "true" if _env_bool("MLB_ODDS_INCLUDE_BET_LIMITS", True) else "false"


def _stable_market_keys() -> List[str]:
    keys = sorted(set(str(v) for v in (PROP_TO_ODDS_MARKET or {}).values() if str(v).strip()))
    configured = set(_parse_csv(str(os.getenv("MLB_ODDS_MARKETS", "") or "")))
    if configured:
        keys = [k for k in keys if k in configured]
    return keys


def _always_alias_market_keys() -> List[str]:
    stable_props = set(str(k or "").strip() for k in (PROP_TO_ODDS_MARKET or {}).keys())
    keys: List[str] = []
    for prop_type, aliases in (PROP_TO_ODDS_MARKET_ALIASES or {}).items():
        prop = str(prop_type or "").strip()
        if not prop or prop in stable_props:
            continue
        for key in aliases or ():
            k = str(key or "").strip()
            if k:
                keys.append(k)
    return sorted(set(keys))


def _experimental_market_keys() -> List[str]:
    keys: List[str] = list(_always_alias_market_keys())
    if _env_bool("MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED", False):
        for _prop_type, aliases in (PROP_TO_ODDS_MARKET_ALIASES or {}).items():
            for key in aliases or ():
                k = str(key or "").strip()
                if k:
                    keys.append(k)
        for key in _parse_csv(str(os.getenv("MLB_ODDS_EXTRA_MARKETS", "") or "")):
            keys.append(key)
    stable = set(_stable_market_keys())
    return sorted(set(k for k in keys if k and k not in stable))


def _markets_query(*, include_experimental: bool = False) -> str:
    # Fetch all stable O/U markets in one call to minimize credit burn.
    keys = list(_stable_market_keys())
    if include_experimental:
        keys.extend(_experimental_market_keys())
    return ",".join(sorted(set(keys)))


def _market_groups(markets_csv: str, *, max_per_group: int) -> List[str]:
    toks = _parse_csv(markets_csv)
    if not toks:
        return []
    step = max(1, int(max_per_group))
    return [",".join(toks[i : i + step]) for i in range(0, len(toks), step)]


def _merge_market_payload(base_payload: Dict[str, Any], next_payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base_payload or {})
    base_books = out.get("bookmakers")
    next_books = (next_payload or {}).get("bookmakers")
    if not isinstance(base_books, list) or not isinstance(next_books, list):
        return out

    by_key: Dict[str, Dict[str, Any]] = {}
    ordered: List[Dict[str, Any]] = []
    for b in base_books:
        if not isinstance(b, dict):
            continue
        key = str(b.get("key") or "").strip()
        if not key:
            continue
        cp = dict(b)
        markets = cp.get("markets")
        cp["markets"] = list(markets) if isinstance(markets, list) else []
        by_key[key] = cp
        ordered.append(cp)

    for b in next_books:
        if not isinstance(b, dict):
            continue
        key = str(b.get("key") or "").strip()
        if not key:
            continue
        if key not in by_key:
            cp = dict(b)
            markets = cp.get("markets")
            cp["markets"] = list(markets) if isinstance(markets, list) else []
            by_key[key] = cp
            ordered.append(cp)
            continue
        ex = by_key[key]
        ex_markets = ex.get("markets")
        if not isinstance(ex_markets, list):
            ex_markets = []
            ex["markets"] = ex_markets
        for m in b.get("markets", []) if isinstance(b.get("markets"), list) else []:
            if isinstance(m, dict):
                ex_markets.append(m)

    out["bookmakers"] = ordered
    return out


def _merge_event_rows(
    *,
    base_rows: List[Dict[str, Any]],
    extra_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not extra_rows:
        return list(base_rows)
    out = [dict(r) for r in base_rows if isinstance(r, dict)]
    by_event: Dict[str, int] = {}
    for i, row in enumerate(out):
        event_id = str(row.get("id") or "").strip()
        if event_id:
            by_event[event_id] = i
    for row in extra_rows:
        if not isinstance(row, dict):
            continue
        event_id = str(row.get("id") or "").strip()
        if not event_id or event_id not in by_event:
            out.append(dict(row))
            if event_id:
                by_event[event_id] = len(out) - 1
            continue
        idx = by_event[event_id]
        out[idx] = _merge_market_payload(out[idx], row)
    return out


def get_supported_market_map() -> Dict[str, str]:
    # Return a copy to keep module constants immutable to callers.
    return dict(PROP_TO_ODDS_MARKET)


def get_market_to_prop_map(*, include_aliases: bool = True) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for prop_type, market_key in (PROP_TO_ODDS_MARKET or {}).items():
        k = str(market_key or "").strip()
        if k:
            out[k] = str(prop_type)
    if include_aliases:
        for prop_type, aliases in (PROP_TO_ODDS_MARKET_ALIASES or {}).items():
            for alias in aliases or ():
                k = str(alias or "").strip()
                if k and k not in out:
                    out[k] = str(prop_type)
    return out


def get_prop_market_candidates(*, prop_type: str, include_aliases: bool = True) -> List[str]:
    normalized = normalize_prop_type(prop_type)
    out: List[str] = []
    primary = PROP_TO_ODDS_MARKET.get(normalized)
    if primary:
        out.append(str(primary))
    if include_aliases:
        for alias in (PROP_TO_ODDS_MARKET_ALIASES.get(normalized) or ()):
            k = str(alias or "").strip()
            if k and k not in out:
                out.append(k)
    return out


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
        "configured_markets": _parse_csv(str(os.getenv("MLB_ODDS_MARKETS", "") or "")),
        "configured_bookmakers": _parse_csv(_bookmakers_query_csv()),
        "experimental_markets_enabled": _env_bool("MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED", False),
        "experimental_market_count": len(_experimental_market_keys()),
        "include_bet_limits": _env_bool("MLB_ODDS_INCLUDE_BET_LIMITS", True),
        "entries": entries,
    }


def refresh_market_cache_for_date(*, game_date: str) -> Dict[str, Any]:
    """Warm/refresh one game-date snapshot in process cache.

    Returns a status payload and never raises for upstream/API errors.
    Validation errors (bad date format) raise ValueError.
    """
    try:
        datetime.fromisoformat(game_date)
    except Exception as e:
        raise ValueError("game_date must be YYYY-MM-DD") from e

    now = time.time()
    cached = _snapshot_cache.get(game_date)
    was_fresh = bool(cached and (now - cached[0]) < SNAPSHOT_TTL_SECONDS)

    try:
        rows = _fetch_market_snapshot(game_date=game_date)
    except Exception as e:
        return {
            "ok": False,
            "game_date": game_date,
            "reason": f"{type(e).__name__}: {_sanitize_error_message(e)}",
        }

    latest = _snapshot_cache.get(game_date)
    age_seconds = max(0, int(time.time() - latest[0])) if latest else None
    return {
        "ok": True,
        "game_date": game_date,
        "rows_cached": len(rows),
        "cache_hit": was_fresh,
        "age_seconds": age_seconds,
        "ttl_seconds": SNAPSHOT_TTL_SECONDS,
    }


def _normalize_name(name: str) -> str:
    return "".join(ch.lower() for ch in str(name or "") if ch.isalnum() or ch.isspace()).strip()


def _sanitize_error_message(msg: Any) -> str:
    return _API_KEY_RE.sub(r"\1[REDACTED]", str(msg or ""))


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

    stable_markets = _markets_query(include_experimental=False)
    params = {
        "apiKey": api_key,
        "regions": str(os.getenv("MLB_ODDS_REGIONS", "us") or "us"),
        "markets": stable_markets,
        "oddsFormat": "american",
        "dateFormat": "iso",
        "includeBetLimits": _include_bet_limits_param(),
    }
    bookmakers_csv = _bookmakers_query_csv()
    if bookmakers_csv:
        params["bookmakers"] = bookmakers_csv

    # Preferred one-call fetch for all events/markets on the sport endpoint.
    # If OddsAPI rejects market keys at this endpoint (422), fallback to per-event odds.
    res = requests.get(ODDS_BASE, params=params, timeout=20)
    if res.status_code == 422:
        rows = _fetch_event_level_market_snapshot(
            api_key=api_key,
            game_date=game_date,
            markets_csv=stable_markets,
            allow_422_skip=False,
        )
        extra_markets = _experimental_market_keys()
        if extra_markets:
            extra_rows = _fetch_event_level_market_snapshot(
                api_key=api_key,
                game_date=game_date,
                markets_csv=",".join(extra_markets),
                allow_422_skip=True,
            )
            rows = _merge_event_rows(base_rows=rows, extra_rows=extra_rows)
        _snapshot_cache[cache_key] = (now, rows)
        return rows

    res.raise_for_status()
    payload = res.json()
    if not isinstance(payload, list):
        raise RuntimeError("unexpected OddsAPI payload shape")
    rows = [ev for ev in payload if _event_date_et(ev) == game_date]
    extra_markets = _experimental_market_keys()
    if extra_markets:
        extra_rows = _fetch_event_level_market_snapshot(
            api_key=api_key,
            game_date=game_date,
            markets_csv=",".join(extra_markets),
            allow_422_skip=True,
        )
        rows = _merge_event_rows(base_rows=rows, extra_rows=extra_rows)

    _snapshot_cache[cache_key] = (now, rows)
    return rows


def _fetch_event_level_market_snapshot(
    *,
    api_key: str,
    game_date: str,
    markets_csv: str,
    allow_422_skip: bool,
) -> List[Dict[str, Any]]:
    events_res = requests.get(
        EVENTS_BASE,
        params={
            "apiKey": api_key,
            "dateFormat": "iso",
        },
        timeout=20,
    )
    events_res.raise_for_status()
    events_payload = events_res.json()
    if not isinstance(events_payload, list):
        raise RuntimeError("unexpected OddsAPI events payload shape")

    target_events = [ev for ev in events_payload if _event_date_et(ev) == game_date]
    if not target_events:
        return []

    try:
        max_per_group = int(str(os.getenv("MLB_ODDS_EVENT_MAX_MARKETS_PER_CALL", "6") or "6"))
    except Exception:
        max_per_group = 6
    market_groups = _market_groups(markets_csv, max_per_group=max_per_group)
    if not market_groups:
        return []

    rows: List[Dict[str, Any]] = []
    regions_csv = str(os.getenv("MLB_ODDS_REGIONS", "us") or "us")
    bookmakers_csv = _bookmakers_query_csv()
    for ev in target_events:
        event_id = ev.get("id")
        if not event_id:
            continue

        merged_payload: Optional[Dict[str, Any]] = None
        for markets_group in market_groups:
            params = {
                "apiKey": api_key,
                "regions": regions_csv,
                "markets": markets_group,
                "oddsFormat": "american",
                "dateFormat": "iso",
                "includeBetLimits": _include_bet_limits_param(),
            }
            if bookmakers_csv:
                params["bookmakers"] = bookmakers_csv
            odds_res = requests.get(
                f"{EVENTS_BASE}/{event_id}/odds",
                params=params,
                timeout=20,
            )

            # Some events may not expose props yet; optionally skip those gracefully.
            if odds_res.status_code == 422 and allow_422_skip:
                continue
            odds_res.raise_for_status()
            payload = odds_res.json()
            if not isinstance(payload, dict):
                continue
            if merged_payload is None:
                merged_payload = payload
            else:
                merged_payload = _merge_market_payload(merged_payload, payload)

        if isinstance(merged_payload, dict):
            rows.append(merged_payload)

    return rows


def _extract_candidate_outcomes(
    *,
    events: List[Dict[str, Any]],
    market_key: Optional[str] = None,
    market_keys: Optional[Sequence[str]] = None,
    player_name: str,
    over_under: str,
    line: Optional[float],
) -> List[Dict[str, Any]]:
    player_norm = _normalize_name(player_name)
    side_norm = (over_under or "over").strip().lower()
    keys: List[str] = []
    if market_keys:
        keys.extend(str(k) for k in market_keys if str(k or "").strip())
    if market_key:
        keys.append(str(market_key))
    allowed_market_keys = set(keys)
    if not allowed_market_keys:
        return []
    out: List[Dict[str, Any]] = []

    for ev in events:
        for book in ev.get("bookmakers") or []:
            bookmaker_key = book.get("key")
            bookmaker_title = book.get("title")
            for market in book.get("markets") or []:
                market_key_actual = str(market.get("key") or "").strip()
                if market_key_actual not in allowed_market_keys:
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
                            "market_key": market_key_actual,
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
    market_candidates = get_prop_market_candidates(prop_type=normalized_prop, include_aliases=True)
    if not market_candidates:
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
            "reason": f"{type(e).__name__}: {_sanitize_error_message(e)}",
            "market_key": market_candidates[0],
            "market_candidates": market_candidates,
        }

    snapshot = _snapshot_cache.get(game_date)
    snapshot_cached_at = (
        datetime.fromtimestamp(snapshot[0], tz=timezone.utc).isoformat()
        if snapshot
        else None
    )
    snapshot_age_seconds = (
        max(0, int(time.time() - snapshot[0]))
        if snapshot
        else None
    )

    candidates = _extract_candidate_outcomes(
        events=events,
        market_keys=market_candidates,
        player_name=player_name,
        over_under=over_under,
        line=line,
    )
    if not candidates:
        return {
            "ok": True,
            "found": False,
            "reason": "no matching market outcome",
            "market_key": market_candidates[0],
            "market_candidates": market_candidates,
            "events_considered": len(events),
            "snapshot_cached_at": snapshot_cached_at,
            "snapshot_age_seconds": snapshot_age_seconds,
        }

    best = candidates[0]
    return {
        "ok": True,
        "found": True,
        "market_key": best.get("market_key") or market_candidates[0],
        "market_candidates": market_candidates,
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
        "snapshot_cached_at": snapshot_cached_at,
        "snapshot_age_seconds": snapshot_age_seconds,
    }

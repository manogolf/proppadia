#!/usr/bin/env python3
"""8rain Station model-upload formatting and validation helpers."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


UPLOAD_COLUMNS = [
    "LEAGUE",
    "DATE",
    "HOME",
    "AWAY",
    "DOUBLEHEADER",
    "SECTION",
    "MARKET",
    "SELECTOR",
    "POINT",
    "SIDE",
    "WIN %",
]

CATALOG_CACHE_DIR = Path("backend/mlb/exports/model_v2/catalog")
CATALOG_CACHE_JSON = CATALOG_CACHE_DIR / "8rain_mlb_catalog.json"
PUBLIC_CATALOG_BASE_URL = "https://app.8rainstation.com"
PUBLIC_CATALOG_HOST = "app.8rainstation.com"
PUBLIC_CATALOG_PATH_PREFIX = "/public/api/catalog/"
PUBLIC_CATALOG_FETCH_ENV = "MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH"
PUBLIC_CATALOG_TIMEOUT_SEC = 12.0
PUBLIC_CATALOG_ENDPOINTS = {
    "model_spec": "/public/api/catalog/model-spec",
    "teams": "/public/api/catalog/teams",
    "players": "/public/api/catalog/players",
    "events": "/public/api/catalog/events",
    "stats": "/public/api/catalog/stats",
    "market_definitions": "/public/api/catalog/market-definitions",
}

TEAM_ABBR_ALIASES = {
    "ARI": "AZ",
    "AZ": "AZ",
    "ATH": "ATH",
    "OAK": "ATH",
    "CHW": "CWS",
    "CWS": "CWS",
    "KCR": "KC",
    "KC": "KC",
    "SDP": "SD",
    "SD": "SD",
    "SFG": "SF",
    "SF": "SF",
    "TBR": "TB",
    "TB": "TB",
    "WAS": "WSH",
    "WSH": "WSH",
}

TEAM_CODE_BY_ABBR = {
    "ARI": "az-arizona-diamondbacks",
    "AZ": "az-arizona-diamondbacks",
    "ATH": "ath-athletics",
    "OAK": "ath-athletics",
    "ATL": "atl-atlanta-braves",
    "BAL": "bal-baltimore-orioles",
    "BOS": "bos-boston-red-sox",
    "CHC": "chc-chicago-cubs",
    "CWS": "cws-chicago-white-sox",
    "CHW": "cws-chicago-white-sox",
    "CIN": "cin-cincinnati-reds",
    "CLE": "cle-cleveland-guardians",
    "COL": "col-colorado-rockies",
    "DET": "det-detroit-tigers",
    "HOU": "hou-houston-astros",
    "KC": "kc-kansas-city-royals",
    "KCR": "kc-kansas-city-royals",
    "LAA": "laa-los-angeles-angels",
    "LAD": "lad-los-angeles-dodgers",
    "MIA": "mia-miami-marlins",
    "MIL": "mil-milwaukee-brewers",
    "MIN": "min-minnesota-twins",
    "NYM": "nym-new-york-mets",
    "NYY": "nyy-new-york-yankees",
    "PHI": "phi-philadelphia-phillies",
    "PIT": "pit-pittsburgh-pirates",
    "SD": "sd-san-diego-padres",
    "SDP": "sd-san-diego-padres",
    "SF": "sf-san-francisco-giants",
    "SFG": "sf-san-francisco-giants",
    "SEA": "sea-seattle-mariners",
    "STL": "stl-st-louis-cardinals",
    "TB": "tb-tampa-bay-rays",
    "TBR": "tb-tampa-bay-rays",
    "TEX": "tex-texas-rangers",
    "TOR": "tor-toronto-blue-jays",
    "WSH": "wsh-washington-nationals",
    "WAS": "wsh-washington-nationals",
}

PUBLIC_SPEC_PLAYER_PROP_MARKETS = {
    "batter_hits",
    "batter_runs",
    "batter_rbis",
    "batter_bases",
    "batter_h+r+rbi",
    "batter_walks",
    "batter_strikeouts",
    "batter_stolen_bases",
    "batter_singles",
    "batter_doubles",
    "batter_triples",
    "batter_home_runs",
    "pitcher_hits",
    "pitcher_earned_runs",
    "pitcher_outs",
    "pitcher_walks",
    "pitcher_strikeouts",
    "pitcher_win",
}


def upload_run_tag() -> str:
    return (
        os.getenv("MLB_UPLOAD_RUN_TAG")
        or os.getenv("MLB_RUN_TAG")
        or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )


def generated_at_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def with_artifact_status(
    diagnostics: dict[str, Any],
    *,
    status: str,
    failure_stage: str = "",
    run_tag: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    out = dict(diagnostics)
    out["status"] = status
    out["failure_stage"] = failure_stage
    out["generated_at"] = generated_at or generated_at_utc()
    out["run_tag"] = run_tag or upload_run_tag()
    return out


def _clean(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _norm_code(value: Any) -> str:
    text = _clean(value).lower().replace("_", "-")
    text = re.sub(r"[^a-z0-9+.-]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def _team_abbr(value: Any) -> str:
    raw = _clean(value).upper()
    if not raw:
        return ""
    if "-" in raw:
        raw = raw.split("-", 1)[0]
    return TEAM_ABBR_ALIASES.get(raw, raw)


def _norm_market(value: Any) -> str:
    text = _clean(value).lower().replace(" ", "_").replace("-", "_")
    return re.sub(r"[^a-z0-9_+]+", "", text)


def _id_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    try:
        return str(int(float(value)))
    except Exception:
        return _clean(value)


def _line_key(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return ""
    text = f"{float(val):.3f}".rstrip("0").rstrip(".")
    return text or "0"


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _known_unresolved_player_ids(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    df = _read_csv_if_exists(path)
    if df.empty:
        return set()
    for col in ("player_id", "SELECTOR", "selector", "mlbam_id", "mlb_id"):
        if col in df.columns:
            return {pid for pid in df[col].map(_id_text).tolist() if pid}
    return set()


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "rows", "items", "teams", "players", "stats", "markets"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def public_catalog_fetch_allowed(value: bool | None = None) -> bool:
    if value is not None:
        return bool(value)
    return _truthy(os.getenv(PUBLIC_CATALOG_FETCH_ENV))


def _public_catalog_fetch_metadata(
    *,
    allowed: bool,
    attempted: bool = False,
    succeeded: bool = False,
    endpoint_used: str = "",
    error: str = "",
) -> dict[str, Any]:
    parsed = urlparse(endpoint_used) if endpoint_used else None
    return {
        "public_catalog_fetch_allowed": bool(allowed),
        "public_catalog_fetch_attempted": bool(attempted),
        "public_catalog_fetch_succeeded": bool(succeeded),
        "public_catalog_endpoint_used": endpoint_used,
        "public_catalog_endpoint_path": parsed.path if parsed else "",
        "public_catalog_fetch_error": error,
        "cache_only_mode": not bool(allowed),
    }


def _public_catalog_url(endpoint: str, params: dict[str, Any] | None = None) -> str:
    path = endpoint.strip()
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    elif not path.startswith("/"):
        path = PUBLIC_CATALOG_ENDPOINTS.get(path, path)
        url = PUBLIC_CATALOG_BASE_URL.rstrip("/") + path
    else:
        url = PUBLIC_CATALOG_BASE_URL.rstrip("/") + path
    if params:
        clean_params = {str(k): str(v) for k, v in params.items() if v is not None and str(v) != ""}
        if clean_params:
            url = f"{url}?{urlencode(clean_params)}"
    _validate_public_catalog_url(url)
    return url


def _validate_public_catalog_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError("8rain public catalog fetch requires https")
    if parsed.hostname != PUBLIC_CATALOG_HOST:
        raise ValueError("8rain public catalog fetch host is not allowlisted")
    if not parsed.path.startswith(PUBLIC_CATALOG_PATH_PREFIX):
        raise ValueError("8rain public catalog fetch path is not allowlisted")


def fetch_public_8rain_catalog(
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = PUBLIC_CATALOG_TIMEOUT_SEC,
    allow_public_catalog_fetch: bool | None = None,
) -> tuple[Any | None, dict[str, Any]]:
    """Fetch a documented public 8rain catalog endpoint when explicitly enabled."""
    allowed = public_catalog_fetch_allowed(allow_public_catalog_fetch)
    url = _public_catalog_url(endpoint, params)
    if not allowed:
        return None, _public_catalog_fetch_metadata(
            allowed=False,
            attempted=False,
            succeeded=False,
            endpoint_used=url,
            error="public_catalog_fetch_not_allowed",
        )

    try:
        # Deliberately no Authorization, Cookie, session, or user-token headers.
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - allowlisted public catalog URL.
            raw = response.read().decode("utf-8")
        return json.loads(raw), _public_catalog_fetch_metadata(
            allowed=True,
            attempted=True,
            succeeded=True,
            endpoint_used=url,
        )
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        return None, _public_catalog_fetch_metadata(
            allowed=True,
            attempted=True,
            succeeded=False,
            endpoint_used=url,
            error=f"{type(exc).__name__}: {exc}",
        )


def refresh_catalog_cache(
    *,
    base_url: str = "",
    cache_json: Path = CATALOG_CACHE_JSON,
    allow_public_catalog_fetch: bool | None = None,
) -> dict[str, Any]:
    allowed = public_catalog_fetch_allowed(allow_public_catalog_fetch)
    if base_url and base_url.rstrip("/") != PUBLIC_CATALOG_BASE_URL:
        raise ValueError("Only documented 8rain public catalog base URL is supported")
    catalog = load_catalog(cache_json)
    responses = dict(catalog.get("responses") or {}) if isinstance(catalog, dict) else {}
    errors = dict(catalog.get("errors") or {}) if isinstance(catalog, dict) else {}
    fetch_metadata = _public_catalog_fetch_metadata(allowed=allowed)
    for key in ("model_spec", "teams", "players", "stats", "market_definitions"):
        payload, metadata = fetch_public_8rain_catalog(
            PUBLIC_CATALOG_ENDPOINTS[key],
            params={"league": "mlb"},
            allow_public_catalog_fetch=allowed,
        )
        fetch_metadata = metadata
        if not metadata.get("public_catalog_fetch_attempted"):
            break
        if metadata.get("public_catalog_fetch_succeeded"):
            responses[key] = payload
            errors.pop(key, None)
        else:
            errors[key] = metadata.get("public_catalog_fetch_error", "public_catalog_fetch_failed")
    out = {
        "generated_at": generated_at_utc(),
        "source": "8rain_public_catalog" if fetch_metadata.get("public_catalog_fetch_succeeded") else "cache",
        "responses": responses,
        "errors": errors,
        "public_catalog_fetch": fetch_metadata,
    }
    if fetch_metadata.get("public_catalog_fetch_attempted") and fetch_metadata.get("public_catalog_fetch_succeeded"):
        cache_json.parent.mkdir(parents=True, exist_ok=True)
        cache_json.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out


def refresh_event_catalog(
    *,
    from_date: str,
    to_date: str,
    base_url: str = "",
    cache_json: Path = CATALOG_CACHE_JSON,
    allow_public_catalog_fetch: bool | None = None,
) -> dict[str, Any]:
    allowed = public_catalog_fetch_allowed(allow_public_catalog_fetch)
    if base_url and base_url.rstrip("/") != PUBLIC_CATALOG_BASE_URL:
        raise ValueError("Only documented 8rain public catalog base URL is supported")
    catalog = load_catalog(cache_json)
    responses = dict(catalog.get("responses") or {}) if isinstance(catalog, dict) else {}
    errors = dict(catalog.get("errors") or {}) if isinstance(catalog, dict) else {}
    payload, metadata = fetch_public_8rain_catalog(
        PUBLIC_CATALOG_ENDPOINTS["events"],
        params={"league": "mlb", "from": from_date, "to": to_date},
        allow_public_catalog_fetch=allowed,
    )
    fetched_events = _extract_records(payload)
    metadata["raw_fetched_event_count"] = int(len(fetched_events))
    metadata["raw_fetched_event_dates"] = sorted(
        {date for date in (_date_key(row.get("date")) for row in fetched_events) if date}
    )
    if metadata.get("public_catalog_fetch_succeeded"):
        responses["events"] = payload
        errors.pop("events", None)
    elif metadata.get("public_catalog_fetch_attempted"):
        errors["events"] = metadata.get("public_catalog_fetch_error", "public_catalog_fetch_failed")
    current_events_payload = payload if metadata.get("public_catalog_fetch_succeeded") else responses.get("events")
    metadata["written_cache_event_count"] = int(len(_extract_records(current_events_payload)))
    metadata["written_cache_event_dates"] = sorted(
        {date for date in (_date_key(row.get("date")) for row in _extract_records(current_events_payload)) if date}
    )
    out = {
        "generated_at": generated_at_utc(),
        "source": "8rain_public_catalog" if metadata.get("public_catalog_fetch_succeeded") else "cache",
        "responses": responses,
        "errors": errors,
        "public_catalog_fetch": metadata,
    }
    if metadata.get("public_catalog_fetch_attempted") and metadata.get("public_catalog_fetch_succeeded"):
        cache_json.parent.mkdir(parents=True, exist_ok=True)
        cache_json.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out


def load_catalog(cache_json: Path = CATALOG_CACHE_JSON) -> dict[str, Any]:
    if not cache_json.exists():
        return {}
    try:
        return json.loads(cache_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _catalog_records(catalog: dict[str, Any], key: str) -> list[dict[str, Any]]:
    responses = catalog.get("responses") if isinstance(catalog, dict) else {}
    if not isinstance(responses, dict):
        return []
    return _extract_records(responses.get(key))


def _event_date_window(dates: list[str]) -> tuple[str, str]:
    parsed = sorted({_date_key(date) for date in dates if _date_key(date)})
    if not parsed:
        today = datetime.now(timezone.utc).date().isoformat()
        return today, today
    start = parsed[0]
    # 8rain event catalog date can roll to the next UTC date for late local MLB games.
    end = (pd.Timestamp(parsed[-1]) + pd.Timedelta(days=1)).date().isoformat()
    return start, end


def _event_dates(catalog: dict[str, Any] | None) -> list[str]:
    return sorted(
        {
            date
            for date in (_date_key(row.get("date")) for row in _catalog_records(catalog or {}, "events"))
            if date
        }
    )


def _requested_event_dates(dates: list[str]) -> list[str]:
    return sorted({_date_key(date) for date in dates if _date_key(date)})


def _event_catalog_covers_requested_dates(requested_dates: list[str], cached_dates: list[str]) -> bool:
    cached = set(cached_dates)
    if not requested_dates:
        return False
    for date in requested_dates:
        next_date = (pd.Timestamp(date) + pd.Timedelta(days=1)).date().isoformat()
        if date not in cached and next_date not in cached:
            return False
    return True


def _allowed_event_dates(requested_dates: list[str]) -> list[str]:
    allowed: set[str] = set()
    for date in requested_dates:
        if not date:
            continue
        allowed.add(date)
        allowed.add((pd.Timestamp(date) + pd.Timedelta(days=1)).date().isoformat())
    return sorted(allowed)


def ensure_event_catalog(
    catalog: dict[str, Any] | None,
    dates: list[str],
    *,
    force_refresh: bool = False,
    refresh_reason: str = "",
    allow_public_catalog_fetch: bool | None = None,
) -> tuple[dict[str, Any], bool, dict[str, Any]]:
    out = dict(catalog or {})
    fetch_allowed = public_catalog_fetch_allowed(allow_public_catalog_fetch)
    requested_dates = _requested_event_dates(dates)
    allowed_dates = _allowed_event_dates(requested_dates)
    cached_dates = _event_dates(out)
    cache_covers_requested = _event_catalog_covers_requested_dates(requested_dates, cached_dates)
    catalog_events_loaded = len(_catalog_records(out, "events"))
    catalog_refetched = False
    fetch_metadata = _public_catalog_fetch_metadata(allowed=fetch_allowed)
    catalog_unavailable = False

    needs_refresh = bool(force_refresh or fetch_allowed or catalog_events_loaded == 0 or not cache_covers_requested)
    if needs_refresh:
        if not cache_covers_requested and "responses" in out:
            # Cache-only mode must not silently match against stale events from a different slate date.
            out.setdefault("responses", {})["events"] = {"data": []}
            catalog_events_loaded = 0
            cached_dates = []
            cache_covers_requested = False
        catalog_unavailable = catalog_events_loaded == 0
        if not refresh_reason:
            if fetch_allowed and not force_refresh:
                refresh_reason = "public_catalog_fetch_explicitly_allowed"
            elif force_refresh:
                refresh_reason = "force_refresh_requested"
            elif catalog_events_loaded == 0:
                refresh_reason = "catalog_events_loaded_zero"
            else:
                refresh_reason = "catalog_dates_missing_allowed_window"
        if fetch_allowed:
            from_date, to_date = _event_date_window(requested_dates)
            refreshed = refresh_event_catalog(
                from_date=from_date,
                to_date=to_date,
                allow_public_catalog_fetch=True,
            )
            fetch_metadata = dict(refreshed.get("public_catalog_fetch") or fetch_metadata)
            if fetch_metadata.get("public_catalog_fetch_succeeded"):
                out = refreshed
                catalog_refetched = True
                cached_dates = _event_dates(out)
                cache_covers_requested = _event_catalog_covers_requested_dates(requested_dates, cached_dates)
                catalog_events_loaded = len(_catalog_records(out, "events"))
                catalog_unavailable = catalog_events_loaded == 0
            else:
                out.setdefault("errors", {})["events"] = fetch_metadata.get(
                    "public_catalog_fetch_error",
                    "public_catalog_fetch_failed",
                )

    metadata = {
        "requested_event_date_from": requested_dates[0] if requested_dates else "",
        "requested_event_date_to": requested_dates[-1] if requested_dates else "",
        "requested_slate_date": requested_dates[0] if len(requested_dates) == 1 else requested_dates,
        "allowed_catalog_dates": allowed_dates,
        "cached_event_dates": cached_dates,
        "catalog_dates_present": cached_dates,
        "cache_covers_requested_dates": bool(cache_covers_requested),
        "catalog_refetched": bool(catalog_refetched),
        "refresh_attempted": bool(fetch_metadata.get("public_catalog_fetch_attempted")),
        "refresh_succeeded": bool(catalog_refetched and not bool(out.get("errors", {}).get("events")) and catalog_events_loaded > 0),
        "public_catalog_fetch_allowed": bool(fetch_allowed),
        "public_catalog_fetch_attempted": bool(fetch_metadata.get("public_catalog_fetch_attempted")),
        "public_catalog_fetch_succeeded": bool(fetch_metadata.get("public_catalog_fetch_succeeded")),
        "public_catalog_endpoint_used": fetch_metadata.get("public_catalog_endpoint_used", ""),
        "public_catalog_endpoint_path": fetch_metadata.get("public_catalog_endpoint_path", ""),
        "public_catalog_fetch_error": fetch_metadata.get("public_catalog_fetch_error", ""),
        "raw_fetched_event_count": int(fetch_metadata.get("raw_fetched_event_count") or 0),
        "raw_fetched_event_dates": fetch_metadata.get("raw_fetched_event_dates", []),
        "written_cache_event_count": int(fetch_metadata.get("written_cache_event_count") or 0),
        "written_cache_event_dates": fetch_metadata.get("written_cache_event_dates", []),
        "refresh_reason": refresh_reason,
        "catalog_refresh_reason": refresh_reason,
        "cache_only_mode": not bool(fetch_allowed),
        "catalog_cache": str(CATALOG_CACHE_JSON),
        "catalog_cache_source": str(CATALOG_CACHE_JSON),
    }
    return out, bool(catalog_unavailable), metadata


def build_event_map(catalog: dict[str, Any] | None = None) -> dict[tuple[str, str], list[dict[str, Any]]]:
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in _catalog_records(catalog or {}, "events"):
        code = _clean(row.get("code")).lower()
        parts = code.split("-")
        away_abbr = ""
        home_abbr = ""
        if len(parts) >= 6 and parts[0] == "mlb":
            away_abbr = _team_abbr(parts[-2])
            home_abbr = _team_abbr(parts[-1])
        if not away_abbr:
            away_abbr = _team_abbr(row.get("away"))
        if not home_abbr:
            home_abbr = _team_abbr(row.get("home"))
        home = _norm_code(row.get("home"))
        away = _norm_code(row.get("away"))
        if home_abbr and away_abbr and home and away:
            out.setdefault((away_abbr, home_abbr), []).append(
                {
                    "event_code": _clean(row.get("code")),
                    "catalog_date": _clean(row.get("date")),
                    "home": home,
                    "away": away,
                    "home_abbr": home_abbr,
                    "away_abbr": away_abbr,
                }
            )
    for rows in out.values():
        rows.sort(key=lambda row: str(row.get("catalog_date") or ""))
    return out


def build_event_slug_map(catalog: dict[str, Any] | None = None) -> dict[tuple[str, str], list[dict[str, Any]]]:
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in _catalog_records(catalog or {}, "events"):
        home = _norm_code(row.get("home"))
        away = _norm_code(row.get("away"))
        if not home or not away:
            continue
        code = _clean(row.get("code")).lower()
        parts = code.split("-")
        away_abbr = ""
        home_abbr = ""
        if len(parts) >= 6 and parts[0] == "mlb":
            away_abbr = _team_abbr(parts[-2])
            home_abbr = _team_abbr(parts[-1])
        if not away_abbr:
            away_abbr = _team_abbr(row.get("away"))
        if not home_abbr:
            home_abbr = _team_abbr(row.get("home"))
        out.setdefault((away, home), []).append(
            {
                "event_code": _clean(row.get("code")),
                "catalog_date": _clean(row.get("date")),
                "home": home,
                "away": away,
                "home_abbr": home_abbr,
                "away_abbr": away_abbr,
            }
        )
    for rows in out.values():
        rows.sort(key=lambda row: str(row.get("catalog_date") or ""))
    return out


def _select_event_from_rows(rows: list[dict[str, Any]], source_date: str) -> dict[str, Any]:
    if not rows:
        return {}
    for row in rows:
        if row.get("catalog_date") == source_date:
            return row
    next_date = (pd.Timestamp(source_date) + pd.Timedelta(days=1)).date().isoformat() if source_date else ""
    for row in rows:
        if row.get("catalog_date") == next_date:
            return row
    return {}


def select_event(
    event_map: dict[tuple[str, str], list[dict[str, Any]]],
    *,
    away_abbr: str,
    home_abbr: str,
    source_date: str,
    slug_event_map: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    away_slug: str = "",
    home_slug: str = "",
) -> dict[str, Any]:
    rows = event_map.get((away_abbr, home_abbr), [])
    if rows:
        return _select_event_from_rows(rows, source_date)
    if slug_event_map is not None and away_slug and home_slug:
        return _select_event_from_rows(slug_event_map.get((away_slug, home_slug), []), source_date)
    return {}


def build_team_map(catalog: dict[str, Any] | None = None) -> dict[str, str]:
    mapping = dict(TEAM_CODE_BY_ABBR)
    for row in _catalog_records(catalog or {}, "teams"):
        code = _norm_code(row.get("code") or row.get("slug") or row.get("id"))
        if not code:
            continue
        for key in ("abbr", "abbreviation", "team", "team_code", "shortCode", "short_code"):
            raw = _clean(row.get(key)).upper()
            if raw:
                mapping[raw] = code
    return mapping


def build_player_map(catalog: dict[str, Any] | None = None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in _catalog_records(catalog or {}, "players"):
        code = _norm_code(row.get("code") or row.get("slug") or row.get("id"))
        if not code:
            continue
        for key in ("mlbid", "mlb_id", "player_id", "id"):
            raw = _id_text(row.get(key))
            if raw:
                mapping[raw] = code
    return mapping


def valid_player_prop_markets(catalog: dict[str, Any] | None = None) -> set[str]:
    markets = set(PUBLIC_SPEC_PLAYER_PROP_MARKETS)
    for row in _catalog_records(catalog or {}, "model_spec"):
        section = _clean(row.get("section") or row.get("SECTION")).lower()
        market = _norm_market(row.get("market") or row.get("MARKET") or row.get("code"))
        if section == "player_prop" and market:
            markets.add(market)
    return markets


def normalize_team_code(value: Any, team_map: dict[str, str]) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    return team_map.get(raw.upper()) or _norm_code(raw)


def normalize_player_selector(value: Any, player_map: dict[str, str]) -> tuple[str, bool]:
    raw = _id_text(value)
    if not raw:
        return "", False
    mapped = player_map.get(raw)
    return (mapped, True) if mapped else (raw, False)


def _base_pair_key_cols(df: pd.DataFrame) -> list[str]:
    return ["LEAGUE", "DATE", "HOME", "AWAY", "DOUBLEHEADER", "SECTION", "MARKET", "SELECTOR", "POINT"]


def pair_over_under_rows(upload: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    work = upload.copy()
    work["WIN %"] = pd.to_numeric(work["WIN %"], errors="coerce")
    work["SIDE"] = work["SIDE"].map(lambda v: _clean(v).lower())
    player_ou = work["SECTION"].eq("player_prop") & work["SIDE"].isin(["over", "under"])
    passthrough = work[~player_ou].copy()
    paired_source = work[player_ou & work["WIN %"].between(0.0, 1.0, inclusive="both")].copy()
    if paired_source.empty:
        return pd.concat([passthrough, paired_source], ignore_index=True, sort=False)[UPLOAD_COLUMNS], int(player_ou.sum())

    paired_source["p_over"] = np.where(
        paired_source["SIDE"].eq("over"),
        paired_source["WIN %"],
        1.0 - paired_source["WIN %"],
    )
    key_cols = _base_pair_key_cols(paired_source)
    bases = paired_source.groupby(key_cols, dropna=False, as_index=False).agg(p_over=("p_over", "mean"))
    over = bases.copy()
    over["SIDE"] = "over"
    over["WIN %"] = over["p_over"].round(6)
    under = bases.copy()
    under["SIDE"] = "under"
    under["WIN %"] = (1.0 - under["p_over"]).round(6)
    out = pd.concat([passthrough, over, under], ignore_index=True, sort=False)
    out = out[UPLOAD_COLUMNS].sort_values(key_cols + ["SIDE"], kind="stable").reset_index(drop=True)
    return out, 0


def validate_upload(upload: pd.DataFrame, *, required_pairs: bool = True) -> dict[str, Any]:
    work = upload.copy()
    missing_required = {
        col: int(work[col].isna().sum() + work[col].astype(str).str.strip().eq("").sum())
        for col in UPLOAD_COLUMNS
        if col in work.columns and col not in {"SELECTOR"}
    }
    win = pd.to_numeric(work["WIN %"], errors="coerce")
    invalid_win = win.isna() | ~win.between(0.0, 1.0, inclusive="both")
    player_rows = work["SECTION"].eq("player_prop")
    missing_selector = player_rows & work["SELECTOR"].astype(str).str.strip().eq("")

    unpaired = 0
    bad_pair_sum = 0
    if required_pairs:
        grouped = work[player_rows & work["SIDE"].isin(["over", "under"])].groupby(_base_pair_key_cols(work), dropna=False)
        for _, group in grouped:
            sides = set(group["SIDE"].astype(str).str.lower())
            if sides != {"over", "under"}:
                unpaired += len(group)
                continue
            total = pd.to_numeric(group["WIN %"], errors="coerce").sum()
            if abs(float(total) - 1.0) > 0.001:
                bad_pair_sum += len(group)

    return {
        "rows": int(len(work)),
        "league_not_lowercase_mlb": int((~work["LEAGUE"].eq("mlb")).sum()) if "LEAGUE" in work else 0,
        "invalid_date_rows": int(work["DATE"].map(_date_key).eq("").sum()) if "DATE" in work else 0,
        "missing_required": missing_required,
        "missing_selector_rows": int(missing_selector.sum()),
        "invalid_win_pct_rows": int(invalid_win.sum()),
        "unpaired_market_rows": int(unpaired),
        "bad_pair_probability_sum_rows": int(bad_pair_sum),
    }


def prepare_player_prop_upload(
    upload: pd.DataFrame,
    *,
    catalog: dict[str, Any] | None = None,
    source_rows_before: int | None = None,
    allow_public_catalog_fetch: bool | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = upload.copy()
    initial_dates = work["DATE"].map(_date_key).dropna().astype(str).tolist() if "DATE" in work else []
    catalog, catalog_unavailable, catalog_metadata = ensure_event_catalog(
        catalog,
        initial_dates,
        allow_public_catalog_fetch=allow_public_catalog_fetch,
    )

    rows_before = int(source_rows_before if source_rows_before is not None else len(work))
    work["LEAGUE"] = "mlb"
    work["DATE"] = work["DATE"].map(_date_key)
    work["DOUBLEHEADER"] = pd.to_numeric(work["DOUBLEHEADER"], errors="coerce").fillna(0).astype(int)
    work["SECTION"] = work["SECTION"].map(lambda v: _clean(v).lower() or "player_prop")
    work["MARKET"] = work["MARKET"].map(_norm_market)
    work["home_upload_input"] = work["HOME"].map(_clean)
    work["away_upload_input"] = work["AWAY"].map(_clean)
    home_source = work["HOME_SOURCE"] if "HOME_SOURCE" in work.columns else work["HOME"]
    away_source = work["AWAY_SOURCE"] if "AWAY_SOURCE" in work.columns else work["AWAY"]
    work["home_source_abbr"] = home_source.map(_team_abbr)
    work["away_source_abbr"] = away_source.map(_team_abbr)
    work["POINT"] = work["POINT"].map(_line_key)
    work["SIDE"] = work["SIDE"].map(lambda v: _clean(v).lower())
    work["WIN %"] = pd.to_numeric(work["WIN %"], errors="coerce").round(6)

    def apply_catalog(current_catalog: dict[str, Any], current_work: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        mapped = current_work.copy()
        current_team_map = build_team_map(current_catalog)
        current_player_map = build_player_map(current_catalog)
        current_valid_markets = valid_player_prop_markets(current_catalog)
        current_event_map = build_event_map(current_catalog)
        current_slug_event_map = build_event_slug_map(current_catalog)
        mapped["home_upload_input_slug"] = mapped["home_upload_input"].map(lambda v: normalize_team_code(v, current_team_map))
        mapped["away_upload_input_slug"] = mapped["away_upload_input"].map(lambda v: normalize_team_code(v, current_team_map))
        event_matches_local = mapped.apply(
            lambda row: select_event(
                current_event_map,
                away_abbr=row["away_source_abbr"],
                home_abbr=row["home_source_abbr"],
                source_date=row["DATE"],
                slug_event_map=current_slug_event_map,
                away_slug=row["away_upload_input_slug"],
                home_slug=row["home_upload_input_slug"],
            ),
            axis=1,
        )
        mapped["event_code"] = event_matches_local.map(lambda row: row.get("event_code", ""))
        mapped["event_catalog_date"] = event_matches_local.map(lambda row: row.get("catalog_date", ""))
        mapped["home_source_code"] = event_matches_local.map(lambda row: row.get("home", ""))
        mapped["away_source_code"] = event_matches_local.map(lambda row: row.get("away", ""))
        mapped["HOME"] = mapped["home_source_code"]
        mapped["AWAY"] = mapped["away_source_code"]
        return mapped, {
            "team_map": current_team_map,
            "player_map": current_player_map,
            "valid_markets": current_valid_markets,
            "event_map": current_event_map,
            "slug_event_map": current_slug_event_map,
        }

    work, catalog_indexes = apply_catalog(catalog, work)
    refresh_reason = ""
    raw_event_count = len(_catalog_records(catalog, "events"))
    unknown_event_initial = work["event_code"].eq("")
    allowed_dates = set(catalog_metadata.get("allowed_catalog_dates", []))
    catalog_dates = set(catalog_metadata.get("catalog_dates_present", []))
    missing_allowed_date_coverage = bool(allowed_dates and catalog_dates.isdisjoint(allowed_dates))
    if (
        (raw_event_count == 0 or missing_allowed_date_coverage or (len(work) > 0 and bool(unknown_event_initial.all())))
        and not catalog_metadata.get("refresh_attempted")
    ):
        if raw_event_count == 0:
            refresh_reason = "catalog_events_loaded_zero"
        elif missing_allowed_date_coverage:
            refresh_reason = "catalog_dates_missing_allowed_window"
        else:
            refresh_reason = "all_requested_games_failed_initial_lookup"
        refreshed_catalog, refreshed_unavailable, refreshed_metadata = ensure_event_catalog(
            catalog,
            initial_dates,
            force_refresh=True,
            refresh_reason=refresh_reason,
            allow_public_catalog_fetch=allow_public_catalog_fetch,
        )
        catalog = refreshed_catalog
        catalog_unavailable = bool(refreshed_unavailable)
        catalog_metadata.update(refreshed_metadata)
        catalog_metadata["catalog_refresh_reason"] = refresh_reason
        catalog_metadata["refresh_reason"] = refresh_reason
        work, catalog_indexes = apply_catalog(catalog, work)

    team_map = catalog_indexes["team_map"]
    player_map = catalog_indexes["player_map"]
    valid_markets = catalog_indexes["valid_markets"]
    event_map = catalog_indexes["event_map"]
    slug_event_map = catalog_indexes["slug_event_map"]
    catalog_event_records = _catalog_records(catalog, "events")
    first_catalog_event_keys = [
        f"mlb/{row.get('catalog_date', '')}/{row.get('away_abbr', '')}@{row.get('home_abbr', '')}"
        for rows in event_map.values()
        for row in rows[:1]
    ][:10]

    selectors = work["SELECTOR"].map(lambda v: normalize_player_selector(v, player_map))
    work["SELECTOR"] = selectors.map(lambda pair: pair[0])
    used_catalog_player = selectors.map(lambda pair: pair[1])

    unknown_event = work["event_code"].eq("")
    valid_event_work = work[~unknown_event].copy()
    excluded_unknown_event_rows = work[unknown_event].copy()
    missing_team = valid_event_work["HOME"].eq("") | valid_event_work["AWAY"].eq("")
    reversed_home_away = (
        valid_event_work["home_source_code"].ne("")
        & valid_event_work["away_source_code"].ne("")
        & valid_event_work["HOME"].eq(valid_event_work["away_source_code"])
        & valid_event_work["AWAY"].eq(valid_event_work["home_source_code"])
    )
    missing_market = ~valid_event_work["MARKET"].isin(valid_markets)
    missing_selector = valid_event_work["SELECTOR"].eq("")
    invalid_win = valid_event_work["WIN %"].isna() | ~valid_event_work["WIN %"].between(0.0, 1.0, inclusive="both")

    paired, skipped_pairing = pair_over_under_rows(valid_event_work[UPLOAD_COLUMNS])
    validation = validate_upload(paired)
    event_key_home = work["home_source_code"].where(work["home_source_code"].ne(""), work["home_upload_input"])
    event_key_away = work["away_source_code"].where(work["away_source_code"].ne(""), work["away_upload_input"])
    requested_input_key = "mlb/" + work["DATE"].astype(str) + "/" + work["away_upload_input"].astype(str) + "@" + work["home_upload_input"].astype(str)
    requested_slug_key = "mlb/" + work["DATE"].astype(str) + "/" + work["away_upload_input_slug"].astype(str) + "@" + work["home_upload_input_slug"].astype(str)
    failed_games = (
        work.loc[unknown_event, ["DATE", "away_upload_input", "home_upload_input"]]
        .drop_duplicates()
        .assign(game=lambda df: df["away_upload_input"].astype(str) + "@" + df["home_upload_input"].astype(str))
        .sort_values(["DATE", "game"], kind="stable")
    )
    unknown_event_exclusion_rows = excluded_unknown_event_rows.assign(
        event_key_input_away_at_home=lambda df: (
            "mlb/" + df["DATE"].astype(str) + "/" + df["away_upload_input"].astype(str) + "@" + df["home_upload_input"].astype(str)
        ),
        requested_slug_key=lambda df: (
            "mlb/" + df["DATE"].astype(str) + "/" + df["away_upload_input_slug"].astype(str) + "@" + df["home_upload_input_slug"].astype(str)
        ),
        exclusion_reason="unknown_8rain_event",
    )[
        [
            "DATE",
            "event_key_input_away_at_home",
            "requested_slug_key",
            "home_upload_input",
            "away_upload_input",
            "home_upload_input_slug",
            "away_upload_input_slug",
            "home_source_abbr",
            "away_source_abbr",
            "SELECTOR",
            "MARKET",
            "POINT",
            "SIDE",
            "WIN %",
            "exclusion_reason",
        ]
    ]
    if len(paired) == 0:
        upload_status = "failed"
    elif int(unknown_event.sum()) > 0:
        upload_status = "partial_success"
    else:
        upload_status = "success"
    diagnostics = {
        "rows_before": rows_before,
        "rows_after_pairing": int(len(paired)),
        "missing_team_code": int(missing_team.sum()),
        "catalog_unavailable": bool(catalog_unavailable),
        **catalog_metadata,
        "catalog_events_loaded": int(len(catalog_event_records)),
        "usable_catalog_event_count": int(len(event_map)),
        "event_abbr_index_count": int(len(event_map)),
        "event_slug_index_count": int(len(slug_event_map)),
        "upload_rows_attempting_event_lookup": int(len(work)),
        "catalog_event_found_false_rows": int(unknown_event.sum()),
        "unknown_event_rows": int(unknown_event.sum()),
        "excluded_unknown_event_candidates": int(unknown_event.sum()),
        "excluded_unknown_event_games": failed_games["game"].tolist(),
        "valid_event_candidates": int(len(valid_event_work)),
        "valid_paired_upload_rows": int(len(paired)),
        "upload_status": upload_status,
        "unique_failed_games": failed_games["game"].tolist(),
        "unique_failed_game_count": int(failed_games["game"].nunique()),
        "catalog_coverage_dates": catalog_metadata.get("catalog_dates_present", []),
        "home_away_reversed_rows": int(reversed_home_away.sum()),
        "missing_player_code": int(missing_selector.sum()),
        "missing_player_catalog_code": int((~used_catalog_player).sum()),
        "missing_market_code": int(missing_market.sum()),
        "unpaired_market_rows": int(validation["unpaired_market_rows"] + skipped_pairing),
        "invalid_win_pct_rows": int(invalid_win.sum() + validation["invalid_win_pct_rows"]),
        "catalog_loaded": bool(catalog),
        "team_catalog_codes_loaded": int(len(team_map)),
        "player_catalog_codes_loaded": int(len(player_map)),
        "market_codes_loaded": int(len(valid_markets)),
        "first_5_catalog_event_keys": first_catalog_event_keys[:5],
        "first_10_catalog_event_keys": first_catalog_event_keys,
        "catalog_lookup_failures": work.loc[unknown_event].assign(
            requested_input_key=requested_input_key[unknown_event],
            requested_slug_key=requested_slug_key[unknown_event],
        )[
            [
                "DATE",
                "requested_input_key",
                "requested_slug_key",
                "home_upload_input",
                "away_upload_input",
                "home_upload_input_slug",
                "away_upload_input_slug",
                "home_source_abbr",
                "away_source_abbr",
            ]
        ]
        .drop_duplicates()
        .head(25)
        .to_dict(orient="records"),
        "unknown_event_exclusion_rows": unknown_event_exclusion_rows.to_dict(orient="records"),
        "validation": validation,
        "sample_output_rows": paired.head(10).to_dict(orient="records"),
        "event_diagnostics_rows": work.assign(
            event_key_expected_away_at_home=lambda df: (
                "mlb/" + df["DATE"].astype(str) + "/" + event_key_away.astype(str) + "@" + event_key_home.astype(str)
            ),
            event_key_catalog_away_at_home=lambda df: (
                "mlb/" + df["DATE"].astype(str) + "/" + df["away_source_code"].astype(str) + "@" + df["home_source_code"].astype(str)
            ),
            event_key_input_away_at_home=lambda df: (
                "mlb/" + df["DATE"].astype(str) + "/" + df["away_upload_input"].astype(str) + "@" + df["home_upload_input"].astype(str)
            ),
            home_upload=work["HOME"],
            away_upload=work["AWAY"],
            home_upload_input=work["home_upload_input"],
            away_upload_input=work["away_upload_input"],
            home_upload_input_slug=work["home_upload_input_slug"],
            away_upload_input_slug=work["away_upload_input_slug"],
            requested_input_key=requested_input_key,
            requested_slug_key=requested_slug_key,
            home_source=work["home_source_code"],
            away_source=work["away_source_code"],
            source_home_abbr=work["home_source_abbr"],
            source_away_abbr=work["away_source_abbr"],
            event_code=work["event_code"],
            event_catalog_date=work["event_catalog_date"],
            catalog_event_found=~unknown_event,
            home_away_reversed_flag=reversed_home_away,
        )[
            [
                "event_key_expected_away_at_home",
                "event_key_catalog_away_at_home",
                "event_key_input_away_at_home",
                "event_code",
                "event_catalog_date",
                "catalog_event_found",
                "home_upload",
                "away_upload",
                "home_upload_input",
                "away_upload_input",
                "home_upload_input_slug",
                "away_upload_input_slug",
                "requested_input_key",
                "requested_slug_key",
                "home_source",
                "away_source",
                "source_home_abbr",
                "source_away_abbr",
                "home_away_reversed_flag",
                "SELECTOR",
                "MARKET",
                "POINT",
                "SIDE",
                "WIN %",
            ]
        ]
        .drop_duplicates()
        .to_dict(orient="records"),
    }

    hard_fail = (
        diagnostics["catalog_unavailable"]
        or diagnostics["valid_event_candidates"] == 0
        or diagnostics["valid_paired_upload_rows"] == 0
        or diagnostics["missing_team_code"]
        or diagnostics["home_away_reversed_rows"]
        or diagnostics["missing_player_code"]
        or diagnostics["missing_market_code"]
        or diagnostics["unpaired_market_rows"]
        or diagnostics["invalid_win_pct_rows"]
        or validation["missing_selector_rows"]
        or validation["bad_pair_probability_sum_rows"]
    )
    if hard_fail:
        raise ValueError(json.dumps(diagnostics, indent=2))
    return paired, diagnostics


def write_unknown_event_exclusions(diagnostics: dict[str, Any], diagnostics_csv: Path) -> Path:
    exclusions_csv = diagnostics_csv.with_name(f"{diagnostics_csv.stem}_unknown_event_exclusions.csv")
    columns = [
        "DATE",
        "event_key_input_away_at_home",
        "requested_slug_key",
        "home_upload_input",
        "away_upload_input",
        "home_upload_input_slug",
        "away_upload_input_slug",
        "home_source_abbr",
        "away_source_abbr",
        "SELECTOR",
        "MARKET",
        "POINT",
        "SIDE",
        "WIN %",
        "exclusion_reason",
    ]
    rows = pd.DataFrame(diagnostics.get("unknown_event_exclusion_rows", []), columns=columns)
    exclusions_csv.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(exclusions_csv, index=False)
    diagnostics["unknown_event_exclusions_csv"] = str(exclusions_csv)
    return exclusions_csv


def write_prepare_failure_diagnostics(exc: Exception, diagnostics_csv: Path) -> dict[str, Any]:
    """Persist upload-preparation diagnostics before the exporter exits."""
    text = str(exc)
    try:
        diagnostics = json.loads(text)
    except json.JSONDecodeError:
        diagnostics = {"error": text}
    run_tag = upload_run_tag()
    diagnostics = with_artifact_status(
        diagnostics,
        status="failed",
        failure_stage="prepare_player_prop_upload",
        run_tag=run_tag,
    )
    diagnostics_csv.parent.mkdir(parents=True, exist_ok=True)
    write_unknown_event_exclusions(diagnostics, diagnostics_csv)
    summary_json = diagnostics_csv.with_name(f"{diagnostics_csv.stem}_summary.json")
    event_diag = diagnostics_csv.with_name(f"{diagnostics_csv.stem}_event_diagnostics.csv")
    timestamped_summary_json = summary_json.with_name(f"{summary_json.stem}__{run_tag}{summary_json.suffix}")
    timestamped_event_diag = event_diag.with_name(f"{event_diag.stem}__{run_tag}{event_diag.suffix}")
    if diagnostics_csv.exists():
        timestamped_diagnostics_csv = diagnostics_csv.with_name(
            f"{diagnostics_csv.stem}__{run_tag}{diagnostics_csv.suffix}"
        )
        try:
            diagnostics_rows = pd.read_csv(diagnostics_csv)
            diagnostics_rows["artifact_status"] = "failed"
            diagnostics_rows["failure_stage"] = "prepare_player_prop_upload"
            diagnostics_rows["generated_at"] = diagnostics["generated_at"]
            diagnostics_rows["run_tag"] = run_tag
            diagnostics_rows.to_csv(diagnostics_csv, index=False)
            diagnostics_rows.to_csv(timestamped_diagnostics_csv, index=False)
        except Exception:
            failure_csv = (
                "artifact_status,failure_stage,generated_at,run_tag\n"
                f"failed,prepare_player_prop_upload,{diagnostics['generated_at']},{run_tag}\n"
            )
            diagnostics_csv.write_text(failure_csv, encoding="utf-8")
            timestamped_diagnostics_csv.write_text(failure_csv, encoding="utf-8")
    summary_json.write_text(json.dumps(diagnostics, indent=2) + "\n", encoding="utf-8")
    timestamped_summary_json.write_text(json.dumps(diagnostics, indent=2) + "\n", encoding="utf-8")
    event_rows = pd.DataFrame(diagnostics.get("event_diagnostics_rows", []))
    event_rows["artifact_status"] = "failed"
    event_rows["failure_stage"] = "prepare_player_prop_upload"
    event_rows["generated_at"] = diagnostics["generated_at"]
    event_rows["run_tag"] = run_tag
    event_rows.to_csv(event_diag, index=False)
    event_rows.to_csv(timestamped_event_diag, index=False)
    return diagnostics


def write_unresolved_player_candidates(
    *,
    source_rows: pd.DataFrame,
    source_name: str,
    out_csv: Path,
    player_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    work = source_rows.copy()
    if work.empty or "player_id" not in work.columns:
        return {"players_using_mlbam_selector": 0, "rows_likely_to_fail_selector_resolution": 0}
    player_map = player_map or {}
    work["player_id_key"] = work["player_id"].map(_id_text)
    work = work[work["player_id_key"].ne("")].copy()
    if work.empty:
        return {"players_using_mlbam_selector": 0, "rows_likely_to_fail_selector_resolution": 0}
    work["uses_mlbam_selector_fallback"] = ~work["player_id_key"].isin(player_map)
    work = work[work["uses_mlbam_selector_fallback"]].copy()
    if work.empty:
        return {"players_using_mlbam_selector": 0, "rows_likely_to_fail_selector_resolution": 0}

    home = work["home_team_code"] if "home_team_code" in work.columns else work.get("home_upload", "")
    away = work["away_team_code"] if "away_team_code" in work.columns else work.get("away_upload", "")
    work["game"] = away.map(_team_abbr).astype(str) + "@" + home.map(_team_abbr).astype(str)
    work["team"] = work["player_team"] if "player_team" in work.columns else ""
    work["source_lane"] = work["source_lane"] if "source_lane" in work.columns else source_name
    work["player_name"] = work["player_name"] if "player_name" in work.columns else work.get("player", "")

    grouped = (
        work.groupby(["player_id_key", "player_name", "team", "game", "source_lane"], dropna=False)
        .size()
        .reset_index(name="source_rows")
    )
    grouped["upload rows affected"] = grouped["source_rows"].astype(int) * 2
    grouped["reason"] = "possible_8rain_catalog_missing"
    grouped["source_upload"] = source_name
    grouped = grouped.rename(columns={"player_id_key": "player_id"})
    out_cols = [
        "player_id",
        "player_name",
        "team",
        "game",
        "source_lane",
        "upload rows affected",
        "reason",
        "source_upload",
    ]
    new_rows = grouped[out_cols]
    existing = _read_csv_if_exists(out_csv)
    if not existing.empty:
        combined = pd.concat([existing, new_rows], ignore_index=True, sort=False)
        dedupe = ["player_id", "player_name", "team", "game", "source_lane", "source_upload"]
        combined = combined.drop_duplicates([c for c in dedupe if c in combined.columns], keep="last")
    else:
        combined = new_rows
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_csv, index=False)
    return {
        "players_using_mlbam_selector": int(new_rows["player_id"].nunique()),
        "rows_likely_to_fail_selector_resolution": int(new_rows["upload rows affected"].sum()),
    }

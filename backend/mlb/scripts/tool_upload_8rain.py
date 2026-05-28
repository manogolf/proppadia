#!/usr/bin/env python3
"""8rain Station model-upload formatting and validation helpers."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def _fetch_json(url: str, timeout: float = 8.0) -> tuple[Any | None, str]:
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8")), ""
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return None, str(exc)


def refresh_catalog_cache(
    *,
    base_url: str = "https://app.8rainstation.com",
    cache_json: Path = CATALOG_CACHE_JSON,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    endpoints = {
        "model_spec": f"{base}/catalog/model-spec?league=mlb",
        "teams": f"{base}/catalog/teams?league=mlb",
        "players": f"{base}/catalog/players?league=mlb",
    }
    payload: dict[str, Any] = {
        "league": "mlb",
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_url": base,
        "endpoints": endpoints,
        "responses": {},
        "errors": {},
    }
    for key, url in endpoints.items():
        data, err = _fetch_json(url)
        if err:
            payload["errors"][key] = err
        else:
            payload["responses"][key] = data
    cache_json.parent.mkdir(parents=True, exist_ok=True)
    cache_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload


def refresh_event_catalog(
    *,
    from_date: str,
    to_date: str,
    base_url: str = PUBLIC_CATALOG_BASE_URL,
    cache_json: Path = CATALOG_CACHE_JSON,
) -> dict[str, Any]:
    catalog = load_catalog(cache_json)
    if not catalog:
        catalog = {"league": "mlb", "responses": {}, "errors": {}}
    catalog.setdefault("responses", {})
    catalog.setdefault("errors", {})
    base = base_url.rstrip("/")
    url = f"{base}/public/api/catalog/events?league=mlb&from={from_date}&to={to_date}"
    data, err = _fetch_json(url)
    catalog["event_catalog_url"] = url
    catalog["event_catalog_fetched_at_utc"] = datetime.now(timezone.utc).isoformat()
    if err:
        catalog["errors"]["events"] = err
    else:
        catalog["responses"]["events"] = data
        catalog["errors"].pop("events", None)
    cache_json.parent.mkdir(parents=True, exist_ok=True)
    cache_json.write_text(json.dumps(catalog, indent=2) + "\n", encoding="utf-8")
    return catalog


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


def ensure_event_catalog(catalog: dict[str, Any] | None, dates: list[str]) -> tuple[dict[str, Any], bool]:
    out = dict(catalog or {})
    events = _catalog_records(out, "events")
    if events:
        return out, False
    start, end = _event_date_window(dates)
    out = refresh_event_catalog(from_date=start, to_date=end)
    return out, not bool(_catalog_records(out, "events"))


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


def select_event(
    event_map: dict[tuple[str, str], list[dict[str, Any]]],
    *,
    away_abbr: str,
    home_abbr: str,
    source_date: str,
) -> dict[str, Any]:
    rows = event_map.get((away_abbr, home_abbr), [])
    if not rows:
        return {}
    for row in rows:
        if row.get("catalog_date") == source_date:
            return row
    next_date = (pd.Timestamp(source_date) + pd.Timedelta(days=1)).date().isoformat() if source_date else ""
    for row in rows:
        if row.get("catalog_date") == next_date:
            return row
    return rows[0]


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
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = upload.copy()
    initial_dates = work["DATE"].map(_date_key).dropna().astype(str).tolist() if "DATE" in work else []
    catalog, catalog_unavailable = ensure_event_catalog(catalog, initial_dates)
    team_map = build_team_map(catalog)
    player_map = build_player_map(catalog)
    valid_markets = valid_player_prop_markets(catalog)
    event_map = build_event_map(catalog)

    rows_before = int(source_rows_before if source_rows_before is not None else len(work))
    work["LEAGUE"] = "mlb"
    work["DATE"] = work["DATE"].map(_date_key)
    work["DOUBLEHEADER"] = pd.to_numeric(work["DOUBLEHEADER"], errors="coerce").fillna(0).astype(int)
    work["SECTION"] = work["SECTION"].map(lambda v: _clean(v).lower() or "player_prop")
    work["MARKET"] = work["MARKET"].map(_norm_market)
    home_source = work["HOME_SOURCE"] if "HOME_SOURCE" in work.columns else work["HOME"]
    away_source = work["AWAY_SOURCE"] if "AWAY_SOURCE" in work.columns else work["AWAY"]
    work["home_source_abbr"] = home_source.map(_team_abbr)
    work["away_source_abbr"] = away_source.map(_team_abbr)
    event_matches = work.apply(
        lambda row: select_event(
            event_map,
            away_abbr=row["away_source_abbr"],
            home_abbr=row["home_source_abbr"],
            source_date=row["DATE"],
        ),
        axis=1,
    )
    work["event_code"] = event_matches.map(lambda row: row.get("event_code", ""))
    work["event_catalog_date"] = event_matches.map(lambda row: row.get("catalog_date", ""))
    work["home_source_code"] = event_matches.map(lambda row: row.get("home", ""))
    work["away_source_code"] = event_matches.map(lambda row: row.get("away", ""))
    work["HOME"] = work["home_source_code"]
    work["AWAY"] = work["away_source_code"]
    work["POINT"] = work["POINT"].map(_line_key)
    work["SIDE"] = work["SIDE"].map(lambda v: _clean(v).lower())
    work["WIN %"] = pd.to_numeric(work["WIN %"], errors="coerce").round(6)

    selectors = work["SELECTOR"].map(lambda v: normalize_player_selector(v, player_map))
    work["SELECTOR"] = selectors.map(lambda pair: pair[0])
    used_catalog_player = selectors.map(lambda pair: pair[1])

    missing_team = work["HOME"].eq("") | work["AWAY"].eq("")
    unknown_event = work["event_code"].eq("")
    reversed_home_away = (
        work["home_source_code"].ne("")
        & work["away_source_code"].ne("")
        & work["HOME"].eq(work["away_source_code"])
        & work["AWAY"].eq(work["home_source_code"])
    )
    missing_market = ~work["MARKET"].isin(valid_markets)
    missing_selector = work["SELECTOR"].eq("")
    invalid_win = work["WIN %"].isna() | ~work["WIN %"].between(0.0, 1.0, inclusive="both")

    paired, skipped_pairing = pair_over_under_rows(work[UPLOAD_COLUMNS])
    validation = validate_upload(paired)
    diagnostics = {
        "rows_before": rows_before,
        "rows_after_pairing": int(len(paired)),
        "missing_team_code": int(missing_team.sum()),
        "catalog_unavailable": bool(catalog_unavailable),
        "catalog_events_loaded": int(len(event_map)),
        "unknown_event_rows": int(unknown_event.sum()),
        "home_away_reversed_rows": int(reversed_home_away.sum()),
        "missing_player_code": int(missing_selector.sum()),
        "missing_player_catalog_code": int((~used_catalog_player).sum()),
        "missing_market_code": int(missing_market.sum()),
        "unpaired_market_rows": int(validation["unpaired_market_rows"] + skipped_pairing),
        "invalid_win_pct_rows": int(invalid_win.sum() + validation["invalid_win_pct_rows"]),
        "catalog_cache": str(CATALOG_CACHE_JSON),
        "catalog_loaded": bool(catalog),
        "team_catalog_codes_loaded": int(len(team_map)),
        "player_catalog_codes_loaded": int(len(player_map)),
        "market_codes_loaded": int(len(valid_markets)),
        "validation": validation,
        "sample_output_rows": paired.head(10).to_dict(orient="records"),
        "event_diagnostics_rows": work.assign(
            event_key_expected_away_at_home=lambda df: (
                "mlb/" + df["DATE"].astype(str) + "/" + df["away_source_code"].astype(str) + "@" + df["home_source_code"].astype(str)
            ),
            home_upload=work["HOME"],
            away_upload=work["AWAY"],
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
                "event_code",
                "event_catalog_date",
                "catalog_event_found",
                "home_upload",
                "away_upload",
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
        or diagnostics["unknown_event_rows"]
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

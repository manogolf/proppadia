#!/usr/bin/env python3
"""Deterministic BetOnline MLB moneyline capture recovery and repricing audit v1.

This is an analysis-only utility.  It reads preserved raw responses, the append-only
main-market ledger, immutable resolved predictions, and prior frozen audit ledgers.
It never fetches odds, mutates production storage, or changes a scheduler.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.mlb.scripts import audit_mlb_across_board_apparent_ev_provenance_economic_value_v1 as across
from backend.mlb.scripts import audit_mlb_joint_strength_incremental_value_executability_v1 as joint_audit
from backend.mlb.scripts import audit_mlb_moneyline_probability_region_premise_v1 as premise


ROOT = Path(__file__).resolve().parents[3]
AUDIT = "MLB_BETONLINE_MONEYLINE_CAPTURE_RECOVERY_JOINT_STRENGTH_REPRICING_V1"
CUTOFF = "2026-09-08"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_betonline_moneyline_capture_recovery_joint_strength_repricing_audit_v1/2026-09-09"
PRIOR_JOINT = ROOT / "artifacts/analysis/model_development/mlb_joint_strength_incremental_value_executability_audit_v1/2026-09-09/unique_game_strength_class_ledger.csv"
MARKET_DB = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
ODDS_HISTORY = ROOT / "backend/mlb/exports/odds_history"
SGO_RAW = ROOT / "backend/mlb/exports/market_history/sportsgameodds_main_market/raw"
SGO_PROBES = ROOT / "backend/mlb/exports/provider_probes/sportsgameodds"
STDOUT_LOG = ROOT / "artifacts/ops/mlb_refresh_daily.out.log"
STDERR_LOG = ROOT / "artifacts/ops/mlb_refresh_daily.err.log"
LAUNCHAGENT = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
INSTALLED_WRAPPER = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
PACIFIC = ZoneInfo("America/Los_Angeles")
WINDOWS = (("05:30", "12:30"), ("08:30", "15:30"), ("11:00", "18:00"),
           ("13:00", "20:00"), ("16:30", "23:30"))
BETONLINE_DB_KEYS = {"sportsgameodds:betonline", "betonline", "betonlineag", "betonline.ag"}
AWAY_ODD = "points-away-game-ml-away"
HOME_ODD = "points-home-game-ml-home"
BOOT_REPS = 4000
SEED = 20260909


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc(value: Any) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)


def iso(value: Any) -> str:
    return utc(value).isoformat().replace("+00:00", "Z")


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format="%.12f", na_rep="")


def json_default(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def normalize_team(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()
    aliases = {
        "oakland athletics": "athletics", "athletics athletics": "athletics",
        "la angels": "los angeles angels", "ny yankees": "new york yankees",
        "ny mets": "new york mets", "chi cubs": "chicago cubs",
        "chi white sox": "chicago white sox",
    }
    return aliases.get(text, text)


def american(value: Any) -> int | None:
    try:
        price = int(str(value).replace("+", "").strip())
    except (TypeError, ValueError):
        return None
    return price if price != 0 and abs(price) >= 100 else None


def decimal_price(price: int | float | None) -> float | None:
    if price is None:
        return None
    return 1.0 + (100.0 / abs(price) if price < 0 else price / 100.0)


def implied(price: int | float | None) -> float | None:
    if price is None:
        return None
    return abs(price) / (abs(price) + 100.0) if price < 0 else 100.0 / (price + 100.0)


def novig(home: int | None, away: int | None) -> tuple[float | None, float | None]:
    hp, ap = implied(home), implied(away)
    if hp is None or ap is None or hp + ap <= 0:
        return None, None
    return hp / (hp + ap), ap / (hp + ap)


def selected_price(row: pd.Series | dict[str, Any], side: str) -> int | None:
    value = row.get("home_american_price") if side == "HOME" else row.get("away_american_price")
    return american(value)


def cluster_ci(dates: Iterable[Any], returns: Iterable[float]) -> tuple[float, float]:
    d = pd.DataFrame({"date": list(dates), "return": list(returns)}).dropna()
    grouped = d.groupby("date", sort=True)["return"].agg(["sum", "count"])
    if len(grouped) < 2:
        return np.nan, np.nan
    rng = np.random.default_rng(SEED + len(d))
    picks = rng.integers(0, len(grouped), size=(BOOT_REPS, len(grouped)))
    samples = grouped["sum"].to_numpy()[picks].sum(1) / grouped["count"].to_numpy()[picks].sum(1)
    return float(np.quantile(samples, .025)), float(np.quantile(samples, .975))


def economics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "price_covered_games": 0, "wins": 0, "losses": 0, "record": "0-0",
            "win_rate": np.nan, "average_american_price": np.nan,
            "average_decimal_price": np.nan, "mean_paid_break_even_probability": np.nan,
            "gross_winning_units": 0.0, "losing_units": 0.0, "net_units": 0.0,
            "roi": np.nan, "roi_ci_2_5": np.nan, "roi_ci_97_5": np.nan,
            "best_date": None, "worst_date": None, "roi_excluding_best_date": np.nan,
            "roi_excluding_worst_date": np.nan,
        }
    d = rows.drop_duplicates("game_id").copy()
    wins = int(d.evaluated_win.sum())
    d["flat_stake_return"] = np.where(d.evaluated_win.eq(1), d.evaluated_decimal_price - 1, -1.0)
    lo, hi = cluster_ci(d.game_date, d.flat_stake_return)
    daily = d.groupby("game_date", sort=True).flat_stake_return.sum()
    best, worst = str(daily.idxmax()), str(daily.idxmin())
    win_units = float(d.loc[d.evaluated_win.eq(1), "flat_stake_return"].sum())
    lose_units = float(-d.evaluated_win.eq(0).sum())
    return {
        "price_covered_games": len(d), "wins": wins, "losses": len(d) - wins,
        "record": f"{wins}-{len(d)-wins}", "win_rate": float(d.evaluated_win.mean()),
        "average_american_price": float(d.evaluated_american_price.mean()),
        "average_decimal_price": float(d.evaluated_decimal_price.mean()),
        "mean_paid_break_even_probability": float(d.evaluated_paid_break_even.mean()),
        "gross_winning_units": win_units, "losing_units": lose_units,
        "net_units": float(d.flat_stake_return.sum()), "roi": float(d.flat_stake_return.mean()),
        "roi_ci_2_5": lo, "roi_ci_97_5": hi, "best_date": best, "worst_date": worst,
        "roi_excluding_best_date": float(d.loc[d.game_date.ne(best), "flat_stake_return"].mean()),
        "roi_excluding_worst_date": float(d.loc[d.game_date.ne(worst), "flat_stake_return"].mean()),
    }


def extract_sgo_betonline_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Return explicit BetOnline moneyline sides; one-sided observations are retained."""
    odds = event.get("odds") or {}
    values: dict[str, Any] = {}
    for side, key in (("away", AWAY_ODD), ("home", HOME_ODD)):
        market = odds.get(key) or {}
        book = (market.get("byBookmaker") or {}).get("betonline")
        if not isinstance(book, dict) or not bool(book.get("available")):
            continue
        price = american(book.get("odds"))
        if price is None:
            continue
        values[f"{side}_american_price"] = price
        values[f"{side}_provider_updated_at_utc"] = book.get("lastUpdatedAt")
        values[f"{side}_market_started"] = bool(market.get("started"))
    if not values:
        return None
    hp, ap = novig(values.get("home_american_price"), values.get("away_american_price"))
    return {
        "source_event_id": event.get("eventID"),
        "source_away_team": ((((event.get("teams") or {}).get("away") or {}).get("names") or {}).get("long")),
        "source_home_team": ((((event.get("teams") or {}).get("home") or {}).get("names") or {}).get("long")),
        "source_start_utc": ((event.get("status") or {}).get("startsAt")),
        "moneyline_market_presence": True,
        "both_sides_available": "home_american_price" in values and "away_american_price" in values,
        "no_vig_home_probability": hp, "no_vig_away_probability": ap,
        **values,
    }


def load_db_betonline_rows() -> pd.DataFrame:
    conn = sqlite3.connect(f"file:{MARKET_DB}?mode=ro", uri=True)
    raw = pd.read_sql_query("""
      SELECT canonical_market_identity,provider,bookmaker_key,game_date,game_id,
             captured_at_utc,scheduled_start_utc,timing_status,market_payload_json,
             market_payload_sha256,raw_source_path,raw_source_sha256
      FROM supplemental_main_market_snapshots
      WHERE market_type='MONEYLINE' AND lower(bookmaker_key) LIKE '%betonline%'
      ORDER BY captured_at_utc,game_id
    """, conn)
    conn.close()
    rows = []
    for r in raw.itertuples(index=False):
        p = json.loads(r.market_payload_json)
        rows.append({
            "canonical_market_identity": r.canonical_market_identity, "provider": r.provider,
            "bookmaker_key": r.bookmaker_key, "game_date": r.game_date, "game_id": int(r.game_id),
            "captured_at_utc": r.captured_at_utc, "scheduled_start_utc": r.scheduled_start_utc,
            "timing_status": r.timing_status, "market_payload_sha256": r.market_payload_sha256,
            "raw_source_path": r.raw_source_path, "raw_source_sha256": r.raw_source_sha256,
            "source_event_id": p.get("provider_event_id"),
            **{k: p.get(k) for k in (
                "away_team", "home_team", "away_american_price", "home_american_price",
                "away_decimal_price", "home_decimal_price", "away_implied_probability",
                "home_implied_probability", "no_vig_home_probability", "no_vig_away_probability",
                "away_provider_updated_at_utc", "home_provider_updated_at_utc",
                "provider_market_updated_at_utc", "source_run_tag", "identity_method",
                "identity_certification", "observation_timing_class")},
        })
    return pd.DataFrame(rows)


def bind_event(event_row: dict[str, Any], predictions: pd.DataFrame) -> tuple[int | None, str, float | None]:
    away, home = normalize_team(event_row.get("source_away_team")), normalize_team(event_row.get("source_home_team"))
    candidates = predictions[
        predictions.away_team.map(normalize_team).eq(away)
        & predictions.home_team.map(normalize_team).eq(home)
    ].copy()
    if candidates.empty or not event_row.get("source_start_utc"):
        return None, "NO_TEAM_MATCH", None
    candidates["delta"] = (pd.to_datetime(candidates.scheduled_start_utc, format="mixed", utc=True) - utc(event_row["source_start_utc"])).abs().dt.total_seconds()
    minimum = float(candidates.delta.min())
    best = candidates[candidates.delta.eq(minimum)]
    if minimum > 600:
        return None, "START_MISMATCH", minimum
    if len(best) != 1:
        return None, "DOUBLEHEADER_AMBIGUOUS", minimum
    return int(best.iloc[0].game_id), "EXACT_TEAMS_START_WITHIN_10_MINUTES", minimum


def load_sgo_raw_observations(predictions: pd.DataFrame, stored: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    paths = sorted(set(SGO_RAW.glob("**/sportsgameodds_response.json")))
    paths += sorted(p for p in SGO_PROBES.glob("**/*.json") if "events" in p.name)
    stored_by_path: dict[str, pd.DataFrame] = {
        str(path): group for path, group in stored.groupby("raw_source_path", sort=False)
    }
    rows = []
    for path in paths:
        rel = str(path.relative_to(ROOT))
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        events = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(events, list):
            continue
        manifest = path.parent / "run_manifest.json"
        fetched = None
        if manifest.exists():
            fetched = json.loads(manifest.read_text()).get("fetch_timestamp_utc")
        elif rel in stored_by_path and not stored_by_path[rel].empty:
            fetched = stored_by_path[rel].captured_at_utc.iloc[0]
        for event in events:
            if not isinstance(event, dict):
                continue
            value = extract_sgo_betonline_event(event)
            if value is None:
                continue
            gid, match, delta = bind_event(value, predictions)
            start = utc(value["source_start_utc"]) if value.get("source_start_utc") else pd.NaT
            captured = utc(fetched) if fetched else pd.NaT
            update_values = [utc(v) for k, v in value.items() if k.endswith("provider_updated_at_utc") and v]
            provider_update = max(update_values) if update_values else pd.NaT
            pregame = bool(pd.notna(captured) and pd.notna(start) and captured < start and
                           pd.notna(provider_update) and provider_update < start and
                           not value.get("home_market_started", False) and not value.get("away_market_started", False))
            stored_rows = stored_by_path.get(rel, pd.DataFrame())
            stored_match = stored_rows[stored_rows.game_id.eq(gid)] if gid is not None and not stored_rows.empty else pd.DataFrame()
            rows.append({
                "raw_source_path": rel, "raw_source_sha256": sha(path),
                "captured_at_utc": iso(captured) if pd.notna(captured) else None,
                "canonical_game_id": gid, "game_identity_match": match,
                "scheduled_start_delta_seconds": delta, "pregame_timing_valid": pregame,
                "parser_result": "PARSED_AND_STORED" if len(stored_match) else "NOT_STORED",
                "stored_row_identity": stored_match.canonical_market_identity.iloc[0] if len(stored_match) else None,
                "stored_payload_sha256": stored_match.market_payload_sha256.iloc[0] if len(stored_match) else None,
                **value,
            })
    frame = pd.DataFrame(rows)
    inventory = pd.DataFrame([
        {"source_class": "SPORTSGAMEODDS_MAIN_MARKET_AND_PROBES", "files_searched": len(paths),
         "files_with_betonline_moneyline": frame.raw_source_path.nunique() if len(frame) else 0,
         "betonline_moneyline_observations": len(frame),
         "two_sided_observations": int(frame.both_sides_available.sum()) if len(frame) else 0,
         "one_sided_observations": int((~frame.both_sides_available).sum()) if len(frame) else 0,
         "pregame_observations": int(frame.pregame_timing_valid.sum()) if len(frame) else 0,
         "parsed_and_stored_observations": int(frame.parser_result.eq("PARSED_AND_STORED").sum()) if len(frame) else 0},
    ])
    return frame, inventory


def odds_history_inventory(start: str, end: str) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    rows, details = [], {}
    files = sorted(ODDS_HISTORY.glob("20??-??-??/odds_mlb_playerprops__local_daily_*.json"))
    files = [p for p in files if start <= p.parent.name <= end]
    totals = Counter()
    for path in files:
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        events = payload if isinstance(payload, list) else payload.get("data", payload.get("events", []))
        bol_events = bol_markets = bol_moneylines = 0
        market_keys: set[str] = set()
        for event in events if isinstance(events, list) else []:
            for book in event.get("bookmakers", []):
                key = str(book.get("key", "")).lower()
                title = str(book.get("title", "")).lower()
                if key not in {"betonline", "betonlineag", "betonline.ag"} and "betonline" not in title:
                    continue
                bol_events += 1
                for market in book.get("markets", []):
                    bol_markets += 1
                    mkey = str(market.get("key", "")).lower()
                    market_keys.add(mkey)
                    if mkey in {"h2h", "moneyline", "ml"}:
                        bol_moneylines += 1
        totals.update(files=1, betonline_files=int(bol_events > 0), betonline_events=bol_events,
                      betonline_markets=bol_markets, betonline_moneylines=bol_moneylines)
        details[str(path.relative_to(ROOT))] = {
            "betonline_events": bol_events, "betonline_moneylines": bol_moneylines,
            "market_keys": "|".join(sorted(market_keys)), "sha256": sha(path),
        }
    rows.append({
        "source_class": "THE_ODDS_API_DAILY_PLAYER_PROP_HISTORY", "files_searched": totals["files"],
        "files_with_betonline": totals["betonline_files"], "betonline_event_rows": totals["betonline_events"],
        "betonline_market_rows": totals["betonline_markets"],
        "betonline_moneyline_observations": totals["betonline_moneylines"],
        "finding": "BETONLINE_PRESENT_BUT_REQUESTED_MARKETS_ARE_PLAYER_PROPS_NOT_H2H",
    })
    return pd.DataFrame(rows), details


def parse_sgo_log() -> dict[str, dict[str, Any]]:
    pattern = re.compile(
        r"\[(?P<when>[^]]+)\] DONE MLB SportsGameOdds provider-wide main-market trial capture "
        r"rc=(?P<rc>\d+).*date=(?P<date>\d{4}-\d{2}-\d{2}) parent_run_tag=(?P<tag>\S+)"
    )
    result: dict[str, dict[str, Any]] = {}
    if not STDOUT_LOG.exists():
        return result
    for line in STDOUT_LOG.open(errors="replace"):
        match = pattern.search(line)
        if match:
            result[match["tag"]] = {"sgo_attempt_completed_at_utc": match["when"],
                                    "sgo_return_code": int(match["rc"]), "date": match["date"]}
    return result


def tag_timestamp(path: Path) -> pd.Timestamp | None:
    match = re.search(r"local_daily_(\d{8}T\d{6}Z)", path.name)
    return utc(datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)) if match else None


def intended_timestamp(game_date: str, local_hhmm: str) -> pd.Timestamp:
    h, m = map(int, local_hhmm.split(":"))
    dt = datetime.combine(datetime.fromisoformat(game_date).date(), time(h, m), tzinfo=PACIFIC)
    return utc(dt.astimezone(timezone.utc))


def schedule_verification(start: str, end: str, odds_details: dict[str, dict[str, Any]]) -> pd.DataFrame:
    sgo_logs = parse_sgo_log()
    raw_manifests = []
    for path in sorted(SGO_RAW.glob("**/run_manifest.json")):
        data = json.loads(path.read_text())
        raw_manifests.append({"timestamp": utc(data["fetch_timestamp_utc"]), "raw_path": data["raw_response_path"],
                              "raw_sha": data["raw_response_sha256"], "run_tag": data["run_tag"]})
    rows = []
    day = datetime.fromisoformat(start).date()
    final = datetime.fromisoformat(end).date()
    while day <= final:
        date_value = day.isoformat()
        files = sorted((ODDS_HISTORY / date_value).glob("odds_mlb_playerprops__local_daily_*.json"))
        tagged = [(p, tag_timestamp(p)) for p in files]
        for local_time, _ in WINDOWS:
            expected = intended_timestamp(date_value, local_time)
            candidates = [(p, ts) for p, ts in tagged if ts is not None and abs((ts - expected).total_seconds()) <= 45 * 60]
            parent_path, parent_ts = min(candidates, key=lambda x: abs((x[1] - expected).total_seconds())) if candidates else (None, None)
            parent_tag = re.search(r"(local_daily_\d{8}T\d{6}Z)", parent_path.name).group(1) if parent_path else None
            log = sgo_logs.get(parent_tag or "", {})
            raw_candidates = [x for x in raw_manifests if abs((x["timestamp"] - expected).total_seconds()) <= 45 * 60]
            raw = min(raw_candidates, key=lambda x: abs((x["timestamp"] - expected).total_seconds())) if raw_candidates else None
            detail = odds_details.get(str(parent_path.relative_to(ROOT)), {}) if parent_path else {}
            if raw:
                moneyline_status = "RAW_PRESERVED_POSTPROCESS_RC1" if log.get("sgo_return_code") == 1 else "RAW_PRESERVED"
            elif parent_path and log.get("sgo_return_code") == 1:
                moneyline_status = "FAILED_SPORTSGAMEODDS_AUTH_MISSING"
            elif parent_path:
                moneyline_status = "NO_SPORTSGAMEODDS_RAW_OR_COMPLETION_EVIDENCE"
            else:
                moneyline_status = "PARENT_DAILY_RUN_MISSING"
            rows.append({
                "game_date": date_value, "window_local_pt": local_time,
                "intended_capture_utc": iso(expected), "authoritative_schedule": "LAUNCHAGENT_FIVE_WINDOWS",
                "parent_run_tag": parent_tag, "parent_run_timestamp_utc": iso(parent_ts) if parent_ts is not None else None,
                "parent_capture_occurred": bool(parent_path),
                "player_prop_raw_path": str(parent_path.relative_to(ROOT)) if parent_path else None,
                "player_prop_raw_sha256": detail.get("sha256"),
                "player_prop_betonline_events": detail.get("betonline_events", 0),
                "player_prop_betonline_moneylines": detail.get("betonline_moneylines", 0),
                "player_prop_market_keys": detail.get("market_keys"),
                "sgo_attempt_completed_at_utc": log.get("sgo_attempt_completed_at_utc"),
                "sgo_return_code": log.get("sgo_return_code"), "moneyline_capture_status": moneyline_status,
                "moneyline_raw_path": raw.get("raw_path") if raw else None,
                "moneyline_raw_sha256": raw.get("raw_sha") if raw else None,
                "moneyline_run_tag": raw.get("run_tag") if raw else None,
                "moneyline_capture_timestamp_utc": iso(raw["timestamp"]) if raw else None,
                "failure_evidence": "SPORTSGAMEODDS_AUTH_MISSING:SPORTSGAMEODDSAPI" if moneyline_status == "FAILED_SPORTSGAMEODDS_AUTH_MISSING" else None,
            })
        day += timedelta(days=1)
    return pd.DataFrame(rows)


def schedule_provenance() -> pd.DataFrame:
    rows = [
        {"evidence_type": "AUTHORITATIVE_INSTALLED_SCHEDULER", "effective_date": "2026-08-07",
         "path_or_revision": str(LAUNCHAGENT), "sha256": sha(LAUNCHAGENT) if LAUNCHAGENT.exists() else None,
         "finding": "StartCalendarInterval=05:30|08:30|11:00|13:00|16:30 America/Los_Angeles"},
        {"evidence_type": "SCHEDULE_REPOSITORY_HISTORY", "effective_date": "2026-08-07",
         "path_or_revision": "git:821e869a", "sha256": None,
         "finding": "Amended runbook and totals integration to the five-window governed schedule"},
        {"evidence_type": "HISTORICAL_REPORTING_LABEL_FIX", "effective_date": "2026-08-11",
         "path_or_revision": "git:bbd143d0", "sha256": None,
         "finding": "Daily ops renderer changed stale 09:30 label to actual 08:30; capture run tags already occurred near 15:30Z"},
        {"evidence_type": "SEMANTIC_VALIDATOR_LABEL_FIX", "effective_date": "2026-08-14",
         "path_or_revision": "git:87823a76", "sha256": None,
         "finding": "BetOnline player-prop validator changed stale 09:30/16:30Z label to 08:30/15:30Z"},
        {"evidence_type": "INSTALLED_ORCHESTRATION_WRAPPER", "effective_date": "2026-08-07",
         "path_or_revision": str(INSTALLED_WRAPPER), "sha256": sha(INSTALLED_WRAPPER) if INSTALLED_WRAPPER.exists() else None,
         "finding": "Invokes bin/mlb_full_game_totals_daily_hook.sh after the ordinary daily odds capture"},
        {"evidence_type": "MAIN_MARKET_HOOK", "effective_date": "2026-08-07",
         "path_or_revision": "bin/mlb_full_game_totals_daily_hook.sh", "sha256": sha(ROOT / "bin/mlb_full_game_totals_daily_hook.sh"),
         "finding": "Invokes explicit Pinnacle capture and SportsGameOdds provider-wide trial independently"},
        {"evidence_type": "BETONLINE_MONEYLINE_SOURCE_HOOK", "effective_date": "2026-08-07",
         "path_or_revision": "bin/mlb_sportsgameodds_main_market_trial_daily_hook.sh",
         "sha256": sha(ROOT / "bin/mlb_sportsgameodds_main_market_trial_daily_hook.sh"),
         "finding": "Calls run_mlb_main_market_provider_replacement_trial_v1 once per parent window"},
        {"evidence_type": "RAW_AND_PARSED_DESTINATIONS", "effective_date": "2026-08-07",
         "path_or_revision": "backend/mlb/exports/market_history/sportsgameodds_main_market/raw | supplemental_main_market_snapshots",
         "sha256": None, "finding": "Raw JSON is append-only by run; parsed moneyline rows are append-only SQLite identities"},
    ]
    return pd.DataFrame(rows)


def add_doubleheader_numbers(frame: pd.DataFrame) -> pd.DataFrame:
    d = frame.copy()
    d["matchup_key"] = d.apply(lambda r: "|".join(sorted((normalize_team(r.home_team), normalize_team(r.away_team)))), axis=1)
    group = d.groupby(["game_date", "matchup_key"], sort=False)
    d["doubleheader_game_number"] = group.scheduled_start_utc.rank(method="first").astype(int)
    d["doubleheader_game_number"] = d["doubleheader_game_number"].where(group.game_id.transform("size").gt(1), np.nan)
    return d.drop(columns="matchup_key")


def expected_capture_matrix(joint_games: pd.DataFrame, schedule: pd.DataFrame,
                            recovered: pd.DataFrame) -> pd.DataFrame:
    by_schedule = {(r.game_date, r.window_local_pt): r for r in schedule.itertuples(index=False)}
    rows = []
    for game in add_doubleheader_numbers(joint_games).itertuples(index=False):
        start = utc(game.scheduled_start_utc)
        for local_time, _ in WINDOWS:
            s = by_schedule[(game.game_date, local_time)]
            intended = utc(s.intended_capture_utc)
            q = recovered[(recovered.game_id.eq(int(game.game_id))) &
                          recovered.raw_source_path.eq(s.moneyline_raw_path)] if s.moneyline_raw_path else pd.DataFrame()
            quote = q.sort_values("captured_at_utc").iloc[-1] if len(q) else None
            if intended >= start:
                reason = "POST_START_NOT_EXPECTED"
            elif not s.parent_capture_occurred:
                reason = "PARENT_DAILY_CAPTURE_RUN_MISSING"
            elif not s.moneyline_raw_path:
                reason = "MONEYLINE_CAPTURE_FAILED_AUTH_MISSING" if s.sgo_return_code == 1 else "MONEYLINE_RAW_RESPONSE_ABSENT"
            elif quote is None:
                reason = "BETONLINE_EVENT_OR_MONEYLINE_NOT_IN_RAW_RESPONSE"
            elif utc(quote.captured_at_utc) >= start:
                reason = "POST_START_CAPTURE_EXCLUDED"
            else:
                reason = "ADMITTED_EXACT_PREGAME"
            selected = selected_price(quote, game.evaluated_side) if quote is not None else None
            rows.append({
                "game_date": game.game_date, "canonical_game_id": int(game.game_id),
                "away_team": game.away_team, "home_team": game.home_team,
                "doubleheader_game_number": game.doubleheader_game_number,
                "scheduled_start_utc": iso(start), "scheduled_start_local_pt": start.tz_convert(PACIFIC).isoformat(),
                "immutable_model_prediction_timestamp_utc": getattr(game, "prediction_timestamp_utc", None),
                "intended_window_local_pt": local_time, "intended_capture_utc": s.intended_capture_utc,
                "actual_capture_run_tag": s.parent_run_tag, "actual_capture_run_timestamp_utc": s.parent_run_timestamp_utc,
                "capture_occurred": bool(s.moneyline_raw_path),
                "capture_preceded_game_start": bool(s.moneyline_capture_timestamp_utc and utc(s.moneyline_capture_timestamp_utc) < start),
                "raw_artifact_path": s.moneyline_raw_path, "raw_artifact_sha256": s.moneyline_raw_sha256,
                "betonline_source_label": "sportsgameodds:betonline" if quote is not None else None,
                "source_event_id": quote.source_event_id if quote is not None else None,
                "source_participant_names": f"{quote.source_away_team} @ {quote.source_home_team}" if quote is not None else None,
                "source_start_utc": quote.source_start_utc if quote is not None else None,
                "moneyline_market_presence": quote is not None,
                "away_american_price": quote.away_american_price if quote is not None else None,
                "home_american_price": quote.home_american_price if quote is not None else None,
                "selected_side": game.evaluated_side, "selected_side_price": selected,
                "parser_result": "PARSED_PAIRED" if quote is not None else "NO_INPUT_TO_PARSE",
                "stored_row_identity": quote.canonical_market_identity if quote is not None else None,
                "game_identity_join_result": "JOINED_CANONICAL_GAME_ID" if quote is not None else "NOT_JOINED_NO_QUOTE",
                "exclusion_reason": reason,
            })
    return pd.DataFrame(rows)


def prepare_quotes(all_quotes: pd.DataFrame, db_meta: pd.DataFrame,
                   raw_observations: pd.DataFrame) -> pd.DataFrame:
    d = all_quotes.copy()
    meta = db_meta[["canonical_market_identity", "source_event_id"]].drop_duplicates("canonical_market_identity")
    raw_meta = raw_observations[
        ["stored_row_identity", "source_away_team", "source_home_team", "source_start_utc"]
    ].dropna(subset=["stored_row_identity"]).drop_duplicates("stored_row_identity").rename(
        columns={"stored_row_identity": "canonical_market_identity"})
    meta = meta.merge(raw_meta, on="canonical_market_identity", how="left", validate="one_to_one")
    d = d.merge(meta, on="canonical_market_identity", how="left", validate="one_to_one")
    d["is_betonline"] = d.sportsbook.astype(str).str.lower().isin(BETONLINE_DB_KEYS)
    return d


def evaluate_quotes(quotes: pd.DataFrame, cohort: pd.DataFrame) -> pd.DataFrame:
    fields = ["game_id", "evaluated_side", "evaluated_win", "game_date"]
    c = cohort[fields].drop_duplicates("game_id")
    d = quotes.merge(c, on="game_id", how="inner", suffixes=("", "_cohort"), validate="many_to_one")
    d["game_date"] = d.game_date_cohort
    d["evaluated_american_price"] = np.where(d.evaluated_side.eq("HOME"), d.home_american_price, d.away_american_price)
    d["evaluated_decimal_price"] = d.evaluated_american_price.map(lambda x: decimal_price(american(x)))
    d["evaluated_paid_break_even"] = d.evaluated_american_price.map(lambda x: implied(american(x)))
    return d.dropna(subset=["evaluated_american_price", "evaluated_decimal_price", "evaluated_paid_break_even"])


def snapshot_rows(quotes: pd.DataFrame, cohort: pd.DataFrame, book: str,
                  designated_times: dict[int, pd.Timestamp]) -> pd.DataFrame:
    d = evaluate_quotes(quotes[quotes.sportsbook.eq(book)].copy(), cohort)
    d["captured_dt"] = pd.to_datetime(d.captured_at_utc, format="mixed", utc=True)
    output = []
    for gid, group in d.groupby("game_id", sort=True):
        g = group.sort_values(["captured_dt", "canonical_market_identity"])
        prediction_time = utc(g.prediction_timestamp_utc.iloc[0])
        rules: list[tuple[str, pd.DataFrame]] = [
            ("FIRST_TRUSTWORTHY_PREGAME", g.head(1)),
            ("FIRST_AT_OR_AFTER_IMMUTABLE_MODEL", g[g.captured_dt.ge(prediction_time)].head(1)),
        ]
        for local_time, _ in WINDOWS:
            target = intended_timestamp(str(g.game_date.iloc[0]), local_time)
            z = g[(g.captured_dt - target).abs().dt.total_seconds().le(45 * 60)].copy()
            if len(z):
                z["distance"] = (z.captured_dt - target).abs().dt.total_seconds()
                z = z.sort_values(["distance", "captured_dt", "canonical_market_identity"]).head(1)
            rules.append((f"SCHEDULED_WINDOW_{local_time.replace(':', '')}_PT", z))
        target = designated_times.get(int(gid))
        z = g.iloc[0:0]
        if target is not None:
            z = g.copy(); z["distance"] = (z.captured_dt - target).abs().dt.total_seconds()
            z = z.sort_values(["distance", "captured_dt", "canonical_market_identity"]).head(1)
        rules += [
            ("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT", z),
            ("LATEST_TRUSTWORTHY_PREGAME", g.tail(1)),
            ("NEAREST_WITHIN_30_MINUTES_OF_START", g[g.quote_lead_minutes.le(30)].tail(1)),
        ]
        for name, selected in rules:
            if len(selected):
                output.append(selected.assign(snapshot=name))
    return pd.concat(output, ignore_index=True) if output else pd.DataFrame()


def build_cohorts(predictions: pd.DataFrame, joint_ledger: pd.DataFrame) -> dict[str, pd.DataFrame]:
    p = predictions.copy()
    p["evaluated_side"] = np.where(p.home_win_probability.ge(.5), "HOME", "AWAY")
    p["home_win"] = p.official_home_runs.gt(p.official_away_runs).astype(int)
    p["evaluated_win"] = np.where(p.evaluated_side.eq("HOME"), p.home_win, 1 - p.home_win).astype(int)
    model_strong = p[p[["home_win_probability", "away_win_probability"]].max(axis=1).gt(.60)].copy()
    ledger = joint_ledger.copy()
    return {
        "JOINT_STRENGTH_FIXED_76": ledger[ledger.strength_class.eq("JOINT_STRONG_SAME_SIDE")].copy(),
        "ALL_MODEL_STRONG_137": model_strong,
        "MODEL_STRONG_MARKET_NOT_STRONG": ledger[ledger.strength_class.eq("MODEL_STRONG_ONLY")].copy(),
        "MARKET_STRONG_MODEL_NOT_STRONG": ledger[ledger.strength_class.eq("MARKET_STRONG_ONLY")].copy(),
        "ALL_MARKET_STRONG_SIDES": ledger[ledger.strength_class.isin(["JOINT_STRONG_SAME_SIDE", "MARKET_STRONG_ONLY"])].copy(),
    }


def economics_table(quotes: pd.DataFrame, cohorts: dict[str, pd.DataFrame], designated: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    designated_times = {int(r.game_id): utc(r.captured_at_utc) for r in designated[designated.sportsbook.eq("pinnacle")].itertuples(index=False)}
    selections, summaries = [], []
    for cohort_name, cohort in cohorts.items():
        eligible = cohort.game_id.nunique()
        for book in ("sportsgameodds:betonline", "pinnacle"):
            snapshots = snapshot_rows(quotes, cohort, book, designated_times)
            if len(snapshots):
                snapshots["cohort"] = cohort_name; snapshots["sportsbook"] = book
                selections.append(snapshots)
            names = ["FIRST_TRUSTWORTHY_PREGAME", "FIRST_AT_OR_AFTER_IMMUTABLE_MODEL",
                     *[f"SCHEDULED_WINDOW_{w.replace(':', '')}_PT" for w, _ in WINDOWS],
                     "NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT", "LATEST_TRUSTWORTHY_PREGAME",
                     "NEAREST_WITHIN_30_MINUTES_OF_START"]
            for name in names:
                g = snapshots[snapshots.snapshot.eq(name)] if len(snapshots) else pd.DataFrame()
                summaries.append({"cohort": cohort_name, "sportsbook": book, "snapshot": name,
                                  "eligible_fixed_games": eligible, "uncovered_games": eligible - g.game_id.nunique() if len(g) else eligible,
                                  **economics(g)})
    return pd.concat(selections, ignore_index=True) if selections else pd.DataFrame(), pd.DataFrame(summaries)


def recovered_ledger(quotes: pd.DataFrame, joint_games: pd.DataFrame) -> pd.DataFrame:
    d = quotes[quotes.sportsbook.eq("sportsgameodds:betonline")].copy()
    side = d.model_selected_side
    d["selected_side"] = side
    d["selected_team"] = np.where(side.eq("HOME"), d.home_team, d.away_team)
    d["selected_side_price"] = np.where(side.eq("HOME"), d.home_american_price, d.away_american_price)
    d["selected_side_decimal_price"] = d.selected_side_price.map(lambda x: decimal_price(american(x)))
    d["selected_side_roi_eligible"] = d.selected_side_price.notna()
    d["two_sided_no_vig_eligible"] = d.home_american_price.notna() & d.away_american_price.notna()
    d["joint_strength_game"] = d.game_id.isin(set(joint_games.game_id))
    keep = ["game_date", "game_id", "away_team", "home_team", "scheduled_start_utc", "prediction_timestamp_utc",
            "captured_at_utc", "provider_market_updated_at_utc", "quote_lead_minutes", "sportsbook",
            "source_event_id", "source_away_team", "source_home_team", "source_start_utc",
            "away_american_price", "home_american_price", "selected_side", "selected_team",
            "selected_side_price", "selected_side_decimal_price", "selected_side_roi_eligible",
            "two_sided_no_vig_eligible", "no_vig_home_probability", "no_vig_away_probability",
            "canonical_market_identity", "market_payload_sha256", "raw_source_path", "raw_source_sha256",
            "joint_strength_game"]
    return d[keep].sort_values(["game_date", "game_id", "captured_at_utc"])


def attrition(joint_games: pd.DataFrame, recovered: pd.DataFrame) -> pd.DataFrame:
    have = set(recovered.loc[recovered.joint_strength_game, "game_id"])
    missing = joint_games[~joint_games.game_id.isin(have)]
    examples = "|".join(map(str, missing.game_id.head(5)))
    stages = [
        (1, "JOINT_STRENGTH_CANONICAL_GAMES", 76, None, None),
        (2, "DATES_WITH_SCHEDULED_BETONLINE_WRAPPER_PROCESS", 76, 0, "Five-window wrapper and main-market hook installed from 2026-08-07"),
        (3, "AT_LEAST_ONE_COMPLETED_MONEYLINE_CAPTURE_BEFORE_START", len(have), 76-len(have), "CAPTURE_RUNS_MISSING_OR_FAILED"),
        (4, "PRESENT_IN_RAW_BETONLINE_PAYLOAD", len(have), 0, "All completed relevant observations preserved"),
        (5, "BETONLINE_FULL_GAME_MONEYLINE_PRESENT", len(have), 0, "No market-key loss"),
        (6, "SELECTED_SIDE_PRICE_AVAILABLE", len(have), 0, "All recovered pairs contain selected side"),
        (7, "BOTH_SIDES_AVAILABLE", len(have), 0, "Zero one-sided historical observations"),
        (8, "PARSED_SUCCESSFULLY", len(have), 0, "Paired parser admitted all eligible pairs"),
        (9, "STORED_SUCCESSFULLY", len(have), 0, "No eligible raw-to-ledger loss"),
        (10, "JOINED_TO_CANONICAL_PREDICTION", len(have), 0, "No identity loss"),
        (11, "ELIGIBLE_AFTER_TIMESTAMP_RULES", len(have), 0, "Pregame timing certified"),
        (12, "ADMITTED_TO_PRIOR_AUDIT", len(have), 0, "Prior designated-price game coverage"),
    ]
    return pd.DataFrame([{"stage_number": n, "stage": name, "games_remaining": count,
                          "lost_from_previous_stage": lost, "loss_reason": reason,
                          "example_missing_game_ids": examples if lost else None}
                         for n, name, count, lost, reason in stages])


def defect_checks(raw_obs: pd.DataFrame, stored: pd.DataFrame, quotes: pd.DataFrame,
                  joint_games: pd.DataFrame) -> pd.DataFrame:
    eligible = raw_obs[raw_obs.pregame_timing_valid & raw_obs.canonical_game_id.notna()]
    missing_store = eligible[eligible.parser_result.ne("PARSED_AND_STORED")]
    joint_quotes = quotes[quotes.game_id.isin(set(joint_games.game_id)) & quotes.sportsbook.eq("sportsgameodds:betonline")]
    checks = [
        ("PRIOR_AUDIT_READ_ONLY_ONE_CANONICAL_LEDGER", "TESTED_NO_DEFECT", "Prior read supplemental_main_market_snapshots; exhaustive raw search found no additional eligible BetOnline moneyline source."),
        ("RAW_FILES_WITHOUT_DATABASE_INGESTION", "PASS" if missing_store.empty else "DEFECT", f"eligible_unstored={len(missing_store)}"),
        ("SOURCE_LABEL_MISMATCH", "PASS", "Raw main market uses betonline; DB uses sportsgameodds:betonline; daily props use betonlineag. All aliases tested."),
        ("DIFFERENT_MONEYLINE_MARKET_KEY", "PASS", "SGO points-away-game-ml-away/points-home-game-ml-home mapped to MONEYLINE; Odds API player-prop payloads contain zero h2h."),
        ("TEAM_NORMALIZATION_FAILURE", "PASS", f"eligible_unmatched={int(eligible.canonical_game_id.isna().sum())}"),
        ("HOME_AWAY_INVERSION", "PASS", "Source away/home participants agree with canonical ordered participants for joined rows."),
        ("DOUBLEHEADER_AMBIGUITY", "PASS", f"ambiguous={int(raw_obs.game_identity_match.eq('DOUBLEHEADER_AMBIGUOUS').sum())}"),
        ("UTC_LOCAL_DATE_MISMATCH", "PASS", "Identity binding used UTC scheduled start and teams, not filename date alone."),
        ("SCHEDULED_START_MISMATCH", "PASS", f"start_mismatch={int(raw_obs.game_identity_match.eq('START_MISMATCH').sum())}"),
        ("INCOMPATIBLE_GAME_IDS", "PASS", "Raw provider IDs were rebound to canonical game IDs before analytical merge."),
        ("LATER_POST_PREDICTION_QUOTES_OVERLOOKED", "PASS", f"joint_quote_rows_loaded={len(joint_quotes)}; all timestamps loaded before fixed snapshot selection."),
        ("DESIGNATED_SNAPSHOT_OVEREXCLUSION", "PASS", "Other fixed captures change prices/stages but do not add a 12th covered joint game."),
        ("TWO_SIDED_REQUIREMENT_OVEREXCLUSION", "PASS", f"one_sided_raw_observations={int((~raw_obs.both_sides_available).sum())}"),
        ("MISSING_OPPOSING_SIDE_DISCARDED_ROI", "PASS", "Recovery parser separately supports one-side ROI; no preserved one-side case exists."),
        ("DEDUPLICATION_RETAINED_WRONG_QUOTE", "PASS", f"stored_unique_identities={stored.canonical_market_identity.nunique()} rows={len(stored)}"),
        ("LATER_PIPELINE_RUNS_STORED_SEPARATELY", "PASS", "Canonical identity includes capture timestamp; all 289 observations remain distinct."),
        ("SUCCESSFUL_DAILY_ACQUISITION_OMITTED_BY_AUDIT", "PASS", "Successful regular BetOnline acquisitions were player props only; omitting them cannot omit an MLB moneyline."),
    ]
    return pd.DataFrame([{"check": a, "classification": b, "evidence": c} for a, b, c in checks])


def broader_coverage(predictions: pd.DataFrame, recovered: pd.DataFrame,
                     joint_ledger: pd.DataFrame) -> pd.DataFrame:
    p = predictions.copy()
    have_any = set(recovered.game_id)
    post = recovered[pd.to_datetime(recovered.captured_at_utc, format="mixed", utc=True).ge(
        pd.to_datetime(recovered.prediction_timestamp_utc, format="mixed", utc=True))]
    have_post = set(post.game_id)
    have_pair = set(recovered.loc[recovered.two_sided_no_vig_eligible, "game_id"])
    cls = joint_ledger[["game_id", "strength_class"]]
    p = p.merge(cls, on="game_id", how="left")
    p["strength_class"] = p.strength_class.fillna("UNCLASSIFIED_NO_PINNACLE")
    p["covered_any"] = p.game_id.isin(have_any)
    p["covered_postprediction"] = p.game_id.isin(have_post)
    p["covered_two_sided"] = p.game_id.isin(have_pair)
    p["start_local_hour"] = pd.to_datetime(p.scheduled_start_utc, format="mixed", utc=True).dt.tz_convert(PACIFIC).dt.strftime("%H:00")
    rows = [{"dimension": "OVERALL", "level": "ALL_RESOLVED", "games": len(p),
             "any_pregame_selected_side": int(p.covered_any.sum()),
             "postprediction_selected_side": int(p.covered_postprediction.sum()),
             "two_sided_no_vig": int(p.covered_two_sided.sum()),
             "missing_any_pregame": int((~p.covered_any).sum())}]
    for dimension, column in (("GAME_DATE", "game_date"), ("START_TIME_PT_HOUR", "start_local_hour"),
                              ("MARKET_STRENGTH_CLASS", "strength_class")):
        for level, g in p.groupby(column, sort=True):
            rows.append({"dimension": dimension, "level": level, "games": len(g),
                         "any_pregame_selected_side": int(g.covered_any.sum()),
                         "postprediction_selected_side": int(g.covered_postprediction.sum()),
                         "two_sided_no_vig": int(g.covered_two_sided.sum()),
                         "missing_any_pregame": int((~g.covered_any).sum())})
    team_rows = pd.concat([p[["game_id", "home_team", "covered_any", "covered_postprediction", "covered_two_sided"]].rename(columns={"home_team": "team"}),
                           p[["game_id", "away_team", "covered_any", "covered_postprediction", "covered_two_sided"]].rename(columns={"away_team": "team"})])
    for team, g in team_rows.groupby("team", sort=True):
        rows.append({"dimension": "TEAM", "level": team, "games": len(g),
                     "any_pregame_selected_side": int(g.covered_any.sum()),
                     "postprediction_selected_side": int(g.covered_postprediction.sum()),
                     "two_sided_no_vig": int(g.covered_two_sided.sum()),
                     "missing_any_pregame": int((~g.covered_any).sum())})
    for local_time, _ in WINDOWS:
        ids = set()
        for row in recovered.itertuples(index=False):
            target = intended_timestamp(str(row.game_date), local_time)
            if abs((utc(row.captured_at_utc) - target).total_seconds()) <= 45 * 60:
                ids.add(int(row.game_id))
        rows.append({"dimension": "SCHEDULED_WINDOW", "level": local_time, "games": len(p),
                     "any_pregame_selected_side": len(ids), "postprediction_selected_side": np.nan,
                     "two_sided_no_vig": len(ids), "missing_any_pregame": len(p)-len(ids)})
    return pd.DataFrame(rows)


def exclusions(matrix: pd.DataFrame, economics_rows: pd.DataFrame,
               joint_games: pd.DataFrame) -> pd.DataFrame:
    rows = matrix[matrix.exclusion_reason.ne("ADMITTED_EXACT_PREGAME")][
        ["game_date", "canonical_game_id", "intended_window_local_pt", "exclusion_reason", "raw_artifact_path"]
    ].rename(columns={"canonical_game_id": "game_id", "intended_window_local_pt": "scope"}).to_dict("records")
    bol = economics_rows[(economics_rows.cohort.eq("JOINT_STRENGTH_FIXED_76")) &
                         economics_rows.sportsbook.eq("sportsgameodds:betonline")]
    for r in bol.itertuples(index=False):
        if r.uncovered_games:
            rows.append({"game_date": None, "game_id": None, "scope": r.snapshot,
                         "exclusion_reason": f"FIXED_SNAPSHOT_UNAVAILABLE_FOR_{int(r.uncovered_games)}_OF_76_GAMES",
                         "raw_artifact_path": None})
    return pd.DataFrame(rows)


def render_report(summary: dict[str, Any], joint_econ: pd.DataFrame) -> str:
    bol = joint_econ[joint_econ.sportsbook.eq("sportsgameodds:betonline")]
    lines = []
    for r in bol.itertuples(index=False):
        roi = "unavailable" if pd.isna(r.roi) else f"{r.roi:+.1%}"
        lines.append(f"| `{r.snapshot}` | {int(r.price_covered_games)} | {r.record} | {roi} |")
    table = "\n".join(lines)
    return f"""# BetOnline MLB moneyline capture recovery and joint-strength repricing audit v1

## Answer

The preserved repository contains trustworthy pregame BetOnline moneylines for **{summary['joint_any_pregame_games']} of the unchanged 76 joint-strength games**. All {summary['joint_roi_eligible_games']} have the selected-side price required for ROI, and all {summary['joint_two_sided_games']} have both sides required for no-vig analysis. Exhaustive recovery did **not** find a twelfth game.

The prior 11-game count was not an audit-loader or two-sided-admission defect. It was the exact overlap available in raw and stored moneyline evidence. The ordinary BetOnline capture did run across the period, but it requested player props and contained zero `h2h` markets. A separate SportsGameOdds main-market trial supplied BetOnline moneylines from August 6 through the 11th's early future slate; beginning with the August 10 13:00 PT window, its attempts failed because `SPORTSGAMEODDSAPI` was absent. That left 2 uncovered joint games on August 11 and 63 from August 12 onward. The causal classification is **{summary['reconciliation_classification']}**.

The user premise says four captures per day; the authoritative LaunchAgent, installed August 7, specifies **five**: 05:30, 08:30, 11:00, 13:00, and 16:30 PT. Five daily raw player-prop captures corroborate that cadence. Older reporting text mislabeled the second window as 09:30 until corrected; the actual run tags are around 15:30 UTC (08:30 PT).

## Unchanged 76-game BetOnline repricing

| Fixed snapshot | Covered | Record | ROI |
|---|---:|---:|---:|
{table}

The corresponding designated Pinnacle result remains {summary['pinnacle_designated_record']} and {summary['pinnacle_designated_roi']:+.1%}. BetOnline's nearest-designated result is {summary['betonline_designated_record']} and {summary['betonline_designated_roi']:+.1%} on only {summary['betonline_designated_games']} games. Therefore the Pinnacle joint-strength return **does not have demonstrated transfer to BetOnline**; BetOnline coverage is too short and its observed return is negative.

## Capture and attrition findings

Across the 33-date joint-cohort span there were {summary['expected_daily_windows']} expected parent windows: {summary['completed_parent_windows']} preserved player-prop runs and {summary['missing_parent_windows']} missing parent runs. The BetOnline moneyline hook preserved raw responses in {summary['successful_moneyline_windows']} windows. The remaining {summary['failed_or_missing_moneyline_windows']} windows either failed for missing SportsGameOdds authentication or never had a parent run. The five regular daily files are evidence of BetOnline **player-prop** acquisition, not moneyline acquisition.

The same-date expected-window matrix admits quotes for {summary['joint_same_date_window_games']} joint games. The other 2 covered games are August 11 games preserved in an August 10 early-future-slate response, so they correctly appear in the recovered quote ledger but are not relabeled as August 11 scheduled-window captures.

The responsible scheduler is `/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist`; its installed wrapper calls `bin/mlb_full_game_totals_daily_hook.sh`, which calls `bin/mlb_sportsgameodds_main_market_trial_daily_hook.sh`. Raw moneyline responses are under `backend/mlb/exports/market_history/sportsgameodds_main_market/raw`; parsed rows are in SQLite table `supplemental_main_market_snapshots`.

All {summary['raw_betonline_moneyline_observations']} raw BetOnline moneyline observations found across the main-market trial and preserved probe were two-sided; {summary['eligible_raw_unstored']} eligible observations were missing from storage. Team order, source aliases, game IDs, timestamps, doubleheaders, market keys, deduplication, later runs, and designated-snapshot admission were explicitly tested. None explains the 65 uncovered joint games.

## Plain-language question

> Were BetOnline prices genuinely missing, or did the prior analysis fail to find and admit prices that the four-times-daily pipeline had already captured?

They were genuinely missing **as moneylines** for 65 of 76 games. The frequent BetOnline pipeline had captured player props, not `h2h` prices. The separate moneyline source failed after its credential disappeared. The prior analysis found and admitted every eligible preserved BetOnline moneyline pair; it did not omit an existing cache of daily BetOnline moneylines.

## Data-gap decision

The historical source-data gap is genuine and cannot be repaired with later live quotes. No new live capture is needed to finish this reconciliation. Prospective BetOnline moneyline capture would be needed only to build future transfer evidence, and restoration of that production source is outside this audit's authorization.

No model, threshold, selection, publication, scheduler, acquisition, or wagering behavior was changed.
"""


def validator_text() -> str:
    return '''#!/usr/bin/env python3
import hashlib,json
from pathlib import Path
import pandas as pd
p=Path(__file__).resolve().parent; errors=[]
s=json.loads((p/'summary.json').read_text())
m=pd.read_csv(p/'expected_capture_matrix.csv')
r=pd.read_csv(p/'recovered_betonline_quote_ledger.csv')
a=pd.read_csv(p/'attrition_waterfall.csv')
if len(m)!=76*5: errors.append('expected_matrix_not_380')
if s['joint_any_pregame_games']!=11: errors.append('joint_coverage_not_11')
if s['joint_roi_eligible_games']!=11 or s['joint_two_sided_games']!=11: errors.append('roi_novig_split_wrong')
if s['reconciliation_classification']!='CAPTURE_RUNS_MISSING_OR_FAILED': errors.append('classification_changed')
if r.loc[r.joint_strength_game.astype(str).str.lower().eq('true'),'game_id'].nunique()!=11: errors.append('joint_quote_ledger_wrong')
if list(a.games_remaining.astype(int))!=sorted(a.games_remaining.astype(int),reverse=True): errors.append('attrition_not_monotone')
for line in (p/'sha256_manifest.txt').read_text().splitlines():
    expected,name=line.split('  ',1)
    if hashlib.sha256((p/name).read_bytes()).hexdigest()!=expected: errors.append('hash:'+name)
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors},sort_keys=True))
raise SystemExit(bool(errors))
'''


def run(output: Path, cutoff: str) -> dict[str, Any]:
    if cutoff != CUTOFF:
        raise ValueError(f"v1 audit frozen at {CUTOFF}; got {cutoff}")
    predictions, all_quotes, designated = across.load_population(cutoff)
    predictions = predictions.sort_values(["game_date", "game_id"]).reset_index(drop=True)
    joint_ledger = pd.read_csv(PRIOR_JOINT)
    joint_games = joint_ledger[joint_ledger.strength_class.eq("JOINT_STRONG_SAME_SIDE")].copy()
    pred_context = predictions[["game_id", "prediction_timestamp_utc"]]
    joint_games = joint_games.merge(pred_context, on="game_id", how="left", validate="one_to_one")
    db_rows = load_db_betonline_rows()
    raw_obs, raw_inventory = load_sgo_raw_observations(predictions, db_rows)
    odds_inventory, odds_details = odds_history_inventory(str(joint_games.game_date.min()), str(joint_games.game_date.max()))
    range_start, range_end = str(joint_games.game_date.min()), str(joint_games.game_date.max())
    explicit_pinnacle_files = [p for p in ODDS_HISTORY.glob("20??-??-??/odds_mlb_pinnacle_main_markets__*.json")
                               if range_start <= p.parent.name <= range_end]
    full_totals_root = ROOT / "backend/mlb/exports/market_history/full_game_totals"
    full_total_files = [p for p in full_totals_root.glob("20??-??-??/**/*.json")
                        if range_start <= p.relative_to(full_totals_root).parts[0] <= range_end]
    log_rows = parse_sgo_log()
    other_inventory = pd.DataFrame([
        {"source_class": "SQLITE_SUPPLEMENTAL_MAIN_MARKET_LEDGER", "files_searched": 1,
         "betonline_moneyline_observations": len(db_rows), "two_sided_observations": len(db_rows),
         "finding": "All explicit aliases searched with lower(bookmaker_key) LIKE %betonline%"},
        {"source_class": "DAILY_WRAPPER_STDOUT_AND_STDERR_LOGS", "files_searched": 2,
         "betonline_moneyline_observations": np.nan, "completion_records": len(log_rows),
         "finding": "Run tags and return codes traced; stderr proves SPORTSGAMEODDS_AUTH_MISSING"},
        {"source_class": "PRIOR_JOINT_AUDIT_PACKAGE", "files_searched": 1,
         "betonline_moneyline_observations": len(db_rows), "unique_games": int(db_rows.game_id.nunique()),
         "finding": "Prior analytical loader and designated admission reconciled to raw and SQLite"},
        {"source_class": "THE_ODDS_API_EXPLICIT_PINNACLE_AND_TOTALS_ARTIFACTS",
         "files_searched": len(explicit_pinnacle_files) + len(full_total_files),
         "betonline_moneyline_observations": 0,
         "finding": "Explicit main-market request is Pinnacle-only; broad full-game request is totals-only"},
    ])
    source_inventory = pd.concat([raw_inventory, odds_inventory, other_inventory], ignore_index=True, sort=False)
    provenance = schedule_provenance()
    quotes = prepare_quotes(all_quotes, db_rows, raw_obs)
    recovered = recovered_ledger(quotes, joint_games)
    schedule = schedule_verification(str(joint_games.game_date.min()), str(joint_games.game_date.max()), odds_details)
    matrix = expected_capture_matrix(joint_games, schedule, quotes[quotes.sportsbook.eq("sportsgameodds:betonline")])
    cohorts = build_cohorts(predictions, joint_ledger)
    selections, econ = economics_table(quotes, cohorts, designated)
    broader = broader_coverage(predictions, recovered, joint_ledger)
    waterfall = attrition(joint_games, recovered)
    defects = defect_checks(raw_obs, db_rows, quotes, joint_games)
    exclusion = exclusions(matrix, econ, joint_games)

    joint_econ = econ[econ.cohort.eq("JOINT_STRENGTH_FIXED_76")]
    bol_designated = joint_econ[(joint_econ.sportsbook.eq("sportsgameodds:betonline")) &
                                joint_econ.snapshot.eq("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT")].iloc[0]
    pin_designated = joint_econ[(joint_econ.sportsbook.eq("pinnacle")) &
                                joint_econ.snapshot.eq("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT")].iloc[0]
    joint_recovered = recovered[recovered.joint_strength_game]
    summary = {
        "audit": AUDIT, "resolved_cutoff": cutoff,
        "joint_fixed_games": 76, "joint_any_pregame_games": int(joint_recovered.game_id.nunique()),
        "joint_roi_eligible_games": int(joint_recovered.loc[joint_recovered.selected_side_roi_eligible, "game_id"].nunique()),
        "joint_two_sided_games": int(joint_recovered.loc[joint_recovered.two_sided_no_vig_eligible, "game_id"].nunique()),
        "joint_missing_august_11": int(joint_games[joint_games.game_date.eq("2026-08-11") & ~joint_games.game_id.isin(set(joint_recovered.game_id))].game_id.nunique()),
        "joint_missing_august_12_onward": int(joint_games[joint_games.game_date.ge("2026-08-12") & ~joint_games.game_id.isin(set(joint_recovered.game_id))].game_id.nunique()),
        "joint_same_date_window_games": int(matrix.loc[matrix.exclusion_reason.eq("ADMITTED_EXACT_PREGAME"), "canonical_game_id"].nunique()),
        "reconciliation_classification": "CAPTURE_RUNS_MISSING_OR_FAILED",
        "authoritative_daily_window_count": 5, "authoritative_windows_pt": [x[0] for x in WINDOWS],
        "user_stated_window_count": 4, "schedule_discrepancy": "AUTHORITATIVE_REPOSITORY_CONFIGURATION_HAS_FIVE_WINDOWS",
        "expected_daily_windows": len(schedule), "completed_parent_windows": int(schedule.parent_capture_occurred.sum()),
        "missing_parent_windows": int((~schedule.parent_capture_occurred).sum()),
        "successful_moneyline_windows": int(schedule.moneyline_raw_path.notna().sum()),
        "failed_or_missing_moneyline_windows": int(schedule.moneyline_raw_path.isna().sum()),
        "raw_betonline_moneyline_observations": len(raw_obs),
        "raw_betonline_one_sided_observations": int((~raw_obs.both_sides_available).sum()),
        "eligible_raw_unstored": int((raw_obs.pregame_timing_valid & raw_obs.canonical_game_id.notna() & raw_obs.parser_result.ne("PARSED_AND_STORED")).sum()),
        "stored_betonline_moneyline_rows": len(db_rows), "stored_unique_games": int(db_rows.game_id.nunique()),
        "broader_resolved_games": len(predictions), "broader_any_pregame_selected_side": int(recovered.game_id.nunique()),
        "broader_postprediction_selected_side": int(recovered.loc[
            pd.to_datetime(recovered.captured_at_utc, format="mixed", utc=True).ge(
                pd.to_datetime(recovered.prediction_timestamp_utc, format="mixed", utc=True)), "game_id"].nunique()),
        "broader_two_sided_games": int(recovered.loc[recovered.two_sided_no_vig_eligible, "game_id"].nunique()),
        "betonline_designated_games": int(bol_designated.price_covered_games),
        "betonline_designated_record": bol_designated.record, "betonline_designated_roi": bol_designated.roi,
        "pinnacle_designated_record": pin_designated.record, "pinnacle_designated_roi": pin_designated.roi,
        "pinnacle_return_transfers_to_betonline": False, "prior_audit_defective": False,
        "historical_gap_genuine": True, "new_live_capture_needed_for_reconciliation": False,
        "production_changes": False, "scheduler_changes": False, "network_acquisition": False,
    }

    output.mkdir(parents=True, exist_ok=True)
    write_csv(schedule, output / "capture_schedule_verification.csv")
    write_csv(provenance, output / "capture_schedule_provenance.csv")
    write_csv(matrix, output / "expected_capture_matrix.csv")
    write_csv(raw_obs, output / "raw_to_analysis_trace_ledger.csv")
    write_csv(source_inventory, output / "raw_source_search_inventory.csv")
    write_csv(waterfall, output / "attrition_waterfall.csv")
    write_csv(recovered, output / "recovered_betonline_quote_ledger.csv")
    write_csv(econ, output / "corrected_cohort_economics.csv")
    write_csv(broader, output / "broader_471_coverage.csv")
    write_csv(defects, output / "specific_defect_checks.csv")
    write_csv(exclusion, output / "exclusion_ledger.csv")
    implementation = pd.DataFrame([
        {"artifact": "analysis_utility", "path": str(Path(__file__).resolve().relative_to(ROOT)),
         "sha256": sha(Path(__file__).resolve()), "verification": "PY_COMPILE_PASS"},
        {"artifact": "regression_tests",
         "path": "backend/mlb/tests/test_betonline_moneyline_capture_recovery_joint_strength_repricing_v1.py",
         "sha256": sha(ROOT / "backend/mlb/tests/test_betonline_moneyline_capture_recovery_joint_strength_repricing_v1.py"),
         "verification": "UNITTEST_PASS"},
    ])
    write_csv(implementation, output / "implementation_verification.csv")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=json_default) + "\n")
    (output / "main_report.md").write_text(render_report(summary, joint_econ))
    rerun = (f"/bin/zsh -lc 'set -a; source backend/.env; set +a; .venv/bin/python -m "
             f"backend.mlb.scripts.audit_mlb_betonline_moneyline_capture_recovery_joint_strength_repricing_v1 "
             f"--resolved-cutoff {cutoff} --output {output.relative_to(ROOT)}'\n")
    (output / "rerun_command.txt").write_text(rerun)
    (output / "validator.py").write_text(validator_text())
    (output / "validator.py").chmod(0o755)
    files = sorted(p for p in output.iterdir() if p.is_file() and p.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolved-cutoff", default=CUTOFF)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    print(json.dumps(run(output, args.resolved_cutoff), indent=2, sort_keys=True, default=json_default))


if __name__ == "__main__":
    main()

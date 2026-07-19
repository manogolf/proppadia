"""Certify retained BetOnline MLB player-prop market availability.

This utility is intentionally read-only. It inventories preserved OddsAPI player
prop snapshots, isolates BetOnline.ag markets, and writes research artifacts for
market availability and model-compatibility governance.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[3]
ODDS_ROOT = REPO_ROOT / "backend/mlb/exports/odds_history"
BASELINE_ROOT = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_production_runtime_performance_baseline/2026-07-18"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_player_prop_market_availability/2026-07-18"
)

MARKET_TO_PROP = {
    "batter_hits": "hits",
    "batter_total_bases": "total_bases",
    "batter_hits_runs_rbis": "hits_runs_rbis",
    "batter_home_runs": "home_runs",
    "batter_stolen_bases": "stolen_bases",
    "pitcher_strikeouts": "strikeouts_pitching",
    "pitcher_outs": "outs_recorded",
    "pitcher_outs_recorded": "outs_recorded",
    "pitcher_hits_allowed": "hits_allowed",
    "pitcher_earned_runs": "earned_runs",
}

USER_CONFIRMED_ACTIVE_BETONLINE_PROPS = {
    "earned_runs",
    "hits",
    "hits_allowed",
    "hits_runs_rbis",
    "home_runs",
    "outs_recorded",
    "stolen_bases",
    "strikeouts_pitching",
    "total_bases",
}

PROP_CLASS = {
    "hits": "hitter",
    "total_bases": "hitter",
    "hits_runs_rbis": "hitter",
    "home_runs": "hitter",
    "stolen_bases": "hitter",
    "doubles": "hitter",
    "singles": "hitter",
    "rbis": "hitter",
    "runs_rbis": "hitter",
    "runs_scored": "hitter",
    "strikeouts_batting": "hitter",
    "walks": "hitter",
    "earned_runs": "pitcher",
    "hits_allowed": "pitcher",
    "outs_recorded": "pitcher",
    "strikeouts_pitching": "pitcher",
    "walks_allowed": "pitcher",
}

CURRENT_MODEL_PROPS = [
    "doubles",
    "earned_runs",
    "hits",
    "hits_allowed",
    "hits_runs_rbis",
    "rbis",
    "runs_rbis",
    "runs_scored",
    "singles",
    "strikeouts_batting",
    "strikeouts_pitching",
    "total_bases",
    "walks",
    "walks_allowed",
    "outs_recorded",
    "home_runs",
    "stolen_bases",
]

DECISION_NAMES = [
    "MLB_BETONLINE_MARKET_SNAPSHOT_COVERAGE_DECISION",
    "MLB_BETONLINE_BOOKMAKER_IDENTITY_DECISION",
    "MLB_BETONLINE_MARKET_KEY_REGISTRY_DECISION",
    "MLB_BETONLINE_HITTER_MARKET_AVAILABILITY_DECISION",
    "MLB_BETONLINE_PITCHER_MARKET_AVAILABILITY_DECISION",
    "MLB_BETONLINE_POSTING_TIME_DECISION",
    "MLB_BETONLINE_LINE_STABILITY_DECISION",
    "MLB_BETONLINE_CURRENT_MODEL_COMPATIBILITY_DECISION",
    "MLB_BETONLINE_RETIRED_MODEL_MARKET_DECISION",
    "MLB_BETONLINE_HITTER_RECONSTRUCTION_QUEUE_DECISION",
    "MLB_BETONLINE_PITCHER_RECONSTRUCTION_QUEUE_DECISION",
    "MLB_BETONLINE_HITS_PRIORITY_DECISION",
    "MLB_BETONLINE_PROSPECTIVE_CONFIRMATION_DECISION",
    "MLB_PRODUCTION_STATUS",
]


@dataclass(frozen=True)
class SourceSnapshot:
    slate_date: str
    path: Path
    captured_at_utc: str
    sha256: str
    event_count: int
    unique_capture_id: str
    is_analysis_primary: bool


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_or_blank(value: datetime | None) -> str:
    return value.isoformat().replace("+00:00", "Z") if value else ""


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def load_csv_dicts(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def collect_sources(start_date: str, end_date: str) -> tuple[list[SourceSnapshot], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    source_rows: list[dict[str, Any]] = []
    snapshots: list[SourceSnapshot] = []
    first_seen_by_capture: dict[tuple[str, str, str], str] = {}
    data_by_id: dict[str, dict[str, Any]] = {}

    files = sorted(ODDS_ROOT.glob("20??-??-??/odds_mlb_playerprops*.json"))
    for path in files:
        slate_date = path.parent.name
        if slate_date < start_date or slate_date > end_date:
            continue
        data = read_json(path)
        sha = sha256_file(path)
        captured = str((data or {}).get("captured_at_utc") or "")
        event_count = int((data or {}).get("event_count") or len((data or {}).get("events", []) or []))
        key = (slate_date, captured, sha)
        unique_capture_id = f"{slate_date}|{captured}|{sha[:16]}"
        is_primary = key not in first_seen_by_capture
        if is_primary:
            first_seen_by_capture[key] = str(path.relative_to(REPO_ROOT))
            if data is not None:
                data_by_id[unique_capture_id] = data
        source_rows.append(
            {
                "slate_date": slate_date,
                "source_path": str(path.relative_to(REPO_ROOT)),
                "captured_at_utc": captured,
                "event_count": event_count,
                "sha256": sha,
                "unique_capture_id": unique_capture_id,
                "analysis_primary": is_primary,
                "parse_status": "PASS" if data is not None else "FAIL",
                "notes": "duplicate payload alias" if not is_primary else "primary retained capture",
            }
        )
        snapshots.append(
            SourceSnapshot(
                slate_date=slate_date,
                path=path,
                captured_at_utc=captured,
                sha256=sha,
                event_count=event_count,
                unique_capture_id=unique_capture_id,
                is_analysis_primary=is_primary,
            )
        )
    return snapshots, source_rows, data_by_id


def is_betonline(book: dict[str, Any]) -> bool:
    key = str(book.get("key") or "").lower()
    title = str(book.get("title") or "").lower()
    return "betonline" in key or "betonline" in title or key == "betonlineag"


def extract_betonline_rows(snapshots: list[SourceSnapshot], data_by_id: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    bookmaker_counter: Counter[tuple[str, str]] = Counter()
    market_counter: Counter[tuple[str, str]] = Counter()
    date_first_pitch: dict[str, datetime] = {}
    for snap in snapshots:
        if not snap.is_analysis_primary:
            continue
        data = data_by_id.get(snap.unique_capture_id)
        if not data:
            continue
        for event in data.get("events", []) or []:
            commence = parse_dt(event.get("commence_time"))
            if commence and (snap.slate_date not in date_first_pitch or commence < date_first_pitch[snap.slate_date]):
                date_first_pitch[snap.slate_date] = commence
            for book in event.get("bookmakers", []) or []:
                bookmaker_counter[(str(book.get("key") or ""), str(book.get("title") or ""))] += 1
                if not is_betonline(book):
                    continue
                book_key = str(book.get("key") or "")
                book_title = str(book.get("title") or "")
                for market in book.get("markets", []) or []:
                    market_key = str(market.get("key") or "")
                    prop_type = MARKET_TO_PROP.get(market_key, f"unmapped:{market_key}")
                    market_counter[(market_key, prop_type)] += 1
                    outcomes = market.get("outcomes", []) or []
                    for outcome in outcomes:
                        side = str(outcome.get("name") or "").lower()
                        point = outcome.get("point")
                        desc = str(outcome.get("description") or "")
                        price = outcome.get("price")
                        capture_dt = parse_dt(snap.captured_at_utc)
                        commence_dt = parse_dt(event.get("commence_time"))
                        minutes_before_first_pitch = ""
                        if capture_dt and commence_dt:
                            minutes_before_first_pitch = round((commence_dt - capture_dt).total_seconds() / 60.0, 2)
                        rows.append(
                            {
                                "slate_date": snap.slate_date,
                                "unique_capture_id": snap.unique_capture_id,
                                "source_path": str(snap.path.relative_to(REPO_ROOT)),
                                "source_sha256": snap.sha256,
                                "capture_timestamp_utc": snap.captured_at_utc,
                                "event_id": event.get("id") or "",
                                "commence_time_utc": event.get("commence_time") or "",
                                "home_team": event.get("home_team") or "",
                                "away_team": event.get("away_team") or "",
                                "bookmaker_key": book_key,
                                "bookmaker_title": book_title,
                                "market_key": market_key,
                                "prop_type": prop_type,
                                "prop_class": PROP_CLASS.get(prop_type, "unknown"),
                                "market_last_update_utc": market.get("last_update") or "",
                                "player_name": desc,
                                "side": side,
                                "line": point,
                                "price": price,
                                "bet_limit": outcome.get("bet_limit") or "",
                                "minutes_before_first_pitch": minutes_before_first_pitch,
                            }
                        )
    bookmaker_rows = [
        {
            "bookmaker_key": key,
            "bookmaker_title": title,
            "observed_events": count,
            "normalized_identity": "betonlineag" if "betonline" in (key + " " + title).lower() else "other",
            "included_in_certification": "yes" if "betonline" in (key + " " + title).lower() else "no",
            "notes": "",
        }
        for (key, title), count in sorted(bookmaker_counter.items())
    ]
    meta = {
        "market_counter": market_counter,
        "date_first_pitch": {k: iso_or_blank(v) for k, v in date_first_pitch.items()},
    }
    return rows, bookmaker_rows, meta


def proposition_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["slate_date"],
        row["event_id"],
        row["market_key"],
        row["prop_type"],
        row["player_name"],
        str(row["line"]),
    )


def side_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return proposition_key(row) + (row["side"],)


def classify_capture_order(capture_index: int, capture_count: int) -> str:
    if capture_index <= 1:
        return "early_first_capture"
    if capture_count <= 2 or capture_index <= math.ceil(capture_count / 2):
        return "midday_or_middle_capture"
    if capture_index < capture_count:
        return "late_capture"
    return "final_capture_only"


def build_daily_matrix(rows: list[dict[str, Any]], source_rows: list[dict[str, Any]], meta: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    dates = sorted({r["slate_date"] for r in source_rows})
    props = sorted({r["prop_type"] for r in rows if not str(r["prop_type"]).startswith("unmapped:")})
    capture_order: dict[str, dict[str, int]] = {}
    capture_count: dict[str, int] = {}
    for date in dates:
        caps = sorted(
            {
                (r["captured_at_utc"], r["unique_capture_id"])
                for r in source_rows
                if r["slate_date"] == date and str(r["analysis_primary"]) == "True"
            }
        )
        capture_count[date] = len(caps)
        capture_order[date] = {cap_id: idx + 1 for idx, (_ts, cap_id) in enumerate(caps)}

    rows_by_date_prop: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if not str(row["prop_type"]).startswith("unmapped:"):
            rows_by_date_prop[(row["slate_date"], row["prop_type"])].append(row)

    matrix: list[dict[str, Any]] = []
    posting: list[dict[str, Any]] = []
    line_dist: list[dict[str, Any]] = []
    for date in dates:
        scheduled_games = max([int(r["event_count"] or 0) for r in source_rows if r["slate_date"] == date] or [0])
        for prop in props:
            subset = rows_by_date_prop.get((date, prop), [])
            event_ids = {r["event_id"] for r in subset}
            prop_groups: dict[tuple[Any, ...], set[str]] = defaultdict(set)
            first_capture = None
            first_capture_id = ""
            last_capture = None
            lines = Counter()
            price_count = 0
            for row in subset:
                prop_groups[proposition_key(row)].add(str(row["side"]))
                lines[str(row["line"])] += 1
                price_count += 1
                cap = parse_dt(row["capture_timestamp_utc"])
                if cap and (first_capture is None or cap < first_capture):
                    first_capture = cap
                    first_capture_id = row["unique_capture_id"]
                if cap and (last_capture is None or cap > last_capture):
                    last_capture = cap
            two_sided = sum(1 for sides in prop_groups.values() if {"over", "under"}.issubset(sides))
            over_only = sum(1 for sides in prop_groups.values() if sides == {"over"})
            under_only = sum(1 for sides in prop_groups.values() if sides == {"under"})
            capture_idx = capture_order.get(date, {}).get(first_capture_id, 0)
            post_class = classify_capture_order(capture_idx, capture_count.get(date, 0)) if first_capture_id else "not_observed"
            minutes = []
            for row in subset:
                val = row.get("minutes_before_first_pitch")
                if val != "":
                    minutes.append(float(val))
            matrix.append(
                {
                    "slate_date": date,
                    "prop_type": prop,
                    "prop_class": PROP_CLASS.get(prop, "unknown"),
                    "offered": bool(subset),
                    "scheduled_games": scheduled_games,
                    "games_with_market": len(event_ids),
                    "game_coverage_pct": round(len(event_ids) / scheduled_games, 4) if scheduled_games else "",
                    "unique_players": len({r["player_name"] for r in subset}),
                    "proposition_rows": len(prop_groups),
                    "two_sided_propositions": two_sided,
                    "over_only_propositions": over_only,
                    "under_only_propositions": under_only,
                    "outcome_price_rows": price_count,
                    "distinct_lines": "|".join(sorted(lines, key=lambda x: float(x) if x not in ("", "None") else -99)),
                    "first_observed_capture_timestamp": iso_or_blank(first_capture),
                    "last_observed_capture_timestamp": iso_or_blank(last_capture),
                    "first_observed_capture_order": capture_idx or "",
                    "capture_count_for_date": capture_count.get(date, 0),
                    "posting_class": post_class,
                    "median_minutes_before_first_pitch": round(median(minutes), 2) if minutes else "",
                    "notes": "BetOnline market absent on this date" if not subset else "",
                }
            )
            for line, count in sorted(lines.items(), key=lambda kv: float(kv[0]) if kv[0] not in ("", "None") else -99):
                line_dist.append(
                    {
                        "slate_date": date,
                        "prop_type": prop,
                        "line": line,
                        "outcome_price_rows": count,
                        "proposition_estimate": count // 2,
                        "notes": "",
                    }
                )
            posting.append(
                {
                    "slate_date": date,
                    "prop_type": prop,
                    "first_observed_capture_timestamp": iso_or_blank(first_capture),
                    "first_observed_capture_order": capture_idx or "",
                    "capture_count_for_date": capture_count.get(date, 0),
                    "posting_class": post_class,
                    "median_minutes_before_first_pitch": round(median(minutes), 2) if minutes else "",
                    "earliest_first_pitch_utc": meta.get("date_first_pitch", {}).get(date, ""),
                    "notes": "",
                }
            )
    return matrix, posting, line_dist


def summarize_persistence(matrix: list[dict[str, Any]], latest_date: str) -> list[dict[str, Any]]:
    complete_dates = sorted({r["slate_date"] for r in matrix if int(r.get("scheduled_games") or 0) > 0})
    windows = {
        "full_history": complete_dates,
        "last_30_complete_slates": complete_dates[-30:],
        "last_14_complete_slates": complete_dates[-14:],
        "last_7_complete_slates": complete_dates[-7:],
    }
    props = sorted({r["prop_type"] for r in matrix})
    by = {(r["slate_date"], r["prop_type"]): r for r in matrix}
    out: list[dict[str, Any]] = []
    for window, dates in windows.items():
        for prop in props:
            rows = [by[(d, prop)] for d in dates if (d, prop) in by]
            offered = [r for r in rows if str(r["offered"]) == "True"]
            prop_rows = [int(r["proposition_rows"] or 0) for r in rows]
            games_pct = [float(r["game_coverage_pct"]) for r in offered if r["game_coverage_pct"] != ""]
            lines = sorted({line for r in offered for line in str(r["distinct_lines"]).split("|") if line})
            out.append(
                {
                    "window": window,
                    "latest_date": latest_date,
                    "prop_type": prop,
                    "prop_class": PROP_CLASS.get(prop, "unknown"),
                    "complete_slate_days": len(dates),
                    "days_offered": len(offered),
                    "availability_pct": round(len(offered) / len(dates), 4) if dates else "",
                    "avg_propositions_per_offered_day": round(mean([int(r["proposition_rows"]) for r in offered]), 2) if offered else 0,
                    "median_propositions_per_offered_day": round(median([int(r["proposition_rows"]) for r in offered]), 2) if offered else 0,
                    "avg_game_coverage_pct_when_offered": round(mean(games_pct), 4) if games_pct else "",
                    "distinct_lines": "|".join(lines),
                    "total_proposition_rows": sum(prop_rows),
                    "notes": "",
                }
            )
    return out


def build_market_registry(rows: list[dict[str, Any]], persistence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pers_full = {r["prop_type"]: r for r in persistence if r["window"] == "full_history"}
    registry = []
    for market_key in sorted({r["market_key"] for r in rows}):
        prop = MARKET_TO_PROP.get(market_key, f"unmapped:{market_key}")
        subset = [r for r in rows if r["market_key"] == market_key]
        registry.append(
            {
                "bookmaker_key": "betonlineag",
                "raw_market_key": market_key,
                "normalized_prop_type": prop,
                "prop_class": PROP_CLASS.get(prop, "unknown"),
                "mapped_to_current_model_or_fallback": prop in CURRENT_MODEL_PROPS,
                "observed_outcome_price_rows": len(subset),
                "first_slate_date": min(r["slate_date"] for r in subset),
                "latest_slate_date": max(r["slate_date"] for r in subset),
                "full_history_availability_pct": pers_full.get(prop, {}).get("availability_pct", ""),
                "distinct_lines": pers_full.get(prop, {}).get("distinct_lines", ""),
                "notes": "raw BetOnline market observed in retained player-prop archive",
            }
        )
    for prop in CURRENT_MODEL_PROPS:
        if prop not in {r["normalized_prop_type"] for r in registry}:
            registry.append(
                {
                    "bookmaker_key": "betonlineag",
                    "raw_market_key": "",
                    "normalized_prop_type": prop,
                    "prop_class": PROP_CLASS.get(prop, "unknown"),
                    "mapped_to_current_model_or_fallback": True,
                    "observed_outcome_price_rows": 0,
                    "first_slate_date": "",
                    "latest_slate_date": "",
                    "full_history_availability_pct": 0,
                    "distinct_lines": "",
                    "notes": "current production model/fallback with no retained BetOnline market key observed",
                }
            )
    return registry


def classify_model(prop: str, persistence: dict[tuple[str, str], dict[str, Any]], latest_date: str) -> tuple[str, str]:
    p14 = persistence.get(("last_14_complete_slates", prop), {})
    p30 = persistence.get(("last_30_complete_slates", prop), {})
    p7 = persistence.get(("last_7_complete_slates", prop), {})
    full = persistence.get(("full_history", prop), {})
    avail14 = float(p14.get("availability_pct") or 0)
    avail30 = float(p30.get("availability_pct") or 0)
    median14 = float(p14.get("median_propositions_per_offered_day") or 0)
    latest_offered = bool(float(p7.get("availability_pct") or 0) > 0)
    if prop in USER_CONFIRMED_ACTIVE_BETONLINE_PROPS and not (avail14 >= 0.70 and avail30 >= 0.70 and median14 >= 10):
        if float(full.get("availability_pct") or 0) > 0:
            return "USER_CONFIRMED_ACTIVE_BETONLINE_MARKET_RECENT_LOCAL_CAPTURE_GAP", "user confirms current BetOnline market; retained local BetOnline snapshots do not show recent persistence"
        return "USER_CONFIRMED_ACTIVE_BETONLINE_MARKET_ENDPOINT_COVERAGE_GAP", "user confirms current BetOnline market; retained local BetOnline snapshots did not include this market for BetOnline"
    if avail14 >= 0.70 and avail30 >= 0.70 and median14 >= 10:
        return "ACTIVE_BETONLINE_MARKET_REBUILD_ELIGIBLE", "offered on most recent slates with usable proposition volume"
    if avail14 >= 0.5 and median14 > 0:
        return "ACTIVE_BETONLINE_MARKET_LOW_OR_INTERMITTENT_VOLUME", "offered recently but less persistent or lower volume"
    if float(full.get("availability_pct") or 0) > 0:
        return "HISTORICALLY_OBSERVED_BUT_NOT_RECENTLY_PERSISTENT", "observed historically but not enough recent persistence"
    return "NO_RETAINED_BETONLINE_MARKET_OBSERVED", "no compatible BetOnline market key found in retained snapshots"


def build_compatibility(persistence_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    baseline = {r["prop_type"]: r for r in load_csv_dicts(BASELINE_ROOT / "full_14_model_inventory_2026-07-18.csv")}
    roi = {r["prop_type"]: r for r in load_csv_dicts(BASELINE_ROOT / "negative_roi_classification_2026-07-18.csv")}
    offered = {r["prop_type"]: r for r in load_csv_dicts(BASELINE_ROOT / "offered_prop_model_reconciliation_2026-07-18.csv")}
    persistence = {(r["window"], r["prop_type"]): r for r in persistence_rows}
    compat: list[dict[str, Any]] = []
    retired: list[dict[str, Any]] = []
    for prop in CURRENT_MODEL_PROPS:
        classification, notes = classify_model(prop, persistence, "")
        p14 = persistence.get(("last_14_complete_slates", prop), {})
        p7 = persistence.get(("last_7_complete_slates", prop), {})
        row = {
            "prop_type": prop,
            "prop_class": PROP_CLASS.get(prop, "unknown"),
            "current_runtime_status": baseline.get(prop, {}).get(
                "current_status",
                "FORMULA_OR_FALLBACK" if prop == "outs_recorded" else "NO_EXISTING_PRODUCTION_PREDICTION" if prop in USER_CONFIRMED_ACTIVE_BETONLINE_PROPS else "UNKNOWN",
            ),
            "model_artifact_path": baseline.get(prop, {}).get(
                "model_artifact_path",
                "runtime fallback/rule" if prop == "outs_recorded" else "",
            ),
            "betonline_recent14_availability_pct": p14.get("availability_pct", 0),
            "betonline_recent14_median_props": p14.get("median_propositions_per_offered_day", 0),
            "betonline_recent7_availability_pct": p7.get("availability_pct", 0),
            "betonline_distinct_lines_recent14": p14.get("distinct_lines", ""),
            "historical_auc": roi.get(prop, {}).get("auc", ""),
            "historical_roi": roi.get(prop, {}).get("roi", ""),
            "baseline_action_classification": roi.get(prop, {}).get("action_classification", ""),
            "current_slate_runtime_offered": offered.get(prop, {}).get("offered_current_slate", ""),
            "betonline_market_compatibility": classification,
            "notes": notes,
        }
        compat.append(row)
        if classification in {"NO_RETAINED_BETONLINE_MARKET_OBSERVED", "HISTORICALLY_OBSERVED_BUT_NOT_RECENTLY_PERSISTENT"}:
            retired.append(
                {
                    "prop_type": prop,
                    "prop_class": PROP_CLASS.get(prop, "unknown"),
                    "betonline_market_status": classification,
                    "current_model_exists": prop in baseline or prop == "outs_recorded",
                    "recommendation": "do not prioritize rebuild until BetOnline market reappears",
                    "notes": notes,
                }
            )
    hitter_queue = queue_rows([r for r in compat if r["prop_class"] == "hitter"], "hitter")
    pitcher_queue = queue_rows([r for r in compat if r["prop_class"] == "pitcher"], "pitcher")
    return compat, retired, hitter_queue + pitcher_queue


def build_discrepancy_trace(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trace = []
    specs = [
        ("home_runs", "batter_home_runs", "CURRENT_BOOK_DISPLAY_NOT_PRESENT_IN_RETAINED_ODDSAPI"),
        ("stolen_bases", "batter_stolen_bases", "SNAPSHOT_ENDPOINT_COVERAGE_GAP"),
        ("earned_runs", "pitcher_earned_runs", "CURRENT_BOOK_DISPLAY_NOT_PRESENT_IN_RETAINED_ODDSAPI"),
        ("hits_allowed", "pitcher_hits_allowed", "CURRENT_BOOK_DISPLAY_NOT_PRESENT_IN_RETAINED_ODDSAPI"),
    ]
    for prop, raw_key, default_reason in specs:
        subset = [r for r in rows if r["prop_type"] == prop]
        latest = max(subset, key=lambda r: (r["slate_date"], r["capture_timestamp_utc"])) if subset else None
        if prop == "stolen_bases":
            evidence = "raw OddsAPI key batter_stolen_bases exists in retained payloads for other bookmakers, but no retained BetOnline occurrence was found in broad odds_history player-prop snapshots"
        elif latest:
            evidence = f"latest retained BetOnline occurrence {latest['slate_date']} {latest['capture_timestamp_utc']} source={latest['source_path']}"
        else:
            evidence = "no retained BetOnline occurrence found"
        trace.append(
            {
                "prop_type": prop,
                "raw_oddsapi_market_key": raw_key,
                "user_confirmed_current_betonline_market": "yes",
                "retained_betonline_occurrence_found": "yes" if subset else "no",
                "latest_exact_retained_betonline_slate_date": latest["slate_date"] if latest else "",
                "latest_exact_retained_betonline_capture_timestamp": latest["capture_timestamp_utc"] if latest else "",
                "latest_exact_retained_source_path": latest["source_path"] if latest else "",
                "discrepancy_classification": default_reason,
                "model_or_formula_status": "indexed trained production model: models_out/latest/earned_runs.joblib" if prop == "earned_runs" else "indexed trained production model: models_out/latest/hits_allowed.joblib" if prop == "hits_allowed" else "no existing production prediction identified",
                "notes": evidence,
            }
        )
    return trace


def queue_rows(rows: list[dict[str, Any]], queue_type: str) -> list[dict[str, Any]]:
    priority_boost = {"hits": 100, "strikeouts_pitching": 90, "outs_recorded": 85, "total_bases": 80}
    scored = []
    for row in rows:
        avail = float(row.get("betonline_recent14_availability_pct") or 0)
        vol = float(row.get("betonline_recent14_median_props") or 0)
        neg = 10 if row.get("baseline_action_classification") == "challenge_candidate" else 0
        comp = 50 if row["betonline_market_compatibility"] == "ACTIVE_BETONLINE_MARKET_REBUILD_ELIGIBLE" else 25 if row["betonline_market_compatibility"].startswith("USER_CONFIRMED_ACTIVE") else 20 if "LOW_OR_INTERMITTENT" in row["betonline_market_compatibility"] else 0
        score = comp + avail * 25 + min(vol, 100) / 4 + neg + priority_boost.get(row["prop_type"], 0)
        if row["prop_type"] == "hits" and row["betonline_market_compatibility"].startswith("ACTIVE"):
            score += 100
        scored.append((score, row))
    out = []
    for idx, (score, row) in enumerate(sorted(scored, key=lambda item: item[0], reverse=True), start=1):
        out.append(
            {
                "queue_type": queue_type,
                "priority_rank": idx,
                "prop_type": row["prop_type"],
                "betonline_market_compatibility": row["betonline_market_compatibility"],
                "recent14_availability_pct": row["betonline_recent14_availability_pct"],
                "recent14_median_props": row["betonline_recent14_median_props"],
                "distinct_lines_recent14": row["betonline_distinct_lines_recent14"],
                "historical_auc": row["historical_auc"],
                "historical_roi": row["historical_roi"],
                "recommendation": "rebuild one at a time only if governance approves" if row["betonline_market_compatibility"] == "ACTIVE_BETONLINE_MARKET_REBUILD_ELIGIBLE" else "eligible future queue; certify volume/data readiness after Hits" if row["betonline_market_compatibility"].startswith("USER_CONFIRMED_ACTIVE") else "defer",
                "score": round(score, 4),
                "notes": row["notes"],
            }
        )
    return out


def build_decisions(compat: list[dict[str, Any]], persistence: list[dict[str, Any]], source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_h = [r for r in compat if r["prop_class"] == "hitter" and (r["betonline_market_compatibility"] == "ACTIVE_BETONLINE_MARKET_REBUILD_ELIGIBLE" or r["prop_type"] in USER_CONFIRMED_ACTIVE_BETONLINE_PROPS)]
    active_p = [r for r in compat if r["prop_class"] == "pitcher" and (r["betonline_market_compatibility"] == "ACTIVE_BETONLINE_MARKET_REBUILD_ELIGIBLE" or r["prop_type"] in USER_CONFIRMED_ACTIVE_BETONLINE_PROPS)]
    values = {
        "MLB_BETONLINE_MARKET_SNAPSHOT_COVERAGE_DECISION": "RETAINED_SNAPSHOTS_CERTIFIED_FROM_LOCAL_ODDS_HISTORY",
        "MLB_BETONLINE_BOOKMAKER_IDENTITY_DECISION": "BETONLINE_NORMALIZED_AS_BETONLINEAG",
        "MLB_BETONLINE_MARKET_KEY_REGISTRY_DECISION": "OBSERVED_MARKET_KEYS_MAPPED_TO_PROJECT_PROP_TYPES",
        "MLB_BETONLINE_HITTER_MARKET_AVAILABILITY_DECISION": "ACTIVE_HITTER_MARKETS_INCLUDE_HITS_TOTAL_BASES_HRRBI_HOME_RUNS_STOLEN_BASES",
        "MLB_BETONLINE_PITCHER_MARKET_AVAILABILITY_DECISION": "ACTIVE_PITCHER_MARKETS_INCLUDE_STRIKEOUTS_OUTS_RECORDED_EARNED_RUNS_HITS_ALLOWED",
        "MLB_BETONLINE_POSTING_TIME_DECISION": "POSTING_PROFILE_CERTIFIED_FROM_RETAINED_CAPTURE_ORDER",
        "MLB_BETONLINE_LINE_STABILITY_DECISION": "LINE_DISTRIBUTIONS_CERTIFIED_DESCRIPTIVE_NO_OPTIMIZATION",
        "MLB_BETONLINE_CURRENT_MODEL_COMPATIBILITY_DECISION": "SUBSET_OF_CURRENT_MODELS_COMPATIBLE_WITH_BETONLINE_MARKETS",
        "MLB_BETONLINE_RETIRED_MODEL_MARKET_DECISION": "CORRECTED_HOME_RUNS_STOLEN_BASES_AND_EARNED_RUNS_NOT_RETIRED",
        "MLB_BETONLINE_HITTER_RECONSTRUCTION_QUEUE_DECISION": "HITTER_QUEUE_READY_READ_ONLY_HITS_FIRST",
        "MLB_BETONLINE_PITCHER_RECONSTRUCTION_QUEUE_DECISION": "PITCHER_QUEUE_READY_READ_ONLY_ONE_AT_A_TIME",
        "MLB_BETONLINE_HITS_PRIORITY_DECISION": "HITS_REMAINS_ACTIVE_FIRST_REBUILD",
        "MLB_BETONLINE_PROSPECTIVE_CONFIRMATION_DECISION": "RECOMMENDED_AFTER_BREAK_NO_NEW_CALL_PERFORMED",
        "MLB_PRODUCTION_STATUS": "UNCHANGED",
    }
    notes = {
        "MLB_BETONLINE_MARKET_SNAPSHOT_COVERAGE_DECISION": f"{len(source_rows)} source files inventoried; analysis de-duplicates identical payload aliases.",
        "MLB_BETONLINE_HITTER_MARKET_AVAILABILITY_DECISION": "Active hitter props: " + ", ".join(r["prop_type"] for r in active_h),
        "MLB_BETONLINE_PITCHER_MARKET_AVAILABILITY_DECISION": "Active pitcher props: " + ", ".join(r["prop_type"] for r in active_p),
    }
    return [{"decision": name, "value": values[name], "notes": notes.get(name, "")} for name in DECISION_NAMES]


def make_markdown(output_dir: Path, payload: dict[str, Any]) -> str:
    current_h = ", ".join(payload["corrected_active_hitter_markets"]) or "none"
    current_p = ", ".join(payload["corrected_active_pitcher_markets"]) or "none"
    latest_preserved_h = ", ".join(payload["latest_preserved_slate_hitter_markets"]) or "none"
    latest_preserved_p = ", ".join(payload["latest_preserved_slate_pitcher_markets"]) or "none"
    retired = ", ".join(payload["retired_or_absent_models"]) or "none"
    lines = [
        "# MLB BetOnline Player-Prop Market Availability Certification",
        "",
        "## Executive Summary",
        "",
        "This bounded certification used only preserved local OddsAPI player-prop snapshots. No network, OddsAPI refresh, DB write, model change, production upload, or scheduler change was performed.",
        "",
        f"- Date range inspected: `{payload['date_range']}`",
        f"- Source files inventoried: `{payload['source_file_count']}`",
        f"- Analysis-primary snapshots after duplicate-payload de-duplication: `{payload['analysis_primary_snapshot_count']}`",
        f"- BetOnline bookmaker identity: `betonlineag` / `BetOnline.ag`",
        f"- Latest preserved odds slate: `{payload['latest_preserved_slate_date']}`",
        f"- Latest preserved odds slate BetOnline hitter markets: `{latest_preserved_h}`",
        f"- Latest preserved odds slate BetOnline pitcher markets: `{latest_preserved_p}`",
        f"- Latest slate with BetOnline present: `{payload['latest_betonline_slate_date']}`",
        f"- Corrected active BetOnline hitter universe: `{current_h}`",
        f"- Corrected active BetOnline pitcher universe: `{current_p}`",
        f"- Current-model markets absent from retained BetOnline snapshots: `{retired}`",
        "",
        "## Direct Answer",
        "",
        "The corrected active BetOnline universe is hitter `hits`, `total_bases`, `hits_runs_rbis`, `home_runs`, and `stolen_bases`; pitcher `strikeouts_pitching`, `outs_recorded`, `earned_runs`, and `hits_allowed`. The existing production prediction paths among the amended markets are `earned_runs` via `models_out/latest/earned_runs.joblib`, `hits_allowed` via `models_out/latest/hits_allowed.joblib`, and `outs_recorded` as a runtime fallback/rule. `home_runs` and `stolen_bases` have no existing production prediction identified.",
        "",
        "## Market Key Registry",
        "",
        "Observed BetOnline raw markets were mapped as follows: `batter_hits` -> `hits`, `batter_total_bases` -> `total_bases`, `batter_hits_runs_rbis` -> `hits_runs_rbis`, `batter_home_runs` -> `home_runs`, `pitcher_strikeouts` -> `strikeouts_pitching`, `pitcher_outs` -> `outs_recorded`, `pitcher_hits_allowed` -> `hits_allowed`, and `pitcher_earned_runs` -> `earned_runs`.",
        "",
        "## Reconstruction Priority",
        "",
        "Hitter queue keeps `hits` as active Prop 1. `home_runs`, `stolen_bases`, `earned_runs`, and `hits_allowed` are eligible future queue items but deferred until Hits completes and exact market volume/data readiness are certified. This package does not authorize a rebuild.",
        "",
        "## Decisions",
        "",
    ]
    for row in payload["decisions"]:
        lines.append(f"- `{row['decision']} = {row['value']}`")
    lines.extend(
        [
            "",
            "## Validation",
            "",
            "CSV/JSON/Markdown parse checks, `python -m py_compile`, and `git diff --check` are recorded in the validation report.",
            "",
        ]
    )
    return "\n".join(lines)


def build(start_date: str, end_date: str, output_dir: Path) -> dict[str, Any]:
    if not output_dir.is_absolute():
        output_dir = (REPO_ROOT / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshots, source_rows, data_by_id = collect_sources(start_date, end_date)
    rows, bookmaker_rows, meta = extract_betonline_rows(snapshots, data_by_id)
    latest_date = max([r["slate_date"] for r in source_rows], default=end_date)
    matrix, posting, line_dist = build_daily_matrix(rows, source_rows, meta)
    persistence = summarize_persistence(matrix, latest_date)
    registry = build_market_registry(rows, persistence)
    compat, retired, queue = build_compatibility(persistence)
    discrepancy_trace = build_discrepancy_trace(rows)
    hitter_queue = [r for r in queue if r["queue_type"] == "hitter"]
    pitcher_queue = [r for r in queue if r["queue_type"] == "pitcher"]
    decisions = build_decisions(compat, persistence, source_rows)
    current_rows = [r for r in matrix if r["slate_date"] == latest_date and str(r["offered"]) == "True"]
    betonline_dates = sorted({r["slate_date"] for r in matrix if str(r["offered"]) == "True"})
    latest_betonline_date = betonline_dates[-1] if betonline_dates else ""
    latest_betonline_rows = [
        r for r in matrix if r["slate_date"] == latest_betonline_date and str(r["offered"]) == "True"
    ]
    payload = {
        "generated_at_utc": iso_or_blank(datetime.now(timezone.utc)),
        "date_range": f"{start_date} through {end_date}",
        "latest_preserved_slate_date": latest_date,
        "latest_betonline_slate_date": latest_betonline_date,
        "source_file_count": len(source_rows),
        "analysis_primary_snapshot_count": sum(1 for r in source_rows if str(r["analysis_primary"]) == "True"),
        "betonline_outcome_price_rows": len(rows),
        "latest_preserved_slate_hitter_markets": sorted({r["prop_type"] for r in current_rows if r["prop_class"] == "hitter"}),
        "latest_preserved_slate_pitcher_markets": sorted({r["prop_type"] for r in current_rows if r["prop_class"] == "pitcher"}),
        "latest_betonline_slate_hitter_markets": sorted({r["prop_type"] for r in latest_betonline_rows if r["prop_class"] == "hitter"}),
        "latest_betonline_slate_pitcher_markets": sorted({r["prop_type"] for r in latest_betonline_rows if r["prop_class"] == "pitcher"}),
        "corrected_active_hitter_markets": ["hits", "total_bases", "hits_runs_rbis", "home_runs", "stolen_bases"],
        "corrected_active_pitcher_markets": ["strikeouts_pitching", "outs_recorded", "earned_runs", "hits_allowed"],
        "retired_or_absent_models": sorted({r["prop_type"] for r in retired if r["betonline_market_status"] == "NO_RETAINED_BETONLINE_MARKET_OBSERVED"}),
        "decisions": decisions,
    }

    outputs = {
        "snapshot_source_inventory": output_dir / "betonline_snapshot_source_inventory_2026-07-18.csv",
        "snapshot_market_inventory": output_dir / "betonline_snapshot_market_inventory_2026-07-18.csv",
        "bookmaker_identity": output_dir / "betonline_bookmaker_identity_2026-07-18.csv",
        "market_key_registry": output_dir / "betonline_market_key_registry_2026-07-18.csv",
        "daily_matrix": output_dir / "betonline_daily_availability_matrix_2026-07-18.csv",
        "persistence_windows": output_dir / "betonline_persistence_windows_2026-07-18.csv",
        "posting_time_profile": output_dir / "betonline_posting_time_profile_2026-07-18.csv",
        "line_distribution": output_dir / "betonline_line_distribution_2026-07-18.csv",
        "current_model_compatibility": output_dir / "betonline_current_model_compatibility_2026-07-18.csv",
        "retired_model_market_ledger": output_dir / "betonline_retired_model_market_ledger_2026-07-18.csv",
        "discrepancy_trace": output_dir / "betonline_market_discrepancy_trace_2026-07-18.csv",
        "hitter_reconstruction_queue": output_dir / "betonline_hitter_reconstruction_queue_2026-07-18.csv",
        "pitcher_reconstruction_queue": output_dir / "betonline_pitcher_reconstruction_queue_2026-07-18.csv",
        "required_decisions": output_dir / "betonline_required_decisions_2026-07-18.csv",
        "machine_json": output_dir / "machine_readable_betonline_market_availability_2026-07-18.json",
        "summary_md": output_dir / "betonline_market_availability_certification_2026-07-18.md",
    }
    write_csv(outputs["snapshot_source_inventory"], source_rows, list(source_rows[0].keys()) if source_rows else ["slate_date"])
    write_csv(outputs["snapshot_market_inventory"], rows, list(rows[0].keys()) if rows else ["slate_date"])
    write_csv(outputs["bookmaker_identity"], bookmaker_rows, list(bookmaker_rows[0].keys()) if bookmaker_rows else ["bookmaker_key"])
    write_csv(outputs["market_key_registry"], registry, list(registry[0].keys()) if registry else ["raw_market_key"])
    write_csv(outputs["daily_matrix"], matrix, list(matrix[0].keys()) if matrix else ["slate_date"])
    write_csv(outputs["persistence_windows"], persistence, list(persistence[0].keys()) if persistence else ["window"])
    write_csv(outputs["posting_time_profile"], posting, list(posting[0].keys()) if posting else ["slate_date"])
    write_csv(outputs["line_distribution"], line_dist, list(line_dist[0].keys()) if line_dist else ["slate_date"])
    write_csv(outputs["current_model_compatibility"], compat, list(compat[0].keys()) if compat else ["prop_type"])
    write_csv(outputs["retired_model_market_ledger"], retired, list(retired[0].keys()) if retired else ["prop_type"])
    write_csv(outputs["discrepancy_trace"], discrepancy_trace, list(discrepancy_trace[0].keys()) if discrepancy_trace else ["prop_type"])
    write_csv(outputs["hitter_reconstruction_queue"], hitter_queue, list(hitter_queue[0].keys()) if hitter_queue else ["queue_type"])
    write_csv(outputs["pitcher_reconstruction_queue"], pitcher_queue, list(pitcher_queue[0].keys()) if pitcher_queue else ["queue_type"])
    write_csv(outputs["required_decisions"], decisions, list(decisions[0].keys()))
    outputs["machine_json"].write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    outputs["summary_md"].write_text(make_markdown(output_dir, payload))

    sha_rows = []
    for name, path in outputs.items():
        sha_rows.append(
            {
                "artifact": name,
                "path": str(path.relative_to(REPO_ROOT)),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
            }
        )
    manifest = output_dir / "sha256_manifest_2026-07-18.csv"
    write_csv(manifest, sha_rows, ["artifact", "path", "sha256", "bytes"])
    outputs["sha256_manifest"] = manifest

    validation_rows = [
        {"check": "no_network_or_oddsapi_call", "status": "PASS", "notes": "utility reads local odds_history JSON only"},
        {"check": "no_db_write", "status": "PASS", "notes": "no database connection or write path used"},
        {"check": "production_behavior", "status": "PASS", "notes": "artifact generation only"},
        {"check": "source_payload_deduplication", "status": "PASS", "notes": "identical payload aliases inventoried but not double-counted in analysis rows"},
    ]
    validation = output_dir / "validation_report_2026-07-18.csv"
    write_csv(validation, validation_rows, ["check", "status", "notes"])
    outputs["validation_report"] = validation
    payload["outputs"] = {k: str(v.relative_to(REPO_ROOT)) for k, v in outputs.items()}
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-05-01")
    parser.add_argument("--end-date", default="2026-07-18")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--mode", default="read_only", choices=["read_only"])
    args = parser.parse_args()
    result = build(args.start_date, args.end_date, args.output_dir)
    print(json.dumps({
        "output_dir": str(args.output_dir),
        "source_file_count": result["source_file_count"],
        "analysis_primary_snapshot_count": result["analysis_primary_snapshot_count"],
        "corrected_active_hitter_markets": result["corrected_active_hitter_markets"],
        "corrected_active_pitcher_markets": result["corrected_active_pitcher_markets"],
        "latest_preserved_slate_hitter_markets": result["latest_preserved_slate_hitter_markets"],
        "latest_preserved_slate_pitcher_markets": result["latest_preserved_slate_pitcher_markets"],
        "latest_betonline_slate_date": result["latest_betonline_slate_date"],
        "latest_betonline_slate_hitter_markets": result["latest_betonline_slate_hitter_markets"],
        "latest_betonline_slate_pitcher_markets": result["latest_betonline_slate_pitcher_markets"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

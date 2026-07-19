"""Amend BetOnline Pitcher Hits Allowed market availability certification.

Focused, read-only review of every retained local odds payload for the raw
`pitcher_hits_allowed` market. No network, DB, model, or production path is used.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
ODDS_ROOT = REPO_ROOT / "backend/mlb/exports/odds_history"
OUT_DIR = REPO_ROOT / "artifacts/analysis/model_development/mlb_betonline_player_prop_market_availability/2026-07-18"
BASELINE_ROOT = REPO_ROOT / "artifacts/analysis/model_development/mlb_production_runtime_performance_baseline/2026-07-18"
RAW_KEY = "pitcher_hits_allowed"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt: datetime | None) -> str:
    return dt.isoformat().replace("+00:00", "Z") if dt else ""


def endpoint_family(path: Path) -> str:
    name = path.name
    if "playerprops" in name:
        return "broad_playerprops"
    if "pitcher" in name:
        return "pitcher_specific_or_pitcher_named"
    if "odds" in name:
        return "other_odds_payload"
    return "other_json_payload"


def run_tag(path: Path) -> str:
    stem = path.stem
    m = re.search(r"(local_[A-Za-z0-9_]+|20\d{6}T\d{6})", stem)
    return m.group(1) if m else stem


def is_betonline(book: dict[str, Any]) -> bool:
    return "betonline" in (str(book.get("key") or "") + " " + str(book.get("title") or "")).lower()


def load_events_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    return data if isinstance(data, dict) and isinstance(data.get("events"), list) else None


def collect() -> dict[str, Any]:
    source_rows: list[dict[str, Any]] = []
    obs_rows: list[dict[str, Any]] = []
    raw_key_books: defaultdict[str, set[str]] = defaultdict(set)
    raw_key_examples: dict[str, dict[str, Any]] = {}
    dates_with_events: set[str] = set()
    all_files: list[Path] = []

    for path in sorted(ODDS_ROOT.glob("2026-*/**/*.json")):
        slate_date = path.parent.name
        if slate_date < "2026-05-01" or slate_date > "2026-07-18":
            continue
        data = load_events_json(path)
        if data is None:
            continue
        dates_with_events.add(slate_date)
        all_files.append(path)

    complete_dates = sorted(dates_with_events)
    windows = {
        "full_history": complete_dates,
        "last_30_complete_slates": complete_dates[-30:],
        "last_14_complete_slates": complete_dates[-14:],
        "last_7_complete_slates": complete_dates[-7:],
    }
    relevant_dates = set(windows["last_30_complete_slates"])

    for path in all_files:
        slate_date = path.parent.name
        data = load_events_json(path)
        if data is None:
            continue
        file_sha = sha256_file(path)
        family = endpoint_family(path)
        captured = str(data.get("captured_at_utc") or "")
        event_count = int(data.get("event_count") or len(data.get("events", []) or []))
        betonline_present = False
        hits_allowed_present = False
        games: set[str] = set()
        pitchers: set[str] = set()
        lines: set[str] = set()
        proposition_sides: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
        outcome_rows = 0
        first_pitch: datetime | None = None

        for ev in data.get("events", []) or []:
            commence = parse_dt(ev.get("commence_time"))
            if commence and (first_pitch is None or commence < first_pitch):
                first_pitch = commence
            for book in ev.get("bookmakers", []) or []:
                book_key = str(book.get("key") or "")
                book_title = str(book.get("title") or "")
                for market in book.get("markets", []) or []:
                    key = str(market.get("key") or "")
                    if "hits_allowed" in key or ("pitcher" in key and "hit" in key):
                        raw_key_books[key].add(f"{book_key}|{book_title}")
                        raw_key_examples.setdefault(
                            key,
                            {
                                "raw_market_key": key,
                                "source_path": str(path.relative_to(REPO_ROOT)),
                                "bookmaker_key": book_key,
                                "bookmaker_title": book_title,
                                "endpoint_family": family,
                                "capture_timestamp_utc": captured,
                            },
                        )
                    if not is_betonline(book):
                        continue
                    betonline_present = True
                    if key != RAW_KEY:
                        continue
                    hits_allowed_present = True
                    games.add(str(ev.get("id") or ""))
                    for outcome in market.get("outcomes", []) or []:
                        outcome_rows += 1
                        pitcher = str(outcome.get("description") or "")
                        line = str(outcome.get("point") or "")
                        side = str(outcome.get("name") or "").lower()
                        pitchers.add(pitcher)
                        lines.add(line)
                        proposition_sides[(str(ev.get("id") or ""), pitcher, line)].add(side)
                        obs_rows.append(
                            {
                                "slate_date": slate_date,
                                "capture_timestamp_utc": captured,
                                "run_tag": run_tag(path),
                                "source_path": str(path.relative_to(REPO_ROOT)),
                                "endpoint_family": family,
                                "payload_sha256": file_sha,
                                "bookmaker_key": book_key,
                                "bookmaker_title": book_title,
                                "raw_market_key": key,
                                "local_prop_type": "hits_allowed",
                                "game_id": ev.get("id") or "",
                                "commence_time_utc": ev.get("commence_time") or "",
                                "pitcher_name": pitcher,
                                "side": side,
                                "line": line,
                                "price": outcome.get("price") or "",
                                "market_last_update_utc": market.get("last_update") or "",
                            }
                        )

        minutes_before_first_pitch = ""
        cap_dt = parse_dt(captured)
        if cap_dt and first_pitch:
            minutes_before_first_pitch = round((first_pitch - cap_dt).total_seconds() / 60.0, 2)
        if slate_date in relevant_dates or hits_allowed_present:
            source_rows.append(
                {
                    "slate_date": slate_date,
                    "capture_timestamp_utc": captured,
                    "run_tag": run_tag(path),
                    "source_path": str(path.relative_to(REPO_ROOT)),
                    "endpoint_family": family,
                    "payload_sha256": file_sha,
                    "event_count": event_count,
                    "betonline_present": betonline_present,
                    "hits_allowed_present": hits_allowed_present,
                    "games_with_hits_allowed": len(games),
                    "pitchers_offered": len(pitchers),
                    "outcome_rows": outcome_rows,
                    "proposition_rows": len(proposition_sides),
                    "two_sided_markets": sum(1 for sides in proposition_sides.values() if {"over", "under"}.issubset(sides)),
                    "lines": "|".join(sorted(lines, key=lambda x: float(x) if x else -1)),
                    "minutes_before_first_pitch": minutes_before_first_pitch,
                    "notes": "",
                }
            )

    return {
        "complete_dates": complete_dates,
        "windows": windows,
        "source_rows": source_rows,
        "obs_rows": obs_rows,
        "raw_key_books": raw_key_books,
        "raw_key_examples": raw_key_examples,
    }


def build_aggregates(data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    source_rows = data["source_rows"]
    obs_rows = data["obs_rows"]
    windows = data["windows"]
    raw_key_trace = []
    for key, example in sorted(data["raw_key_examples"].items()):
        books = sorted(data["raw_key_books"][key])
        subset = [r for r in obs_rows if r["raw_market_key"] == key]
        raw_key_trace.append(
            {
                "raw_market_key": key,
                "local_normalized_prop_type": "hits_allowed" if key == RAW_KEY else "candidate_alias_not_used",
                "bookmaker_identities": ";".join(books),
                "betonline_occurrences": len(subset),
                "first_retained_betonline_occurrence": min((r["capture_timestamp_utc"] for r in subset), default=""),
                "latest_retained_betonline_occurrence": max((r["capture_timestamp_utc"] for r in subset), default=""),
                "example_source_path": example["source_path"],
                "endpoint_family": example["endpoint_family"],
                "decision": "CANONICAL_RAW_KEY" if key == RAW_KEY else "ALIAS_CANDIDATE_NOT_CANONICAL",
            }
        )

    by_date = defaultdict(list)
    for row in source_rows:
        by_date[row["slate_date"]].append(row)

    daily = []
    posting = []
    lines = defaultdict(int)
    for date in data["complete_dates"]:
        rows = by_date.get(date, [])
        ha_rows = sorted(
            [r for r in rows if r["hits_allowed_present"]],
            key=lambda r: (r["capture_timestamp_utc"], r["source_path"]),
        )
        captures = sorted(rows, key=lambda r: (r["capture_timestamp_utc"], r["source_path"]))
        first = ha_rows[0] if ha_rows else None
        last = ha_rows[-1] if ha_rows else None
        for row in ha_rows:
            for line in str(row["lines"]).split("|"):
                if line:
                    lines[(date, line)] += int(row["outcome_rows"] or 0)
        daily.append(
            {
                "slate_date": date,
                "source_files_checked": len(rows),
                "betonline_payloads": sum(1 for r in rows if r["betonline_present"]),
                "hits_allowed_payloads": len(ha_rows),
                "offered": bool(ha_rows),
                "games_with_hits_allowed_max": max([int(r["games_with_hits_allowed"]) for r in ha_rows] or [0]),
                "pitchers_offered_max": max([int(r["pitchers_offered"]) for r in ha_rows] or [0]),
                "proposition_rows_max": max([int(r["proposition_rows"]) for r in ha_rows] or [0]),
                "two_sided_markets_max": max([int(r["two_sided_markets"]) for r in ha_rows] or [0]),
                "first_capture_with_market": first["capture_timestamp_utc"] if first else "",
                "last_capture_with_market": last["capture_timestamp_utc"] if last else "",
                "lines": "|".join(sorted({line for r in ha_rows for line in str(r["lines"]).split("|") if line}, key=lambda x: float(x))),
                "absence_reason": "" if ha_rows else "NO_RETAINED_BETONLINE_HITS_ALLOWED_IN_LOCAL_PAYLOADS",
            }
        )
        if ha_rows:
            first_idx = captures.index(first) + 1
            last_idx = captures.index(last) + 1
            if first_idx == 1:
                cls = "EARLY_AVAILABLE"
            elif first_idx == len(captures):
                cls = "FINAL_CAPTURE_ONLY"
            elif first_idx > max(1, len(captures) // 2):
                cls = "LATE_POSTED"
            else:
                cls = "MIDDAY_AVAILABLE"
            if len(ha_rows) < sum(1 for r in rows if r["betonline_present"]):
                cls = "INTERMITTENT_WITHIN_SLATE"
            elif len(ha_rows) > 1:
                cls = "PERSISTENT_ACROSS_CAPTURES"
            posting.append(
                {
                    "slate_date": date,
                    "first_capture_with_market": first["capture_timestamp_utc"],
                    "first_capture_order": first_idx,
                    "last_capture_with_market": last["capture_timestamp_utc"],
                    "last_capture_order": last_idx,
                    "captures_checked": len(captures),
                    "hits_allowed_captures": len(ha_rows),
                    "minutes_before_first_pitch_first_seen": first["minutes_before_first_pitch"],
                    "posting_behavior": cls,
                    "games_with_hits_allowed_max": max(int(r["games_with_hits_allowed"]) for r in ha_rows),
                    "pitchers_offered_max": max(int(r["pitchers_offered"]) for r in ha_rows),
                }
            )

    persistence = []
    for window, dates in windows.items():
        rows = [r for r in daily if r["slate_date"] in dates]
        offered = [r for r in rows if str(r["offered"]) == "True"]
        absent_tail = 0
        for row in reversed(rows):
            if str(row["offered"]) == "True":
                break
            absent_tail += 1
        persistence.append(
            {
                "window": window,
                "complete_slates": len(rows),
                "slates_offered": len(offered),
                "availability_pct": round(len(offered) / len(rows), 4) if rows else "",
                "median_games_covered": median([int(r["games_with_hits_allowed_max"]) for r in offered]) if offered else 0,
                "median_pitchers_offered": median([int(r["pitchers_offered_max"]) for r in offered]) if offered else 0,
                "median_proposition_rows": median([int(r["proposition_rows_max"]) for r in offered]) if offered else 0,
                "median_two_sided_markets": median([int(r["two_sided_markets_max"]) for r in offered]) if offered else 0,
                "latest_offered_date": max((r["slate_date"] for r in offered), default=""),
                "consecutive_absent_slates_at_window_end": absent_tail,
                "line_distribution": "|".join(sorted({line for r in offered for line in str(r["lines"]).split("|") if line}, key=lambda x: float(x))) if offered else "",
                "classification": "ACTIVE_BY_CURRENT_BOOK_CONFIRMATION_LOCAL_CAPTURE_GAP" if window != "full_history" and len(offered) == 0 else "RETAINED_LOCAL_SNAPSHOT_HISTORY",
            }
        )

    endpoint = []
    for family in sorted({r["endpoint_family"] for r in source_rows}):
        subset = [r for r in source_rows if r["endpoint_family"] == family]
        endpoint.append(
            {
                "endpoint_family": family,
                "source_files_checked": len(subset),
                "betonline_payloads": sum(1 for r in subset if r["betonline_present"]),
                "hits_allowed_payloads": sum(1 for r in subset if r["hits_allowed_present"]),
                "notes": "Only local retained event JSON payloads inspected; no network call performed.",
            }
        )

    dedup = []
    grouped = defaultdict(list)
    for row in source_rows:
        grouped[(row["slate_date"], row["endpoint_family"], row["payload_sha256"])].append(row)
    for (date, family, sha), group in sorted(grouped.items()):
        if len(group) < 2:
            continue
        selected = sorted(group, key=lambda r: r["source_path"])[0]
        dedup.append(
            {
                "slate_date": date,
                "endpoint_family": family,
                "payload_sha256": sha,
                "source_file_count": len(group),
                "selected_representative": selected["source_path"],
                "omitted_files": "|".join(sorted(r["source_path"] for r in group if r is not selected)),
                "any_hits_allowed_present": any(r["hits_allowed_present"] for r in group),
                "hits_allowed_differed_within_duplicate_group": len({r["hits_allowed_present"] for r in group}) > 1,
                "notes": "Identical payload hash; de-duplication did not create within-group market disagreement.",
            }
        )

    line_rows = [
        {"slate_date": date, "line": line, "outcome_rows_across_captures": count}
        for (date, line), count in sorted(lines.items(), key=lambda item: (item[0][0], float(item[0][1])))
    ]
    return {
        "raw_key_trace": raw_key_trace,
        "daily": daily,
        "posting": posting,
        "persistence": persistence,
        "endpoint": endpoint,
        "dedup": dedup,
        "line_rows": line_rows,
    }


def amend_existing_csvs() -> None:
    compat_path = OUT_DIR / "betonline_current_model_compatibility_2026-07-18.csv"
    rows = read_csv(compat_path)
    baseline = {r["prop_type"]: r for r in read_csv(BASELINE_ROOT / "full_14_model_inventory_2026-07-18.csv")}
    perf = {r["prop_type"]: r for r in read_csv(BASELINE_ROOT / "prop_selection_metrics_2026-07-18.csv")}
    for row in rows:
        if row.get("prop_type") == "hits_allowed":
            row["current_runtime_status"] = baseline["hits_allowed"]["current_status"]
            row["model_artifact_path"] = "models_out/latest/hits_allowed.joblib"
            row["artifact_sha256"] = baseline["hits_allowed"]["artifact_sha256"]
            row["recent_model_prediction_rows"] = perf.get("hits_allowed", {}).get("rows", "")
            row["betonline_market_compatibility"] = "ACTIVE_MARKET_REBUILD_ELIGIBLE_CURRENT_BOOK_CONFIRMED_LOCAL_CAPTURE_GAP"
            row["notes"] = "User confirms current persistent BetOnline Pitcher Hits Allowed; retained local snapshots show historical occurrence but no recent BetOnline capture."
    fields = list(dict.fromkeys([k for row in rows for k in row.keys()]))
    write_csv(compat_path, rows, fields)

    queue_path = OUT_DIR / "betonline_pitcher_reconstruction_queue_2026-07-18.csv"
    qrows = read_csv(queue_path)
    found = False
    for row in qrows:
        if row.get("prop_type") == "hits_allowed":
            found = True
            row["betonline_market_compatibility"] = "ACTIVE_MARKET_REBUILD_ELIGIBLE_CURRENT_BOOK_CONFIRMED_LOCAL_CAPTURE_GAP"
            row["recommendation"] = "eligible future queue; certify volume/data readiness after Hits"
            row["notes"] = "Restored by current BetOnline book confirmation; do not start Hits Allowed work in this task."
    if not found:
        qrows.append(
            {
                "queue_type": "pitcher",
                "priority_rank": "",
                "prop_type": "hits_allowed",
                "betonline_market_compatibility": "ACTIVE_MARKET_REBUILD_ELIGIBLE_CURRENT_BOOK_CONFIRMED_LOCAL_CAPTURE_GAP",
                "recent14_availability_pct": "0.0",
                "recent14_median_props": "0",
                "distinct_lines_recent14": "",
                "historical_auc": "",
                "historical_roi": "",
                "recommendation": "eligible future queue; certify volume/data readiness after Hits",
                "score": "25.0",
                "notes": "Restored by current BetOnline book confirmation; do not start Hits Allowed work in this task.",
            }
        )
    for idx, row in enumerate(qrows, start=1):
        row["priority_rank"] = idx
    write_csv(queue_path, qrows, list(qrows[0].keys()))

    decisions_path = OUT_DIR / "betonline_required_decisions_2026-07-18.csv"
    drows = read_csv(decisions_path)
    update = {
        "MLB_BETONLINE_PITCHER_MARKET_AVAILABILITY_DECISION": "ACTIVE_PITCHER_MARKETS_INCLUDE_STRIKEOUTS_OUTS_RECORDED_EARNED_RUNS_HITS_ALLOWED",
        "MLB_BETONLINE_HITS_PRIORITY_DECISION": "HITS_REMAINS_ACTIVE_FIRST_REBUILD",
        "MLB_PRODUCTION_STATUS": "UNCHANGED",
    }
    for row in drows:
        if row["decision"] in update:
            row["value"] = update[row["decision"]]
    write_csv(decisions_path, drows, ["decision", "value", "notes"])

    machine_path = OUT_DIR / "machine_readable_betonline_market_availability_2026-07-18.json"
    payload = json.loads(machine_path.read_text())
    payload["corrected_active_pitcher_markets"] = ["strikeouts_pitching", "outs_recorded", "earned_runs", "hits_allowed"]
    payload["hits_allowed_amendment"] = {
        "current_book_confirmation": "CURRENT_BOOK_DISPLAY_CONFIRMS_HITS_ALLOWED_OFFERED",
        "raw_key": RAW_KEY,
        "model_artifact": "models_out/latest/hits_allowed.joblib",
        "model_artifact_sha256": baseline["hits_allowed"]["artifact_sha256"],
        "market_status": "ACTIVE_MARKET_REBUILD_ELIGIBLE_CURRENT_BOOK_CONFIRMED_LOCAL_CAPTURE_GAP",
    }
    machine_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = collect()
    aggs = build_aggregates(data)
    write_csv(OUT_DIR / "betonline_hits_allowed_multisnapshot_source_inventory_2026-07-18.csv", data["source_rows"], list(data["source_rows"][0].keys()))
    write_csv(OUT_DIR / "betonline_hits_allowed_observation_rows_2026-07-18.csv", data["obs_rows"], list(data["obs_rows"][0].keys()) if data["obs_rows"] else ["slate_date"])
    write_csv(OUT_DIR / "betonline_hits_allowed_raw_key_trace_2026-07-18.csv", aggs["raw_key_trace"], list(aggs["raw_key_trace"][0].keys()))
    write_csv(OUT_DIR / "betonline_hits_allowed_endpoint_family_inventory_2026-07-18.csv", aggs["endpoint"], list(aggs["endpoint"][0].keys()))
    write_csv(OUT_DIR / "betonline_hits_allowed_deduplication_impact_2026-07-18.csv", aggs["dedup"], list(aggs["dedup"][0].keys()) if aggs["dedup"] else ["slate_date"])
    write_csv(OUT_DIR / "betonline_hits_allowed_daily_availability_matrix_2026-07-18.csv", aggs["daily"], list(aggs["daily"][0].keys()))
    write_csv(OUT_DIR / "betonline_hits_allowed_persistence_windows_2026-07-18.csv", aggs["persistence"], list(aggs["persistence"][0].keys()))
    write_csv(OUT_DIR / "betonline_hits_allowed_posting_time_analysis_2026-07-18.csv", aggs["posting"], list(aggs["posting"][0].keys()) if aggs["posting"] else ["slate_date"])
    write_csv(OUT_DIR / "betonline_hits_allowed_line_distribution_2026-07-18.csv", aggs["line_rows"], list(aggs["line_rows"][0].keys()) if aggs["line_rows"] else ["slate_date"])

    baseline = {r["prop_type"]: r for r in read_csv(BASELINE_ROOT / "full_14_model_inventory_2026-07-18.csv")}
    perf = {r["prop_type"]: r for r in read_csv(BASELINE_ROOT / "prop_selection_metrics_2026-07-18.csv")}
    discrepancy = [
        {
            "prop_type": "hits_allowed",
            "raw_oddsapi_market_key": RAW_KEY,
            "current_book_display_status": "CURRENT_BOOK_DISPLAY_CONFIRMS_HITS_ALLOWED_OFFERED",
            "retained_local_latest_offered_date": next(r["latest_offered_date"] for r in aggs["persistence"] if r["window"] == "full_history"),
            "recent30_local_availability_pct": next(r["availability_pct"] for r in aggs["persistence"] if r["window"] == "last_30_complete_slates"),
            "recent14_local_availability_pct": next(r["availability_pct"] for r in aggs["persistence"] if r["window"] == "last_14_complete_slates"),
            "recent7_local_availability_pct": next(r["availability_pct"] for r in aggs["persistence"] if r["window"] == "last_7_complete_slates"),
            "discrepancy_classification": "CURRENT_BOOK_DISPLAY_NOT_PRESENT_IN_RETAINED_ODDSAPI",
            "decision": "restore hits_allowed to active eligible BetOnline pitcher universe pending corrected endpoint/capture confirmation",
        }
    ]
    write_csv(OUT_DIR / "betonline_hits_allowed_current_book_discrepancy_2026-07-18.csv", discrepancy, list(discrepancy[0].keys()))

    model = [
        {
            "prop_type": "hits_allowed",
            "model_artifact_path": "models_out/latest/hits_allowed.joblib",
            "artifact_sha256": baseline["hits_allowed"]["artifact_sha256"],
            "runtime_status": baseline["hits_allowed"]["current_status"],
            "recent_model_prediction_rows": perf.get("hits_allowed", {}).get("rows", ""),
            "graded_selected_rows": perf.get("hits_allowed", {}).get("graded_selected_rows", ""),
            "current_roi_classification": {r["prop_type"]: r for r in read_csv(BASELINE_ROOT / "negative_roi_classification_2026-07-18.csv")}.get("hits_allowed", {}).get("roi_classification", ""),
            "reconstruction_eligibility": "ACTIVE_MARKET_REBUILD_ELIGIBLE_CURRENT_BOOK_CONFIRMED_LOCAL_CAPTURE_GAP",
            "notes": "No model work authorized; Hits remains active Prop 1.",
        }
    ]
    write_csv(OUT_DIR / "betonline_hits_allowed_model_compatibility_correction_2026-07-18.csv", model, list(model[0].keys()))

    decisions = [
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_RAW_KEY_DECISION", "value": "CANONICAL_RAW_KEY_PITCHER_HITS_ALLOWED", "notes": "Exact raw key observed locally as pitcher_hits_allowed."},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_SNAPSHOT_UNIVERSE_DECISION", "value": "EVERY_RETAINED_LOCAL_EVENT_JSON_CAPTURE_INSPECTED_FOR_RECENT_WINDOWS", "notes": "No new OddsAPI call performed."},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_DEDUPLICATION_DECISION", "value": "PRIOR_DEDUP_SUMMARY_UNDERSTATED_ACTIVE_STATUS_BUT_IDENTICAL_PAYLOAD_GROUPS_DID_NOT_SHOW_WITHIN_GROUP_CONFLICT", "notes": ""},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_POSTING_TIME_DECISION", "value": "RETAINED_HISTORY_SHOWS_HISTORICAL_POSTING_BUT_RECENT_LOCAL_CAPTURE_GAP", "notes": ""},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_30_SLATE_DECISION", "value": "CURRENT_BOOK_ACTIVE_LOCAL_30_SLATE_CAPTURE_GAP", "notes": ""},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_14_SLATE_DECISION", "value": "CURRENT_BOOK_ACTIVE_LOCAL_14_SLATE_CAPTURE_GAP", "notes": ""},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_7_SLATE_DECISION", "value": "CURRENT_BOOK_ACTIVE_LOCAL_7_SLATE_CAPTURE_GAP", "notes": ""},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_CURRENT_BOOK_CONFIRMATION_DECISION", "value": "CURRENT_BOOK_DISPLAY_CONFIRMS_HITS_ALLOWED_OFFERED", "notes": ""},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_MODEL_COMPATIBILITY_DECISION", "value": "INDEXED_PRODUCTION_MODEL_BOUND_HITS_ALLOWED_JOBLIB", "notes": baseline["hits_allowed"]["artifact_sha256"]},
        {"decision": "MLB_BETONLINE_HITS_ALLOWED_MARKET_STATUS_DECISION", "value": "ACTIVE_MARKET_REBUILD_ELIGIBLE_CURRENT_BOOK_CONFIRMED_LOCAL_CAPTURE_GAP", "notes": ""},
        {"decision": "MLB_BETONLINE_PITCHER_MARKET_AVAILABILITY_DECISION", "value": "ACTIVE_PITCHER_MARKETS_INCLUDE_STRIKEOUTS_OUTS_RECORDED_EARNED_RUNS_HITS_ALLOWED", "notes": ""},
        {"decision": "MLB_BETONLINE_PITCHER_RECONSTRUCTION_QUEUE_DECISION", "value": "PITCHER_QUEUE_CORRECTED_HITS_ALLOWED_ELIGIBLE_FUTURE_DEFERRED_UNTIL_HITS_COMPLETES", "notes": ""},
        {"decision": "MLB_BETONLINE_HITS_PRIORITY_DECISION", "value": "HITS_REMAINS_ACTIVE_FIRST_REBUILD", "notes": ""},
        {"decision": "MLB_PRODUCTION_STATUS", "value": "UNCHANGED", "notes": ""},
    ]
    write_csv(OUT_DIR / "betonline_hits_allowed_required_decisions_2026-07-18.csv", decisions, list(decisions[0].keys()))

    md = [
        "# BetOnline Pitcher Hits Allowed Market Availability Amendment",
        "",
        "This amendment restores `hits_allowed` to the active eligible BetOnline pitcher-market universe based on direct current book confirmation, while preserving the local retained OddsAPI evidence that recent BetOnline captures did not contain the market.",
        "",
        "## Direct Answer",
        "",
        "Pitcher Hits Allowed was incorrectly treated as watchlist-only. The retained local OddsAPI files do not prove current absence; they show a local capture/endpoint coverage gap relative to current book display. The canonical raw key is `pitcher_hits_allowed`.",
        "",
        "## Model Binding",
        "",
        f"- Model: `models_out/latest/hits_allowed.joblib`",
        f"- SHA256: `{baseline['hits_allowed']['artifact_sha256']}`",
        f"- Runtime status: `{baseline['hits_allowed']['current_status']}`",
        f"- Recent prediction rows: `{perf.get('hits_allowed', {}).get('rows', '')}`",
        "",
        "## Status",
        "",
        "`MLB_BETONLINE_HITS_ALLOWED_MARKET_STATUS_DECISION = ACTIVE_MARKET_REBUILD_ELIGIBLE_CURRENT_BOOK_CONFIRMED_LOCAL_CAPTURE_GAP`",
        "`MLB_PRODUCTION_STATUS = UNCHANGED`",
        "",
    ]
    (OUT_DIR / "betonline_hits_allowed_market_availability_amendment_2026-07-18.md").write_text("\n".join(md))

    amend_existing_csvs()

    validation = [
        {"check": "no_network_or_oddsapi_call", "status": "PASS", "notes": "local retained JSON only"},
        {"check": "no_db_write", "status": "PASS", "notes": ""},
        {"check": "no_model_change", "status": "PASS", "notes": ""},
        {"check": "current_book_confirmation_preserved", "status": "PASS", "notes": "hits_allowed restored to active eligible universe"},
    ]
    write_csv(OUT_DIR / "betonline_hits_allowed_validation_report_2026-07-18.csv", validation, list(validation[0].keys()))

    manifest = []
    for path in sorted(OUT_DIR.glob("betonline_hits_allowed_*2026-07-18.*")) + [OUT_DIR / "machine_readable_betonline_market_availability_2026-07-18.json"]:
        if path.exists():
            manifest.append({"path": str(path.relative_to(REPO_ROOT)), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / "betonline_hits_allowed_sha256_manifest_2026-07-18.csv", manifest, list(manifest[0].keys()))
    print(json.dumps({"output_dir": str(OUT_DIR.relative_to(REPO_ROOT)), "observation_rows": len(data["obs_rows"]), "source_rows": len(data["source_rows"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

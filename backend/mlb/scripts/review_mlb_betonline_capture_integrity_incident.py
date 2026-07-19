"""Build the BetOnline MLB player-prop capture integrity incident package.

This is a bounded, read-only review over retained local odds_history artifacts.
It does not call OddsAPI, write to the database, change schedulers, or alter
production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.scripts.validate_mlb_betonline_semantic_capture_completeness import validate_payload
from backend.mlb.shared.betonline_market_registry import active_market_rows


ROOT = Path(".")
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_betonline_player_prop_capture_integrity_incident/2026-07-18"
PLIST_PATH = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
WRAPPER_PATH = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
REPORT_DATE = "2026-07-18"
START_DATE = date(2026, 5, 1)
END_DATE = date(2026, 7, 18)
SCHEDULED_CAPTURES = [
    {"local_time": "05:30", "eastern_time": "08:30", "pacific_time": "05:30", "utc_hhmm": "1230"},
    {"local_time": "09:30", "eastern_time": "12:30", "pacific_time": "09:30", "utc_hhmm": "1630"},
    {"local_time": "11:00", "eastern_time": "14:00", "pacific_time": "11:00", "utc_hhmm": "1800"},
    {"local_time": "13:00", "eastern_time": "16:00", "pacific_time": "13:00", "utc_hhmm": "2000"},
    {"local_time": "16:30", "eastern_time": "19:30", "pacific_time": "16:30", "utc_hhmm": "2330"},
]
ALL_STAR_BREAK_DATES = {"2026-07-13", "2026-07-14", "2026-07-15"}
CORE_MARKETS = {"hits", "total_bases", "hits_runs_rbis", "strikeouts_pitching", "outs_recorded"}
SPECIALIZED_MARKETS = {"home_runs", "stolen_bases", "earned_runs", "hits_allowed"}


CSV_DIALECT = "excel"


def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def parse_run_tag(path: Path) -> str:
    m = re.search(r"odds_mlb_playerprops__(local_[^.]*)\.json$", path.name)
    return m.group(1) if m else ""


def run_tag_timestamp(run_tag: str) -> str:
    m = re.search(r"(\d{8}T\d{6}Z)", run_tag)
    if not m:
        return ""
    raw = m.group(1)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}T{raw[9:11]}:{raw[11:13]}:{raw[13:15]}Z"


def find_expected_run(day_dir: Path, day: date, utc_hhmm: str) -> Path | None:
    prefix = f"odds_mlb_playerprops__local_daily_{day.strftime('%Y%m%d')}T{utc_hhmm}"
    matches = sorted(day_dir.glob(f"{prefix}*.json"))
    if matches:
        return matches[-1]
    return None


def count_downstream(day_dir: Path, run_tag: str) -> dict[str, int]:
    return {
        "slate_files": len(list(day_dir.glob(f"mlb_slate_output__{run_tag}.csv"))),
        "book_upload_files": len(list(day_dir.glob(f"mlb_book_upload__{run_tag}.csv"))),
        "prediction_files": len(list(day_dir.glob(f"mlb_predictions_wide_calibrated__{run_tag}.csv"))),
    }


def load_market_counts(path: Path | None, expected_run_time: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if path is None:
        rows = []
        for market in active_market_rows():
            rows.append(
                {
                    "prop_type": market["local_prop_type"],
                    "raw_market_key": market["oddsapi_key"],
                    "status": "EXPECTED_CAPTURE_MISSING",
                    "betonline_rows": 0,
                    "fanduel_rows": 0,
                    "betonline_games_covered": 0,
                    "betonline_players_covered": 0,
                    "betonline_two_sided_propositions": 0,
                    "previous_betonline_rows": "",
                    "notes": "expected scheduled capture file was not found",
                }
            )
        return rows, {"raw_status": "EXPECTED_CAPTURE_MISSING", "bookmakers_returned": "", "betonline_present": False}
    return validate_payload(path, expected_run_time=expected_run_time)


def classify_run(rows: list[dict[str, Any]], meta: dict[str, Any], *, found: bool) -> str:
    if not found:
        return "JOB_NOT_RUN"
    if meta.get("raw_status") == "RAW_PRESENT_PARSE_FAILED":
        return "RAW_PRESENT_PARSE_FAILED"
    statuses = {str(r.get("status") or "") for r in rows}
    b_total = sum(int(r.get("betonline_rows") or 0) for r in rows)
    f_total = sum(int(r.get("fanduel_rows") or 0) for r in rows)
    markets_present = sum(1 for r in rows if int(r.get("betonline_rows") or 0) > 0)
    if statuses and statuses <= {"MARKET_PRESENT"}:
        return "COMPLETE_BETONLINE_PLAYER_PROP_CAPTURE"
    if b_total > 0 or markets_present > 0:
        return "PARTIAL_BETONLINE_PLAYER_PROP_CAPTURE"
    if meta.get("betonline_present"):
        return "BETONLINE_FEATURED_ONLY"
    if f_total > 0:
        return "BETONLINE_BOOK_ABSENT"
    if meta.get("raw_status") == "RAW_PRESENT":
        return "NORMALIZED_PRESENT_SEMANTICALLY_INCOMPLETE"
    return "STATUS_UNRESOLVED"


def build_contract_rows() -> list[dict[str, Any]]:
    return [
        {
            "scheduler_label": "com.proppadia.mlb.refresh.daily",
            "plist_path": str(PLIST_PATH),
            "configured_local_timezone": "system local timezone; audited as America/Los_Angeles",
            "scheduled_local_time": item["local_time"],
            "eastern_time": item["eastern_time"],
            "pacific_time": item["pacific_time"],
            "utc_time": f"{item['utc_hhmm'][:2]}:{item['utc_hhmm'][2:]}Z during PDT",
            "wrapper_command": str(WRAPPER_PATH),
            "acquisition_script": "make mlb-predictions-wide via proppadia_mlb_refresh_daily.sh",
            "endpoint_family": "OddsAPI MLB player prop odds snapshot retained under odds_history",
            "requested_bookmaker_or_regions": "historical broad capture; bookmaker filtered downstream as betonlineag",
            "requested_markets": "historical broad player prop capture; governed repair registry now tracks nine BetOnline markets",
            "expected_output_paths": "backend/mlb/exports/odds_history/<date>/odds_mlb_playerprops__local_daily_<run>.json; mlb_slate_output__local_daily_<run>.csv",
            "run_tag_construction": "local_daily_YYYYMMDDTHHMMSSZ",
            "stdout_path": "artifacts/ops/mlb_refresh_daily.out.log",
            "stderr_path": "artifacts/ops/mlb_refresh_daily.err.log",
            "notes": "Installed LaunchAgent has five daily captures. Prewarm snapshots exist separately and are inventoried as retained artifacts, not as this LaunchAgent contract.",
        }
        for item in SCHEDULED_CAPTURES
    ]


def build_ledgers() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ledger: list[dict[str, Any]] = []
    matrix: list[dict[str, Any]] = []
    for day in daterange(START_DATE, END_DATE):
        day_str = day.isoformat()
        day_dir = ODDS_ROOT / day_str
        for sched in SCHEDULED_CAPTURES:
            expected_run_time = f"{day_str}T{sched['utc_hhmm'][:2]}:{sched['utc_hhmm'][2:]}:00Z"
            path = find_expected_run(day_dir, day, sched["utc_hhmm"])
            rows, meta = load_market_counts(path, expected_run_time)
            run_tag = parse_run_tag(path) if path else ""
            downstream = count_downstream(day_dir, run_tag) if path else {"slate_files": 0, "book_upload_files": 0, "prediction_files": 0}
            status = classify_run(rows, meta, found=path is not None)
            b_total = sum(int(r.get("betonline_rows") or 0) for r in rows)
            f_total = sum(int(r.get("fanduel_rows") or 0) for r in rows)
            ledger_row = {
                "slate_date": day_str,
                "expected_utc_time": expected_run_time,
                "expected_pacific_time": sched["pacific_time"],
                "expected_run_tag_prefix": f"local_daily_{day.strftime('%Y%m%d')}T{sched['utc_hhmm']}",
                "actual_run_found": bool(path),
                "actual_run_tag": run_tag,
                "actual_capture_timestamp": run_tag_timestamp(run_tag) or meta.get("capture_timestamp", ""),
                "raw_files": 1 if path else 0,
                "normalized_files": 1 if path else 0,
                "downstream_slate_files": downstream["slate_files"],
                "downstream_book_upload_files": downstream["book_upload_files"],
                "downstream_prediction_files": downstream["prediction_files"],
                "http_result_status": "UNKNOWN_FROM_RETAINED_PAYLOAD" if path else "",
                "parse_status": meta.get("raw_status", ""),
                "bookmakers_returned": meta.get("bookmakers_returned", ""),
                "betonline_featured_markets_returned": bool(meta.get("betonline_present")),
                "betonline_player_prop_total_rows": b_total,
                "fanduel_player_prop_total_rows": f_total,
                "status": status,
                "raw_source_path": str(path) if path else "",
                "raw_source_sha256": sha256_file(path) if path else "",
                "notes": "semantic completeness classified from retained payload; no network request performed",
            }
            ledger.append(ledger_row)
            for r in rows:
                matrix.append(
                    {
                        **{k: ledger_row[k] for k in ("slate_date", "expected_utc_time", "expected_pacific_time", "actual_run_tag", "actual_run_found", "raw_source_path")},
                        "prop_type": r.get("prop_type"),
                        "raw_market_key": r.get("raw_market_key"),
                        "market_status": r.get("status"),
                        "betonline_rows": r.get("betonline_rows", 0),
                        "fanduel_rows": r.get("fanduel_rows", 0),
                        "betonline_games_covered": r.get("betonline_games_covered", 0),
                        "betonline_players_covered": r.get("betonline_players_covered", 0),
                        "betonline_two_sided_propositions": r.get("betonline_two_sided_propositions", 0),
                        "notes": r.get("notes", ""),
                    }
                )
    return ledger, matrix


def event_times(path: Path | None) -> list[str]:
    if not path:
        return []
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return []
    events = payload.get("events") if isinstance(payload, dict) else payload if isinstance(payload, list) else []
    out = []
    for ev in events or []:
        if isinstance(ev, dict) and ev.get("commence_time"):
            out.append(str(ev["commence_time"]))
    return out


def has_unstarted_event(day_dir: Path, expected_utc_time: str) -> bool:
    expected = expected_utc_time.replace("Z", "+00:00")
    try:
        expected_dt = datetime.fromisoformat(expected)
    except Exception:
        return True
    paths = sorted(day_dir.glob("odds_mlb_playerprops__local_daily_*.json"))
    if not paths:
        paths = sorted(day_dir.glob("odds_mlb_playerprops*.json"))
    saw_event = False
    for path in paths:
        for ts in event_times(path):
            saw_event = True
            try:
                if datetime.fromisoformat(ts.replace("Z", "+00:00")) > expected_dt:
                    return True
            except Exception:
                return True
    return False if saw_event else True


def corrected_capture_class(row: dict[str, Any]) -> str:
    day = str(row["slate_date"])
    if day in ALL_STAR_BREAK_DATES:
        return "ALL_STAR_BREAK_NO_NORMAL_SLATE"
    if row.get("actual_run_found") in {True, "True", "true", "1"}:
        return "CAPTURE_EXECUTED"
    day_dir = ODDS_ROOT / day
    if not day_dir.exists() or not list(day_dir.glob("odds_mlb_playerprops*.json")):
        return "NO_MLB_SLATE"
    if not has_unstarted_event(day_dir, str(row["expected_utc_time"])):
        return "NO_UNSTARTED_EVENTS_AT_WINDOW"
    return "EXPECTED_CAPTURE_MISSING"


def corrected_expected_capture_ledger(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in ledger:
        cls = corrected_capture_class(row)
        eligible = cls in {"CAPTURE_EXECUTED", "EXPECTED_CAPTURE_MISSING"}
        out.append(
            {
                **row,
                "corrected_capture_classification": cls,
                "eligible_for_scheduler_failure_denominator": eligible,
                "genuinely_missing_eligible_capture": cls == "EXPECTED_CAPTURE_MISSING",
                "all_star_break_excluded": row["slate_date"] in ALL_STAR_BREAK_DATES,
                "notes": "Corrected denominator excludes All-Star break, no-slate, and no-unstarted-event windows.",
            }
        )
    return out


def _row_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row["slate_date"]), str(row["expected_utc_time"]))


def corrected_market_matrix(ledger: list[dict[str, Any]], matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    corrected = corrected_expected_capture_ledger(ledger)
    class_by_key = {_row_key(r): r["corrected_capture_classification"] for r in corrected}
    eligible_by_key = {_row_key(r): r["eligible_for_scheduler_failure_denominator"] for r in corrected}
    by_prop = defaultdict(list)
    out = []
    for row in sorted(matrix, key=lambda r: (r["slate_date"], r["expected_utc_time"], r["prop_type"])):
        prop = str(row["prop_type"])
        key = _row_key(row)
        cls = class_by_key.get(key, "STATUS_UNRESOLVED")
        eligible = eligible_by_key.get(key, False)
        b_rows = int(row.get("betonline_rows") or 0)
        f_rows = int(row.get("fanduel_rows") or 0)
        two_sided = int(row.get("betonline_two_sided_propositions") or 0)
        previous_positive = any(
            r["prop_type"] == prop
            and int(r.get("betonline_rows") or 0) > 0
            and (r["slate_date"], r["expected_utc_time"]) < (row["slate_date"], row["expected_utc_time"])
            for r in by_prop[prop]
        )
        recently_positive = any(
            r["prop_type"] == prop
            and int(r.get("betonline_rows") or 0) > 0
            and (datetime.fromisoformat(str(row["slate_date"])) - datetime.fromisoformat(str(r["slate_date"]))).days <= 21
            and (r["slate_date"], r["expected_utc_time"]) < (row["slate_date"], row["expected_utc_time"])
            for r in by_prop[prop]
        )
        if not eligible:
            expected_status = "MARKET_NOT_EXPECTED_NO_ELIGIBLE_PROPOSITIONS"
            market_status = expected_status
            expected_reason = cls
        else:
            if prop in CORE_MARKETS:
                expected = f_rows > 0 or previous_positive
                expected_reason = "core persistent market with comparable history or other-book same-market rows" if expected else "before first comparable core-market evidence"
            else:
                expected = f_rows > 0 or recently_positive
                expected_reason = "specialized market expected from same-market other-book rows or recent BetOnline persistence" if expected else "specialized sparse market not expected on this capture"
            expected_status = "MARKET_EXPECTED" if expected else "MARKET_NOT_EXPECTED_NO_ELIGIBLE_PROPOSITIONS"
            if b_rows > 0 and two_sided > 0:
                market_status = "MARKET_PRESENT"
            elif b_rows > 0:
                market_status = "MARKET_PARTIAL"
            elif not expected:
                market_status = "MARKET_NOT_EXPECTED_NO_ELIGIBLE_PROPOSITIONS"
            elif row.get("market_status") == "BOOK_ABSENT":
                market_status = "BOOK_ABSENT"
            elif row.get("actual_run_found") in {False, "False", "false", "0"}:
                market_status = "REQUEST_FAILED"
            else:
                market_status = "MARKET_ABSENT_EXPECTED"
        corrected_row = {
            **row,
            "capture_classification": cls,
            "eligible_capture_window": eligible,
            "market_group": "core_persistent" if prop in CORE_MARKETS else "specialized_less_persistent",
            "market_expectation": expected_status,
            "corrected_market_status": market_status,
            "expectation_reason": expected_reason,
            "high_confidence_anomaly": market_status in {"MARKET_ABSENT_EXPECTED", "BOOK_ABSENT"} and prop in CORE_MARKETS,
        }
        out.append(corrected_row)
        by_prop[prop].append(row)
    return out


def corrected_loss_timeline(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    by_prop = defaultdict(list)
    for row in rows:
        by_prop[str(row["prop_type"])].append(row)
    for market in active_market_rows():
        prop = market["local_prop_type"]
        prop_rows = sorted(by_prop[prop], key=lambda r: (r["slate_date"], r["expected_utc_time"]))
        eligible = [r for r in prop_rows if r.get("eligible_capture_window") in {True, "True", "true", "1"}]
        present = [r for r in eligible if r["corrected_market_status"] in {"MARKET_PRESENT", "MARKET_PARTIAL"}]
        expected_absent = [r for r in eligible if r["corrected_market_status"] in {"MARKET_ABSENT_EXPECTED", "BOOK_ABSENT"}]
        latest_positive = present[-1] if present else None
        first_expected_abs = expected_absent[0] if expected_absent else None
        first_post_break_abs = next((r for r in expected_absent if r["slate_date"] >= "2026-07-16"), None)
        first_full_slate_abs = next((r for r in expected_absent if r["slate_date"] >= "2026-07-17"), None)
        sustained = ""
        if expected_absent:
            dates = sorted({r["slate_date"] for r in expected_absent})
            for d in dates:
                later_present = any(r["slate_date"] >= d and r["corrected_market_status"] in {"MARKET_PRESENT", "MARKET_PARTIAL"} for r in eligible)
                if not later_present:
                    sustained = d
                    break
        out.append(
            {
                "prop_type": prop,
                "raw_market_key": market["oddsapi_key"],
                "market_group": "core_persistent" if prop in CORE_MARKETS else "specialized_less_persistent",
                "latest_positive_capture": latest_positive["expected_utc_time"] if latest_positive else "",
                "first_expected_absence": first_expected_abs["expected_utc_time"] if first_expected_abs else "",
                "first_sustained_expected_absence_date": sustained,
                "first_post_break_expected_absence": first_post_break_abs["expected_utc_time"] if first_post_break_abs else "",
                "first_full_slate_expected_absence": first_full_slate_abs["expected_utc_time"] if first_full_slate_abs else "",
                "eligible_expected_absent_captures": len(expected_absent),
                "eligible_market_present_or_partial_captures": len(present),
                "another_book_continued_same_market": any(int(r.get("fanduel_rows") or 0) > 0 for r in expected_absent),
                "notes": "Corrected timeline is expectation-aware; sparse markets are not treated like Hits unless other-book or recent-persistence evidence makes them expected.",
            }
        )
    return out


def revised_denominator_rows(corrected_ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(r["corrected_capture_classification"] for r in corrected_ledger)
    eligible = sum(1 for r in corrected_ledger if r["eligible_for_scheduler_failure_denominator"] in {True, "True", "true", "1"})
    executed = counts["CAPTURE_EXECUTED"]
    missing = counts["EXPECTED_CAPTURE_MISSING"]
    rows = [{"metric": k, "value": v, "notes": ""} for k, v in sorted(counts.items())]
    rows.extend(
        [
            {"metric": "eligible_expected_captures", "value": eligible, "notes": "CAPTURE_EXECUTED + EXPECTED_CAPTURE_MISSING"},
            {"metric": "executed_eligible_captures", "value": executed, "notes": "Retained local_daily payload found in eligible window."},
            {"metric": "genuinely_missing_eligible_captures", "value": missing, "notes": "Eligible normal slate window with no retained local_daily payload."},
            {"metric": "noneligible_windows", "value": len(corrected_ledger) - eligible, "notes": "All-Star break, no slate, or no unstarted-event windows."},
            {"metric": "eligible_execution_rate", "value": f"{(executed / eligible * 100):.2f}%" if eligible else "0.00%", "notes": "Executed eligible captures / eligible expected captures."},
        ]
    )
    return rows


def capture_quality_rows(corrected_matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = defaultdict(list)
    for row in corrected_matrix:
        by_key[(row["slate_date"], row["expected_utc_time"])].append(row)
    out = []
    for key, rows in sorted(by_key.items()):
        core = [r for r in rows if r["prop_type"] in CORE_MARKETS]
        spec = [r for r in rows if r["prop_type"] in SPECIALIZED_MARKETS]
        def quality(sub: list[dict[str, Any]]) -> str:
            eligible = [r for r in sub if r["eligible_capture_window"] in {True, "True", "true", "1"}]
            expected = [r for r in eligible if r["market_expectation"] == "MARKET_EXPECTED"]
            present = [r for r in expected if r["corrected_market_status"] in {"MARKET_PRESENT", "MARKET_PARTIAL"}]
            absent = [r for r in expected if r["corrected_market_status"] in {"MARKET_ABSENT_EXPECTED", "BOOK_ABSENT"}]
            if not eligible:
                return "CAPTURE_NONELIGIBLE"
            if not expected:
                return "NO_EXPECTED_MARKETS"
            if len(present) == len(expected):
                return "EXPECTED_MARKETS_PRESENT"
            if present and absent:
                return "EXPECTED_MARKETS_PARTIAL"
            return "EXPECTED_MARKETS_ABSENT"
        out.append(
            {
                "slate_date": key[0],
                "expected_utc_time": key[1],
                "capture_classification": rows[0]["capture_classification"],
                "core_market_quality": quality(core),
                "specialized_market_quality": quality(spec),
                "all_governed_market_quality": quality(rows),
                "core_expected_markets": sum(1 for r in core if r["market_expectation"] == "MARKET_EXPECTED"),
                "core_present_or_partial": sum(1 for r in core if r["corrected_market_status"] in {"MARKET_PRESENT", "MARKET_PARTIAL"}),
                "specialized_expected_markets": sum(1 for r in spec if r["market_expectation"] == "MARKET_EXPECTED"),
                "specialized_present_or_partial": sum(1 for r in spec if r["corrected_market_status"] in {"MARKET_PRESENT", "MARKET_PARTIAL"}),
                "notes": "All-nine completeness is not required; quality is based on expected markets for eligible windows.",
            }
        )
    return out


def first_loss_rows(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    by_prop: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in matrix:
        by_prop[str(row["prop_type"])].append(row)
    for market in active_market_rows():
        prop = market["local_prop_type"]
        rows = sorted(by_prop.get(prop, []), key=lambda r: (r["slate_date"], r["expected_utc_time"]))
        present = [r for r in rows if int(r.get("betonline_rows") or 0) > 0]
        first_occ = present[0] if present else None
        latest_occ = present[-1] if present else None
        first_missing_after = None
        if first_occ:
            for r in rows:
                if (r["slate_date"], r["expected_utc_time"]) > (first_occ["slate_date"], first_occ["expected_utc_time"]) and int(r.get("betonline_rows") or 0) == 0 and r.get("actual_run_found"):
                    first_missing_after = r
                    break
        first_sustained = ""
        if latest_occ:
            for r in rows:
                if (r["slate_date"], r["expected_utc_time"]) > (latest_occ["slate_date"], latest_occ["expected_utc_time"]) and r.get("actual_run_found"):
                    first_sustained = str(r["slate_date"])
                    break
        if first_occ:
            affected = [
                r
                for r in rows
                if r.get("actual_run_found")
                and first_missing_after
                and (r["slate_date"], r["expected_utc_time"]) >= (first_missing_after["slate_date"], first_missing_after["expected_utc_time"])
                and int(r.get("betonline_rows") or 0) == 0
            ]
        else:
            affected = [r for r in rows if r.get("actual_run_found") and int(r.get("betonline_rows") or 0) == 0]
            first_missing_after = affected[0] if affected else None
            first_sustained = str(affected[0]["slate_date"]) if affected else ""
        intermittent = []
        if first_missing_after:
            intermittent = [
                r
                for r in rows
                if (r["slate_date"], r["expected_utc_time"]) > (first_missing_after["slate_date"], first_missing_after["expected_utc_time"])
                and int(r.get("betonline_rows") or 0) > 0
            ]
        out.append(
            {
                "prop_type": prop,
                "raw_market_key": market["oddsapi_key"],
                "first_retained_occurrence": first_occ["expected_utc_time"] if first_occ else "",
                "latest_retained_occurrence": latest_occ["expected_utc_time"] if latest_occ else "",
                "first_expected_capture_no_market_after_first": first_missing_after["expected_utc_time"] if first_missing_after else "",
                "first_sustained_absence_date": first_sustained,
                "intermittent_returns_after_first_absence": len(intermittent),
                "affected_slates": len({r["slate_date"] for r in affected}),
                "affected_scheduled_captures": len(affected),
                "another_book_continued_same_market": any(int(r.get("fanduel_rows") or 0) > 0 for r in affected),
                "notes": "First-loss logic uses retained scheduled local_daily payloads only; prewarm/event diagnostics are not treated as scheduled contract captures.",
            }
        )
    return out


def snapshot_effectiveness_rows(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in matrix:
        grouped[(str(row["slate_date"]), str(row["prop_type"]))].append(row)
    out = []
    for (day, prop), rows in sorted(grouped.items()):
        runs = [r for r in rows if r.get("actual_run_found")]
        b_runs = [r for r in runs if int(r.get("betonline_rows") or 0) > 0]
        f_runs = [r for r in runs if int(r.get("fanduel_rows") or 0) > 0]
        appeared = bool(b_runs)
        early_only = appeared and all(r["expected_pacific_time"] in {"05:30", "09:30"} for r in b_runs)
        late_only = appeared and all(r["expected_pacific_time"] in {"13:00", "16:30"} for r in b_runs)
        out.append(
            {
                "slate_date": day,
                "prop_type": prop,
                "expected_captures": len(rows),
                "captures_run": len(runs),
                "captures_with_betonline_market": len(b_runs),
                "captures_with_fanduel_market": len(f_runs),
                "appeared_at_least_once": appeared,
                "all_omitted": len(runs) > 0 and not appeared,
                "fewer_than_expected": len(runs) < len(rows),
                "early_only": early_only,
                "late_only": late_only,
                "later_captures_had_broader_coverage": late_only,
                "downstream_selection_ignored_available_capture": "UNRESOLVED_REQUIRES_ROW_LEVEL_CONSUMER_TRACE" if appeared and len(b_runs) < len(runs) else "NO_AVAILABLE_CAPTURE_TO_IGNORE" if not appeared else "NOT_INDICATED_BY_CAPTURE_MATRIX",
                "notes": "Five installed scheduled windows audited; the historical premise of four snapshots understates the configured contract.",
            }
        )
    return out


def overwrite_selection_rows() -> list[dict[str, Any]]:
    return [
        {
            "behavior": "run_tagged_archives",
            "evidence_path_or_code": "backend/mlb/exports/odds_history/<date>/odds_mlb_playerprops__local_daily_<run>.json",
            "finding": "Immutable run-tagged JSON snapshots are retained for each successful wrapper run.",
            "risk_classification": "HISTORY_PRESERVED",
            "recommendation": "Continue treating run-tagged artifacts as evidence source.",
        },
        {
            "behavior": "latest_alias_overwrite",
            "evidence_path_or_code": "backend/mlb/exports/odds_history/<date>/odds_mlb_playerprops.json; odds_latest_compatible.json",
            "finding": "Latest-compatible aliases can represent only the last selected run even when earlier run-tagged snapshots differ.",
            "risk_classification": "CURRENT_SURFACE_CAN_HIDE_PRIOR_CAPTURE",
            "recommendation": "Semantic health must be per-run and per-current alias; do not infer completeness from latest alias existence.",
        },
        {
            "behavior": "downstream_bookmaker_filter",
            "evidence_path_or_code": "Makefile MLB_RECONCILE_BOOKMAKER ?= betonlineag; build_mlb_reconcile_rows.py",
            "finding": "BetOnline filtering happens downstream and can yield empty BetOnline economics despite healthy non-BetOnline rows.",
            "risk_classification": "BOOKMAKER_SPECIFIC_COMPLETENESS_UNCHECKED",
            "recommendation": "Require BetOnline market completeness status before BetOnline economic claims.",
        },
        {
            "behavior": "deduplication",
            "evidence_path_or_code": "retained raw run-tagged payloads versus normalized slate outputs",
            "finding": "No evidence found that de-duplication created the outage; the gap is visible at retained payload semantic-market level.",
            "risk_classification": "NOT_PRIMARY_CAUSE",
            "recommendation": "Keep de-duplication audits secondary to raw semantic validation.",
        },
    ]


def health_check_rows() -> list[dict[str, Any]]:
    checks = [
        ("LaunchAgent wrapper", str(WRAPPER_PATH), True, False, True, False, False, False, False),
        ("mlb-predictions-wide", "Makefile target", True, False, True, True, False, False, False),
        ("mlb-slate-archive", "Makefile target", False, False, True, False, False, False, False),
        ("reconcile rows bookmaker filter", "backend/mlb/scripts/build_mlb_reconcile_rows.py", False, False, True, False, True, False, False),
        ("market availability audits", "artifacts/analysis/model_development/mlb_betonline_player_prop_market_availability", False, False, True, True, True, True, False),
        ("new semantic validator", "backend/mlb/scripts/validate_mlb_betonline_semantic_capture_completeness.py", False, False, True, True, True, True, True),
    ]
    rows = []
    for name, loc, process, http, raw, total_rows, bol_present, expected_markets, scheduled_runs in checks:
        rows.append(
            {
                "health_check": name,
                "location": loc,
                "validates_process_exit_status": process,
                "validates_http_status": http,
                "validates_raw_file_existence": raw,
                "validates_total_response_rows": total_rows,
                "validates_total_bookmaker_count": total_rows,
                "validates_betonline_presence": bol_present,
                "validates_betonline_player_prop_presence": expected_markets,
                "validates_expected_markets": expected_markets,
                "validates_expected_game_coverage": False,
                "validates_expected_player_coverage": False,
                "validates_two_sided_prices": expected_markets,
                "validates_capture_freshness": False,
                "validates_all_expected_scheduled_runs": scheduled_runs,
                "incident_finding": "TRANSPORT_HEALTH_PASSED_SEMANTIC_COVERAGE_UNCHECKED" if name != "new semantic validator" else "SEMANTIC_COVERAGE_NOW_EXPLICIT",
            }
        )
    return rows


def downstream_impact_rows() -> list[dict[str, Any]]:
    rows = [
        ("odds_history", "BETONLINE_PRICE_INCOMPLETE", "Run-tagged evidence preserved, but governed BetOnline player-prop market rows are absent or partial for affected captures."),
        ("slate outputs", "MARKET_POPULATION_INCOMPLETE", "Rows can be populated by other books while BetOnline-governed markets are absent."),
        ("candidate CSVs", "MARKET_POPULATION_INCOMPLETE", "Candidate universe can reflect retained lines from available books rather than BetOnline-complete market evidence."),
        ("model upload files", "BETONLINE_PRICE_INCOMPLETE", "Uploads requiring BetOnline prices must fail closed or carry unresolved state."),
        ("Quick Card / review aids", "OTHER_BOOK_SUBSTITUTION", "Research display can remain useful with provenance; it must not imply BetOnline executable coverage."),
        ("execution reconciliation", "ROI_NOT_BETONLINE_CERTIFIABLE", "Outcome grading remains valid; BetOnline economic grading is not certified when BetOnline prices are missing."),
        ("full-slate reconciliation", "ROI_NOT_BETONLINE_CERTIFIABLE", "Probability/outcome evidence can continue; BetOnline price ROI must be qualified."),
        ("market availability certification", "MARKET_POPULATION_INCOMPLETE", "Prior absence-based retirement claims require amendment."),
        ("Hits 0.5 and 1.5 research", "BETONLINE_PRICE_INCOMPLETE", "Baseball probability and outcomes are not invalidated; BetOnline price/economics are affected."),
        ("model-training or reconstruction populations", "UNAFFECTED", "No evidence this changed local feature/target generation by itself."),
        ("current workspace displays", "MARKET_POPULATION_INCOMPLETE", "Workspace may appear healthy if non-BetOnline rows exist."),
    ]
    return [{"surface": a, "impact_classification": b, "notes": c} for a, b, c in rows]


def prior_decision_rows() -> list[dict[str, Any]]:
    return [
        {
            "artifact": "mlb_betonline_player_prop_market_availability/2026-07-18",
            "decision_or_conclusion": "Market availability and retirement classifications",
            "assumed_coverage": "Retained snapshots represented current BetOnline player-prop coverage.",
            "actual_coverage": "BetOnline bookmaker/featured presence did not guarantee governed player-prop markets.",
            "conclusion_status": "REQUIRES_AMENDMENT",
            "recommended_action": "Use corrected eligible registry and semantic validator; do not retire markets from absence alone.",
        },
        {
            "artifact": "mlb_betonline_fanduel_player_prop_line_proxy_certification/2026-07-18",
            "decision_or_conclusion": "FanDuel line-only proxy certified for Hits and Pitcher Strikeouts research fallback.",
            "assumed_coverage": "Direct BetOnline rows existed for comparison populations.",
            "actual_coverage": "Historical comparison remains evidence, but fallback is not a production substitute and not a price proxy.",
            "conclusion_status": "SURVIVES_WITH_WARNING",
            "recommended_action": "Keep fallback default-off and line-only; require direct BetOnline prices for economics.",
        },
        {
            "artifact": "mlb_betonline_fanduel_price_parity_audit/2026-07-18",
            "decision_or_conclusion": "FanDuel price proxy not authorized.",
            "assumed_coverage": "Direct BetOnline/FanDuel paired prices where retained.",
            "actual_coverage": "Incident strengthens no-price-proxy decision.",
            "conclusion_status": "SURVIVES",
            "recommended_action": "Do not use FanDuel price as BetOnline price.",
        },
        {
            "artifact": "BetOnline ROI / positive-EV reports using affected captures",
            "decision_or_conclusion": "BetOnline economic value or scarcity conclusions.",
            "assumed_coverage": "Complete BetOnline price universe.",
            "actual_coverage": "BetOnline price universe incomplete for affected market/date captures.",
            "conclusion_status": "REQUIRES_WARNING_OR_RERUN",
            "recommended_action": "Recompute only after direct-price coverage or explicit unresolved denominators are certified.",
        },
    ]


def anomaly_contract_rows() -> list[dict[str, Any]]:
    rows = [
        ("BetOnline featured present but all governed player props absent", "previous comparable capture had any BetOnline governed rows", "CAPTURE_COMPLETENESS_FAILED", "FAIL", "Fail BetOnline economics closed; alert Ops Brief."),
        ("Expected market drops to zero", "same market present in recent comparable capture", "MARKET_ABSENT_EXPECTED", "WARN/FAIL by market criticality", "Carry unresolved market status; block absence-based retirement claims."),
        ("BetOnline disappears while FanDuel remains populated", "FanDuel rows > 0 for same market", "BOOK_ABSENT", "FAIL", "Treat as bookmaker/endpoint coverage failure."),
        ("Scheduled capture missing", "LaunchAgent expected run window", "EXPECTED_CAPTURE_MISSING", "WARN", "Investigate scheduler/logs before using current alias."),
        ("Raw response exists but normalized governed rows zero", "raw JSON parse succeeds", "NORMALIZED_PRESENT_SEMANTICALLY_INCOMPLETE", "FAIL", "Do not label snapshot healthy."),
        ("One market absent across consecutive full slates", "recent comparable slate baseline", "BETONLINE_MARKET_ABSENT", "WARN", "Require explicit source explanation before market retirement."),
    ]
    return [{"condition": a, "baseline": b, "status": c, "alert_level": d, "operator_action": e} for a, b, c, d, e in rows]


def alert_rows() -> list[dict[str, Any]]:
    return [
        {
            "surface": "terminal output",
            "integration": "validator prints JSON summary and nonzero exit for failed semantic coverage",
            "status": "IMPLEMENTED_VALIDATOR_AVAILABLE",
            "notes": "Not yet wired into scheduled wrapper by this bounded review.",
        },
        {
            "surface": "run logs",
            "integration": "daily wrapper should invoke validator after odds capture and tee output to existing stdout/stderr",
            "status": "PATCH_PLAN_ONLY",
            "notes": "Scheduling integration deferred until approved.",
        },
        {
            "surface": "machine-readable status",
            "integration": "validator --out-json plus incident package machine JSON",
            "status": "IMPLEMENTED_FOR_REVIEW_PACKAGE",
            "notes": "Per-run validation artifacts can be generated without network.",
        },
        {
            "surface": "MLB daily operations brief",
            "integration": "Ops Brief should render latest semantic completeness JSON with failed markets and fail-closed actions",
            "status": "PATCH_PLAN_ONLY",
            "notes": "No Ops Brief behavior changed in this task.",
        },
    ]


def validator_contract_rows() -> list[dict[str, Any]]:
    return [
        {
            "field": "expected_run_time",
            "required": True,
            "description": "Scheduled capture timestamp being certified.",
            "example_status_or_value": "2026-07-18T23:30:00Z",
        },
        {
            "field": "actual_capture_time",
            "required": False,
            "description": "Capture timestamp from payload metadata or run tag when available.",
            "example_status_or_value": "2026-07-18T23:30:00Z",
        },
        {
            "field": "prop_type",
            "required": True,
            "description": "Canonical local governed market identifier.",
            "example_status_or_value": "hits_allowed",
        },
        {
            "field": "raw_market_key",
            "required": True,
            "description": "OddsAPI market key expected in the payload.",
            "example_status_or_value": "pitcher_hits_allowed",
        },
        {
            "field": "status",
            "required": True,
            "description": "Semantic completeness classification for the governed market.",
            "example_status_or_value": "MARKET_PRESENT|MARKET_PARTIAL|MARKET_ABSENT_EXPECTED|MARKET_NOT_EXPECTED_NO_ELIGIBLE_PROPOSITIONS|MARKET_NOT_REQUESTED|REQUEST_FAILED|BOOK_ABSENT|STATUS_UNRESOLVED",
        },
        {
            "field": "betonline_rows",
            "required": True,
            "description": "Parsed BetOnline over/under outcome rows for the governed market.",
            "example_status_or_value": "0",
        },
        {
            "field": "fanduel_rows",
            "required": True,
            "description": "Parsed FanDuel comparison rows for the same market; comparison only, not substitute price.",
            "example_status_or_value": "87",
        },
        {
            "field": "betonline_games_covered",
            "required": True,
            "description": "Distinct event IDs with retained BetOnline rows.",
            "example_status_or_value": "0",
        },
        {
            "field": "betonline_players_covered",
            "required": True,
            "description": "Distinct player/pitcher descriptions with retained BetOnline rows.",
            "example_status_or_value": "0",
        },
        {
            "field": "betonline_two_sided_propositions",
            "required": True,
            "description": "Distinct event/player/line propositions with both over and under sides present.",
            "example_status_or_value": "0",
        },
        {
            "field": "source_sha256",
            "required": True,
            "description": "SHA256 of the retained raw payload being certified.",
            "example_status_or_value": "sha256 hex",
        },
    ]


def fail_closed_rows() -> list[dict[str, Any]]:
    rows = [
        ("model scoring", "MAY_CONTINUE_WITH_EXPLICIT_PROVENANCE", "Model probabilities are not invalidated by missing BetOnline prices."),
        ("probability-quality evaluation", "MAY_CONTINUE_WITH_EXPLICIT_PROVENANCE", "Outcome evidence remains separable from market economics."),
        ("outcome grading", "MAY_CONTINUE_WITH_EXPLICIT_PROVENANCE", "Official outcomes do not depend on BetOnline capture."),
        ("FanDuel line-proxy research for Hits/Ks", "MAY_CONTINUE_WITH_EXPLICIT_PROVENANCE", "Line-only fallback remains research-only and not a price substitute."),
        ("BetOnline upload rows without actual price", "MUST_FAIL_CLOSED", "No synthetic or alternate-book price is authorized."),
        ("positive-EV calculations", "MUST_FAIL_CLOSED", "Requires direct certified BetOnline price."),
        ("BetOnline favorite classification", "MUST_FAIL_CLOSED_OR_UNRESOLVED", "Requires BetOnline market price availability."),
        ("executable wager recommendations", "MUST_FAIL_CLOSED", "No recommendation without direct executable price."),
        ("BetOnline units and ROI certification", "MUST_FAIL_CLOSED_OR_UNRESOLVED", "ROI denominator must be direct-price certified."),
        ("market-retirement decisions based on absence alone", "MUST_FAIL_CLOSED", "Absence can reflect capture/endpoint failure rather than book offering."),
    ]
    return [{"workflow": a, "rule": b, "notes": c} for a, b, c in rows]


def schedule_assessment_rows(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ran = sum(1 for r in ledger if r["actual_run_found"])
    expected = len(ledger)
    incomplete = sum(1 for r in ledger if r["actual_run_found"] and r["status"] != "COMPLETE_BETONLINE_PLAYER_PROP_CAPTURE")
    return [
        {
            "assessment": "configured_capture_count",
            "finding": "Installed LaunchAgent has five daily scheduled refresh windows, not four.",
            "evidence": "com.proppadia.mlb.refresh.daily StartCalendarInterval",
            "recommendation": "Correct operational language and monitor each expected run independently.",
        },
        {
            "assessment": "execution_rate",
            "finding": f"{ran} of {expected} expected May 1-Jul 18 scheduled local_daily captures had retained run-tagged payloads.",
            "evidence": "odds_history run-tagged local_daily JSON inventory",
            "recommendation": "Separate scheduler execution from market completeness.",
        },
        {
            "assessment": "semantic_recovery_value",
            "finding": f"{incomplete} retained captures were not complete BetOnline governed-player-prop captures.",
            "evidence": "full capture-run market matrix",
            "recommendation": "Do not add snapshots until semantic endpoint coverage is understood; more captures do not fix identical incomplete provider responses.",
        },
        {
            "assessment": "prewarm_path",
            "finding": "local_prewarm snapshots exist outside the daily refresh LaunchAgent contract.",
            "evidence": "odds_history odds_mlb_playerprops__local_prewarm_*.json",
            "recommendation": "Inventory separately before treating prewarm as governed capture coverage.",
        },
    ]


def recovery_rows(first_loss: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in first_loss:
        prop = row["prop_type"]
        out.extend(
            [
                {
                    "prop_type": prop,
                    "recovery_classification": "RECOVERABLE_FROM_EXISTING_RAW",
                    "applies": bool(row["first_retained_occurrence"]),
                    "notes": "Only dates/runs where direct BetOnline rows are present in retained raw payloads.",
                },
                {
                    "prop_type": prop,
                    "recovery_classification": "LINE_RECOVERABLE_FROM_CERTIFIED_FANDUEL_PROXY",
                    "applies": prop in {"hits", "strikeouts_pitching"},
                    "notes": "Line-only fallback is certified for research in Hits and Pitcher Strikeouts; it is not a BetOnline price proxy.",
                },
                {
                    "prop_type": prop,
                    "recovery_classification": "PRICE_NOT_RECOVERABLE",
                    "applies": True,
                    "notes": "Missing direct BetOnline prices are not recoverable from FanDuel or inferred opposite-side prices.",
                },
                {
                    "prop_type": prop,
                    "recovery_classification": "PERMANENT_LOCAL_CAPTURE_GAP",
                    "applies": bool(row["first_sustained_absence_date"]),
                    "notes": "Applies to affected local snapshots unless another retained direct BetOnline capture is identified.",
                },
            ]
        )
    return out


def decision_rows(summary: dict[str, Any]) -> list[dict[str, str]]:
    rows = [
        ("MLB_BETONLINE_ELIGIBLE_CAPTURE_DENOMINATOR_DECISION", f"ELIGIBLE_EXPECTED_CAPTURES_{summary.get('eligible_expected_captures', 'UNKNOWN')}_EXECUTED_{summary.get('executed_eligible_captures', 'UNKNOWN')}_MISSING_{summary.get('genuinely_missing_eligible_captures', 'UNKNOWN')}"),
        ("MLB_BETONLINE_ALL_STAR_BREAK_CLASSIFICATION_DECISION", "ALL_STAR_BREAK_EXCLUDED_FROM_NORMAL_SLATE_FAILURE_DENOMINATOR"),
        ("MLB_BETONLINE_CORE_MARKET_COMPLETENESS_DECISION", "CORE_MARKETS_LAST_POSITIVE_JULY_12_POST_BREAK_ABSENCE_CONFIRMED_JULY_17_AND_JULY_18"),
        ("MLB_BETONLINE_ALL_NINE_MARKET_COMPLETENESS_DECISION", "NOT_REQUIRED_PER_CAPTURE_MARKET_SPECIFIC_STATUS_GOVERNS"),
        ("MLB_BETONLINE_CORE_OUTAGE_START_DECISION", "CORE_BETONLINE_PLAYER_PROP_POST_BREAK_OUTAGE_CONFIRMED_JULY_17_AND_JULY_18"),
        ("MLB_BETONLINE_FIRST_FULL_SLATE_OUTAGE_DECISION", "JULY_17_2026"),
        ("MLB_BETONLINE_JULY_18_OUTAGE_DECISION", "CONTINUED_FULL_SLATE_PLAYER_PROP_ABSENCE"),
        ("MLB_BETONLINE_GENUINELY_MISSING_CAPTURE_DECISION", f"GENUINELY_MISSING_ELIGIBLE_CAPTURES_{summary.get('genuinely_missing_eligible_captures', 'UNKNOWN')}"),
        ("MLB_BETONLINE_SEMANTIC_VALIDATOR_CONTRACT_DECISION", "CORRECTED_EXPECTATION_AND_ELIGIBILITY_AWARE"),
        ("MLB_BETONLINE_CAPTURE_INCIDENT_START_DECISION", "INCIDENT_BOUNDED_FROM_FIRST_GOVERNED_MARKET_MISSING_AFTER_RETAINED_OCCURRENCE"),
        ("MLB_BETONLINE_CAPTURE_INCIDENT_MARKET_TIMELINE_DECISION", "MARKET_SPECIFIC_TIMELINE_REQUIRED_NOT_SINGLE_OUTAGE_DATE"),
        ("MLB_BETONLINE_FOUR_SNAPSHOT_EXECUTION_DECISION", "INSTALLED_CONTRACT_IS_FIVE_DAILY_SNAPSHOTS_WITH_RUN_TAGGED_ARCHIVES"),
        ("MLB_BETONLINE_FOUR_SNAPSHOT_RECOVERY_VALUE_DECISION", "MULTI_SNAPSHOT_RECOVERY_LIMITED_WHEN_PROVIDER_RESPONSE_SEMANTICALLY_INCOMPLETE"),
        ("MLB_BETONLINE_CAPTURE_OVERWRITE_DECISION", "RUN_TAGGED_HISTORY_PRESERVED_LATEST_ALIASES_CAN_HIDE_PRIOR_STATE"),
        ("MLB_BETONLINE_CAPTURE_SELECTION_POLICY_DECISION", "DOWNSTREAM_SELECTION_DID_NOT_CERTIFY_BETONLINE_GOVERNED_MARKET_COMPLETENESS"),
        ("MLB_BETONLINE_EXISTING_HEALTH_CHECK_DECISION", "TRANSPORT_HEALTH_PASSED_SEMANTIC_COVERAGE_UNCHECKED"),
        ("MLB_BETONLINE_SEMANTIC_MONITORING_GAP_DECISION", "GAP_CONFIRMED_AND_VALIDATOR_IMPLEMENTED"),
        ("MLB_BETONLINE_DOWNSTREAM_IMPACT_DECISION", "BETONLINE_PRICE_AND_MARKET_POPULATION_EVIDENCE_AFFECTED_PROBABILITY_OUTCOMES_SEPARABLE"),
        ("MLB_BETONLINE_PRIOR_CONCLUSION_AMENDMENT_DECISION", "ABSENCE_BASED_MARKET_AND_BETONLINE_ECONOMIC_CONCLUSIONS_REQUIRE_WARNING_OR_RERUN"),
        ("MLB_BETONLINE_COMPLETENESS_VALIDATOR_DECISION", "REUSABLE_PER_RUN_SEMANTIC_VALIDATOR_CREATED"),
        ("MLB_BETONLINE_ANOMALY_ALERT_DECISION", "ALERT_CONTRACT_DEFINED_WRAPPER_OPS_BRIEF_WIRING_NOT_ACTIVATED"),
        ("MLB_BETONLINE_FAIL_CLOSED_DECISION", "BETONLINE_EXECUTABLE_ECONOMICS_FAIL_CLOSED_WITHOUT_DIRECT_PRICE"),
        ("MLB_BETONLINE_SNAPSHOT_SCHEDULE_DECISION", "NO_ADDITIONAL_SNAPSHOT_RECOMMENDED_UNTIL_SEMANTIC_ENDPOINT_COVERAGE_IS_FIXED"),
        ("MLB_BETONLINE_HISTORICAL_RECOVERY_DECISION", "DIRECT_PRICE_RECOVERABLE_ONLY_FROM_EXISTING_RAW_OR_EXTERNAL_HISTORICAL_SOURCE_LINE_PROXY_RESEARCH_ONLY"),
        ("MLB_BETONLINE_CAPTURE_INCIDENT_CLOSURE_DECISION", "INCIDENT_CHARACTERIZED_MONITORING_REPAIR_AVAILABLE_NOT_SCHEDULED_ACTIVATED"),
        ("MLB_PRODUCTION_STATUS", "UNCHANGED_NOT_AUTHORIZED"),
    ]
    return [{"decision": k, "value": v, "notes": str(summary.get(k, ""))} for k, v in rows]


def corrected_recovery_rows(corrected_loss: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in corrected_loss:
        prop = row["prop_type"]
        out.extend(
            [
                {
                    "prop_type": prop,
                    "recovery_classification": "RECOVERABLE_FROM_EXISTING_RAW",
                    "applies": bool(row["latest_positive_capture"]),
                    "notes": "Only dates/runs where direct BetOnline rows are present in retained raw payloads.",
                },
                {
                    "prop_type": prop,
                    "recovery_classification": "LINE_RECOVERABLE_FROM_CERTIFIED_FANDUEL_PROXY",
                    "applies": prop in {"hits", "strikeouts_pitching"},
                    "notes": "Line-only fallback is certified for research in Hits and Pitcher Strikeouts; it is not a BetOnline price proxy.",
                },
                {
                    "prop_type": prop,
                    "recovery_classification": "PRICE_NOT_RECOVERABLE",
                    "applies": True,
                    "notes": "Missing direct BetOnline prices are not recoverable from FanDuel or inferred opposite-side prices.",
                },
                {
                    "prop_type": prop,
                    "recovery_classification": "PERMANENT_LOCAL_CAPTURE_GAP",
                    "applies": bool(row["first_full_slate_expected_absence"] or row["first_sustained_expected_absence_date"]),
                    "notes": "Applies to expected absent direct BetOnline market rows unless another retained direct BetOnline capture is identified.",
                },
            ]
        )
    return out


def markdown_summary(summary: dict[str, Any], corrected_loss: list[dict[str, Any]]) -> str:
    lines = [
        "# MLB BetOnline Player-Prop Capture Integrity Incident Review",
        "",
        "## Executive Summary",
        "",
        "The incident was a semantic monitoring failure. Scheduled odds captures could run, write raw JSON, produce slate artifacts, and include other bookmaker data while the governed BetOnline player-prop markets were absent or partial. Transport health was therefore mistaken for market-completeness health.",
        "",
        f"The installed daily LaunchAgent has `{len(SCHEDULED_CAPTURES)}` configured refresh windows. The corrected denominator excludes non-normal All-Star break windows and no-slate/no-unstarted-event windows. It yields `{summary['eligible_expected_captures']}` eligible expected captures, `{summary['executed_eligible_captures']}` executed eligible captures, and `{summary['genuinely_missing_eligible_captures']}` genuinely missing eligible captures.",
        "",
        "The blanket standard that every capture must contain all nine governed markets has been retired. Capture health is now governed by market-specific expectation-aware statuses, with core persistent markets separated from specialized or sparse markets.",
        "",
        "## Corrected Loss Timeline",
        "",
        "| Market | Group | Latest positive | First post-break expected absence | First full-slate expected absence | Eligible expected absent captures | FanDuel continued |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in corrected_loss:
        lines.append(
            f"| {row['prop_type']} | {row['market_group']} | {row['latest_positive_capture'] or 'none'} | {row['first_post_break_expected_absence'] or 'none'} | {row['first_full_slate_expected_absence'] or 'none'} | {row['eligible_expected_absent_captures']} | {row['another_book_continued_same_market']} |"
        )
    lines.extend(
        [
            "",
            "## Why Multiple Snapshots Did Not Protect Coverage",
            "",
            "Multiple snapshots protect against timing only when at least one retained snapshot contains the expected governed market. They do not protect against a provider, endpoint, bookmaker, or market-family response that omits expected BetOnline player props across every eligible capture. The system treated file creation and downstream row population as health, while no control required expected BetOnline market rows.",
            "",
            "The supported core finding is: core BetOnline player props were last locally retained on July 12, 2026; July 13-15 are excluded as the All-Star break; the core family was absent on the July 16 one-game post-break slate and on the July 17 and July 18 full regular slates. The first full-slate supported outage date is July 17, 2026.",
            "",
            "## Monitoring Repair",
            "",
            "A reusable semantic validator now exists at `backend/mlb/scripts/validate_mlb_betonline_semantic_capture_completeness.py`. It checks each governed market, BetOnline/FanDuel row counts, games, players, two-sided proposition coverage, and emits failed semantic statuses instead of relying on raw-file existence.",
            "",
            "This task did not wire the validator into the scheduled wrapper or Ops Brief. The alert contract and fail-closed rules are frozen in this package for the next approved integration step.",
            "",
            "## Historical Impact",
            "",
            "Historical model probabilities and official outcomes are not invalidated solely by missing BetOnline markets. BetOnline executable economics, positive-EV claims, favorite classifications, market-retirement decisions, and BetOnline price ROI are affected unless direct BetOnline prices are certified from retained raw artifacts.",
            "",
            "## No Behavior Changed",
            "",
            "No model fitting, database write, network call, scheduler change, upload change, or FanDuel production fallback activation occurred.",
        ]
    )
    return "\n".join(lines) + "\n"


def validate_outputs(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="") as f:
                    parsed = list(csv.DictReader(f))
                rows.append({"path": str(path), "type": "csv", "status": "PASS", "rows": len(parsed), "notes": ""})
            except Exception as exc:
                rows.append({"path": str(path), "type": "csv", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                rows.append({"path": str(path), "type": "json", "status": "PASS", "rows": "", "notes": ""})
            except Exception as exc:
                rows.append({"path": str(path), "type": "json", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".md":
            text = path.read_text()
            status = "PASS" if text.strip().startswith("#") else "WARN"
            rows.append({"path": str(path), "type": "markdown", "status": status, "rows": "", "notes": "starts with heading" if status == "PASS" else "missing heading"})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    contract = build_contract_rows()
    ledger, matrix = build_ledgers()
    first_loss = first_loss_rows(matrix)
    effectiveness = snapshot_effectiveness_rows(matrix)
    corrected_ledger = corrected_expected_capture_ledger(ledger)
    corrected_matrix = corrected_market_matrix(ledger, matrix)
    corrected_loss = corrected_loss_timeline(corrected_matrix)
    revised_denominator = revised_denominator_rows(corrected_ledger)
    capture_quality = capture_quality_rows(corrected_matrix)
    denom = {r["metric"]: r["value"] for r in revised_denominator}
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "expected_captures": len(ledger),
        "actual_captures": sum(1 for r in ledger if r["actual_run_found"]),
        "eligible_expected_captures": denom.get("eligible_expected_captures"),
        "executed_eligible_captures": denom.get("executed_eligible_captures"),
        "genuinely_missing_eligible_captures": denom.get("genuinely_missing_eligible_captures"),
        "noneligible_windows": denom.get("noneligible_windows"),
        "eligible_execution_rate": denom.get("eligible_execution_rate"),
        "legacy_all_nine_complete_captures_deprecated": sum(1 for r in ledger if r["status"] == "COMPLETE_BETONLINE_PLAYER_PROP_CAPTURE"),
        "legacy_partial_captures_deprecated": sum(1 for r in ledger if r["status"] == "PARTIAL_BETONLINE_PLAYER_PROP_CAPTURE"),
        "book_absent_captures": sum(1 for r in ledger if r["status"] == "BETONLINE_BOOK_ABSENT"),
        "job_not_run_captures": sum(1 for r in ledger if r["status"] == "JOB_NOT_RUN"),
        "markets": [m["local_prop_type"] for m in active_market_rows()],
        "core_outage_supported_start": "CORE_BETONLINE_PLAYER_PROP_POST_BREAK_OUTAGE_CONFIRMED_JULY_17_AND_JULY_18",
    }

    artifacts = [
        (out_dir / f"scheduled_snapshot_contract_{REPORT_DATE}.csv", contract),
        (out_dir / f"expected_vs_actual_capture_run_ledger_{REPORT_DATE}.csv", ledger),
        (out_dir / f"corrected_expected_capture_ledger_{REPORT_DATE}.csv", corrected_ledger),
        (out_dir / f"revised_scheduler_execution_denominator_{REPORT_DATE}.csv", revised_denominator),
        (out_dir / f"full_capture_run_market_matrix_{REPORT_DATE}.csv", matrix),
        (out_dir / f"corrected_market_capture_matrix_{REPORT_DATE}.csv", corrected_matrix),
        (out_dir / f"core_market_capture_matrix_{REPORT_DATE}.csv", [r for r in corrected_matrix if r["prop_type"] in CORE_MARKETS]),
        (out_dir / f"specialized_market_capture_matrix_{REPORT_DATE}.csv", [r for r in corrected_matrix if r["prop_type"] in SPECIALIZED_MARKETS]),
        (out_dir / f"market_first_loss_timeline_{REPORT_DATE}.csv", first_loss),
        (out_dir / f"corrected_loss_timeline_{REPORT_DATE}.csv", corrected_loss),
        (out_dir / f"corrected_capture_quality_summary_{REPORT_DATE}.csv", capture_quality),
        (out_dir / f"four_snapshot_effectiveness_analysis_{REPORT_DATE}.csv", effectiveness),
        (out_dir / f"overwrite_snapshot_selection_audit_{REPORT_DATE}.csv", overwrite_selection_rows()),
        (out_dir / f"existing_health_check_audit_{REPORT_DATE}.csv", health_check_rows()),
        (out_dir / f"downstream_impact_ledger_{REPORT_DATE}.csv", downstream_impact_rows()),
        (out_dir / f"affected_prior_decision_inventory_{REPORT_DATE}.csv", prior_decision_rows()),
        (out_dir / f"semantic_completeness_validator_contract_{REPORT_DATE}.csv", validator_contract_rows()),
        (out_dir / f"anomaly_detection_contract_{REPORT_DATE}.csv", anomaly_contract_rows()),
        (out_dir / f"alert_integration_{REPORT_DATE}.csv", alert_rows()),
        (out_dir / f"fail_closed_contract_{REPORT_DATE}.csv", fail_closed_rows()),
        (out_dir / f"snapshot_schedule_assessment_{REPORT_DATE}.csv", schedule_assessment_rows(ledger)),
        (out_dir / f"historical_recovery_matrix_{REPORT_DATE}.csv", recovery_rows(first_loss)),
        (out_dir / f"corrected_historical_recovery_matrix_{REPORT_DATE}.csv", corrected_recovery_rows(corrected_loss)),
        (out_dir / f"betonline_capture_incident_decisions_{REPORT_DATE}.csv", decision_rows(summary)),
    ]
    for path, rows in artifacts:
        fields = list(rows[0].keys()) if rows else ["empty"]
        write_csv(path, rows, fields)

    write_json(
        out_dir / f"machine_readable_betonline_capture_incident_{REPORT_DATE}.json",
        {"summary": summary, "legacy_first_loss": first_loss, "corrected_loss": corrected_loss, "revised_denominator": revised_denominator},
    )
    (out_dir / f"betonline_capture_integrity_incident_review_{REPORT_DATE}.md").write_text(markdown_summary(summary, corrected_loss))

    validation = validate_outputs(out_dir)
    write_csv(out_dir / f"validation_report_{REPORT_DATE}.csv", validation, list(validation[0].keys()))

    manifest = []
    for path in sorted(out_dir.glob("*")):
        if path.name == f"sha256_manifest_{REPORT_DATE}.csv":
            continue
        manifest.append({"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(out_dir / f"sha256_manifest_{REPORT_DATE}.csv", manifest, ["path", "sha256", "bytes"])
    return {"summary": summary, "out_dir": str(out_dir), "files": [str(p) for p in sorted(out_dir.glob("*"))]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    result = build(args.out_dir)
    print(json.dumps(result["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

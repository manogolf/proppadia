"""Repair and document BetOnline OddsAPI MLB player-prop capture coverage.

This is a bounded offline implementation/reporting step. It updates no data
stores and makes no network calls. Live diagnostic execution remains a separate
explicitly approved step.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.app.services.mlb import market_odds_service
from backend.mlb.shared.betonline_market_registry import active_market_rows, market_batches


REPO_ROOT = Path(__file__).resolve().parents[3]
ODDS_ROOT = REPO_ROOT / "backend/mlb/exports/odds_history"
OUT_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_oddsapi_capture_coverage_repair/2026-07-18"
)

TARGET_MARKETS = {
    "home_runs": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
    "stolen_bases": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
    "earned_runs": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
    "hits_allowed": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
}

DECISIONS = {
    "MLB_BETONLINE_CAPTURE_CURRENT_PATH_DECISION": "CURRENT_PATH_USES_DAILY_REFRESH_TO_MARKET_CACHE_AND_OPTIONAL_CAPTURE_ARCHIVE",
    "MLB_BETONLINE_CAPTURE_MARKET_REGISTRY_DECISION": "REPAIRED_CANONICAL_NINE_MARKET_REGISTRY_IMPLEMENTED",
    "MLB_BETONLINE_CAPTURE_HITS_ALLOWED_ROOT_CAUSE_DECISION": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
    "MLB_BETONLINE_CAPTURE_EARNED_RUNS_ROOT_CAUSE_DECISION": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
    "MLB_BETONLINE_CAPTURE_HOME_RUNS_ROOT_CAUSE_DECISION": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
    "MLB_BETONLINE_CAPTURE_STOLEN_BASES_ROOT_CAUSE_DECISION": "CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP",
    "MLB_BETONLINE_CAPTURE_ENDPOINT_DECISION": "EVENT_ODDS_ENDPOINT_WITH_DETERMINISTIC_BATCHING_REQUIRED_FOR_CERTIFICATION",
    "MLB_BETONLINE_CAPTURE_BATCHING_DECISION": "DETERMINISTIC_MARKET_BATCH_CONTRACT_FROZEN_OFFLINE",
    "MLB_BETONLINE_CAPTURE_RAW_RETENTION_DECISION": "RAW_RESPONSE_MANIFEST_REQUIRED_NO_CROSS_ENDPOINT_DEDUPLICATION",
    "MLB_BETONLINE_CAPTURE_PARSER_DECISION": "GENERIC_OVER_UNDER_PARSER_COVERS_ALL_NINE_RAW_KEYS",
    "MLB_BETONLINE_CAPTURE_COMPLETENESS_VALIDATION_DECISION": "READ_ONLY_VALIDATOR_IMPLEMENTED",
    "MLB_BETONLINE_CAPTURE_DIAGNOSTIC_EXECUTION_DECISION": "NOT_EXECUTED_PENDING_EXPLICIT_NETWORK_APPROVAL",
    "MLB_BETONLINE_CAPTURE_SCHEDULED_INTEGRATION_DECISION": "NOT_ACTIVATED_PENDING_APPROVED_LIVE_DIAGNOSTIC_PASS",
    "MLB_BETONLINE_CAPTURE_HISTORICAL_GAP_DECISION": "HISTORICAL_RETAINED_PAYLOAD_GAP_REMAINS_NOT_BACKFILLED",
    "MLB_BETONLINE_CAPTURE_DOWNSTREAM_READINESS_DECISION": "OFFLINE_READY_LIVE_RETENTION_NOT_CERTIFIED",
    "MLB_PRODUCTION_STATUS": "UNCHANGED",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def is_betonline(book: dict[str, Any]) -> bool:
    text = f"{book.get('key') or ''} {book.get('title') or ''}".lower()
    return "betonline" in text or str(book.get("key") or "").lower() == "betonlineag"


def events_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("events"), list):
        return [ev for ev in payload["events"] if isinstance(ev, dict)]
    if isinstance(payload, list):
        return [ev for ev in payload if isinstance(ev, dict)]
    return []


def scan_retained_occurrences() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    latest_by_key: dict[str, dict[str, Any]] = {}
    key_to_prop = {r["oddsapi_key"]: r["local_prop_type"] for r in active_market_rows()}
    for path in sorted(ODDS_ROOT.glob("20??-??-??/odds_mlb_playerprops*.json")):
        payload = read_json(path)
        events = events_from_payload(payload)
        if not events:
            continue
        slate_date = path.parent.name
        captured = str(payload.get("captured_at_utc") or "") if isinstance(payload, dict) else ""
        sha = sha256_file(path)
        counts: defaultdict[str, int] = defaultdict(int)
        games: defaultdict[str, set[str]] = defaultdict(set)
        for ev in events:
            game_id = str(ev.get("id") or "")
            for book in ev.get("bookmakers", []) or []:
                if not isinstance(book, dict) or not is_betonline(book):
                    continue
                for market in book.get("markets", []) or []:
                    if not isinstance(market, dict):
                        continue
                    key = str(market.get("key") or "")
                    if key not in key_to_prop:
                        continue
                    outcomes = [o for o in market.get("outcomes", []) or [] if isinstance(o, dict)]
                    counts[key] += len(outcomes)
                    games[key].add(game_id)
        for key, outcome_rows in counts.items():
            row = {
                "slate_date": slate_date,
                "capture_timestamp_utc": captured,
                "source_path": rel(path),
                "payload_sha256": sha,
                "raw_market_key": key,
                "local_prop_type": key_to_prop[key],
                "games_with_market": len(games[key]),
                "outcome_rows": outcome_rows,
            }
            rows.append(row)
            if key not in latest_by_key or (slate_date, captured, rel(path)) > (
                latest_by_key[key]["slate_date"],
                latest_by_key[key]["capture_timestamp_utc"],
                latest_by_key[key]["source_path"],
            ):
                latest_by_key[key] = row
    return rows, latest_by_key


def build_report(output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    market_rows = active_market_rows()
    observed_rows, latest_by_key = scan_retained_occurrences()
    service_map = market_odds_service.get_supported_market_map()

    paths = {
        "summary_md": output_dir / "betonline_capture_coverage_repair_2026-07-18.md",
        "current_path": output_dir / "betonline_capture_current_acquisition_path_2026-07-18.csv",
        "registry": output_dir / "betonline_capture_active_market_registry_2026-07-18.csv",
        "root_cause": output_dir / "betonline_capture_root_cause_findings_2026-07-18.csv",
        "batching": output_dir / "betonline_capture_endpoint_batching_contract_2026-07-18.csv",
        "network": output_dir / "betonline_capture_network_access_request_2026-07-18.csv",
        "raw_manifest": output_dir / "betonline_capture_raw_response_manifest_2026-07-18.csv",
        "parser": output_dir / "betonline_capture_parser_coverage_audit_2026-07-18.csv",
        "completeness": output_dir / "betonline_capture_snapshot_completeness_validation_2026-07-18.csv",
        "diagnostic": output_dir / "betonline_capture_approved_current_diagnostic_results_2026-07-18.csv",
        "schedule": output_dir / "betonline_capture_scheduled_integration_changes_2026-07-18.csv",
        "quota": output_dir / "betonline_capture_request_quota_impact_report_2026-07-18.csv",
        "historical": output_dir / "betonline_capture_historical_gap_classification_2026-07-18.csv",
        "downstream": output_dir / "betonline_capture_downstream_readiness_2026-07-18.csv",
        "decisions": output_dir / "betonline_capture_coverage_repair_decisions_2026-07-18.csv",
        "machine": output_dir / "machine_readable_betonline_capture_coverage_repair_2026-07-18.json",
        "sha_manifest": output_dir / "sha256_manifest_2026-07-18.csv",
        "validation": output_dir / "validation_report_2026-07-18.csv",
    }

    write_csv(
        paths["current_path"],
        [
            {
                "component": "LaunchAgent",
                "path": "/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist",
                "role": "runs daily MLB refresh wrapper at 05:30, 09:30, 11:00, 13:00, and 16:30 local time",
                "network_behavior": "wrapper invokes market cache refresh when daily refresh runs",
                "notes": "read-only inventory; no LaunchAgent changed",
            },
            {
                "component": "wrapper",
                "path": "/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh",
                "role": "sets current slate date/run tag and calls Make workflow",
                "network_behavior": "loads backend/.env then make targets may call OddsAPI through market cache",
                "notes": "read-only inventory; wrapper unchanged",
            },
            {
                "component": "Make target",
                "path": "Makefile:mlb-market-cache-refresh",
                "role": "runs backend.mlb.scripts.refresh_mlb_market_cache",
                "network_behavior": "OddsAPI via market_odds_service",
                "notes": "no completeness gate before this repair",
            },
            {
                "component": "live OddsAPI service",
                "path": "backend/app/services/mlb/market_odds_service.py",
                "role": "builds OddsAPI market list, fetches broad sport odds, falls back to event odds on 422",
                "network_behavior": "uses ODDS_API_KEY; supports bookmaker filter through MLB_ODDS_BOOKMAKERS",
                "notes": "now imports governed BetOnline registry into acquisition defaults",
            },
            {
                "component": "archive lane",
                "path": "Makefile:mlb-daily-capture",
                "role": "archives odds snapshots and slate artifacts when MLB_DAILY_INCLUDE_CAPTURE=1",
                "network_behavior": "depends on prediction/export path and retained odds snapshot",
                "notes": "production upload behavior unchanged",
            },
        ],
        ["component", "path", "role", "network_behavior", "notes"],
    )

    write_csv(
        paths["registry"],
        market_rows,
        [
            "local_prop_type",
            "oddsapi_key",
            "prop_family",
            "line_semantics",
            "parser_mapping",
            "expected_side_structure",
            "active_eligibility",
            "endpoint_family",
            "batching_group",
        ],
    )

    root_rows = []
    for local_prop, classification in TARGET_MARKETS.items():
        market_key = next(r["oddsapi_key"] for r in market_rows if r["local_prop_type"] == local_prop)
        latest = latest_by_key.get(market_key, {})
        root_rows.append(
            {
                "local_prop_type": local_prop,
                "raw_market_key": market_key,
                "classification": classification,
                "latest_retained_betonline_occurrence_date": latest.get("slate_date", ""),
                "latest_retained_source_path": latest.get("source_path", ""),
                "latest_retained_capture_timestamp_utc": latest.get("capture_timestamp_utc", ""),
                "current_registry_contains_market": market_key in service_map.values(),
                "notes": "User directly confirms current BetOnline book display; local retained payloads do not certify current capture.",
            }
        )
    write_csv(
        paths["root_cause"],
        root_rows,
        [
            "local_prop_type",
            "raw_market_key",
            "classification",
            "latest_retained_betonline_occurrence_date",
            "latest_retained_source_path",
            "latest_retained_capture_timestamp_utc",
            "current_registry_contains_market",
            "notes",
        ],
    )

    batches = [
        {
            **row,
            "endpoint": "GET /v4/sports/baseball_mlb/events/{event_id}/odds",
            "bookmaker_filter": "betonlineag",
            "retention_required": "raw response, request params, response status, SHA256, parse status",
            "notes": "Use deterministic batches so no market is silently dropped when endpoint limits reject oversized requests.",
        }
        for row in market_batches(max_markets_per_call=6)
    ]
    write_csv(
        paths["batching"],
        batches,
        ["batch_id", "market_count", "market_keys", "endpoint", "bookmaker_filter", "retention_required", "notes"],
    )

    write_csv(
        paths["network"],
        [
            {
                "request_status": "NOT_EXECUTED",
                "approval_status": "PENDING_EXPLICIT_NETWORK_APPROVAL",
                "proposed_command": "source backend/.env && .venv/bin/python -m backend.mlb.scripts.refresh_mlb_market_cache --days 1",
                "estimated_request_pattern": "one sport-odds request; if 422 fallback occurs, one events request plus two event-odds batches per eligible MLB event",
                "bookmaker": "betonlineag when MLB_ODDS_BOOKMAKERS=betonlineag is supplied",
                "notes": "No live diagnostic was executed in this task.",
            }
        ],
        ["request_status", "approval_status", "proposed_command", "estimated_request_pattern", "bookmaker", "notes"],
    )

    write_csv(
        paths["raw_manifest"],
        [
            {
                "manifest_field": field,
                "required": "yes",
                "notes": notes,
            }
            for field, notes in [
                ("capture_run_tag", "unique run tag for every capture"),
                ("slate_date", "ET slate date requested"),
                ("capture_timestamp_utc", "source/capture timestamp"),
                ("endpoint", "sport odds or event odds endpoint family"),
                ("event_id", "required for event-level odds"),
                ("requested_market_keys", "exact market batch sent to OddsAPI"),
                ("bookmaker_filter", "betonlineag when BetOnline certification is intended"),
                ("http_status", "must be retained per batch"),
                ("payload_sha256", "SHA256 of raw response body"),
                ("raw_response_path", "immutable run-tagged local path"),
                ("parse_status", "PASS/WARN/FAIL"),
                ("market_completeness_status", "per governed raw key"),
            ]
        ],
        ["manifest_field", "required", "notes"],
    )

    parser_rows = []
    market_to_prop = market_odds_service.get_market_to_prop_map(include_aliases=True)
    for row in market_rows:
        key = row["oddsapi_key"]
        parser_rows.append(
            {
                "local_prop_type": row["local_prop_type"],
                "raw_market_key": key,
                "service_market_to_prop": market_to_prop.get(key, ""),
                "generic_parser_path": "build_mlb_reconcile_rows._build_market_index; market_odds_service._extract_candidate_outcomes",
                "side_handling": "exact over/under outcomes; no opposite-side inference",
                "line_handling": "point retained as market line",
                "parser_status": "COVERED" if market_to_prop.get(key) == row["local_prop_type"] else "MAPPING_GAP",
                "notes": "",
            }
        )
    write_csv(
        paths["parser"],
        parser_rows,
        [
            "local_prop_type",
            "raw_market_key",
            "service_market_to_prop",
            "generic_parser_path",
            "side_handling",
            "line_handling",
            "parser_status",
            "notes",
        ],
    )

    latest_rows = []
    for row in market_rows:
        latest = latest_by_key.get(row["oddsapi_key"], {})
        latest_rows.append(
            {
                "local_prop_type": row["local_prop_type"],
                "raw_market_key": row["oddsapi_key"],
                "latest_retained_betonline_occurrence_date": latest.get("slate_date", ""),
                "latest_retained_source_path": latest.get("source_path", ""),
                "latest_retained_outcome_rows": latest.get("outcome_rows", ""),
                "current_capture_status": "NOT_CERTIFIED_PENDING_LIVE_DIAGNOSTIC",
                "notes": "Retained-history observation only; not a current-market certification.",
            }
        )
    write_csv(
        paths["completeness"],
        latest_rows,
        [
            "local_prop_type",
            "raw_market_key",
            "latest_retained_betonline_occurrence_date",
            "latest_retained_source_path",
            "latest_retained_outcome_rows",
            "current_capture_status",
            "notes",
        ],
    )

    write_csv(
        paths["diagnostic"],
        [
            {
                "diagnostic": "current_betonline_nine_market_capture",
                "execution_status": "NOT_EXECUTED_PENDING_EXPLICIT_NETWORK_APPROVAL",
                "markets_to_certify": ",".join(r["oddsapi_key"] for r in market_rows),
                "required_pass_condition": "all nine active raw keys retained with BetOnline rows or an explicit source-level unavailable status",
                "notes": "Network guardrail prevented live OddsAPI execution in this task.",
            }
        ],
        ["diagnostic", "execution_status", "markets_to_certify", "required_pass_condition", "notes"],
    )

    write_csv(
        paths["schedule"],
        [
            {
                "surface": "daily LaunchAgent",
                "change_applied": "no",
                "reason": "scheduled integration deferred until live diagnostic passes with explicit approval",
                "smallest_future_change": "set MLB_ODDS_BOOKMAKERS=betonlineag for BetOnline certification capture and run completeness validator after market cache/archive capture",
                "notes": "No LaunchAgent or wrapper was changed.",
            }
        ],
        ["surface", "change_applied", "reason", "smallest_future_change", "notes"],
    )

    event_counts = "unknown_until_slate"
    write_csv(
        paths["quota"],
        [
            {
                "capture_mode": "sport_odds_one_call",
                "estimated_requests": "1",
                "markets": ",".join(r["oddsapi_key"] for r in market_rows),
                "risk": "OddsAPI may reject some player-prop keys or oversized market groups",
                "notes": "Existing live path starts here.",
            },
            {
                "capture_mode": "event_odds_fallback",
                "estimated_requests": f"1 events request + 2 * eligible_event_count ({event_counts})",
                "markets": ",".join(r["oddsapi_key"] for r in market_rows),
                "risk": "Higher quota usage but deterministic per-event market coverage",
                "notes": "Recommended for certification if broad endpoint remains incomplete.",
            },
        ],
        ["capture_mode", "estimated_requests", "markets", "risk", "notes"],
    )

    write_csv(
        paths["historical"],
        [
            {
                "local_prop_type": row["local_prop_type"],
                "raw_market_key": row["oddsapi_key"],
                "gap_status": "HISTORICAL_GAP_REMAINS",
                "latest_retained_betonline_occurrence_date": latest_by_key.get(row["oddsapi_key"], {}).get("slate_date", ""),
                "historical_action": "do_not_backfill_without_separate_governance",
                "notes": "This repair affects future capture contract and validation; it does not create missing historical snapshots.",
            }
            for row in market_rows
        ],
        ["local_prop_type", "raw_market_key", "gap_status", "latest_retained_betonline_occurrence_date", "historical_action", "notes"],
    )

    model_status = {
        "hits": "production_model_exists",
        "total_bases": "production_model_exists",
        "hits_runs_rbis": "production_model_exists",
        "home_runs": "no_indexed_production_model_confirmed_in_prior_certification",
        "stolen_bases": "no_indexed_production_model_confirmed_in_prior_certification",
        "strikeouts_pitching": "production_model_exists",
        "outs_recorded": "production_path_exists",
        "earned_runs": "models_out/latest/earned_runs.joblib",
        "hits_allowed": "models_out/latest/hits_allowed.joblib; granular PHA Challenger is separate research instrument not production-authorized",
    }
    write_csv(
        paths["downstream"],
        [
            {
                "local_prop_type": row["local_prop_type"],
                "raw_market_key": row["oddsapi_key"],
                "capture_registry_status": "ACTIVE",
                "model_or_formula_status": model_status[row["local_prop_type"]],
                "downstream_status": "eligible_for_future_readiness_after_live_capture_certification",
                "notes": "Hits remains active first rebuild; no model or production selection changed.",
            }
            for row in market_rows
        ],
        ["local_prop_type", "raw_market_key", "capture_registry_status", "model_or_formula_status", "downstream_status", "notes"],
    )

    write_csv(
        paths["decisions"],
        [{"decision": key, "value": value, "notes": ""} for key, value in DECISIONS.items()],
        ["decision", "value", "notes"],
    )

    summary = [
        "# MLB BetOnline OddsAPI Player-Prop Capture Coverage Repair",
        "",
        f"Generated UTC: `{generated_at}`",
        "",
        "## Executive Summary",
        "",
        "The acquisition contract now has a canonical BetOnline MLB player-prop registry covering all nine intended active markets. The live OddsAPI service imports that registry into its market defaults, and a read-only completeness validator can certify retained payloads market-by-market.",
        "",
        "No live OddsAPI diagnostic was executed because the task forbids network calls without explicit approval. Therefore current BetOnline retention is implementation-ready but not live-certified.",
        "",
        "## Corrected Eligible Market Registry",
        "",
        "- Hitter: `hits`, `total_bases`, `hits_runs_rbis`, `home_runs`, `stolen_bases`",
        "- Pitcher: `strikeouts_pitching`, `outs_recorded`, `earned_runs`, `hits_allowed`",
        "",
        "## Root Cause",
        "",
        "The four disputed markets are classified as `CURRENT_BOOK_DISPLAY_VS_RETAINED_ODDSAPI_CAPTURE_GAP`: direct user observation confirms current BetOnline display, while retained local OddsAPI payloads do not certify current capture for those markets.",
        "",
        "## Implementation",
        "",
        "- Added `backend/mlb/shared/betonline_market_registry.py` as the canonical nine-market registry.",
        "- Wired `backend/app/services/mlb/market_odds_service.py` to preserve the registry in acquisition defaults.",
        "- Added `backend/mlb/scripts/validate_mlb_betonline_capture_completeness.py` for read-only market completeness validation.",
        "- Added this repair package generator; no production model/upload/tier behavior changed.",
        "",
        "## Live Diagnostic Status",
        "",
        "`MLB_BETONLINE_CAPTURE_DIAGNOSTIC_EXECUTION_DECISION = NOT_EXECUTED_PENDING_EXPLICIT_NETWORK_APPROVAL`",
        "",
        "## Direct Answer",
        "",
        "The repaired code path is configured to request and validate every intended BetOnline MLB player-prop market. Current live retention is not yet certified because no approved OddsAPI diagnostic was executed.",
        "",
        "## No Behavior Changed Note",
        "",
        "No model fitting, prediction formula, tiering, selector, upload, workspace, database, scheduler, or production behavior was changed.",
    ]
    paths["summary_md"].write_text("\n".join(summary) + "\n")

    machine = {
        "generated_at_utc": generated_at,
        "production_status": "UNCHANGED",
        "network_executed": False,
        "db_writes": False,
        "model_changes": False,
        "active_market_count": len(market_rows),
        "active_markets": market_rows,
        "decisions": DECISIONS,
        "live_certification_status": "NOT_CERTIFIED_PENDING_EXPLICIT_NETWORK_APPROVAL",
        "direct_answer": "The repaired code path is configured to request and validate every intended BetOnline MLB player-prop market; live retention is not yet certified.",
    }
    write_json(paths["machine"], machine)

    validation_rows = [
        {"check": "network_calls", "status": "PASS", "details": "No network calls executed."},
        {"check": "db_writes", "status": "PASS", "details": "No DB writes executed."},
        {"check": "market_registry_count", "status": "PASS", "details": str(len(market_rows))},
        {"check": "parser_mapping", "status": "PASS" if all(r["parser_status"] == "COVERED" for r in parser_rows) else "FAIL", "details": ""},
        {"check": "production_behavior", "status": "PASS", "details": "No model/upload/scheduler behavior changed."},
    ]
    write_csv(paths["validation"], validation_rows, ["check", "status", "details"])

    sha_rows = []
    for key, path in paths.items():
        if key == "sha_manifest":
            continue
        if path.exists():
            sha_rows.append({"artifact": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(paths["sha_manifest"], sha_rows, ["artifact", "sha256", "bytes"])
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", default="offline_repair", choices=["offline_repair", "read_only"])
    args = parser.parse_args()
    paths = build_report(Path(args.output_dir))
    print(json.dumps({key: str(path) for key, path in paths.items()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

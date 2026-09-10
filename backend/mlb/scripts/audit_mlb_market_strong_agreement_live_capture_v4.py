#!/usr/bin/env python3
"""Freeze and document the authorized v4 live-capture amendment; never calls an API."""
from __future__ import annotations

import argparse
import hashlib
import json
import plistlib
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts import capture_mlb_market_strong_agreement_live_v4 as capture
from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as v1


ROOT = capture.ROOT
OUT = capture.FREEZE.parent
V1 = v1.OUT / "pre_outcome_freeze.json"
V2 = ROOT / ("artifacts/analysis/model_development/mlb_market_strong_agreement_separation_feasibility_v2/"
             "2026-09-09/pre_outcome_freeze_v2.json")
V3 = ROOT / ("artifacts/analysis/model_development/mlb_market_strong_agreement_separation_live_timing_v3/"
             "2026-09-09/pre_outcome_freeze_v3.json")
WRAPPER = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
PLIST = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
EXISTING_CLIENT = ROOT / "backend/mlb/scripts/capture_mlb_pinnacle_main_markets_v1.py"
EXISTING_HOOK = ROOT / "bin/mlb_full_game_totals_daily_hook.sh"
NEW_CLIENT = ROOT / "backend/mlb/scripts/capture_mlb_market_strong_agreement_live_v4.py"
LIVE_HOOK = ROOT / "bin/mlb_market_strong_agreement_live_capture_v4.sh"
RECOVERY_HOOK = ROOT / "bin/mlb_market_strong_agreement_historical_recovery_v4.sh"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def line(text: str, needle: str) -> int:
    rows = [i for i, value in enumerate(text.splitlines(), 1) if needle in value]
    if len(rows) != 1:
        raise RuntimeError(f"Expected one installed workflow occurrence of {needle!r}; found {len(rows)}")
    return rows[0]


def row_counts() -> dict[str, int]:
    with sqlite3.connect(capture.LEDGER) as conn:
        v1.schema(conn)
        return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("predictions", "risk_set", "bookmaker_prices", "outcomes")}


def freeze_payload(frozen_at: str, counts: dict[str, int]) -> dict[str, Any]:
    rationale = {key: reason for _, key, _, _, reason in v1.hardened.BOOKS}
    return {
        "study_id": capture.STUDY_ID,
        "amendment_version": 4,
        "frozen_at_utc": frozen_at,
        "pre_outcome": True,
        "prospective_rows_at_amendment": counts["risk_set"],
        "authorized_primary": "STANDALONE_LIVE_TEN_BOOK_H2H_REQUEST",
        "supersedes_v3_recommendation_only": "EXPAND_AND_MOVE_EXISTING_LIVE_REQUEST",
        "preserved_prior_freezes": [
            {"version": 1, "path": str(V1.relative_to(ROOT)), "sha256": sha(V1)},
            {"version": 2, "path": str(V2.relative_to(ROOT)), "sha256": sha(V2)},
            {"version": 3, "path": str(V3.relative_to(ROOT)), "sha256": sha(V3)},
        ],
        "unchanged_contract": ("model, model threshold, market-strength threshold, agreement indicator, "
            "bookmaker set, risk-set admission, exclusions, grading, outcomes, uncertainty, and decision layers"),
        "prediction_commit_barrier": {
            "authority": "maximum created_at across every immutable designated model row for the slate date",
            "expected_row_count_authority": "games_discovered in the successful durable moneyline lifecycle result",
            "all_expected_rows_required": True,
            "capture_may_start_before_barrier": False,
            "maximum_start_lag_seconds": capture.MAX_START_LAG_SECONDS,
            "failure_mode": "FAIL_CLOSED_WITHOUT_REQUEST",
        },
        "live_request": {
            "endpoint": "GET /v4/sports/baseball_mlb/odds",
            "bookmakers": list(capture.BOOKS), "bookmaker_count": len(capture.BOOKS),
            "bookmaker_selection_rationale": rationale, "bookmaker_selection_used_outcomes": False,
            "markets": ["h2h"], "regions_parameter": "OMITTED", "odds_format": "american",
            "date_format": "iso", "designated_price_timestamp": "first request_started_at_utc after durable barrier",
            "capture_timestamp": "response_received_at_utc", "complete_raw_response_preserved_before_parsing": True,
            "request_parameters_exclude_secret": True, "quota_headers": list(capture.QUOTA_HEADERS),
            "bookmaker_and_market_last_update_preserved": True,
            "pregame_validation": "each book and h2h update must exist, be no later than receipt, and precede commence_time",
        },
        "isolation": {
            "launch_mode": "detached child immediately after successful durable moneyline lifecycle hook",
            "shadow_exit_status_propagates_to_workflow": False,
            "shadow_process_is_awaited_by_workflow": False,
            "market_data_enters_prediction_generation": False,
            "existing_pinnacle_request_moved_expanded_or_modified": False,
            "existing_totals_spreads_props_or_publication_artifacts_modified": False,
            "integrity_failures": "classified and excluded; never silently substituted",
        },
        "authorization": {
            "routine_dates_inclusive": [capture.START_DATE, capture.END_DATE],
            "routine_dates": 18, "expected_live_cost_each": 1,
            "routine_credit_ceiling": capture.ROUTINE_CEILING,
            "historical_recovery_count_ceiling": capture.RECOVERY_COUNT_CEILING,
            "historical_expected_cost_each": capture.RECOVERY_EXPECTED_COST,
            "historical_recovery_credit_ceiling": 30, "total_credit_ceiling": capture.TOTAL_CEILING,
            "actual_cost_authority": "x-requests-last; an unknown charge reserves the full expected request cost",
            "stop_before_request_if_expected_total_would_exceed_ceiling": True,
        },
        "idempotency": {
            "identity": ["game_date", "run_identity", "study_id"],
            "charged_call_guard": "immutable date/mode claim acquired transactionally before credential access",
            "repeat_live_call_after_any_prior_live_claim": False,
            "complete_success_is_never_repeated": True,
        },
        "recovery": {
            "command": "bin/mlb_market_strong_agreement_historical_recovery_v4.sh YYYY-MM-DD",
            "eligible_live_failures": sorted(capture.RECOVERY_ELIGIBLE),
            "successful_response_bookmaker_absence_incomplete_market_or_one_sided_price_is_eligible": False,
            "historical_target": "immutable first live request_started_at_utc",
            "returned_snapshot_must_be_at_or_before_target": True,
            "row_label": "HISTORICAL_RECOVERY",
            "outcomes_or_observed_returns_consulted": False,
        },
        "scheduler": {"existing_0530_orchestration": True, "new_calendar_schedule_created": False,
            "disable_switch": "MLB_AGREEMENT_SEPARATION_CAPTURE_V4_ENABLED=0"},
        "constraints": {"wagering": False, "model_changed": False, "threshold_changed": False,
            "public_output_changed": False, "unrelated_scheduler_changed": False,
            "odds_api_called_during_implementation_or_validation": False},
    }


def implementation_evidence() -> dict[str, Any]:
    wrapper = WRAPPER.read_text()
    plist = plistlib.loads(PLIST.read_bytes())
    lifecycle = line(wrapper, 'bin/mlb_public_game_moneyline_daily_hook.sh "$MLB_DATE_ET"')
    live = line(wrapper, "bin/mlb_market_strong_agreement_live_capture_v4.sh")
    established = line(wrapper, 'bin/mlb_full_game_totals_daily_hook.sh "$MLB_DATE_ET" "$MLB_RUN_TAG"')
    return {
        "installed_wrapper": {"path": str(WRAPPER), "sha256": sha(WRAPPER),
            "moneyline_lifecycle_line": lifecycle, "new_shadow_line": live,
            "existing_main_market_line": established,
            "order_is_commit_then_shadow_then_existing_main_market": lifecycle < live < established,
            "detached_syntax_present": "2>&1 &!" in wrapper,
            "disable_switch_present": "MLB_AGREEMENT_SEPARATION_CAPTURE_V4_ENABLED" in wrapper},
        "launchagent": {"path": str(PLIST), "sha256": sha(PLIST),
            "has_0530_local": {"Hour": 5, "Minute": 30} in plist["StartCalendarInterval"],
            "new_calendar_schedule": False},
        "existing_lane_immutability": {
            "client_path": str(EXISTING_CLIENT.relative_to(ROOT)), "client_sha256": sha(EXISTING_CLIENT),
            "expected_client_sha256": "b756db8e0b9f6575aa80cd708a9d31efcd8d46b9e0437b8c88679e480d3c5aa1",
            "hook_path": str(EXISTING_HOOK.relative_to(ROOT)), "hook_sha256": sha(EXISTING_HOOK),
            "expected_hook_sha256": "1892f92af6cfa1766857b4bac35b9723f789760848807367a27b79269c754be3",
            "unchanged": sha(EXISTING_CLIENT) == "b756db8e0b9f6575aa80cd708a9d31efcd8d46b9e0437b8c88679e480d3c5aa1"
                         and sha(EXISTING_HOOK) == "1892f92af6cfa1766857b4bac35b9723f789760848807367a27b79269c754be3"},
        "new_files": {"client": str(NEW_CLIENT.relative_to(ROOT)), "client_sha256": sha(NEW_CLIENT),
            "live_hook": str(LIVE_HOOK.relative_to(ROOT)), "live_hook_sha256": sha(LIVE_HOOK),
            "manual_recovery_hook": str(RECOVERY_HOOK.relative_to(ROOT)),
            "manual_recovery_hook_sha256": sha(RECOVERY_HOOK)},
        "odds_api_requests_during_implementation": 0,
        "credential_value_read_or_tested": False,
    }


def manifest(output: Path) -> None:
    names = sorted(path for path in output.iterdir() if path.is_file() and path.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(path)}  {path.name}\n" for path in names))


def run(output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    counts = row_counts()
    if any(counts.values()):
        raise RuntimeError("V4 amendment must be frozen before any prospective ledger row exists")
    if not (output / "original_v3_freeze.json").exists():
        shutil.copyfile(V3, output / "original_v3_freeze.json")
    freeze_path = output / "pre_outcome_freeze_v4.json"
    if freeze_path.exists():
        freeze = json.loads(freeze_path.read_text())
    else:
        freeze = freeze_payload(now(), counts); write(freeze_path, freeze)
    evidence = implementation_evidence(); write(output / "implementation_evidence.json", evidence)
    write(output / "credit_controls.json", freeze["authorization"])
    (output / "rollback.md").write_text(
        "# Isolated v4 rollback\n\n"
        "Disable only this stage by setting `MLB_AGREEMENT_SEPARATION_CAPTURE_V4_ENABLED=0` in the "
        "environment loaded by the existing wrapper. No LaunchAgent calendar change is needed.\n\n"
        "For full removal, delete only the conditional v4 launch block immediately after "
        "`mlb_public_game_moneyline_daily_hook.sh` in `/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh`. "
        "The existing moneyline lifecycle and later Pinnacle h2h/totals/spreads hook stay in place. After "
        "disabling/removing the launch, the two v4 shell hooks and v4 client may be removed in a separate "
        "commit. Preserve the freeze, SQLite ledger, raw responses, request metadata, quota headers, and logs "
        "for audit.\n")
    (output / "readiness.md").write_text(
        "# Next-slate readiness\n\n"
        "Status: **READY_FOR_NEXT_ELIGIBLE_SLATE (2026-09-10)**.\n\n"
        "The existing 05:30 orchestration now launches the isolated capture immediately after the durable "
        "moneyline lifecycle returns successfully. The client independently re-verifies the committed row "
        "count and commit barrier, then fails closed unless request start is within 300 seconds. The live "
        "request is date-idempotent and the parent workflow does not wait for it. Tests use fixtures only; no "
        "credential value was read or tested and no Odds API request was made. Runtime credential presence "
        "remains an operational prerequisite and will be handled without persistence.\n")
    with sqlite3.connect(capture.LEDGER) as conn:
        capture.schema(conn); capture.establish_authorization(conn, freeze_path); conn.commit()
    result = {"status": "FROZEN_AND_SCHEDULED", "freeze_sha256": sha(freeze_path),
              "prospective_rows_at_freeze": counts, "network_requests": 0,
              "next_eligible_slate": capture.START_DATE, "installed_wrapper_sha256": evidence["installed_wrapper"]["sha256"]}
    write(output / "implementation_summary.json", result); manifest(output)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args(); print(json.dumps(run(args.output), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

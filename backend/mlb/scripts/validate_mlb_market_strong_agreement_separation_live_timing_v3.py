#!/usr/bin/env python3
"""Deterministically validate the pre-outcome live-timing amendment."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from backend.mlb.scripts import audit_mlb_market_strong_agreement_separation_live_timing_v3 as audit
from backend.mlb.scripts.audit_mlb_oddsapi_credential_containment_v1 import CREDENTIAL


REQUIRED = {"original_v1_freeze.json", "original_v2_freeze.json", "pre_outcome_freeze_v3.json",
    "workflow_evidence.json", "prediction_write_evidence.json", "acquisition_options.json",
    "acquisition_options.csv", "live_timing_review.md", "sha256_manifest.txt"}


def validate(output: Path) -> dict[str, object]:
    checks: dict[str, bool] = {}
    checks["required_outputs_present"] = all((output / name).is_file() for name in REQUIRED)
    freeze = json.loads((output / "pre_outcome_freeze_v3.json").read_text())
    evidence = json.loads((output / "workflow_evidence.json").read_text())
    options = json.loads((output / "acquisition_options.json").read_text())
    checks["prior_freezes_byte_exact"] = (
        (output / "original_v1_freeze.json").read_bytes() == audit.V1_FREEZE.read_bytes()
        and (output / "original_v2_freeze.json").read_bytes() == audit.V2_FREEZE.read_bytes())
    checks["freeze_exact"] = freeze == audit.freeze_payload(freeze["frozen_at_utc"])
    with sqlite3.connect(audit.LEDGER) as conn:
        counts = [conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                  for table in ("risk_set", "bookmaker_prices", "outcomes")]
        metadata = conn.execute("SELECT freeze_sha256 FROM study_metadata WHERE study_id=?",
                                (audit.STUDY_ID,)).fetchone()
    checks["amended_before_rows"] = sum(counts) == 0 and freeze["prospective_rows_at_amendment"] == 0
    checks["ledger_bound"] = bool(metadata and metadata[0] == audit.sha(output / "pre_outcome_freeze_v3.json"))
    checks["actual_0530_workflow_order"] = (evidence["launchagent"]["has_0530_local_interval"]
        and evidence["installed_wrapper"]["moneyline_precedes_market_capture"])
    client = evidence["current_client"]
    checks["actual_endpoint_parameters"] = (client["endpoint"].endswith("/odds")
        and client["uses_bookmakers_pinnacle"] and not client["uses_region_parameter"]
        and client["markets"] == ["h2h", "totals", "spreads"]
        and client["downstream_parser_retains_only_pinnacle_rows"]
        and client["prediction_attachment_occurs_after_immutable_prediction_fetch"])
    sample = evidence["sample_2026_09_09"]
    checks["observed_header_cost"] = sample["x_requests_last"] == 3
    checks["observed_timing_reconciled"] = (sample["meets_post_commit_order"]
        and not sample["meets_immediate_max_lag"]
        and abs(sample["prediction_commit_to_request_lag_seconds"] - 1130.785939) < 1e-6)
    checks["observed_bookmaker_timestamps_valid"] = (sample["pinnacle_h2h_rows"] == 14
        and sample["all_h2h_updates_pregame"] and sample["all_h2h_updates_not_after_response"])
    checks["ten_books_one_group"] = evidence["bookmaker_group_rule"]["remains_one_group"]
    by_name = {item["option"]: item for item in options}
    checks["option_costs_exact"] = (
        by_name["EXPAND_AND_MOVE_EXISTING_LIVE_REQUEST"]["incremental_study_credits_no_failures"] == 0
        and by_name["ADD_SEPARATE_LIVE_TEN_BOOK_H2H_REQUEST"]["incremental_study_credits_no_failures"] == 18
        and by_name["HISTORICAL_SNAPSHOT_FOR_EVERY_DATE"]["incremental_study_credits_no_failures"] == 180)
    checks["live_timing_frozen"] = (freeze["recommended_primary"] == "EXPAND_AND_MOVE_EXISTING_LIVE_REQUEST"
        and freeze["designated_live_capture"]["maximum_prediction_commit_to_first_request_seconds"] == 300
        and freeze["designated_live_capture"]["regions_parameter"] == "OMITTED")
    checks["failure_recovery_frozen"] = (freeze["failure_recovery"]["outcomes_or_prices_may_not_control_recovery"]
        and "HISTORICAL_RECOVERY" in freeze["failure_recovery"]["historical_acceptance"])
    checks["no_scheduler_or_api_change"] = (not freeze["constraints"]["scheduler_changed"]
        and not freeze["odds_api_called_by_review"] and evidence["odds_api_requests_by_this_review"] == 0)
    checks["credential_free"] = not any(CREDENTIAL.search(path.read_bytes())
        for path in output.iterdir() if path.is_file())
    failed = sorted(name for name, passed in checks.items() if not passed)
    result = {"validator": audit.STUDY_ID, "status": "PASS" if not failed else "FAIL",
              "checks": checks, "failed_checks": failed, "network_requests": 0,
              "credential_environment_read": False, "scheduler_changed": False}
    if failed:
        raise RuntimeError("V3 live-timing validation failed: " + ", ".join(failed))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output", type=Path, default=audit.OUT)
    args = parser.parse_args(); result = validate(args.output)
    audit.write_json(args.output / "validation_summary.json", result); audit.write_manifest(args.output)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

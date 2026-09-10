#!/usr/bin/env python3
"""Deterministic, network-free validator for the v4 live-capture activation."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from backend.mlb.scripts import audit_mlb_market_strong_agreement_live_capture_v4 as audit
from backend.mlb.scripts import capture_mlb_market_strong_agreement_live_v4 as capture
from backend.mlb.scripts.audit_mlb_oddsapi_credential_containment_v1 import CREDENTIAL


REQUIRED = {"original_v3_freeze.json", "pre_outcome_freeze_v4.json", "implementation_evidence.json",
            "credit_controls.json", "rollback.md", "readiness.md", "implementation_summary.json"}


def validate(output: Path) -> dict[str, object]:
    freeze = json.loads((output / "pre_outcome_freeze_v4.json").read_text())
    evidence = json.loads((output / "implementation_evidence.json").read_text())
    checks = {
        "required_outputs": all((output / name).is_file() for name in REQUIRED),
        "prior_v3_preserved": (output / "original_v3_freeze.json").read_bytes() == audit.V3.read_bytes(),
        "freeze_exact": freeze == audit.freeze_payload(freeze["frozen_at_utc"],
            {"predictions": 0, "risk_set": 0, "bookmaker_prices": 0, "outcomes": 0}),
        "standalone_h2h_frozen": freeze["authorized_primary"] == "STANDALONE_LIVE_TEN_BOOK_H2H_REQUEST"
            and freeze["live_request"]["markets"] == ["h2h"] and len(freeze["live_request"]["bookmakers"]) == 10,
        "timing_contract": freeze["prediction_commit_barrier"]["maximum_start_lag_seconds"] == 300,
        "credit_contract": freeze["authorization"]["routine_credit_ceiling"] == 18
            and freeze["authorization"]["historical_recovery_count_ceiling"] == 3
            and freeze["authorization"]["total_credit_ceiling"] == 48,
        "recovery_fail_closed": freeze["recovery"]["eligible_live_failures"] == sorted(capture.RECOVERY_ELIGIBLE)
            and not freeze["recovery"]["successful_response_bookmaker_absence_incomplete_market_or_one_sided_price_is_eligible"],
        "workflow_order_and_detachment": evidence["installed_wrapper"]["order_is_commit_then_shadow_then_existing_main_market"]
            and evidence["installed_wrapper"]["detached_syntax_present"],
        "existing_lane_unchanged": evidence["existing_lane_immutability"]["unchanged"],
        "existing_0530_reused": evidence["launchagent"]["has_0530_local"] and not evidence["launchagent"]["new_calendar_schedule"],
        "no_network_or_credential_test": evidence["odds_api_requests_during_implementation"] == 0
            and not evidence["credential_value_read_or_tested"],
        "credential_free_artifacts": not any(CREDENTIAL.search(path.read_bytes()) for path in output.iterdir()
            if path.is_file()),
    }
    with sqlite3.connect(capture.LEDGER) as conn:
        row = conn.execute("SELECT freeze_sha256,routine_credit_ceiling,recovery_count_ceiling,total_credit_ceiling FROM live_capture_authorization_v4 WHERE study_id=?",
                           (capture.STUDY_ID,)).fetchone()
    checks["ledger_authorization_bound"] = row == (audit.sha(output / "pre_outcome_freeze_v4.json"), 18, 3, 48)
    failed = sorted(key for key, value in checks.items() if not value)
    result = {"status": "PASS" if not failed else "FAIL", "checks": checks, "failed_checks": failed,
              "network_requests": 0, "charged_requests": 0, "scheduler_installed": True}
    if failed:
        raise RuntimeError("V4 validation failed: " + ", ".join(failed))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output", type=Path, default=audit.OUT)
    args = parser.parse_args(); result = validate(args.output)
    audit.write(args.output / "validation_summary.json", result); audit.manifest(args.output)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

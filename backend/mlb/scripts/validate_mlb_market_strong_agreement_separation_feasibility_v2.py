#!/usr/bin/env python3
"""Validate the pre-outcome MLB agreement-separation feasibility amendment."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd

from backend.mlb.scripts import audit_mlb_market_strong_agreement_separation_feasibility_v2 as audit
from backend.mlb.scripts.audit_mlb_oddsapi_credential_containment_v1 import CREDENTIAL


REQUIRED = {
    "original_v1_freeze.json", "pre_outcome_freeze_v2.json", "power_derivation.json",
    "historical_feasibility.json", "historical_daily_risk_set_rates.csv", "schedule_inventory.json",
    "remaining_regular_season_schedule.csv", "official_remaining_regular_season_schedule_raw.json",
    "expected_2026_regular_season_rows.json",
    "acquisition_cost_review.json", "x_requests_last_cost_evidence.csv", "bounded_2026_report.json",
    "bounded_2026_report.md", "review_summary.json", "feasibility_review.md", "sha256_manifest.txt",
}


def validate(output: Path) -> dict[str, object]:
    checks: dict[str, bool] = {}
    checks["required_outputs_present"] = all((output / name).is_file() for name in REQUIRED)
    freeze = json.loads((output / "pre_outcome_freeze_v2.json").read_text())
    power = json.loads((output / "power_derivation.json").read_text())
    history = json.loads((output / "historical_feasibility.json").read_text())
    schedule = json.loads((output / "schedule_inventory.json").read_text())
    costs = json.loads((output / "acquisition_cost_review.json").read_text())
    bounded = json.loads((output / "bounded_2026_report.json").read_text())
    review = json.loads((output / "review_summary.json").read_text())
    checks["original_freeze_byte_exact"] = (
        (output / "original_v1_freeze.json").read_bytes() == audit.V1_FREEZE.read_bytes()
        and audit.sha(audit.V1_FREEZE) == freeze["original_v1_freeze_sha256"]
    )
    checks["v2_freeze_exact"] = freeze == audit.amendment_payload(
        freeze["frozen_at_utc"], audit.sha(audit.V1_FREEZE), schedule["source_sha256"])
    with sqlite3.connect(audit.v1.LEDGER) as conn:
        metadata = conn.execute("SELECT freeze_sha256 FROM study_metadata WHERE study_id=?",
                                (audit.V2_STUDY,)).fetchone()
        risk_rows = conn.execute("SELECT COUNT(*) FROM risk_set").fetchone()[0]
    checks["ledger_bound_to_v2_freeze"] = bool(metadata and metadata[0] == audit.sha(output / "pre_outcome_freeze_v2.json"))
    checks["amended_before_first_row"] = freeze["prospective_rows_at_amendment"] == 0 and risk_rows == 0
    checks["power_reproduces_732"] = (power["agreement_ceiling"] == 438
                                      and power["comparison_ceiling"] == 294
                                      and power["total_after_separate_group_ceilings"] == 732)
    checks["power_inputs_explicit"] = (power["baseline_win_rate"] == 31/51
                                       and power["minimum_detectable_incremental_effect_absolute"] == .10
                                       and power["power"] == .80 and power["two_sided_alpha"] == .05)
    checks["target_is_eligible_resolved"] = review["floor_determination"]["meaning_of_732"].startswith(
        "resolved comparison-eligible")
    checks["historical_rates_reconcile"] = (history["market_strong_rows"] == 127
                                             and history["agreement_rows"] == 76
                                             and history["without_agreement_rows"] == 51
                                             and history["ordinary_game_dates"] == 33)
    remaining = pd.read_csv(output / "remaining_regular_season_schedule.csv")
    checks["remaining_schedule_exact"] = (len(remaining) == 18
                                            and remaining.scheduled_regular_season_games.sum() == 235
                                            and schedule["first_date"] == "2026-09-10"
                                            and schedule["last_date"] == "2026-09-27"
                                            and audit.sha(output / "official_remaining_regular_season_schedule_raw.json")
                                            == schedule["source_sha256"])
    evidence = pd.read_csv(output / "x_requests_last_cost_evidence.csv")
    hist = evidence[evidence.evidence_class.eq("IDENTICAL_TEN_BOOK_HISTORICAL_H2H")]
    checks["historical_cost_verified_from_headers"] = len(hist) == 29 and hist.x_requests_last.eq(10).all()
    checks["exact_regular_season_ceiling"] = (freeze["acquisition"]["calls"] == 18
                                               and freeze["acquisition"]["verified_cost_per_call"] == 10
                                               and freeze["acquisition"]["exact_credit_ceiling"] == 180
                                               and review["exact_credit_ceiling"] == 180)
    checks["live_expansion_not_overclaimed"] = costs["current_capture_expansion_conclusion"].startswith(
        "NOT_VERIFIED_AT_ZERO_MARGINAL_COST")
    checks["bounded_categories_exact"] = set(freeze["bounded_2026_regular_season_layer"]["categories"]) == audit.BOUNDED_CATEGORIES
    checks["initial_bounded_state"] = (bounded["category"] == "2026_INSUFFICIENT_COMPARISON_SUPPORT"
                                       and not bounded["bounded_assessment_made"])
    checks["no_promotion_or_state_change"] = (not bounded["promotion_authorized"]
        and not freeze["constraints"]["wagering"] and not freeze["constraints"]["production"]
        and not freeze["constraints"]["scheduler"] and not freeze["constraints"]["model_change"]
        and not freeze["constraints"]["threshold_change"] and not freeze["constraints"]["prediction_change"])
    checks["no_odds_api_request"] = review["odds_api_requests"] == 0 and not freeze["acquisition"]["api_called_by_feasibility_review"]
    checks["package_has_no_credential_literal"] = not any(
        CREDENTIAL.search(path.read_bytes()) for path in output.iterdir() if path.is_file())
    failed = sorted(name for name, passed in checks.items() if not passed)
    result = {"validator": "MLB_MARKET_STRONG_AGREEMENT_SEPARATION_FEASIBILITY_V2",
              "status": "PASS" if not failed else "FAIL", "checks": checks,
              "failed_checks": failed, "network_requests": 0, "credential_environment_read": False}
    if failed: raise RuntimeError("Feasibility v2 validation failed: " + ", ".join(failed))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output", type=Path, default=audit.OUT)
    args = parser.parse_args(); result = validate(args.output)
    audit.write_json(args.output / "validation_summary.json", result); audit.write_manifest(args.output)
    print(json.dumps(audit.clean(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

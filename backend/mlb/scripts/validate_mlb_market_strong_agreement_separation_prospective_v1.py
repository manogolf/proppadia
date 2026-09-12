#!/usr/bin/env python3
"""Deterministic structural validator for the locked prospective separation study."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd

from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as study
from backend.mlb.scripts.audit_mlb_oddsapi_credential_containment_v1 import CREDENTIAL


REQUIRED_OUTPUTS = (
    "append_only_request_ledger.csv",
    "append_only_retry_decision_ledger.csv",
    "pre_outcome_freeze.json",
    "schema.sql",
    "prospective_risk_set_ledger.csv",
    "prospective_date_request_plan.csv",
    "bookmaker_price_ledger.csv",
    "outcome_ledger.csv",
    "outcome_and_probability_metrics.csv",
    "bookmaker_economics.csv",
    "bookmaker_incremental_economics.csv",
    "bookmaker_price_state_coverage.csv",
    "late_season_regime_metrics.csv",
    "blocked_date_agreement_coefficients.csv",
    "blocked_date_score_differences.csv",
    "summary.json",
    "interim_report.md",
)


def validate(output: Path, ledger: Path) -> dict[str, object]:
    checks: dict[str, bool] = {}
    freeze = study.verify_freeze(output, ledger)
    summary = json.loads((output / "summary.json").read_text())
    checks["required_outputs_present"] = all((output / name).is_file() for name in REQUIRED_OUTPUTS)
    checks["package_has_no_credential_literal"] = not any(
        CREDENTIAL.search(path.read_bytes()) for path in output.rglob("*") if path.is_file()
    )
    checks["frozen_model_identity"] = (
        freeze["model"]["version"] == study.MODEL
        and freeze["model"]["hash"] == study.MODEL_HASH
        and freeze["model"]["strong_definition"] == "selected probability > 0.60 strict"
    )
    checks["frozen_market_definition"] = (
        freeze["market"]["reference_book"] == "pinnacle"
        and freeze["market"]["strong_definition"]
        == "reference-book no-vig selected-side probability > 0.60 strict"
    )
    checks["frozen_bookmaker_set"] = freeze["bookmakers"] == list(study.BOOKS) and len(study.BOOKS) == 10
    checks["prospective_only"] = (
        freeze["prospective_start_game_date"] == "2026-09-10"
        and freeze["prospective_end_game_date"] == "2026-12-31"
        and not freeze["prior_56_20_rows_admitted"]
    )
    checks["no_early_stopping"] = not freeze["early_stopping"] and freeze["decision_not_before_utc_date"] == "2027-01-01"
    checks["fixed_evidence_requirement"] = freeze["evidence_requirement"] == study.MINIMUM_EVIDENCE
    checks["fixed_terminal_categories"] = summary["classification"] in study.FINAL_CATEGORIES
    checks["one_outcome_not_bookmaker_cells"] = summary["bookmaker_cells_do_not_inflate_effective_sample_size"] is True
    checks["no_forbidden_action"] = all(not summary[key] for key in (
        "prior_56_20_rows_included", "book_selected_by_roi", "best_price_composite", "wagering",
        "production_changes", "scheduler_changes", "public_prediction_changes",
    ))
    risk = pd.read_csv(output / "prospective_risk_set_ledger.csv")
    if len(risk):
        checks["no_pre_freeze_rows"] = bool(risk.game_date.astype(str).ge(study.PROSPECTIVE_START).all())
        checks["risk_states_closed"] = set(risk.risk_state).issubset(study.RISK_STATES)
        checks["unique_risk_games"] = not risk.game_key.duplicated().any()
    else:
        checks["no_pre_freeze_rows"] = True
        checks["risk_states_closed"] = True
        checks["unique_risk_games"] = True
    with sqlite3.connect(ledger) as conn:
        triggers = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
        outcome_count = conn.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
        price_count = conn.execute("SELECT COUNT(*) FROM bookmaker_prices").fetchone()[0]
    # The live-capture v4 extension adds its own append-only protections to
    # this ledger.  Require every frozen base trigger without rejecting those
    # additional protections.
    with sqlite3.connect(":memory:") as baseline:
        study.schema(baseline)
        required_triggers = {
            row[0] for row in baseline.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
        }
    checks["append_only_triggers"] = required_triggers.issubset(triggers)
    checks["effective_outcome_count_exact"] = summary["effective_outcome_count"] == outcome_count
    checks["bookmaker_cell_count_exact"] = summary["bookmaker_price_cells"] == price_count
    failed = sorted(name for name, passed in checks.items() if not passed)
    result = {"validator": "MLB_MARKET_STRONG_AGREEMENT_SEPARATION_PROSPECTIVE_V1",
              "status": "PASS" if not failed else "FAIL", "checks": checks,
              "failed_checks": failed, "network_requests": 0, "credential_environment_read": False}
    if failed:
        raise RuntimeError("Prospective separation validation failed: " + ", ".join(failed))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=study.OUT)
    parser.add_argument("--ledger", type=Path, default=study.LEDGER)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else study.ROOT / args.output
    ledger = args.ledger if args.ledger.is_absolute() else study.ROOT / args.ledger
    result = validate(output, ledger)
    (output / "validation_summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    study.write_manifest(output)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import csv
import json
from pathlib import Path

import pandas as pd


p = Path(__file__).resolve().parent
root = p.parents[4]
errors = []

cohort = pd.read_csv(p / "frozen_joint_strength_cohort.csv")
prices = pd.read_csv(p / "reconciled_bookmaker_price_ledger.csv")
economics = pd.read_csv(p / "bookmaker_complete_coverage_economics.csv")
findings = list(csv.DictReader((p / "credential_containment_findings.csv").read_text().splitlines()))
containment = json.loads((p / "credential_containment_summary.json").read_text())
information = json.loads((root / "artifacts/analysis/model_development/mlb_strong_moneyline_independent_information_decomposition_v1/2026-09-09/summary.json").read_text())
incremental = json.loads((root / "artifacts/analysis/model_development/mlb_joint_strength_incremental_value_executability_audit_v1/2026-09-09/summary.json").read_text())

if len(cohort) != 76 or int(cohort.evaluated_win.sum()) != 56:
    errors.append("fixed_cohort_or_record_changed")
if len(prices) != 760 or int(prices.admission_class.eq("VALID_PREGAME_PRICE").sum()) != 755:
    errors.append("bookmaker_cell_reconciliation_changed")
alternatives = economics[economics.bookmaker_key.ne("pinnacle")]
if len(alternatives) != 9 or not alternatives.hypothetical_flat_risk_roi.gt(0).all():
    errors.append("alternative_book_economic_transfer_changed")
if not ((economics.clustered_roi_ci_2_5 <= 0) & (economics.clustered_roi_ci_97_5 >= 0)).all():
    errors.append("clustered_interval_statement_changed")
if information["information_tests"]["model_coefficient_positive_fold_rate"] != 0.0:
    errors.append("prior_model_after_market_direction_changed")
if information["information_tests"]["market_coefficient_positive_fold_rate"] != 1.0:
    errors.append("prior_market_after_model_direction_changed")
if incremental["continuous_model_negative_folds"] != 4 or incremental["market_positive_folds"] != 5:
    errors.append("later_incremental_fold_result_changed")
if containment["credential_environment_read"] or containment["credential_values_printed"]:
    errors.append("containment_safety_contract_failed")
outstanding = [row for row in findings if row["remediation_required"].lower() == "true"]
if [row["path"] for row in outstanding] != ["artifacts/analysis/network_monitor/2026-09-03/private_packet_capture"]:
    errors.append("unexpected_unremediated_path_set")
if containment["committed_acquisition_package"] != "NO_CREDENTIAL_LITERAL_DETECTED":
    errors.append("committed_package_scan_failed")
if containment["git_history_odds_api_credential"] != "NO_ODDS_API_CREDENTIAL_DETECTED":
    errors.append("git_history_scan_failed")

print(json.dumps({"errors": errors, "status": "PASS" if not errors else "FAIL"}, sort_keys=True))
raise SystemExit(bool(errors))

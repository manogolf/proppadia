from __future__ import annotations

import csv
import hashlib
import json

import pandas as pd
import pytest

from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as raw
from backend.mlb.scripts import run_mlb_totals_remove_park_history_depth_direct_location_defect_v1 as repair


OUTPUT = repair.DEFAULT_OUTPUT


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_artifact():
    return json.loads((OUTPUT / "TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1.json").read_text())


def test_repair_artifact_has_exact_authorized_feature_delta_and_stable_hash():
    control = json.loads(repair.CONFIG.read_text())
    artifact = load_artifact()
    identity = json.loads((OUTPUT / "totals_park_depth_repair_model_identity.json").read_text())
    expected = [feature for feature in control["feature_order"] if feature != repair.REMOVED_FEATURE]
    assert artifact["feature_order"] == expected
    assert len(artifact["feature_order"]) == 21
    assert repair.REMOVED_FEATURE not in artifact["feature_order"]
    assert "strict_prior_total_run_factor" in artifact["feature_order"]
    assert artifact["canonical_model_hash"] == repair.model_contract_hash(artifact)
    assert identity["canonical_model_hash"] == artifact["canonical_model_hash"]
    assert identity["artifact_sha256"] == digest(OUTPUT / "TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1.json")
    retained_indices = [control["feature_order"].index(feature) for feature in artifact["feature_order"]]
    assert artifact["scaler_mean"] == [control["scaler_mean"][index] for index in retained_indices]
    assert artifact["scaler_scale"] == [control["scaler_scale"][index] for index in retained_indices]


def test_same_row_scoring_is_depth_invariant_only_for_repair():
    control = json.loads(repair.CONFIG.read_text())
    artifact = load_artifact()
    historical = raw.load_historical(control)
    row = historical.iloc[[5000]].copy()
    altered = row.copy(); altered[repair.REMOVED_FEATURE] = float(row[repair.REMOVED_FEATURE].iloc[0]) + 200
    assert repair.score_artifact(row, artifact)[0] == pytest.approx(repair.score_artifact(altered, artifact)[0], abs=0)
    assert raw.score_frame(row, control)[0] != pytest.approx(raw.score_frame(altered, control)[0], abs=1e-12)
    assert "w=n/(n+50)" in repair.BUILDER.read_text()
    assert "weight = n / (n + 50)" in repair.LIVE_BRIDGE.read_text()


def test_historical_gates_and_conservative_decision_are_frozen():
    validation = {row["variant"]: row for row in csv.DictReader((OUTPUT / "totals_park_depth_repair_validation_comparison.csv").open())}
    holdout = {row["variant"]: row for row in csv.DictReader((OUTPUT / "totals_park_depth_repair_holdout_comparison.csv").open())}
    sequential = {row["variant"]: row for row in csv.DictReader((OUTPUT / "totals_park_depth_repair_sequential_2026.csv").open())}
    assert float(validation["REPAIRED"]["actual_minus_forecast_bias"]) == pytest.approx(0.022841271842263648, abs=1e-12)
    assert float(sequential["REPAIRED"]["actual_minus_forecast_bias"]) == pytest.approx(0.17030081717828816, abs=1e-12)
    assert float(holdout["REPAIRED"]["actual_minus_forecast_bias"]) == pytest.approx(0.17216178771991095, abs=1e-12)
    for rows in (validation, sequential, holdout):
        assert float(rows["REPAIRED"]["rmse"]) < float(rows["CONTROL_RAW"]["rmse"])
        assert float(rows["REPAIRED"]["crps"]) < float(rows["CONTROL_RAW"]["crps"])
        assert float(rows["REPAIRED"]["mae"]) > float(rows["CONTROL_RAW"]["mae"])
    summary = (OUTPUT / "totals_park_depth_repair_validation.md").read_text()
    assert "MECHANICAL_DEPTH_SUPPRESSION = REMOVED" in summary
    assert "POINT_FORECAST_EFFECT = WORSE" in summary
    assert "PROBABILITY_DISTRIBUTION_EFFECT = IMPROVED" in summary
    assert "PARK_HISTORY_DEPTH_DIRECT_LOCATION_REPAIR_PROMISING_NEEDS_MORE_REVIEW" in summary


def test_outputs_are_complete_and_protected_state_is_unchanged():
    required = {
        "TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1.json",
        "totals_park_depth_repair_control_identity.json", "totals_park_depth_repair_contract.md",
        "totals_park_depth_repair_feature_contract.md",
        "totals_park_depth_repair_training_parity.csv", "totals_park_depth_repair_model_identity.json",
        "totals_park_depth_repair_coefficients.csv", "totals_park_depth_repair_validation_comparison.csv",
        "totals_park_depth_repair_holdout_comparison.csv", "totals_park_depth_repair_sequential_2026.csv",
        "totals_park_depth_repair_aug6_aug15_diagnostic.csv", "totals_park_depth_repair_bias_chronology.csv",
        "totals_park_depth_repair_depth_invariance.csv", "totals_park_depth_repair_within_park.csv",
        "totals_park_depth_repair_forecast_bands.csv", "totals_park_depth_repair_probability_quality.csv",
        "totals_park_depth_repair_intercept_necessity.csv", "totals_park_depth_repair_clustered_uncertainty.csv",
        "totals_park_depth_repair_leave_block_out.csv", "totals_related_count_feature_safety.csv",
        "totals_park_depth_repair_validation.md",
        "concise_mlb_totals_remove_park_history_depth_direct_location_defect_v1.md",
        "reproducibility_hashes.sha256",
    }
    assert required == {path.name for path in OUTPUT.iterdir() if path.is_file()}
    control_identity = json.loads((OUTPUT / "totals_park_depth_repair_control_identity.json").read_text())
    assert control_identity["protected_hashes_before"] == control_identity["protected_hashes_after"]
    for label, expected in control_identity["protected_hashes_after"].items():
        assert digest(repair.Path(label)) == expected
    for line in (OUTPUT / "reproducibility_hashes.sha256").read_text().splitlines():
        expected, label = line.split("  ", 1)
        path = repair.ROOT / label.removeprefix("INPUT::") if label.startswith("INPUT::") else OUTPUT / label
        assert digest(path) == expected


def test_training_parity_and_no_prospective_fit_use():
    parity = list(csv.DictReader((OUTPUT / "totals_park_depth_repair_training_parity.csv").open()))
    assert all(row["status"] in ("EXACT", "AUTHORIZED_DELTA") for row in parity)
    identity = json.loads((OUTPUT / "totals_park_depth_repair_model_identity.json").read_text())
    assert identity["training_population_parity"] == "EXACT"
    assert identity["prospective_rows_used_for_fit_or_selection"] == 0
    diagnostic = pd.read_csv(OUTPUT / "totals_park_depth_repair_aug6_aug15_diagnostic.csv")
    assert set(diagnostic.evidence_class) == {"RETROSPECTIVE_REPAIRED_CHALLENGER_DIAGNOSTIC"}

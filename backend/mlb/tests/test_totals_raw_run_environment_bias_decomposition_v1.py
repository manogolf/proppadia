from __future__ import annotations

import csv
import hashlib
import json

import pytest

from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as decomposition


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def completed_decomposition(tmp_path_factory):
    output = tmp_path_factory.mktemp("totals_bias_decomposition")
    protected = (decomposition.CONFIG, decomposition.LEDGER,
                 decomposition.SPINE / "totals_core_feature_spine.csv", decomposition.HISTORICAL_RESIDUALS)
    before = {path: digest(path) for path in protected}
    summary = decomposition.run(output)
    after = {path: digest(path) for path in protected}
    return output, summary, before, after


def test_analysis_is_read_only_and_preserves_frozen_contract(completed_decomposition):
    output, summary, before, after = completed_decomposition
    assert before == after
    identity = json.loads((output / "totals_bias_model_identity.json").read_text())
    assert identity["model_hash"] == decomposition.MODEL_HASH
    assert identity["model_hash_verified"] is True
    assert identity["residual_contract"] == "RUN_RESIDUAL = ACTUAL_TOTAL_RUNS - RAW_FORECAST_TOTAL"
    assert identity["frozen_intercept_reviewed_not_changed"] == pytest.approx(0.493550, abs=0)
    assert summary["prospective_games"] == 126
    assert summary["prospective_mean_run_residual"] == pytest.approx(0.558992375399351, abs=1e-12)


def test_historical_reproduction_and_environment_answer_are_consistent(completed_decomposition):
    output, summary, _, _ = completed_decomposition
    rows = {row["period"]: row for row in csv.DictReader((output / "totals_bias_chronology.csv").open())}
    assert float(rows["FROZEN_2025_VALIDATION"]["mean_run_residual"]) == pytest.approx(0.21504740002206743, abs=1e-12)
    assert float(rows["2026_SEQUENTIAL_EARLY"]["mean_run_residual"]) == pytest.approx(0.5774332448470871, abs=1e-12)
    assert float(rows["2026_LATE_HOLDOUT"]["mean_run_residual"]) == pytest.approx(0.6610547573577193, abs=1e-12)
    assert summary["bias_chronology"] == "LONGSTANDING_MODEL_BIAS"
    assert summary["environment_shift"] < 0
    assert summary["scoring_environment_declaration"] == "NO_MATERIAL_UPWARD_RUN_ENVIRONMENT_SHIFT"


def test_root_cause_is_supported_by_frozen_component_algebra(completed_decomposition):
    output, summary, _, _ = completed_decomposition
    rows = list(csv.DictReader((output / "totals_model_component_contribution_drift.csv").open()))
    park = next(row for row in rows if row["period"] == "PROSPECTIVE_AUG06_15" and row["feature"] == "park_history_depth")
    assert float(park["mean_feature_value"]) > float(park["training_center"])
    assert float(park["frozen_coefficient"]) < 0
    assert float(park["mean_log_location_contribution_vs_training_center"]) == pytest.approx(-0.12114490035769707, abs=1e-12)
    assert summary["root_cause"] == "TOTALS_BIAS_MODEL_SPECIFIC_STRUCTURAL_MISS"
    assert summary["intercept_interpretation"] == "V1_INTERCEPT_CORRECTS_AVERAGE_BIAS_BUT_MASKS_STRUCTURE"
    assert summary["causal_followup"] == "NO_CAUSAL_FOLLOWUP_YET"


def test_required_artifacts_and_reproducibility_manifest(completed_decomposition):
    output, _, _, _ = completed_decomposition
    required = {
        "totals_bias_model_identity.json", "totals_raw_forecast_construction_map.md", "totals_bias_chronology.csv",
        "totals_run_environment_comparison.csv", "totals_date_residuals.csv", "totals_forecast_magnitude_residuals.csv",
        "totals_team_side_residuals.csv", "totals_inning_scoring_context.csv", "totals_pitching_context_residuals.csv",
        "totals_offensive_context_residuals.csv", "totals_park_context_residuals.csv",
        "totals_environment_context_residuals.csv", "totals_score_timing_residuals.csv",
        "totals_residual_distribution.csv", "totals_bias_exclusion_stress.csv", "totals_intercept_alignment.csv",
        "totals_intercept_subgroup_crps.csv", "totals_baseline_bias_comparison.csv",
        "totals_model_component_contribution_drift.csv", "totals_component_attribution.csv",
        "totals_bias_root_cause.md", "concise_mlb_totals_raw_run_environment_bias_decomposition_v1.md",
        "reproducibility_hashes.sha256",
    }
    assert required == {path.name for path in output.iterdir() if path.is_file()}
    for line in (output / "reproducibility_hashes.sha256").read_text().splitlines():
        expected, label = line.split("  ", 1)
        path = decomposition.ROOT / label.removeprefix("INPUT::") if label.startswith("INPUT::") else output / label
        assert digest(path) == expected

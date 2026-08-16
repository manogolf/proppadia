from __future__ import annotations

import csv
import hashlib
import json

import pytest

from backend.mlb.scripts import run_mlb_totals_park_history_depth_structural_attribution_v1 as attribution


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def completed_attribution(tmp_path_factory):
    output = tmp_path_factory.mktemp("totals_park_depth_attribution")
    protected = (
        attribution.CONFIG,
        attribution.LEDGER,
        attribution.SPINE / "totals_core_feature_spine.csv",
        attribution.PARK_SPINE,
    )
    before = {path: digest(path) for path in protected}
    summary = attribution.run(output)
    after = {path: digest(path) for path in protected}
    return output, summary, before, after


def test_frozen_inputs_model_and_population_are_unchanged(completed_attribution):
    output, summary, before, after = completed_attribution
    assert before == after
    identity = json.loads((output / "totals_park_history_depth_identity.json").read_text())
    assert identity["model_hash"] == attribution.MODEL_HASH
    assert identity["model_hash_verified"] is True
    assert identity["frozen_coefficient"] == pytest.approx(-0.02681303790069205, abs=0)
    assert identity["prospective_games"] == 126
    assert summary["prospective_raw"]["actual_minus_forecast_bias"] == pytest.approx(0.558992375399351, abs=1e-12)


def test_structural_drift_and_within_park_result_are_supported(completed_attribution):
    output, summary, _, _ = completed_attribution
    distribution_rows = {row["period"]: row for row in csv.DictReader((output / "totals_park_history_depth_distribution.csv").open())}
    assert float(distribution_rows["DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"]["mean"]) == pytest.approx(80.043836180284)
    assert float(distribution_rows["PROSPECTIVE_AUG06_15"]["mean"]) == pytest.approx(291.26984126984127)
    assert summary["drift_severity"] == "EXTREME"
    assert summary["mechanical_calendar_growth"] == "YES"
    assert summary["training_role"] == "LIKELY_SAMPLE_DEPTH_ARTIFACT"
    assert summary["within_park_signal"] == "ABSENT"


def test_counterfactuals_use_only_frozen_algebra(completed_attribution):
    output, summary, _, _ = completed_attribution
    mean_row = next(csv.DictReader((output / "totals_park_depth_counterfactual_training_mean.csv").open()))
    zero_row = next(csv.DictReader((output / "totals_park_depth_counterfactual_coefficient_zero.csv").open()))
    cap_row = next(csv.DictReader((output / "totals_park_depth_counterfactual_p95_cap.csv").open()))
    assert float(mean_row["mean_prediction"]) == pytest.approx(float(zero_row["mean_prediction"]), abs=1e-14)
    assert float(mean_row["actual_minus_forecast_bias"]) == pytest.approx(-0.47247126797656397, abs=1e-12)
    assert float(cap_row["actual_minus_forecast_bias"]) == pytest.approx(-0.10490657570954416, abs=1e-12)
    assert float(cap_row["crps"]) < summary["prospective_raw"]["crps"]
    assert summary["feature_design"] == "BETTER_AS_CONFIDENCE/WEIGHT_SIGNAL"
    assert summary["root_cause"] == "MIXED_STRUCTURAL_DEFECT"
    assert summary["final_declaration"] == "PARK_HISTORY_DEPTH_PRIMARY_STRUCTURAL_DRIVER"


def test_historical_counterfactual_and_required_artifacts(completed_attribution):
    output, _, _, _ = completed_attribution
    historical = list(csv.DictReader((output / "totals_park_depth_historical_counterfactuals.csv").open()))
    caps = {row["period"]: row for row in historical if row["variant"] == "P95_CAP"}
    assert float(caps["FROZEN_2025_VALIDATION"]["actual_minus_forecast_bias"]) == pytest.approx(-0.010502123471059626, abs=1e-12)
    assert float(caps["2026_SEQUENTIAL_EARLY"]["actual_minus_forecast_bias"]) == pytest.approx(0.06447348928956936, abs=1e-12)
    assert float(caps["2026_LATE_HOLDOUT"]["actual_minus_forecast_bias"]) == pytest.approx(0.016648837569145175, abs=1e-12)
    required = {
        "totals_park_history_depth_identity.json", "totals_park_history_depth_lineage.md",
        "totals_park_history_depth_intent.md", "totals_park_history_depth_distribution.csv",
        "totals_park_history_depth_drift.csv", "totals_park_history_depth_calendar_growth.csv",
        "totals_park_history_depth_effect_curve.csv", "totals_park_history_depth_training_correlations.csv",
        "totals_park_history_depth_within_park.csv", "totals_park_history_depth_extrapolation.csv",
        "totals_park_depth_counterfactual_training_mean.csv", "totals_park_depth_counterfactual_p95_cap.csv",
        "totals_park_depth_counterfactual_coefficient_zero.csv", "totals_park_depth_historical_counterfactuals.csv",
        "totals_park_depth_daily_counterfactuals.csv", "totals_park_depth_forecast_band_effect.csv",
        "totals_park_depth_park_level_effect.csv", "totals_park_depth_vs_intercept.csv",
        "totals_park_depth_contribution_accounting.csv", "totals_related_depth_feature_inventory.csv",
        "totals_park_depth_design_assessment.md", "totals_park_depth_root_cause.md",
        "concise_mlb_totals_park_history_depth_structural_attribution_v1.md", "reproducibility_hashes.sha256",
    }
    assert required == {path.name for path in output.iterdir() if path.is_file()}
    for line in (output / "reproducibility_hashes.sha256").read_text().splitlines():
        expected, label = line.split("  ", 1)
        path = attribution.ROOT / label.removeprefix("INPUT::") if label.startswith("INPUT::") else output / label
        assert digest(path) == expected

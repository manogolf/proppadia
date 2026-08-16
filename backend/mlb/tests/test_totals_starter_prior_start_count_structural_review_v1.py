import csv
from pathlib import Path

import pytest

from backend.mlb.scripts import run_mlb_totals_starter_prior_start_count_structural_review_v1 as review


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    output = tmp_path_factory.mktemp("starter-count-review")
    protected = {path: review.sha256(path) for path in (review.CONTROL, review.REPAIR, review.raw.LEDGER,
                                                        review.raw.SPINE / "totals_core_feature_spine.csv",
                                                        review.BUILDER, review.LIVE_BRIDGE)}
    summary = review.run(output)
    assert protected == {path: review.sha256(path) for path in protected}
    return output, summary


def rows(path: Path):
    with path.open() as handle:
        return list(csv.DictReader(handle))


def test_exact_bounded_artifact_set_and_frozen_inputs(result):
    output, summary = result
    expected = {
        "totals_starter_count_feature_inventory.csv", "totals_starter_count_lineage.md",
        "totals_starter_count_intent.md", "totals_starter_count_distributions.csv",
        "totals_starter_count_drift.csv", "totals_starter_count_mechanical_growth.csv",
        "totals_starter_count_confidence_double_use.csv", "totals_starter_count_effect_curve.csv",
        "totals_starter_count_training_correlations.csv", "totals_starter_count_coefficient_flip_analysis.md",
        "totals_starter_count_within_pitcher.csv", "totals_starter_count_between_vs_within.csv",
        "totals_starter_count_experience_bands.csv", "totals_starter_count_extrapolation.csv",
        "totals_starter_count_home_away_asymmetry.md", "totals_starter_count_control_counterfactuals.csv",
        "totals_starter_count_park_repair_counterfactuals.csv", "totals_starter_count_joint_contribution.csv",
        "totals_starter_count_residual_alignment.csv", "totals_starter_count_mae_degradation_attribution.csv",
        "totals_starter_count_probability_effect.csv", "totals_count_feature_design_principle.md",
        "totals_starter_count_feature_fitness.md", "totals_starter_count_root_cause.md",
        "totals_starter_count_repair_scope.md",
        "concise_mlb_totals_starter_prior_start_count_structural_review_v1.md",
        "reproducibility_hashes.sha256",
    }
    assert {path.name for path in output.iterdir()} == expected
    assert summary["files"] == 27
    assert summary["protected_inputs_unchanged"] is True


def test_inventory_and_double_use_are_exact(result):
    output, _ = result
    inventory = {row["field"]: row for row in rows(output / "totals_starter_count_feature_inventory.csv")}
    assert float(inventory[review.FEATURES[0]]["control_coefficient"]) == 0.012677233712015375
    assert float(inventory[review.FEATURES[1]]["control_coefficient"]) == 0.0008268356527004493
    assert float(inventory[review.FEATURES[0]]["park_repair_coefficient"]) == 0.0006045472197127954
    assert float(inventory[review.FEATURES[1]]["park_repair_coefficient"]) == -0.010872043168452675
    assert inventory["home_starter_history_depth"]["direct_location_input"] == "False"
    double_use = rows(output / "totals_starter_count_confidence_double_use.csv")
    assert {row["feature"] for row in double_use} == set(review.FEATURES)
    assert all(row["double_use"] == "YES" and row["n_gates_workload_base"] == "True" for row in double_use)


def test_counterfactual_contract_and_metrics(result):
    output, _ = result
    for filename in ("totals_starter_count_control_counterfactuals.csv",
                     "totals_starter_count_park_repair_counterfactuals.csv"):
        data = rows(output / filename)
        assert len(data) == 2 * 4 * 4
        assert {row["period"] for row in data} == set(review.PERIODS)
        assert {row["variant"] for row in data} == {
            "ORIGINAL", "TRAINING_MEAN", "TRAINING_P95_CAP", "ZERO_COEFFICIENT_CONTRIBUTION"
        }
        assert all(row["evidence_label"] == "COUNTERFACTUAL_ONLY_NOT_A_MODEL" for row in data)
        for metric in ("mae", "rmse", "actual_minus_forecast_bias", "crps", "ladder_brier", "ladder_log_loss", "ladder_ece"):
            assert all(row[metric] != "" for row in data)


def test_structural_findings_and_signed_mae_attribution(result):
    output, summary = result
    flip = (output / "totals_starter_count_coefficient_flip_analysis.md").read_text()
    assert "MULTICOLLINEARITY_REASSIGNMENT" in flip
    within = rows(output / "totals_starter_count_within_pitcher.csv")
    controlled = [row for row in within if row["row_type"] == "AGGREGATE_FIXED_EFFECT" and row["comparison"] == "final_total"]
    assert len(controlled) == 2
    assert all(abs(float(row["within_pitcher_date_and_quality_controlled_pearson"])) < .05 for row in controlled)
    late = next(row for row in rows(output / "totals_starter_count_mae_degradation_attribution.csv")
                if row["period"] == "2026_LATE_HOLDOUT")
    assert float(late["control_mae"]) == pytest.approx(3.6782613120044236)
    assert float(late["park_repair_mae"]) == pytest.approx(3.7588648957244715)
    assert float(late["repair_minus_hybrid_mae_count_coefficient_component"]) < 0
    assert late["attribution_class"] == "MATERIAL_MITIGATION_NOT_DEGRADATION"
    assert summary["final_declaration"] == "STARTER_PRIOR_COUNT_MATERIAL_STRUCTURAL_CONCERN"


def test_no_model_or_activation_claim(result):
    output, summary = result
    concise = (output / "concise_mlb_totals_starter_prior_start_count_structural_review_v1.md").read_text()
    repair_scope = (output / "totals_starter_count_repair_scope.md").read_text()
    assert "COUNTERFACTUAL_ONLY_NOT_A_MODEL" in concise
    assert "no promotion, shadow activation, refit, or production change" in concise
    assert "STARTER_COUNT_REDESIGN_REQUIRES_SEPARATE_BOUNDED_COMPARISON" in repair_scope
    assert summary["park_repair_status"] == "PARK_REPAIR_BLOCKED_BY_STARTER_COUNT_DEFECT"

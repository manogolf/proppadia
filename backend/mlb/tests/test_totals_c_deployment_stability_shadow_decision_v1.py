import csv
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.mlb.scripts import review_mlb_totals_c_deployment_stability_shadow_decision_v1 as review


OUTPUT = review.DEFAULT_OUTPUT


def rows(name):
    with (OUTPUT / name).open() as handle:
        return list(csv.DictReader(handle))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_required_outputs_and_reproducibility_manifest():
    required = {
        "totals_c_artifact_identity.json", "totals_c_direct_feature_inventory.csv",
        "totals_c_feature_support_drift.csv", "totals_c_mechanical_growth_screen.csv",
        "totals_c_sample_depth_double_use.csv", "totals_c_within_entity_signal_screen.csv",
        "totals_c_out_of_support_performance.csv", "totals_c_coefficient_drift_impact.csv",
        "totals_c_coefficient_reassignment.csv", "totals_c_missingness_fallback_drift.csv",
        "totals_c_starter_feature_stability.md", "totals_c_park_context_stability.md",
        "totals_c_related_count_safety.csv", "totals_c_stationarity_perturbation.csv",
        "totals_c_point_product_contract.md", "totals_c_historical_evidence_summary.csv",
        "totals_c_structural_gate_matrix.csv", "totals_c_shadow_contract.md",
        "totals_c_shadow_snapshot_policy.md", "totals_c_shadow_grading_contract.md",
        "totals_c_shadow_review_discipline.md", "totals_c_intercept_policy.md",
        "model_deployment_stability_standard_draft_v1.md", "totals_c_shadow_decision.md",
        "concise_mlb_totals_count_confidence_only_deployment_stability_shadow_decision_v1.md",
        "reproducibility_hashes.sha256",
    }
    assert {path.name for path in OUTPUT.iterdir()} == required
    for line in (OUTPUT / "reproducibility_hashes.sha256").read_text().splitlines():
        expected, label = line.split("  ", 1)
        path = Path(label.removeprefix("PROTECTED_INPUT::")) if label.startswith("PROTECTED_INPUT::") else OUTPUT / label
        assert digest(path) == expected


def test_exact_c_identity_and_feature_contract():
    identity = json.loads((OUTPUT / "totals_c_artifact_identity.json").read_text())
    assert identity["C_ARTIFACT_IDENTITY"] == "PASS"
    assert identity["candidate_identity"] == review.C_NAME
    assert identity["canonical_model_hash"] == review.C_HASH
    assert identity["canonical_hash_recomputed"] == review.C_HASH
    assert identity["artifact_sha256"] == review.C_ARTIFACT_SHA
    assert identity["feature_count"] == 19
    assert identity["fit_count_this_task"] == 0


def test_support_drift_exposes_stale_bullpen_recency():
    support = {row["feature"]: row for row in rows("totals_c_feature_support_drift.csv")}
    assert support["home_bullpen_recent_innings_burden"]["support_status"] == "SEVERE_DRIFT"
    assert support["away_bullpen_recent_innings_burden"]["support_status"] == "SEVERE_DRIFT"
    assert float(support["home_bullpen_recent_innings_burden"]["standardized_mean_shift"]) < -2
    assert float(support["away_bullpen_recent_innings_burden"]["standardized_mean_shift"]) < -2
    related = pd.read_csv(OUTPUT / "totals_c_related_count_safety.csv")
    burden = related[(related.row_type == "CURRENT_DATE") & related.feature.str.contains("recent_innings_burden")]
    assert (burden[burden.game_date >= "2026-08-09"].current_date_mean == 0).all()
    assert set(burden.history_latest_included_game_date.dropna()) == {"2026-08-05"}


def test_no_mechanical_growth_or_depth_double_use_remains():
    mechanical = rows("totals_c_mechanical_growth_screen.csv")
    direct = [row for row in mechanical if row.get("present_in_c_direct_location", "True") != "False"]
    assert {row["mechanically_grows_with_calendar_or_sample_size"] for row in direct} == {"NO"}
    double = rows("totals_c_sample_depth_double_use.csv")
    assert {row["sample_depth_or_confidence_double_use"] for row in double} == {"NO"}


def test_stationarity_and_coefficient_reassignment():
    stationarity = rows("totals_c_stationarity_perturbation.csv")
    assert len(stationarity) == 3
    assert {row["COUNT_STATIONARITY_INVARIANT"] for row in stationarity} == {"PASS"}
    assert all(float(row["absolute_difference"]) == 0 for row in stationarity)
    reassignment = rows("totals_c_coefficient_reassignment.csv")
    assert {row["feature_risk"] for row in reassignment} <= {"LOW"}
    assert not any(row["sign_flip_vs_control"] == "True" for row in reassignment)


def test_authoritative_historical_metrics_reproduce():
    evidence = {row["period"]: row for row in rows("totals_c_historical_evidence_summary.csv")}
    assert set(evidence) == set(review.PERIODS)
    assert {row["authoritative_reproduction"] for row in evidence.values()} == {"PASS"}
    assert max(float(row["max_authoritative_absolute_difference"]) for row in evidence.values()) <= 2e-12
    assert float(evidence["FROZEN_2025_VALIDATION"]["mean_mae"]) == pytest.approx(3.6157122846687173)
    assert float(evidence["2026_LATE_HOLDOUT"]["median_mae"]) == pytest.approx(3.6742596810933943)


def test_material_structural_gate_blocks_shadow():
    gates = {row["gate"]: row for row in rows("totals_c_structural_gate_matrix.csv")}
    assert gates["A"]["status"] == "PASS"
    assert gates["C"]["status"] == "PASS"
    assert gates["D"]["status"] == "FAIL"
    assert gates["H"]["status"] == "FAIL"
    decision = (OUTPUT / "totals_c_shadow_decision.md").read_text()
    assert "TOTALS_COUNT_CONFIDENCE_ONLY_NEEDS_ADDITIONAL_STRUCTURAL_REVIEW" in decision
    assert "no shadow was launched" in decision


def test_contracts_freeze_mean_median_intercept_and_checkpoint():
    point = (OUTPUT / "totals_c_point_product_contract.md").read_text()
    assert "EXPECTED_TOTAL_RUNS=NEGATIVE_BINOMIAL_MEAN" in point
    assert "MAE_OPTIMAL_POINT=NEGATIVE_BINOMIAL_MEDIAN" in point
    assert "PROBABILITY_FOUNDATION=FULL_NEGATIVE_BINOMIAL_DISTRIBUTION" in point
    assert "DO_NOT_APPLY_RAW_INTERCEPT_TO_C" in (OUTPUT / "totals_c_intercept_policy.md").read_text()
    assert "20 completed independent date clusters" in (OUTPUT / "totals_c_shadow_review_discipline.md").read_text()
    assert "SHADOW_LAUNCHED=NO" in (OUTPUT / "totals_c_shadow_contract.md").read_text()

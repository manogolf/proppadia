import csv
import hashlib
import json
from pathlib import Path

import pytest

from backend.mlb.scripts import run_mlb_totals_count_feature_structural_repair_comparison_v1 as comparison


OUTPUT = comparison.DEFAULT_OUTPUT
PRE_FRESHNESS_REPAIR_BRIDGE_SHA = "7727541ecc35fd882fa832b4e6633fd11c0622a432c2f9988562360c3ec5257f"


def rows(name):
    with (OUTPUT / name).open() as handle:
        return list(csv.DictReader(handle))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def assert_historical_input(path, expected):
    if Path(path).name == "live_context_bridge_v1.py":
        assert expected == PRE_FRESHNESS_REPAIR_BRIDGE_SHA
    else:
        assert digest(Path(path)) == expected


def test_exact_artifact_set_and_hash_manifest():
    required = {
        "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json",
        "DIRECT_NEGATIVE_BINOMIAL_LOW_DEPTH_EXPERIENCE_V1.json",
        "totals_count_repair_control_identity.json", "totals_count_repair_variant_contracts.md",
        "totals_count_repair_training_parity.csv", "totals_count_repair_model_identities.json",
        "totals_count_repair_coefficients.csv", "totals_count_repair_coefficient_reassignment.csv",
        "totals_count_repair_2025_validation.csv", "totals_count_repair_early_2026.csv",
        "totals_count_repair_late_holdout.csv", "totals_count_repair_aug6_aug15_diagnostic.csv",
        "totals_count_repair_bias_chronology.csv", "totals_count_repair_stationarity.csv",
        "totals_count_repair_low_depth_analysis.csv", "totals_count_repair_point_quality.csv",
        "totals_count_repair_probability_quality.csv", "totals_count_repair_forecast_bands.csv",
        "totals_count_repair_support_bands.csv", "totals_count_repair_clustered_uncertainty.csv",
        "totals_count_repair_leave_block_out.csv", "totals_count_repair_decision_matrix.csv",
        "totals_count_repair_intercept_status.csv", "totals_count_repair_related_count_safety.csv",
        "totals_count_repair_final_decision.md",
        "concise_mlb_totals_count_feature_structural_repair_comparison_v1.md",
        "reproducibility_hashes.sha256",
    }
    assert {path.name for path in OUTPUT.iterdir()} == required
    for line in (OUTPUT / "reproducibility_hashes.sha256").read_text().splitlines():
        expected, label = line.split("  ", 1)
        path = Path(label.removeprefix("PROTECTED_INPUT::")) if label.startswith("PROTECTED_INPUT::") else OUTPUT / label
        assert_historical_input(path, expected)


def test_new_artifacts_are_frozen_once_with_exact_contracts():
    identities = json.loads((OUTPUT / "totals_count_repair_model_identities.json").read_text())
    assert identities["training_population_parity"] == "EXACT"
    assert identities["variant_e_status"] == "NOT_AUTHORIZED_NO_PREEXISTING_SEMANTIC_TRANSFORM"
    expected_hashes = {
        "A_CONTROL": comparison.CONTROL_HASH,
        "B_PARK_ONLY": comparison.PARK_HASH,
        "C_CONFIDENCE_ONLY": "21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd",
        "D_LOW_DEPTH": "999529a5017168d425730dc5b790f9a5de90c1dbd40c2c119df654475cfa0422",
    }
    assert {key: value["canonical_model_hash"] for key, value in identities["models"].items()} == expected_hashes
    assert identities["models"]["C_CONFIDENCE_ONLY"]["fit_action_this_task"] == "FIT_ONCE_AND_FROZEN"
    assert identities["models"]["D_LOW_DEPTH"]["fit_action_this_task"] == "FIT_ONCE_AND_FROZEN"
    confidence = json.loads((OUTPUT / "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json").read_text())
    low_depth = json.loads((OUTPUT / "DIRECT_NEGATIVE_BINOMIAL_LOW_DEPTH_EXPERIENCE_V1.json").read_text())
    assert confidence["canonical_model_hash"] == comparison.artifact_hash(confidence)
    assert low_depth["canonical_model_hash"] == comparison.artifact_hash(low_depth)
    assert not set(comparison.COUNT_FEATURES) & set(confidence["feature_order"])
    assert not set(comparison.COUNT_FEATURES) & set(low_depth["feature_order"])
    assert set(comparison.LOW_DEPTH_FEATURES) <= set(low_depth["feature_order"])
    assert confidence["fit_count"] == low_depth["fit_count"] == 1
    assert confidence["prospective_rows_used_for_fit_or_selection"] == 0
    assert low_depth["validation_or_holdout_rows_used_for_fit_or_selection"] == 0


def test_training_parity_stationarity_and_no_count_alias():
    assert all(row["status"] == "EXACT" for row in rows("totals_count_repair_training_parity.csv"))
    stationarity = rows("totals_count_repair_stationarity.csv")
    status = {row["variant"]: row["count_stationarity"] for row in stationarity}
    assert status == {"A_CONTROL": "FAIL", "B_PARK_ONLY": "PARTIAL",
                      "C_CONFIDENCE_ONLY": "PASS", "D_LOW_DEPTH": "PASS"}
    mature_d = [row for row in stationarity if row["variant"] == "D_LOW_DEPTH" and row["test"] == "MATURE_RAW_COUNT_PERTURBATION"]
    assert all(row["exact_invariance"] == "True" for row in mature_d)
    transitions = [row for row in stationarity if row["variant"] == "D_LOW_DEPTH" and row["test"] == "GOVERNED_LOW_DEPTH_STATE_TRANSITIONS"]
    assert len(transitions) == 2
    assert all(row["n1_equals_n2"] == "True" and row["mature_n3_equals_n100"] == "True" for row in transitions)


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("totals_count_repair_2025_validation.csv", {"A_CONTROL": (3.597207, 2.531596), "C_CONFIDENCE_ONLY": (3.615712, 2.524164), "D_LOW_DEPTH": (3.616005, 2.524463)}),
        ("totals_count_repair_early_2026.csv", {"A_CONTROL": (3.520946, 2.505148), "C_CONFIDENCE_ONLY": (3.554953, 2.488193), "D_LOW_DEPTH": (3.554434, 2.488027)}),
        ("totals_count_repair_late_holdout.csv", {"A_CONTROL": (3.678261, 2.602277), "C_CONFIDENCE_ONLY": (3.765377, 2.580671), "D_LOW_DEPTH": (3.765506, 2.580996)}),
    ],
)
def test_primary_metrics_show_point_probability_tradeoff(filename, expected):
    data = {row["variant"]: row for row in rows(filename) if row["row_type"] == "MODEL"}
    for variant, (mae, crps) in expected.items():
        assert float(data[variant]["mae"]) == pytest.approx(mae, abs=5e-7)
        assert float(data[variant]["crps"]) == pytest.approx(crps, abs=5e-7)
    for variant in ("C_CONFIDENCE_ONLY", "D_LOW_DEPTH"):
        assert float(data[variant]["mae"]) > float(data["A_CONTROL"]["mae"])
        assert float(data[variant]["crps"]) < float(data["A_CONTROL"]["crps"])


def test_august_is_post_hoc_and_does_not_select_candidate():
    aug = rows("totals_count_repair_aug6_aug15_diagnostic.csv")
    assert all(row["evidence_class"] == "RETROSPECTIVE_POST_HOC_DIAGNOSTIC" for row in aug)
    assert any(row["variant"] == "V1_INTERCEPT_DIAGNOSTIC" and row["row_type"] == "REFERENCE_ONLY" for row in aug)
    decision = (OUTPUT / "totals_count_repair_final_decision.md").read_text()
    assert "COUNT_STRUCTURAL_REPAIR_STRUCTURALLY_BETTER_BUT_POINT_TRADEOFF_UNRESOLVED" in decision
    assert "Preferred research challenger: `NONE_NO_CLEAR_WINNER`" in decision
    assert "TOTALS_REPAIRED_CHALLENGER_NOT_SHADOW_READY" in decision


def test_no_coefficient_absorption_and_intercept_is_unnecessary():
    safety = rows("totals_count_repair_related_count_safety.csv")
    assert all(row["receives_extreme_reassignment"] == "False" for row in safety)
    assert all(row["related_count_safety_decision"] == "NO_OTHER_DRIFTING_COUNT_ABSORPTION_DETECTED" for row in safety)
    risk = rows("totals_count_repair_coefficient_reassignment.csv")
    assert all(row["coefficient_reassignment_risk"] == "LOW" for row in risk)
    intercept = [row for row in rows("totals_count_repair_intercept_status.csv") if row["variant"] == "C_CONFIDENCE_ONLY"]
    assert all(row["intercept_status_after_repair"] == "LIKELY_UNNECESSARY" for row in intercept)
    assert all(row["obviously_overcorrects"] == "True" for row in intercept)


def test_protected_production_and_inputs_are_unchanged():
    identities = json.loads((OUTPUT / "totals_count_repair_model_identities.json").read_text())
    assert identities["protected_hashes_before"] == identities["protected_hashes_after"]
    for path, expected in identities["protected_hashes_after"].items():
        assert_historical_input(path, expected)

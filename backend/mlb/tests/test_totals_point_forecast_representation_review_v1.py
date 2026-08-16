import csv
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from backend.mlb.scripts import review_mlb_totals_point_forecast_representation_v1 as review


OUTPUT = review.DEFAULT_OUTPUT
PRE_FRESHNESS_REPAIR_BRIDGE_SHA = "7727541ecc35fd882fa832b4e6633fd11c0622a432c2f9988562360c3ec5257f"


def rows(name):
    with (OUTPUT / name).open() as handle:
        return list(csv.DictReader(handle))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_exact_artifact_set_and_hash_manifest():
    required = {
        "totals_point_forecast_current_contract.md",
        "totals_negative_binomial_parameterization.md",
        "totals_point_summary_rows.csv",
        "totals_control_point_summary_metrics.csv",
        "totals_confidence_only_point_summary_metrics.csv",
        "totals_point_summary_comparison_matrix.csv",
        "totals_mae_tradeoff_analysis.csv",
        "totals_rmse_mean_analysis.csv",
        "totals_absolute_error_representation.csv",
        "totals_mean_median_gap.csv",
        "totals_point_summary_forecast_bands.csv",
        "totals_point_summary_dispersion_bands.csv",
        "totals_point_summary_market_line_context.csv",
        "totals_point_summary_precision_check.csv",
        "totals_point_summary_clustered_uncertainty.csv",
        "totals_point_summary_leave_block_out.csv",
        "totals_structural_comparison_reinterpretation.md",
        "totals_point_prediction_product_contract.md",
        "totals_intercept_reinterpretation.md",
        "concise_mlb_totals_point_forecast_representation_review_v1.md",
        "reproducibility_hashes.sha256",
    }
    assert {path.name for path in OUTPUT.iterdir()} == required
    for line in (OUTPUT / "reproducibility_hashes.sha256").read_text().splitlines():
        expected, label = line.split("  ", 1)
        path = Path(label.removeprefix("PROTECTED_INPUT::")) if label.startswith("PROTECTED_INPUT::") else OUTPUT / label
        if path.name == "live_context_bridge_v1.py":
            assert expected == PRE_FRESHNESS_REPAIR_BRIDGE_SHA
        else:
            assert digest(path) == expected


def test_exact_subjects_populations_and_point_identity():
    data = pd.read_csv(OUTPUT / "totals_point_summary_rows.csv")
    assert set(data.model_hash) == {review.CONTROL_HASH, review.C_HASH}
    expected = {
        "FROZEN_2025_VALIDATION": 2433,
        "2026_SEQUENTIAL_EARLY": 1281,
        "2026_LATE_HOLDOUT": 439,
        "PROSPECTIVE_AUG06_15": 126,
    }
    for period, count in expected.items():
        group = data[data.period.eq(period)]
        assert len(group) == count * 2
        assert group.groupby("model_key").game_pk.nunique().to_dict() == {
            "A_CONTROL": count, "C_CONFIDENCE_ONLY": count,
        }
    control = data[data.model_key.eq("A_CONTROL")]
    assert control.current_stored_raw_equals_model_theoretical_mean.all()
    assert (abs(control.current_stored_control_raw_point_forecast - control.theoretical_distribution_mean) <= 2e-12).all()
    assert (data.distribution_median % 1 == 0).all()
    assert (data.distribution_mode % 1 == 0).all()
    assert data.theoretical_minus_folded_support_mean.max() < 0.015


@pytest.mark.parametrize(
    "filename,model,expected",
    [
        ("totals_control_point_summary_metrics.csv", "A_CONTROL", {
            "FROZEN_2025_VALIDATION": (3.597206636, 3.578709412, 3.694615701),
            "2026_SEQUENTIAL_EARLY": (3.520945969, 3.556596409, 3.758001561),
            "2026_LATE_HOLDOUT": (3.678261312, 3.687927107, 3.861047836),
        }),
        ("totals_confidence_only_point_summary_metrics.csv", "C_CONFIDENCE_ONLY", {
            "FROZEN_2025_VALIDATION": (3.615712285, 3.577476367, 3.642827785),
            "2026_SEQUENTIAL_EARLY": (3.554953076, 3.537861046, 3.647150664),
            "2026_LATE_HOLDOUT": (3.765376545, 3.674259681, 3.671981777),
        }),
    ],
)
def test_mean_median_mode_metrics(filename, model, expected):
    data = rows(filename)
    assert {row["model_key"] for row in data} == {model}
    by = {(row["period"], row["point_summary"]): row for row in data}
    for period, values in expected.items():
        for summary, value in zip(("MEAN", "MEDIAN", "MODE"), values):
            assert float(by[(period, summary)]["mae"]) == pytest.approx(value, abs=5e-10)


def test_distribution_metrics_are_attached_once_and_do_not_change_by_summary():
    matrix = rows("totals_point_summary_comparison_matrix.csv")
    for period in review.PERIODS:
        for model in review.MODEL_KEYS:
            group = [row for row in matrix if row["period"] == period and row["model_key"] == model]
            assert len(group) == 3
            attached = [row for row in group if row["full_distribution_metrics_attached_once"] == "True"]
            assert len(attached) == 1 and attached[0]["point_summary"] == "MEAN"
            assert all(row["distribution_metrics_contract"] == "MODEL_PERIOD_INVARIANT_ACROSS_POINT_SUMMARIES" for row in group)
            assert all(row["crps"] == "" for row in group if row["point_summary"] != "MEAN")


def test_mae_tradeoff_is_materially_reduced_but_not_universally_eliminated():
    data = {row["period"]: row for row in rows("totals_mae_tradeoff_analysis.csv")}
    assert {row["C_MAE_TRADEOFF"] for row in data.values()} == {"MATERIALLY_REDUCED_BY_POINT_SUMMARY"}
    primary = [data[period] for period in review.PRIMARY_PERIODS]
    assert all(float(row["c_mean_minus_control_mean_mae"]) > 0 for row in primary)
    assert all(float(row["c_median_minus_control_median_mae"]) < 0 for row in primary)
    assert max(float(row["c_median_minus_control_current_point_mae"]) for row in primary) == pytest.approx(0.01691507681636173)
    assert min(float(row["c_median_minus_control_current_point_mae"]) for row in primary) < 0


def test_c_mean_retains_rmse_and_bias_superiority():
    data = [row for row in rows("totals_rmse_mean_analysis.csv") if row["period"] in review.PRIMARY_PERIODS]
    assert all(row["c_mean_superior_on_rmse"] == "True" for row in data)
    assert all(row["c_mean_closer_to_zero_bias"] == "True" for row in data)
    assert all(float(row["c_minus_control_mean_rmse"]) < 0 for row in data)


def test_gap_bands_uncertainty_and_decisions():
    gaps = rows("totals_mean_median_gap.csv")
    primary = [row for row in gaps if row["period"] in review.PRIMARY_PERIODS]
    assert all(0.52 < float(row["mean_mean_minus_median"]) < 0.58 for row in primary)
    assert all(float(row["percentage_absolute_gap_ge_0_50"]) > 50 for row in primary)
    bands = [row for row in rows("totals_point_summary_forecast_bands.csv") if row["period"] in review.PRIMARY_PERIODS and row["model_key"] == "C_CONFIDENCE_ONLY" and row["control_mean_forecast_band"] in ("8.0-8.49", "8.5-8.99")]
    weights = [int(row["games"]) for row in bands]
    mean_delta = sum(float(row["c_mean_minus_control_mean_mae"]) * weight for row, weight in zip(bands, weights)) / sum(weights)
    median_delta = sum(float(row["c_median_minus_control_current_mean_mae"]) * weight for row, weight in zip(bands, weights)) / sum(weights)
    assert mean_delta == pytest.approx(0.038546743, abs=5e-10)
    assert median_delta == pytest.approx(-0.008805773, abs=5e-10)
    clustered = rows("totals_point_summary_clustered_uncertainty.csv")
    assert len(clustered) == 9
    assert all(int(row["draws"]) == review.BOOTSTRAP_DRAWS for row in clustered)
    leave = rows("totals_point_summary_leave_block_out.csv")
    assert {row["POINT_SUMMARY_ROBUSTNESS"] for row in leave} == {"MODERATE"}


def test_precision_market_context_and_terminal_contracts():
    precision = rows("totals_point_summary_precision_check.csv")
    rounded = [row for row in precision if row["presentation"] == "EXISTING_MARKDOWN_MEAN_3_DECIMALS"]
    assert all(abs(float(row["mae_minus_unrounded_mean"])) < 2e-5 for row in rounded)
    market = rows("totals_point_summary_market_line_context.csv")
    assert any(row["row_type"] == "SUMMARY" for row in market)
    assert all("EV" not in row.get("interpretation", "") or row["interpretation"] == "DESCRIPTIVE_ONLY_NO_EDGE_OR_EV" for row in market)
    concise = (OUTPUT / "concise_mlb_totals_point_forecast_representation_review_v1.md").read_text()
    assert "TOTALS_POINT_SUMMARY_PARTLY_EXPLAINS_MAE_TRADEOFF" in concise
    assert "STRUCTURAL_REPAIR_BETTER_DISTRIBUTION_AND_APPROPRIATE_POINT_SUMMARY_RESOLVES_TRADEOFF" in concise
    assert "TOTALS_COUNT_CONFIDENCE_ONLY_READY_FOR_SHADOW_DECISION" in concise
    assert "INTERCEPT_REINTERPRETATION = STRUCTURAL_LOCATION_COMPENSATION" in concise
    assert "No shadow was started" in concise

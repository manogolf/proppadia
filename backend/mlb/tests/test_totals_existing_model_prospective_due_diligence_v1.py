from __future__ import annotations

import csv
import hashlib
import json

import pytest

from backend.mlb.scripts import run_mlb_totals_existing_model_prospective_due_diligence_v1 as review


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def completed_review(tmp_path_factory):
    output = tmp_path_factory.mktemp("totals_due_diligence")
    inputs_before = {path: digest(path) for path in (review.CONFIG, review.LEDGER, review.MARKET_LEDGER)}
    summary = review.run(output)
    inputs_after = {path: digest(path) for path in inputs_before}
    return output, summary, inputs_before, inputs_after


def test_review_is_read_only_and_binds_the_frozen_subjects(completed_review):
    output, summary, before, after = completed_review
    assert before == after
    assert summary["model_hash"] == review.MODEL_HASH
    identity = json.loads((output / "totals_model_identity.json").read_text())
    assert identity["canonical_hash_verified"] is True
    assert identity["variants"]["V1_INTERCEPT"]["intercept_adjustment_runs"] == pytest.approx(0.493550, abs=0)
    assert identity["TOTALS_POINT_FORECAST_FOUNDATION"] == "RAW_V1"
    assert identity["TOTALS_FAIR_PROBABILITY_FOUNDATION"] == "V1_INTERCEPT"


def test_population_is_original_pregame_capture_through_august_15(completed_review):
    output, summary, _, _ = completed_review
    rows = list(csv.DictReader((output / "totals_prospective_population.csv").open()))
    assert len(rows) == summary["games"] == 126
    assert len({row["canonical_prediction_identity"] for row in rows}) == 126
    assert {row["date"] for row in rows} == set(summary["dates"])
    assert max(row["date"] for row in rows) == "2026-08-15"
    assert all(review.iso_utc(row["prediction_timestamp_utc"]) < review.iso_utc(row["scheduled_first_pitch_utc"]) for row in rows)
    assert all(row["model_hash"] == review.MODEL_HASH for row in rows)


def test_known_daily_metrics_reproduce_before_status_review(completed_review):
    output, _, _, _ = completed_review
    rows = list(csv.DictReader((output / "totals_prospective_metric_reproduction.csv").open()))
    checked = [row for row in rows if row["scope"] in {"2026-08-14", "2026-08-15"} and row["variant"] == "RAW_V1"]
    assert len(checked) == 2
    assert all(row["known_summary_check"] == "PASS" for row in checked)
    assert max(abs(float(row["reproduction_delta_mae"])) for row in checked) < 1e-12
    assert max(abs(float(row["reproduction_delta_crps"])) for row in checked) < 1e-12


def test_required_artifacts_and_hash_manifest_are_complete(completed_review):
    output, _, _, _ = completed_review
    required = {
        "totals_model_identity.json", "totals_prospective_population.csv", "totals_prospective_metric_reproduction.csv",
        "totals_baseline_contracts.md", "totals_raw_vs_baselines.csv", "totals_clustered_uncertainty.csv",
        "totals_daily_stability.csv", "totals_cumulative_trajectory.csv", "totals_leave_one_date_out.csv",
        "totals_raw_bias_characterization.csv", "totals_intercept_stress_test.csv", "totals_probability_calibration.csv",
        "totals_line_band_behavior.csv", "totals_forecast_magnitude_bands.csv", "totals_score_timing_comparison.csv",
        "totals_market_reference_comparison.csv", "totals_market_separation_bands.csv",
        "totals_historical_reference_comparison.csv", "totals_run_environment_summary.csv", "totals_due_diligence_status.md",
        "concise_mlb_totals_existing_model_prospective_due_diligence_v1.md", "reproducibility_hashes.sha256",
    }
    assert required == {path.name for path in output.iterdir() if path.is_file()}
    manifest = (output / "reproducibility_hashes.sha256").read_text()
    for name in required - {"reproducibility_hashes.sha256"}:
        assert f"  {name}\n" in manifest
    for line in manifest.splitlines():
        expected, label = line.split("  ", 1)
        path = review.ROOT / label.removeprefix("INPUT::") if label.startswith("INPUT::") else output / label
        assert digest(path) == expected

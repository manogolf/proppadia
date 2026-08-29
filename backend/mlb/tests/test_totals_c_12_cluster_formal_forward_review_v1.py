from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_totals_c_12_cluster_formal_forward_review_v1/2026-08-29"


def rows(name: str) -> list[dict[str, str]]:
    with (OUT / name).open(newline="") as handle:
        return list(csv.DictReader(handle))


def test_required_review_package_is_complete() -> None:
    required = {
        "totals_c_12_cluster_population.csv", "totals_c_12_cluster_model_identity.json",
        "totals_c_12_cluster_input_parity.csv", "totals_c_12_cluster_8_cluster_reproduction.csv",
        "totals_c_12_cluster_point_metrics.csv", "totals_c_12_cluster_distribution_metrics.csv",
        "totals_c_12_cluster_first8_vs_next4.csv", "totals_c_12_cluster_daily_metrics.csv",
        "totals_c_12_cluster_cumulative_trajectory.csv", "totals_c_12_cluster_clustered_uncertainty.csv",
        "totals_c_12_cluster_lodo.csv", "totals_c_12_cluster_bias_review.csv",
        "totals_c_12_cluster_point_summary_review.csv", "totals_c_12_cluster_structural_validation.md",
        "totals_c_12_cluster_watch_summary.csv", "totals_c_12_cluster_baseline_comparison.csv",
        "totals_c_12_cluster_standalone_status.md", "totals_c_12_cluster_pinnacle_timing.csv",
        "totals_c_12_cluster_market_parity.csv", "totals_c_12_cluster_market_parity_stability.csv",
        "totals_c_12_cluster_total_separation.csv", "totals_c_12_cluster_probability_relationship.csv",
        "totals_c_12_cluster_directional_disagreement.csv", "totals_c_12_cluster_unique_correctness.csv",
        "totals_c_12_cluster_separation_bands.csv", "totals_c_12_cluster_market_independence.md",
        "totals_c_12_cluster_incremental_information.csv", "totals_c_12_cluster_raw_challenger_decision.md",
        "totals_c_12_cluster_checkpoint_decision.md", "totals_c_12_cluster_public_readiness.md",
        "totals_c_12_cluster_next_step.md", "concise_mlb_totals_c_12_cluster_formal_forward_review_v1.md",
        "reproducibility_hashes.json", "sha256_manifest.csv",
    }
    assert required <= {path.name for path in OUT.iterdir() if path.is_file()}


def test_population_is_frozen_strict_pregame_and_excludes_august_29() -> None:
    population = rows("totals_c_12_cluster_population.csv")
    admitted = [row for row in population if row["admission_status"] == "ADMITTED_IMMUTABLE"]
    excluded = [row for row in population if row["admission_status"] == "EXCLUDED_FAIL_CLOSED"]
    assert len(population) == 157
    assert len(admitted) == 156
    assert len(excluded) == 1
    assert excluded[0]["game_pk"] == "823745"
    assert len({row["canonical_identity"] for row in admitted}) == 156
    assert all(row["strict_pregame"] == "True" for row in admitted)
    assert all(row["game_date"] <= "2026-08-28" for row in population)
    assert sum(row["scoring_mode"] == "PRIMARY_SCORE" for row in admitted) == 136
    assert sum(row["scoring_mode"] == "SCORE_MISSING" for row in admitted) == 20


def test_identity_parity_and_eight_cluster_reproduction() -> None:
    identity = json.loads((OUT / "totals_c_12_cluster_model_identity.json").read_text())
    assert identity["c_model_hash"] == "21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd"
    assert identity["c_artifact_sha256"] == "ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc"
    assert identity["raw_intercept_applied_to_c_rows"] == 0
    assert identity["outcome_access_rows"] == 0
    assert identity["duplicates"] == identity["overwrites"] == identity["post_start_admissions"] == 0
    assert len(rows("totals_c_12_cluster_input_parity.csv")) == 156
    assert all(row["all_exact"] == "True" and row["unexplained_mismatch"] == "False"
               for row in rows("totals_c_12_cluster_input_parity.csv"))
    assert all(row["reproduced"] == "True" for row in rows("totals_c_12_cluster_8_cluster_reproduction.csv"))


def test_market_timing_and_decisions_remain_bounded() -> None:
    pinnacle = rows("totals_c_12_cluster_pinnacle_timing.csv")
    assert len(pinnacle) == 156
    assert sum(row["within_30_minutes"] == "True" for row in pinnacle) == 155
    assert sum(row["within_60_minutes"] == "True" for row in pinnacle) == 155
    outside = [row for row in pinnacle if row["within_30_minutes"] == "False"]
    assert [(row["game_date"], row["game_pk"]) for row in outside] == [("2026-08-28", "823178")]
    directional = rows("totals_c_12_cluster_directional_disagreement.csv")[-1]
    assert (int(directional["same_side_non_neutral_count"]) + int(directional["opposite_side_count"])
            + int(directional["both_effectively_neutral_count"])) == int(directional["nonpush_rows"])
    decisions = json.loads((OUT / "review_decisions.json").read_text())
    assert decisions["C_12_CLUSTER_PROSPECTIVE_INTEGRITY"] == "PASS"
    assert decisions["C_MARKET_PROBABILITY_INPUTS"] == "NO"
    assert decisions["C_MARKET_PARITY_STABILITY"] == "MIXED"
    assert decisions["C_CERTIFICATION_STATUS"] == "C_STANDALONE_PREDICTION_NOT_CERTIFIED"
    assert decisions["C_PUBLIC_READINESS"] == "C_PUBLIC_PREDICTION_NOT_READY"
    assert decisions["C_POST_12_RECOMMENDATION"] == "C_CONTINUE_PASSIVE_CAPTURE_WITHOUT_NEW_CHECKPOINT"


def test_manifest_hashes_every_declared_output() -> None:
    for row in rows("sha256_manifest.csv"):
        path = ROOT / row["relative_path"]
        assert path.stat().st_size == int(row["bytes"])
        assert hashlib.sha256(path.read_bytes()).hexdigest() == row["sha256"]

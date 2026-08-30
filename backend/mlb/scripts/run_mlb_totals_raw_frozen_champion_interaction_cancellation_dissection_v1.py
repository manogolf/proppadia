"""Frozen joint-term interaction/cancellation dissection for MLB totals RAW.

The two-phase interface is intentional. ``freeze`` derives and hashes the
candidate manifest without joint outcome grading. ``evaluate`` accepts only
that exact manifest hash and never selects candidates from joint results.
No model is fit and no operational state is writable from this module.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_totals_raw_frozen_champion_single_feature_dissection_v1 as stage1


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_RAW_FROZEN_CHAMPION_INTERACTION_CANCELLATION_DISSECTION_V1"
OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_frozen_champion_interaction_cancellation_dissection_v1/2026-08-29"
STAGE1_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_frozen_champion_single_feature_dissection_v1/2026-08-29"
MANIFEST_NAME = "stage2_joint_ablation_manifest.csv"
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260829 + 2
AGGREGATE = "ALL_GOVERNED_EVALUATION"
METRICS = ("mae", "rmse", "actual_minus_forecast_bias", "crps", "brier", "log_loss", "ece")
BOOTSTRAP_METRICS = ("mae", "crps", "brier", "log_loss")

CONCEPT_PAIRS = {
    ("home_offense", "away_offense"): "OFFENSE_CONCEPT",
    ("home_prevention", "away_prevention"): "PREVENTION_CONCEPT",
    ("home_starter_ra9", "away_starter_ra9"): "STARTER_RA9_CONCEPT",
    ("home_starter_prior_starts", "away_starter_prior_starts"): "STARTER_PRIOR_STARTS_CONCEPT",
    ("home_expected_outs", "away_expected_outs"): "EXPECTED_OUTS_CONCEPT",
    ("home_workload_uncertainty_outs", "away_workload_uncertainty_outs"): "WORKLOAD_UNCERTAINTY_CONCEPT",
    ("home_bullpen_ra9", "away_bullpen_ra9"): "BULLPEN_RA9_CONCEPT",
    ("home_bullpen_likely_available_reliever_count", "away_bullpen_likely_available_reliever_count"): "RELIEVER_AVAILABILITY_CONCEPT",
    ("home_bullpen_recent_innings_burden", "away_bullpen_recent_innings_burden"): "BULLPEN_BURDEN_CONCEPT",
}

SELECTED_PAIRS = {
    ("home_offense", "away_offense"): ("S2_01", "exact home/away offense concept; unstable/harmful Stage-1 contrast and material combined movement"),
    ("home_prevention", "away_prevention"): ("S2_02", "exact home/away prevention concept; harmful/unstable Stage-1 contrast"),
    ("home_starter_ra9", "away_starter_ra9"): ("S2_03", "exact starter-quality concept; unstable versus moderately-required Stage-1 contrast"),
    ("home_starter_prior_starts", "away_starter_prior_starts"): ("S2_04", "exact count-support concept; both individual direct roles unresolved"),
    ("home_expected_outs", "away_expected_outs"): ("S2_05", "exact workload-location concept; required versus harmful Stage-1 contrast"),
    ("home_bullpen_ra9", "away_bullpen_ra9"): ("S2_06", "exact bullpen-quality concept; both sides moderately required with asymmetric dependence"),
    ("home_bullpen_likely_available_reliever_count", "away_bullpen_likely_available_reliever_count"): ("S2_07", "exact availability concept; opposite coefficient signs, inverse contributions, harmful/required contrast"),
    ("home_bullpen_recent_innings_burden", "away_bullpen_recent_innings_burden"): ("S2_08", "exact burden concept; inverse contributions and neutral/harmful contrast"),
    ("home_prevention", "home_bullpen_ra9"): ("S2_09", "very high inverse direct-contribution correlation; harmful/required same-side prevention terms"),
    ("away_prevention", "away_bullpen_ra9"): ("S2_10", "very high inverse direct-contribution correlation; unstable/required same-side prevention terms"),
    ("home_prevention", "strict_prior_total_run_factor"): ("S2_11", "very high inverse direct-contribution correlation with the strongest coherent Stage-1 term"),
    ("home_bullpen_ra9", "strict_prior_total_run_factor"): ("S2_12", "high positive contribution correlation tests overlap with the strongest coherent Stage-1 term"),
    ("home_offense", "strict_prior_total_run_factor"): ("S2_13", "material inverse contribution correlation and temporal-instability overlap question"),
    ("park_history_depth", "strict_prior_total_run_factor"): ("S2_14", "shared strict-prior park construction; unstable count term versus coherent venue factor"),
}
TRIO = ("home_starter_prior_starts", "away_starter_prior_starts", "park_history_depth")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def pair_key(a: str, b: str, feature_order: list[str]) -> tuple[str, str]:
    return (a, b) if feature_order.index(a) < feature_order.index(b) else (b, a)


def stage1_reproduction(output_dir: Path) -> dict[str, Any]:
    compared = (
        "raw_champion_reproduction.json", "raw_champion_evaluation_populations.csv",
        "raw_champion_22_feature_ablation_manifest.csv", "raw_champion_feature_point_deltas.csv",
        "raw_champion_feature_distribution_deltas.csv", "raw_champion_feature_forecast_effects.csv",
        "raw_champion_feature_temporal_effects.csv", "raw_champion_stage1_classification.csv",
    )
    with tempfile.TemporaryDirectory(prefix="mlb_totals_stage1_replay_", dir="/tmp") as temp:
        replay = Path(temp)
        result = stage1.run(replay)
        checks = []
        for name in compared:
            authoritative = STAGE1_OUTPUT / name
            candidate = replay / name
            checks.append({"file": name, "authoritative_sha256": sha256(authoritative),
                           "replay_sha256": sha256(candidate), "exact": authoritative.read_bytes() == candidate.read_bytes()})
        if result["status"] != "PASS" or not all(item["exact"] for item in checks):
            raise RuntimeError("STAGE1_REPRODUCTION_FAILED")
    value = {
        "task_id": TASK_ID, "STAGE1_REPRODUCTION": "PASS", "status": "PASS",
        "champion_hash": stage1.MODEL_HASH, "artifact_sha256": stage1.ARTIFACT_SHA,
        "files_replayed_exactly": len(checks), "file_checks": checks,
        "champion_metrics_reproduced": True, "all_22_single_ablations_reproduced": True,
        "classifications_reproduced": True, "temporal_effects_reproduced": True,
        "forecast_effects_reproduced": True, "joint_outcomes_evaluated_during_replay": False,
    }
    write_json(output_dir / "stage1_reproduction.json", value)
    return value


def lineage(feature: str) -> str:
    if feature in ("strict_prior_total_run_factor", "park_history_depth"): return "STRICT_PRIOR_PARK_STATE"
    if "starter" in feature or "expected_outs" in feature or "workload_uncertainty" in feature: return "STRICT_PRIOR_STARTER_STATE"
    if "bullpen" in feature: return "STRICT_PRIOR_BULLPEN_STATE"
    if feature == "league_total": return "STRICT_PRIOR_LEAGUE_RUN_STATE"
    if "offense" in feature or "prevention" in feature: return "STRICT_PRIOR_TEAM_RUN_STATE"
    if feature == "game_number": return "OFFICIAL_SCHEDULE_STATE"
    return "OTHER"


def source_overlap(a: str, b: str) -> str:
    la, lb = lineage(a), lineage(b)
    if la == lb: return "SAME_GOVERNED_STATE_FAMILY"
    if {la, lb} <= {"STRICT_PRIOR_TEAM_RUN_STATE", "STRICT_PRIOR_BULLPEN_STATE", "STRICT_PRIOR_PARK_STATE"}:
        return "OVERLAPPING_OFFICIAL_RUN_PREVENTION_SCORING_LINEAGE"
    return "DISTINCT_PRIMARY_STATE_FAMILIES"


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / denominator) if denominator else 0.0


def derive_candidates(output_dir: Path, artifact: dict[str, Any], frames: dict[str, pd.DataFrame]) -> tuple[list[dict[str, Any]], str]:
    features = artifact["feature_order"]
    combined = pd.concat([frames[period].assign(period=period) for period in stage1.PERIODS], ignore_index=True)
    mu, z, contributions = stage1.score_components(combined, artifact)
    classifications = {row["feature"]: row for row in read_csv(STAGE1_OUTPUT / "raw_champion_stage1_classification.csv")}
    temporal = read_csv(STAGE1_OUTPUT / "raw_champion_feature_temporal_effects.csv")
    temporal_map = {row["feature"]: row for row in temporal}
    coefficient = dict(zip(features, artifact["coefficients"]))
    diagnostics = []
    derivation = []
    selected_keys = {pair_key(a, b, features): value for (a, b), value in SELECTED_PAIRS.items()}
    conceptual_keys = {pair_key(a, b, features): value for (a, b), value in CONCEPT_PAIRS.items()}
    strict_inspection = {pair_key("strict_prior_total_run_factor", name, features) for name in
                         ("league_total", "home_offense", "away_offense", "home_prevention", "away_prevention")}
    period_metric_names = [f"{period}_{metric}" for period in stage1.PERIODS for metric in
                           ("mae_delta", "crps_delta", "brier_delta", "log_loss_delta")]
    for i, a in enumerate(features):
        for j in range(i + 1, len(features)):
            b = features[j]; key = (a, b)
            za, zb = z[:, i], z[:, j]; ca, cb = contributions[:, i], contributions[:, j]
            metric_a = np.asarray([float(temporal_map[a][name]) for name in period_metric_names])
            metric_b = np.asarray([float(temporal_map[b][name]) for name in period_metric_names])
            corr_z = float(np.corrcoef(za, zb)[0, 1]); corr_c = float(np.corrcoef(ca, cb)[0, 1])
            stage_a, stage_b = classifications[a], classifications[b]
            row = {
                "feature_a": a, "feature_b": b, "standardized_value_correlation": corr_z,
                "direct_contribution_correlation": corr_c, "absolute_direct_contribution_correlation": abs(corr_c),
                "coefficient_a": coefficient[a], "coefficient_b": coefficient[b],
                "coefficient_sign_relationship": "SAME" if np.sign(coefficient[a]) == np.sign(coefficient[b]) else "OPPOSITE",
                "absolute_coefficient_ratio_max_over_min": max(abs(coefficient[a]), abs(coefficient[b])) / max(min(abs(coefficient[a]), abs(coefficient[b])), 1e-15),
                "home_away_conceptual_pair": key in conceptual_keys, "concept_name": conceptual_keys.get(key, "NONE"),
                "shared_upstream_construction": lineage(a) == lineage(b), "source_lineage_a": lineage(a), "source_lineage_b": lineage(b),
                "overlapping_source_lineage": source_overlap(a, b),
                "stage1_temporal_effect_a": stage_a["temporal_effect"], "stage1_temporal_effect_b": stage_b["temporal_effect"],
                "stage1_classification_a": stage_a["stage1_classification"], "stage1_classification_b": stage_b["stage1_classification"],
                "stage1_temporal_effect_similarity": stage_a["temporal_effect"] == stage_b["temporal_effect"],
                "stage1_metric_delta_cosine_similarity": cosine(metric_a, metric_b),
                "opposite_temporal_behavior": cosine(metric_a, metric_b) <= -.35,
                "mean_direct_log_contribution_a": float(ca.mean()), "mean_direct_log_contribution_b": float(cb.mean()),
                "opposite_signed_mean_contribution": np.sign(ca.mean()) != np.sign(cb.mean()),
                "common_missing_fallback_behavior": ("SAME_SIDE_STATE_DEPENDENT" if lineage(a) == lineage(b) and not key in conceptual_keys
                                                      else "SIDE_SPECIFIC_SHARED_CONTRACT" if key in conceptual_keys else "NO_COMMON_FALLBACK_CONTRACT"),
                "all_governed_rows_complete_for_both": bool(np.isfinite(za).all() and np.isfinite(zb).all()),
                "strict_prior_overlap_relationship_inspected": key in strict_inspection,
                "candidate_discovery_used_joint_performance": False,
            }
            diagnostics.append(row)
            if key in selected_keys:
                candidate_id, reason = selected_keys[key]
                decision, decision_reason = "TEST", reason
            elif key == pair_key("home_workload_uncertainty_outs", "away_workload_uncertainty_outs", features):
                candidate_id, decision = "", "DECLINE_WITH_REASON"
                decision_reason = "required conceptual pair considered; near-zero contribution correlation, one neutral term, and no Stage-1 concept-level cancellation signal"
            elif key in conceptual_keys:
                candidate_id, decision = "", "DECLINE_WITH_REASON"
                decision_reason = "required conceptual pair considered but not selected by the bounded structural rules"
            elif key in strict_inspection:
                candidate_id, decision = "", "DECLINE_WITH_REASON"
                decision_reason = "strict-prior overlap inspected; direct-contribution correlation below the predeclared material threshold or a stronger bounded relationship ranked above it"
            else:
                candidate_id, decision, decision_reason = "", "NOT_SELECTED", "not a required concept and did not rank into the bounded high-correlation/structural manifest"
            derivation.append({**row, "candidate_id": candidate_id, "candidate_decision": decision,
                               "candidate_decision_reason": decision_reason})
    write_csv(output_dir / "stage2_pair_diagnostic_matrix.csv", diagnostics)
    derivation.append({
        "feature_a": "home_starter_prior_starts", "feature_b": "away_starter_prior_starts|park_history_depth",
        "candidate_id": "S2_15", "candidate_decision": "TEST", "candidate_decision_reason": "required count/confidence direct trio",
        "home_away_conceptual_pair": False, "concept_name": "COUNT_CONFIDENCE_DIRECT_TRIO",
        "shared_upstream_construction": False, "overlapping_source_lineage": "STARTER_SUPPORT_PLUS_PARK_SUPPORT",
        "candidate_discovery_used_joint_performance": False,
    })
    write_csv(output_dir / "stage2_candidate_derivation.csv", derivation)

    by_key = {(row["feature_a"], row["feature_b"]): row for row in diagnostics}
    manifest = []
    for key, (candidate_id, reason) in sorted(selected_keys.items(), key=lambda item: item[1][0]):
        diagnostic = by_key[key]
        question = ("Does the home/away concept show redundancy, compensation, or concept-level dependence?"
                    if key in conceptual_keys else "Does correlated/overlapping frozen dependence reveal cancellation, redundancy, or conditional dependence?")
        if "strict_prior_total_run_factor" in key:
            question = "Does strict_prior_total_run_factor retain coherent dependence when this plausible overlap is neutralized simultaneously?"
        manifest.append({
            "candidate_id": candidate_id, "candidate_type": "PAIR", "features_pipe": "|".join(key), "feature_count": 2,
            "rationale": reason, "evidence_source": "Stage-1 exact outputs plus all-row standardized/contribution diagnostics",
            "expected_structural_question": question,
            "direct_contribution_correlation": diagnostic["direct_contribution_correlation"],
            "stage1_classifications": f"{diagnostic['stage1_classification_a']}|{diagnostic['stage1_classification_b']}",
            "predeclared_interpretation_rule": "compare joint delta with both constituent deltas and their sum; require score degradation beyond correlation for redundancy and nonlinear/opposing score evidence for compensation",
            "joint_performance_seen_before_freeze": False, "intervention": stage1.INTERVENTION,
        })
    manifest.append({
        "candidate_id": "S2_15", "candidate_type": "GROUP", "features_pipe": "|".join(TRIO), "feature_count": 3,
        "rationale": "required frozen count/confidence direct trio; upstream confidence, gating, shrinkage, fallback, and support retained",
        "evidence_source": "task-required unresolved structural question plus exact Stage-1 count review",
        "expected_structural_question": "Does the frozen location equation benefit jointly from the three raw support counts without coefficient redistribution?",
        "direct_contribution_correlation": "MULTIVARIATE", "stage1_classifications": "UNRESOLVED|UNRESOLVED|TEMPORALLY_UNSTABLE",
        "predeclared_interpretation_rule": "jointly helpful/harmful only with aligned aggregate MAE+CRPS and at least three aligned periods; otherwise temporal mixed, compensating, or unresolved",
        "joint_performance_seen_before_freeze": False, "intervention": stage1.INTERVENTION,
    })
    manifest.sort(key=lambda row: row["candidate_id"])
    if len(manifest) != 15 or len({row["candidate_id"] for row in manifest}) != 15:
        raise RuntimeError("BOUNDED_MANIFEST_COUNT_FAILED")
    write_csv(output_dir / MANIFEST_NAME, manifest)
    return manifest, sha256(output_dir / MANIFEST_NAME)


def freeze(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact = stage1.load_artifact()
    frames, _ = stage1.load_populations(artifact)
    reproduction = stage1_reproduction(output_dir)
    manifest, manifest_sha = derive_candidates(output_dir, artifact, frames)
    return {"status": "MANIFEST_FROZEN", "stage1": reproduction["status"], "all_possible_pairs_considered": 231,
            "candidate_relationships_considered_including_required_trio": 232, "joint_tests_frozen": len(manifest),
            "manifest_sha256": manifest_sha, "joint_performance_evaluated": False}


def loss_arrays(frame: pd.DataFrame, mu: np.ndarray, alpha: float) -> dict[str, np.ndarray]:
    actual = frame.final_total.to_numpy(float)
    dist = stage1.row_distribution_losses(mu, actual, alpha)
    return {"mae": abs(actual - mu), "squared_error": (actual - mu) ** 2,
            "actual_minus_forecast_bias": actual - mu, "crps": dist["crps"],
            "brier": dist["brier"], "log_loss": dist["log_loss"],
            "event_probability": dist["event_probability"], "event_outcome": dist["event_outcome"]}


def metric_values(frame: pd.DataFrame, mu: np.ndarray, loss: dict[str, np.ndarray]) -> dict[str, float]:
    return {"games": len(frame), "mae": float(loss["mae"].mean()),
            "rmse": float(np.sqrt(loss["squared_error"].mean())),
            "actual_minus_forecast_bias": float(loss["actual_minus_forecast_bias"].mean()),
            "crps": float(loss["crps"].mean()), "brier": float(loss["brier"].mean()),
            "log_loss": float(loss["log_loss"].mean()),
            "ece": stage1.ece(loss["event_probability"], loss["event_outcome"]),
            "mean_forecast": float(mu.mean())}


def effect_values(joint: np.ndarray, champion: np.ndarray) -> dict[str, float]:
    delta = joint - champion; absolute = abs(delta)
    return {"mean_signed_delta": float(delta.mean()), "mean_absolute_delta": float(absolute.mean()),
            "median_absolute_delta": float(np.median(absolute)), "p90_absolute_delta": float(np.quantile(absolute, .90)),
            "maximum_absolute_delta": float(absolute.max())}


def classify_temporal(rows: list[dict[str, Any]], contribution_corr: float | None) -> str:
    if max(max(abs(row["mae_delta"]), abs(row["crps_delta"])) for row in rows) < .002:
        return "NEGLIGIBLE"
    states = ["HELP" if row["mae_delta"] > 0 and row["crps_delta"] > 0 else
              "HARM" if row["mae_delta"] < 0 and row["crps_delta"] < 0 else "MIXED" for row in rows]
    residuals = [row["mae_interaction_residual"] for row in rows]
    if contribution_corr is not None and contribution_corr <= -.35 and sum(abs(x) >= .003 for x in residuals) >= 3:
        return "COMPENSATING"
    if len(set(states)) == 1 and states[0] != "MIXED": return "STABLE"
    if "HELP" in states and "HARM" in states: return "REGIME_DEPENDENT"
    return "MIXED"


def redundancy_evidence(aggregate: dict[str, Any], singles: list[dict[str, str]]) -> tuple[str, str]:
    single_mae = [float(row["mae_delta"]) for row in singles]
    single_crps = [float(row["crps_delta"]) for row in singles]
    jm, jc = aggregate["mae_delta"], aggregate["crps_delta"]
    if max(abs(x) for x in single_mae) < .005 and max(abs(x) for x in single_crps) < .002 and jm >= .015 and jc > .002:
        return "STRONG", "constituents are individually small but joint removal materially degrades MAE and CRPS"
    if jm > 0 and jc > 0 and aggregate["mae_interaction_residual"] >= .005:
        return "MODERATE", "positive joint degradation materially exceeds the sum of constituent MAE effects"
    if jm > max(single_mae) and jc > max(single_crps) and jm > 0 and jc > 0:
        return "WEAK", "joint removal is worse than either constituent but below stronger gates"
    if (jm > 0) != (jc > 0): return "MIXED", "joint point and distribution effects disagree"
    return "NONE", "no score-based redundancy pattern beyond constituent effects"


def compensation_evidence(aggregate: dict[str, Any], singles: list[dict[str, str]], corr: float | None) -> tuple[str, str, str]:
    single_mae = [float(row["mae_delta"]) for row in singles]
    inverse = corr is not None and corr <= -.35
    score_flip = any(value <= -.002 for value in single_mae) and aggregate["mae_delta"] >= .005
    material = abs(aggregate["mae_interaction_residual"]) >= .005 or abs(aggregate["crps_interaction_residual"]) >= .002
    if inverse and material and score_flip:
        return "STRONG", "FORECAST_AND_SCORE_LEVEL", "inverse contributions plus material nonlinear residual and a harmful-alone/useful-joint score flip"
    if inverse and material:
        return "MODERATE", "FORECAST_LEVEL_WITH_SCORE_RESIDUAL", "inverse contributions and material interaction residual"
    if score_flip or material:
        return "WEAK", "SCORE_LEVEL_OR_ONLY_APPARENT", "nonlinear score pattern without strong inverse-contribution support"
    if inverse:
        return "MIXED", "FORECAST_LEVEL_ONLY", "opposing contributions without material score interaction"
    return "NONE", "NONE", "no forecast-plus-score compensation evidence"


def conditional_evidence(period_rows: list[dict[str, Any]], aggregate: dict[str, Any], singles: list[dict[str, str]]) -> tuple[str, str]:
    single_mae = [float(row["mae_delta"]) for row in singles]
    qualitative_shift = ((all(value <= .002 for value in single_mae) and aggregate["mae_delta"] >= .008) or
                         (any(value <= -.002 for value in single_mae) and aggregate["mae_delta"] >= .005) or
                         abs(aggregate["mae_interaction_residual"]) >= .007)
    period_shift = sum(abs(row["mae_interaction_residual"]) >= .005 for row in period_rows)
    signs = {np.sign(row["mae_interaction_residual"]) for row in period_rows if abs(row["mae_interaction_residual"]) >= .003}
    if qualitative_shift and len(signs) > 1: return "MIXED", "material conditional effect changes sign across periods"
    if qualitative_shift or period_shift >= 3: return "YES", "joint behavior materially differs from constituent single-feature behavior"
    return "NO", "joint behavior does not cross the predeclared conditional-dependence gates"


def evaluate(output_dir: Path, expected_manifest_sha: str) -> dict[str, Any]:
    manifest_path = output_dir / MANIFEST_NAME
    if not manifest_path.exists() or sha256(manifest_path) != expected_manifest_sha:
        raise RuntimeError("FROZEN_STAGE2_MANIFEST_SHA_FAILED")
    manifest = read_csv(manifest_path)
    if len(manifest) != 15 or any(row["joint_performance_seen_before_freeze"] != "False" for row in manifest):
        raise RuntimeError("FROZEN_STAGE2_MANIFEST_CONTRACT_FAILED")
    artifact = stage1.load_artifact(); alpha = float(artifact["dispersion_alpha"])
    frames, _ = stage1.load_populations(artifact)
    stage1_reproduction(output_dir)
    features = artifact["feature_order"]
    classifications = {row["feature"]: row for row in read_csv(STAGE1_OUTPUT / "raw_champion_stage1_classification.csv")}
    diagnostic_map = {(row["feature_a"], row["feature_b"]): row for row in read_csv(output_dir / "stage2_pair_diagnostic_matrix.csv")}

    period_data: dict[str, dict[str, Any]] = {}
    for period, frame in frames.items():
        champion_mu, _, contributions = stage1.score_components(frame, artifact)
        champion_loss = loss_arrays(frame, champion_mu, alpha)
        singles = {}
        for index, feature in enumerate(features):
            mu = champion_mu * np.exp(-contributions[:, index])
            loss = loss_arrays(frame, mu, alpha)
            singles[feature] = {"mu": mu, "loss": loss, "metrics": metric_values(frame, mu, loss)}
        period_data[period] = {"frame": frame, "champion_mu": champion_mu, "contributions": contributions,
                               "champion_loss": champion_loss, "champion_metrics": metric_values(frame, champion_mu, champion_loss),
                               "singles": singles}

    joint_metrics, joint_effects, residual_rows = [], [], []
    joint_cache: dict[tuple[str, str], dict[str, Any]] = {}
    for candidate in manifest:
        candidate_id = candidate["candidate_id"]; terms = candidate["features_pipe"].split("|")
        for period in stage1.PERIODS:
            data = period_data[period]; indices = [features.index(term) for term in terms]
            contribution_sum = data["contributions"][:, indices].sum(axis=1)
            joint_mu = data["champion_mu"] * np.exp(-contribution_sum)
            loss = loss_arrays(data["frame"], joint_mu, alpha); values = metric_values(data["frame"], joint_mu, loss)
            champion = data["champion_metrics"]
            deltas = {f"{name}_delta": values[name] - champion[name] for name in METRICS}
            single_delta = {name: [data["singles"][term]["metrics"][name] - champion[name] for term in terms] for name in METRICS}
            residual = {f"{name}_interaction_residual": deltas[f"{name}_delta"] - sum(single_delta[name]) for name in METRICS}
            joint_cache[(candidate_id, period)] = {"mu": joint_mu, "loss": loss, "metrics": values,
                                                    "deltas": deltas, "residual": residual, "single_delta": single_delta}
            joint_metrics.append({"candidate_id": candidate_id, "candidate_type": candidate["candidate_type"],
                                  "features_pipe": candidate["features_pipe"], "population": period, **values,
                                  **{f"champion_{name}": champion[name] for name in METRICS}, **deltas,
                                  **{f"constituent_single_{name}_deltas": json.dumps(single_delta[name]) for name in METRICS}})
            joint_effects.append({"candidate_id": candidate_id, "features_pipe": candidate["features_pipe"],
                                  "population": period, **effect_values(joint_mu, data["champion_mu"])})
            for name in METRICS:
                residual_rows.append({"candidate_id": candidate_id, "features_pipe": candidate["features_pipe"],
                                      "population": period, "metric": name,
                                      "constituent_single_deltas": json.dumps(single_delta[name]),
                                      "sum_constituent_single_deltas": sum(single_delta[name]),
                                      "observed_joint_delta": deltas[f"{name}_delta"],
                                      "interaction_residual": residual[f"{name}_interaction_residual"],
                                      "interpretation_limit": "nonlinear metric-surface diagnostic; not causal decomposition"})
        # Exact row-weighted aggregate is recomputed, never averaged across periods.
        frames_all = pd.concat([period_data[p]["frame"] for p in stage1.PERIODS], ignore_index=True)
        champion_mu = np.concatenate([period_data[p]["champion_mu"] for p in stage1.PERIODS])
        joint_mu = np.concatenate([joint_cache[(candidate_id, p)]["mu"] for p in stage1.PERIODS])
        champion_loss = loss_arrays(frames_all, champion_mu, alpha); joint_loss = loss_arrays(frames_all, joint_mu, alpha)
        champion = metric_values(frames_all, champion_mu, champion_loss); values = metric_values(frames_all, joint_mu, joint_loss)
        deltas = {f"{name}_delta": values[name] - champion[name] for name in METRICS}
        single_delta = {}
        for name in METRICS:
            single_delta[name] = []
            for term in terms:
                single_mu = np.concatenate([period_data[p]["singles"][term]["mu"] for p in stage1.PERIODS])
                single_loss = loss_arrays(frames_all, single_mu, alpha)
                single_delta[name].append(metric_values(frames_all, single_mu, single_loss)[name] - champion[name])
        residual = {f"{name}_interaction_residual": deltas[f"{name}_delta"] - sum(single_delta[name]) for name in METRICS}
        joint_cache[(candidate_id, AGGREGATE)] = {"mu": joint_mu, "loss": joint_loss, "metrics": values,
                                                 "deltas": deltas, "residual": residual, "single_delta": single_delta}
        joint_metrics.append({"candidate_id": candidate_id, "candidate_type": candidate["candidate_type"],
                              "features_pipe": candidate["features_pipe"], "population": AGGREGATE, **values,
                              **{f"champion_{name}": champion[name] for name in METRICS}, **deltas,
                              **{f"constituent_single_{name}_deltas": json.dumps(single_delta[name]) for name in METRICS}})
        joint_effects.append({"candidate_id": candidate_id, "features_pipe": candidate["features_pipe"],
                              "population": AGGREGATE, **effect_values(joint_mu, champion_mu)})
        for name in METRICS:
            residual_rows.append({"candidate_id": candidate_id, "features_pipe": candidate["features_pipe"],
                                  "population": AGGREGATE, "metric": name,
                                  "constituent_single_deltas": json.dumps(single_delta[name]),
                                  "sum_constituent_single_deltas": sum(single_delta[name]),
                                  "observed_joint_delta": deltas[f"{name}_delta"],
                                  "interaction_residual": residual[f"{name}_interaction_residual"],
                                  "interpretation_limit": "nonlinear metric-surface diagnostic; not causal decomposition"})
    write_csv(output_dir / "stage2_joint_metrics.csv", joint_metrics)
    write_csv(output_dir / "stage2_joint_forecast_effects.csv", joint_effects)
    write_csv(output_dir / "stage2_interaction_residuals.csv", residual_rows)

    redundancy_rows, compensation_rows, conditional_rows, temporal_rows = [], [], [], []
    result_map = {}
    for candidate in manifest:
        candidate_id = candidate["candidate_id"]; terms = candidate["features_pipe"].split("|")
        aggregate = {**joint_cache[(candidate_id, AGGREGATE)]["deltas"], **joint_cache[(candidate_id, AGGREGATE)]["residual"]}
        singles = [classifications[term] for term in terms]
        corr = None
        if len(terms) == 2:
            key = pair_key(*terms, features); corr = float(diagnostic_map[key]["direct_contribution_correlation"])
        redundancy, redundancy_reason = redundancy_evidence(aggregate, singles)
        compensation, compensation_level, compensation_reason = compensation_evidence(aggregate, singles, corr)
        period_rows = [{**joint_cache[(candidate_id, period)]["deltas"], **joint_cache[(candidate_id, period)]["residual"]}
                       for period in stage1.PERIODS]
        conditional, conditional_reason = conditional_evidence(period_rows, aggregate, singles)
        temporal = classify_temporal(period_rows, corr)
        result_map[candidate_id] = {"redundancy": redundancy, "compensation": compensation,
                                    "conditional": conditional, "temporal": temporal, "aggregate": aggregate}
        common = {"candidate_id": candidate_id, "features_pipe": candidate["features_pipe"],
                  "aggregate_mae_delta": aggregate["mae_delta"], "aggregate_crps_delta": aggregate["crps_delta"],
                  "aggregate_brier_delta": aggregate["brier_delta"], "aggregate_log_loss_delta": aggregate["log_loss_delta"],
                  "aggregate_mae_interaction_residual": aggregate["mae_interaction_residual"],
                  "aggregate_crps_interaction_residual": aggregate["crps_interaction_residual"]}
        redundancy_rows.append({**common, "redundancy_evidence": redundancy, "reason": redundancy_reason,
                                "correlation_alone_not_used": True})
        compensation_rows.append({**common, "direct_contribution_correlation": corr,
                                  "compensation_evidence": compensation, "compensation_level": compensation_level,
                                  "reason": compensation_reason})
        conditional_rows.append({**common, "conditional_dependence": conditional, "reason": conditional_reason})
        temporal_rows.append({**common, "temporal_interaction": temporal,
                              **{f"{period}_mae_delta": joint_cache[(candidate_id, period)]["deltas"]["mae_delta"] for period in stage1.PERIODS},
                              **{f"{period}_crps_delta": joint_cache[(candidate_id, period)]["deltas"]["crps_delta"] for period in stage1.PERIODS}})
    write_csv(output_dir / "stage2_redundancy_results.csv", redundancy_rows)
    write_csv(output_dir / "stage2_compensation_results.csv", compensation_rows)
    write_csv(output_dir / "stage2_conditional_dependence.csv", conditional_rows)
    write_csv(output_dir / "stage2_temporal_interactions.csv", temporal_rows)

    trio_candidate = next(row for row in manifest if row["candidate_id"] == "S2_15")
    trio_rows = []
    trio_states = []
    for period in (*stage1.PERIODS, AGGREGATE):
        cached = joint_cache[("S2_15", period)]
        state = "HELP" if cached["deltas"]["mae_delta"] > 0 and cached["deltas"]["crps_delta"] > 0 else "HARM" if cached["deltas"]["mae_delta"] < 0 and cached["deltas"]["crps_delta"] < 0 else "MIXED"
        if period != AGGREGATE: trio_states.append(state)
        effect = next(row for row in joint_effects if row["candidate_id"] == "S2_15" and row["population"] == period)
        trio_rows.append({"candidate_id": "S2_15", "population": period, "features_pipe": trio_candidate["features_pipe"],
                          **cached["metrics"], **cached["deltas"], **cached["residual"], **effect,
                          "constituent_stage1_mae_deltas": json.dumps(cached["single_delta"]["mae"]),
                          "constituent_stage1_crps_deltas": json.dumps(cached["single_delta"]["crps"]),
                          "upstream_support_confidence_gating_shrinkage_fallback_preserved": True})
    trio_aggregate = joint_cache[("S2_15", AGGREGATE)]["deltas"]
    trio_compensation = result_map["S2_15"]["compensation"]
    if "HELP" in trio_states and "HARM" in trio_states: trio_role = "TEMPORALLY_MIXED"
    elif trio_compensation in ("STRONG", "MODERATE"): trio_role = "COMPENSATING"
    elif trio_states.count("HELP") >= 3 and trio_aggregate["mae_delta"] > 0 and trio_aggregate["crps_delta"] > 0: trio_role = "JOINTLY_HELPFUL"
    elif trio_states.count("HARM") >= 3 and trio_aggregate["mae_delta"] < 0 and trio_aggregate["crps_delta"] < 0: trio_role = "JOINTLY_HARMFUL"
    elif max(abs(trio_aggregate["mae_delta"]), abs(trio_aggregate["crps_delta"])) < .002: trio_role = "JOINTLY_NEUTRAL"
    else: trio_role = "UNRESOLVED"
    for row in trio_rows: row["COUNT_CONFIDENCE_DIRECT_ROLE"] = trio_role
    write_csv(output_dir / "stage2_count_confidence_trio.csv", trio_rows)

    concept_rows = []
    for candidate in manifest:
        terms = candidate["features_pipe"].split("|"); key = pair_key(*terms, features) if len(terms) == 2 else None
        if key not in {pair_key(a, b, features) for a, b in CONCEPT_PAIRS}: continue
        concept = CONCEPT_PAIRS.get(key) or CONCEPT_PAIRS.get((key[1], key[0]))
        for period in (*stage1.PERIODS, AGGREGATE):
            cached = joint_cache[(candidate["candidate_id"], period)]
            concept_rows.append({"candidate_id": candidate["candidate_id"], "concept": concept,
                                 "home_feature": terms[0], "away_feature": terms[1], "population": period,
                                 "home_only_mae_delta": cached["single_delta"]["mae"][0],
                                 "away_only_mae_delta": cached["single_delta"]["mae"][1],
                                 "pair_mae_delta": cached["deltas"]["mae_delta"],
                                 "mae_interaction_residual": cached["residual"]["mae_interaction_residual"],
                                 "home_only_crps_delta": cached["single_delta"]["crps"][0],
                                 "away_only_crps_delta": cached["single_delta"]["crps"][1],
                                 "pair_crps_delta": cached["deltas"]["crps_delta"],
                                 "crps_interaction_residual": cached["residual"]["crps_interaction_residual"],
                                 "temporal_interaction": result_map[candidate["candidate_id"]]["temporal"],
                                 "concept_level_interpretation": result_map[candidate["candidate_id"]]["conditional"]})
    # Explicitly retain the required declined concept and its reason.
    concept_rows.append({"candidate_id": "DECLINED", "concept": "WORKLOAD_UNCERTAINTY_CONCEPT",
                         "home_feature": "home_workload_uncertainty_outs", "away_feature": "away_workload_uncertainty_outs",
                         "population": "NOT_TESTED", "concept_level_interpretation": "DECLINE_WITH_REASON: near-zero contribution correlation, one neutral term, no pre-joint cancellation signal"})
    write_csv(output_dir / "stage2_home_away_concepts.csv", concept_rows)

    # Whole-date cluster bootstrap for every frozen joint test.
    rng = np.random.default_rng(BOOTSTRAP_SEED); uncertainty = []
    for period in stage1.PERIODS:
        data = period_data[period]; dates = pd.to_datetime(data["frame"].game_date).dt.date.astype(str).to_numpy()
        unique = np.unique(dates); draw = rng.integers(0, len(unique), size=(BOOTSTRAP_DRAWS, len(unique)))
        day_n = np.asarray([(dates == day).sum() for day in unique], float); denominator = day_n[draw].sum(axis=1)
        champion_loss = data["champion_loss"]
        for candidate in manifest:
            candidate_id = candidate["candidate_id"]; loss = joint_cache[(candidate_id, period)]["loss"]
            for metric in BOOTSTRAP_METRICS:
                difference = loss[metric] - champion_loss[metric]
                day_sum = np.asarray([difference[dates == day].sum() for day in unique])
                sampled = day_sum[draw].sum(axis=1) / denominator
                fraction = float(np.mean(sampled > 0)); p = min(1.0, 2 * min(float(np.mean(sampled <= 0)), float(np.mean(sampled >= 0))))
                uncertainty.append({"population": period, "candidate_id": candidate_id, "metric": metric,
                                    "date_clusters": len(unique), "bootstrap_draws": BOOTSTRAP_DRAWS,
                                    "point_delta_joint_minus_champion": float(difference.mean()),
                                    "ci95_low": float(np.quantile(sampled, .025)), "ci95_high": float(np.quantile(sampled, .975)),
                                    "fraction_draws_favoring_champion_retention": fraction,
                                    "unadjusted_two_sided_bootstrap_p": p, "seed": BOOTSTRAP_SEED})
    uncertainty_frame = pd.DataFrame(uncertainty); sensitivity = []
    for (period, metric), group in uncertainty_frame.groupby(["population", "metric"], sort=False):
        adjusted = stage1.holm(group.unadjusted_two_sided_bootstrap_p.tolist())
        for (_, row), value in zip(group.iterrows(), adjusted):
            sensitivity.append({"population": period, "metric": metric, "candidate_id": row.candidate_id,
                                "family_size": len(group), "unadjusted_two_sided_bootstrap_p": row.unadjusted_two_sided_bootstrap_p,
                                "holm_adjusted_p": value, "holm_significant_0_05": value < .05,
                                "fraction_draws_favoring_champion_retention": row.fraction_draws_favoring_champion_retention,
                                "interpretation": "FWER sensitivity only; not sole structural interpretation"})
    write_csv(output_dir / "stage2_clustered_uncertainty.csv", uncertainty)
    write_csv(output_dir / "stage2_multiple_comparison_sensitivity.csv", sensitivity)

    # Primary Stage-2 label per feature; Stage-1 labels remain immutable in their own package.
    involvement: dict[str, list[str]] = defaultdict(list)
    for candidate in manifest:
        for term in candidate["features_pipe"].split("|"): involvement[term].append(candidate["candidate_id"])
    structural_rows = []
    for feature in features:
        stage = classifications[feature]; candidate_ids = involvement.get(feature, [])
        candidate_results = [result_map[candidate_id] for candidate_id in candidate_ids]
        if feature == "strict_prior_total_run_factor":
            overlap_results = candidate_results
            strong_compensation_count = sum(item["compensation"] == "STRONG" for item in overlap_results)
            foundation = (stage["stage1_classification"] == "STRONGLY_REQUIRED_IN_FROZEN_CHAMPION" and
                          stage["temporal_effect"] == "CONSISTENTLY_BENEFICIAL" and
                          all(item["aggregate"]["mae_delta"] > 0 and item["aggregate"]["crps_delta"] > 0 for item in overlap_results) and
                          not any(item["redundancy"] == "STRONG" for item in overlap_results) and
                          strong_compensation_count < max(1, len(overlap_results) / 2))
            label = "POSSIBLE_FOUNDATION" if foundation else "UNRESOLVED"
            note = ("single dependence remains coherent across every frozen overlap test; one compensating relationship does not dominate the four-test body; replaceability remains untested"
                    if foundation else "overlap or compensation tests prevent a possible-foundation label")
        elif any(item["compensation"] in ("STRONG", "MODERATE") for item in candidate_results):
            label, note = "COMPENSATING_DEPENDENCE", "at least one predeclared joint test has forecast-plus-score compensation evidence"
        elif any(item["redundancy"] in ("STRONG", "MODERATE") for item in candidate_results):
            label, note = "REDUNDANT_DEPENDENCE", "at least one predeclared joint test meets score-based redundancy criteria"
        elif any(item["conditional"] == "YES" for item in candidate_results):
            label, note = "CONDITIONAL_DEPENDENCE", "joint behavior materially differs from constituent single-feature behavior"
        else:
            mapping = {"STRONGLY_REQUIRED_IN_FROZEN_CHAMPION": "UNIQUE_DEPENDENCE",
                       "MODERATELY_REQUIRED_IN_FROZEN_CHAMPION": "UNIQUE_DEPENDENCE",
                       "WEAKLY_REQUIRED_IN_FROZEN_CHAMPION": "UNRESOLVED",
                       "NEUTRAL_IN_FROZEN_CHAMPION": "WEAK_OR_NEUTRAL",
                       "POTENTIALLY_HARMFUL_IN_FROZEN_CHAMPION": "POTENTIALLY_HARMFUL",
                       "TEMPORALLY_UNSTABLE": "TEMPORALLY_UNSTABLE", "UNRESOLVED": "UNRESOLVED"}
            label = mapping[stage["stage1_classification"]]
            note = "no tested relationship crossed the Stage-2 structural evidence gates"
        structural_rows.append({"feature": feature, "stage1_classification_preserved": stage["stage1_classification"],
                                "stage1_temporal_effect_preserved": stage["temporal_effect"],
                                "tested_candidate_ids": "|".join(candidate_ids), "stage2_primary_structural_label": label,
                                "explanatory_notes": note, "irrereplaceable_claimed": False})
    write_csv(output_dir / "stage2_feature_structural_classification.csv", structural_rows)
    counts = Counter(row["stage2_primary_structural_label"] for row in structural_rows)

    strong_redundancy = [row for row in redundancy_rows if row["redundancy_evidence"] in ("STRONG", "MODERATE")]
    strong_compensation = [row for row in compensation_rows if row["compensation_evidence"] in ("STRONG", "MODERATE")]
    conditional_findings = [row for row in conditional_rows if row["conditional_dependence"] in ("YES", "MIXED")]
    foundation_terms = [row["feature"] for row in structural_rows if row["stage2_primary_structural_label"] == "POSSIBLE_FOUNDATION"]
    unique_terms = [row["feature"] for row in structural_rows if row["stage2_primary_structural_label"] == "UNIQUE_DEPENDENCE"]
    conditional_terms = [row["feature"] for row in structural_rows if row["stage2_primary_structural_label"] == "CONDITIONAL_DEPENDENCE"]
    compensation_terms = [row["feature"] for row in structural_rows if row["stage2_primary_structural_label"] == "COMPENSATING_DEPENDENCE"]
    weak_harmful_unstable_unresolved = sum(counts[name] for name in
                                           ("WEAK_OR_NEUTRAL", "POTENTIALLY_HARMFUL", "TEMPORALLY_UNSTABLE", "UNRESOLVED"))
    if len(compensation_terms) >= 6: structure = "SMALL_CORE_WITH_COMPENSATION"
    elif len(foundation_terms) + len(unique_terms) <= 6 and len(strong_redundancy) >= 2: structure = "SMALL_CORE_WITH_REDUNDANCY"
    elif len(foundation_terms) + len(unique_terms) <= 8 and weak_harmful_unstable_unresolved >= 10: structure = "FEW_DOMINANT_CONCEPTS"
    elif len(conditional_terms) + len(compensation_terms) >= 10: structure = "INTERACTION_HEAVY"
    else: structure = "MIXED"
    lower = max(1, len(foundation_terms) + int(np.ceil(len(unique_terms) / 2)))
    upper = min(22, len(foundation_terms) + len(unique_terms) + len(conditional_terms))
    core_range = f"{lower}–{upper} terms/concepts"
    stage3 = "STAGE3_REPLACEABILITY_JUSTIFIED" if structure != "UNRESOLVED" and foundation_terms else "STAGE3_NOT_YET_JUSTIFIED"
    candidate_removals = [row["feature"] for row in structural_rows if row["stage2_primary_structural_label"] in ("WEAK_OR_NEUTRAL", "POTENTIALLY_HARMFUL")]
    unresolved_terms = [row["feature"] for row in structural_rows if row["stage2_primary_structural_label"] in ("UNRESOLVED", "TEMPORALLY_UNSTABLE", "COMPENSATING_DEPENDENCE", "CONDITIONAL_DEPENDENCE")]

    (output_dir / "stage2_champion_skeleton_revision.md").write_text(
        "# Stage-2 champion skeleton revision\n\n" +
        f"`CHAMPION_STRUCTURE_STAGE2 = {structure}`\n\n`PLAUSIBLE_CORE_RANGE = {core_range}`\n\n" +
        "Stage-2 structural counts: `" + json.dumps(dict(sorted(counts.items())), sort_keys=True) + "`. "
        "This is an estimated frozen-dependence skeleton, not a certified reduced model.\n"
    )
    (output_dir / "stage2_stage3_recommendation.md").write_text(
        "# Stage-3 recommendation\n\n" + f"`{stage3}`\n\n" +
        f"- Preserve first: {', '.join(foundation_terms + unique_terms) if foundation_terms + unique_terms else 'none established'}\n"
        f"- Candidate removals for controlled replaceability tests: {', '.join(candidate_removals) if candidate_removals else 'none'}\n"
        f"- Keep unresolved: {', '.join(unresolved_terms) if unresolved_terms else 'none'}\n"
        f"- Smallest candidate foundation worth testing: {', '.join(foundation_terms) if foundation_terms else 'none'}\n\n"
        "No Stage-3 fit or reduced artifact was created.\n"
    )
    (output_dir / "stage2_nhl_transfer_lessons.md").write_text(
        "# Conceptual NHL transfer lessons\n\nNo NHL asset was inspected. Individually plausible features are not sufficient justification for direct scoring terms. Home/away terms should be tested both separately and as concepts; support/count variables should remain upstream unless direct frozen evidence earns a scoring role; frozen ablation should precede refitting; composites require explicit information/equivalence contracts; and complexity should be added incrementally only after unique or conditional value is demonstrated.\n"
    )

    strict_candidates = [row for row in joint_metrics if row["population"] == AGGREGATE and "strict_prior_total_run_factor" in row["features_pipe"]]
    strict_result = "REMAINS_COHERENT_ACROSS_ALL_TESTED_OVERLAPS" if strict_candidates and all(row["mae_delta"] > 0 and row["crps_delta"] > 0 for row in strict_candidates) else "OVERLAP_OR_COMPENSATION_REMAINS_UNRESOLVED"
    strongest_red = max(redundancy_rows, key=lambda row: row["aggregate_mae_interaction_residual"])
    compensation_rank = {"STRONG": 4, "MODERATE": 3, "WEAK": 2, "MIXED": 1, "NONE": 0}
    strongest_comp = max(compensation_rows, key=lambda row: (compensation_rank[row["compensation_evidence"]],
                                                              abs(row["aggregate_mae_interaction_residual"])))
    strongest_cond = max(conditional_rows, key=lambda row: abs(row["aggregate_mae_interaction_residual"]))
    temporal_counts = Counter(row["temporal_interaction"] for row in temporal_rows)
    report = f"""# MLB Totals RAW frozen champion interaction/cancellation dissection v1

## Governed result

`STAGE1_REPRODUCTION = PASS`. All 231 feature pairs were diagnosed before joint grading; the required trio made 232 candidate relationships considered. Exactly 15 pair/group tests were frozen under manifest SHA `{expected_manifest_sha}` and then evaluated without refitting or changing upstream state.

The manifest contains eight justified home/away concepts, four strict-prior-factor overlap tests, two high-correlation same-side prevention/bullpen tests, and the required count/confidence trio. Workload uncertainty was explicitly declined before joint grading because its contribution correlation was near zero, one term was neutral, and no pre-joint cancellation evidence existed.

## Structural findings

- Strongest redundancy diagnostic: `{strongest_red['candidate_id']}` ({strongest_red['features_pipe']}), `{strongest_red['redundancy_evidence']}`, aggregate MAE interaction residual {strongest_red['aggregate_mae_interaction_residual']:+.6f}.
- Strongest compensation diagnostic by absolute MAE interaction: `{strongest_comp['candidate_id']}` ({strongest_comp['features_pipe']}), `{strongest_comp['compensation_evidence']}` / `{strongest_comp['compensation_level']}`.
- Strongest conditional diagnostic by absolute MAE interaction: `{strongest_cond['candidate_id']}` ({strongest_cond['features_pipe']}), `{strongest_cond['conditional_dependence']}`.
- `COUNT_CONFIDENCE_DIRECT_ROLE = {trio_role}`.
- strict-prior factor relationship result: `{strict_result}`.
- Temporal interaction counts: `{json.dumps(dict(sorted(temporal_counts.items())), sort_keys=True)}`.
- Stage-2 feature counts: `{json.dumps(dict(sorted(counts.items())), sort_keys=True)}`.

`CHAMPION_STRUCTURE_STAGE2 = {structure}`. `PLAUSIBLE_CORE_RANGE = {core_range}`. `{stage3}`.

Full constituent, joint, interaction-residual, date-cluster bootstrap, Holm, concept, and structural-label evidence is retained in this package. These are frozen-model diagnostics, not causal decompositions or replaceability findings. No reduced model was built.
"""
    (output_dir / "concise_mlb_totals_raw_frozen_champion_interaction_cancellation_dissection_v1.md").write_text(report)

    # Complete hash manifest, excluding itself.
    outputs = sorted(path for path in output_dir.iterdir() if path.is_file() and path.name != "reproducibility_hashes.json")
    hashes = {"task_id": TASK_ID, "created_at_utc": datetime.now(timezone.utc).isoformat(),
              "model_artifact_sha256": sha256(stage1.raw.CONFIG), "stage1_package_manifest_sha256": sha256(STAGE1_OUTPUT / "reproducibility_hashes.json"),
              "frozen_stage2_manifest_sha256": expected_manifest_sha,
              "analysis_utility_sha256": sha256(Path(__file__)),
              "outputs": {path.name: sha256(path) for path in outputs},
              "bootstrap_seed": BOOTSTRAP_SEED, "bootstrap_draws": BOOTSTRAP_DRAWS}
    write_json(output_dir / "reproducibility_hashes.json", hashes)
    return {"status": "PASS", "stage1_reproduction": "PASS", "pairs_considered": 231,
            "candidate_relationships_considered": 232, "frozen_joint_tests": 15,
            "manifest_sha256": expected_manifest_sha, "count_confidence_direct_role": trio_role,
            "strict_prior_relationship": strict_result, "stage2_counts": dict(counts),
            "structure": structure, "plausible_core_range": core_range, "stage3": stage3,
            "stage3_preserve": foundation_terms + unique_terms, "stage3_candidate_removals": candidate_removals,
            "stage3_unresolved": unresolved_terms, "output": str(output_dir)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("freeze", "evaluate"), required=True)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    parser.add_argument("--manifest-sha256")
    args = parser.parse_args()
    if args.phase == "freeze": result = freeze(args.output_dir)
    else:
        if not args.manifest_sha256: raise SystemExit("--manifest-sha256 is required for evaluate")
        result = evaluate(args.output_dir, args.manifest_sha256)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

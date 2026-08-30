"""Governed 22-way leave-one-feature-out replaceability map for MLB totals RAW.

Every candidate uses the original development rows, StandardScaler and
PoissonRegressor contract. Evaluation rows never participate in fitting or
candidate selection. This analysis creates research artifacts only.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import sklearn
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.scripts import run_mlb_totals_raw_frozen_champion_single_feature_dissection_v1 as stage1


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_RAW_CHAMPION_SINGLE_FEATURE_REPLACEABILITY_REFIT_V1"
OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_champion_single_feature_replaceability_refit_v1/2026-08-29"
STAGE1_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_frozen_champion_single_feature_dissection_v1/2026-08-29"
STAGE2_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_frozen_champion_interaction_cancellation_dissection_v1/2026-08-29"
STAGE2_MANIFEST_SHA = "bfd7d1c1b27908831e83a0b2dcdef6276598ba06ba2e1669979304bc3514551c"
AGGREGATE = "ALL_GOVERNED_EVALUATION"
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260829 + 3
POINT_METRICS = ("mae", "rmse", "actual_minus_forecast_bias")
DIST_METRICS = ("crps", "brier", "log_loss", "ece")
BOOTSTRAP_METRICS = ("mae", "crps", "brier", "log_loss")
MAE_MATERIAL = .005
CRPS_MATERIAL = .002


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


def frame_hash(frame: pd.DataFrame, columns: list[str]) -> str:
    payload = frame.sort_values(["game_date", "game_pk"])[columns].to_csv(
        index=False, lineterminator="\n", float_format="%.17g"
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def verify_prior_packages() -> dict[str, Any]:
    stage1_reproduction = json.loads((STAGE1_OUTPUT / "raw_champion_reproduction.json").read_text())
    stage2_reproduction = json.loads((STAGE2_OUTPUT / "stage1_reproduction.json").read_text())
    stage2_hashes = json.loads((STAGE2_OUTPUT / "reproducibility_hashes.json").read_text())
    manifest = STAGE2_OUTPUT / "stage2_joint_ablation_manifest.csv"
    checks = {
        "stage1_reproduction_pass": stage1_reproduction.get("RAW_CHAMPION_REPRODUCTION") == "PASS",
        "stage2_stage1_reproduction_pass": stage2_reproduction.get("STAGE1_REPRODUCTION") == "PASS",
        "stage2_manifest_sha256": sha256(manifest),
        "stage2_manifest_hash_expected": sha256(manifest) == STAGE2_MANIFEST_SHA,
        "stage2_hash_manifest_agrees": stage2_hashes.get("frozen_stage2_manifest_sha256") == STAGE2_MANIFEST_SHA,
        "stage2_output_hashes_valid": all(sha256(STAGE2_OUTPUT / name) == digest
                                          for name, digest in stage2_hashes["outputs"].items()),
    }
    if not all(value for key, value in checks.items() if key not in ("stage2_manifest_sha256",)):
        raise RuntimeError("PRIOR_DISSECTION_INTEGRITY_FAILED")
    return checks


def fit_pipeline(training: pd.DataFrame, features: list[str], artifact: dict[str, Any]) -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("location", PoissonRegressor(alpha=float(artifact["location_regularization_alpha"]),
                                      max_iter=int(artifact["location_max_iter"]), solver="lbfgs", tol=1e-4,
                                      fit_intercept=True, warm_start=False)),
    ]).fit(training[features], training.final_total)


def dispersion(actual: np.ndarray, forecast: np.ndarray) -> float:
    return max(0.0, float((((actual - forecast) ** 2 - actual).sum()) /
                          np.maximum((forecast ** 2).sum(), 1)))


def reproduce_training(training: pd.DataFrame, artifact: dict[str, Any], prior_checks: dict[str, Any]) -> tuple[Pipeline, dict[str, Any]]:
    features = artifact["feature_order"]
    pipeline = fit_pipeline(training, features, artifact)
    prediction = pipeline.predict(training[features])
    fitted_dispersion = dispersion(training.final_total.to_numpy(float), prediction)
    exact = {
        "scaler_mean_exact": np.array_equal(pipeline["scaler"].mean_, np.asarray(artifact["scaler_mean"])),
        "scaler_scale_exact": np.array_equal(pipeline["scaler"].scale_, np.asarray(artifact["scaler_scale"])),
        "coefficients_exact": np.array_equal(pipeline["location"].coef_, np.asarray(artifact["coefficients"])),
        "intercept_exact": float(pipeline["location"].intercept_) == float(artifact["intercept"]),
        "dispersion_exact": fitted_dispersion == float(artifact["dispersion_alpha"]),
    }
    stored_prediction = stage1.score_components(training, artifact)[0]
    max_prediction_error = float(np.max(abs(prediction - stored_prediction)))
    status = "PASS" if all(exact.values()) and max_prediction_error == 0 else (
        "PASS_WITH_NUMERICAL_TOLERANCE" if all(exact[key] for key in exact if key != "dispersion_exact") and max_prediction_error <= 2e-12 else "FAIL"
    )
    result = {
        "task_id": TASK_ID, "RAW_REFIT_REPRODUCTION": status,
        "model_identity": stage1.MODEL_IDENTITY, "model_hash": stage1.MODEL_HASH,
        "artifact_sha256": sha256(stage1.raw.CONFIG), "training_population": artifact["development_population"],
        "training_date_min": str(training.game_date.min().date()), "training_date_max": str(training.game_date.max().date()),
        "training_rows": len(training), "training_row_target_hash": frame_hash(training, ["game_pk", "game_date", "final_total"]),
        "training_matrix_hash": frame_hash(training, ["game_pk", "final_total", *features]),
        "target": artifact["outcome_target"], "model_class": "sklearn.linear_model.PoissonRegressor",
        "objective": "Poisson deviance with L2 regularization", "standardization": artifact["normalization"],
        "missing_handling": "governed upstream fallbacks, then non-finite direct matrix values replaced/fillna(0) by authoritative loader",
        "dispersion_estimation": artifact["dispersion_alpha"], "dispersion_formula": "max(0,sum(((y-mu)^2-y))/sum(mu^2))",
        "intercept_fitted": True, "hyperparameters": pipeline["location"].get_params(),
        "optimizer": "lbfgs", "random_seed": "NOT_APPLICABLE_DETERMINISTIC_LBFGS",
        "feature_order": features, "fixed_distribution_support": artifact["distribution_support"],
        "sklearn_version": sklearn.__version__, "max_training_prediction_abs_error": max_prediction_error,
        "exact_checks": exact, "prior_package_checks": prior_checks,
    }
    if status == "FAIL":
        raise RuntimeError("RAW_REFIT_REPRODUCTION_FAILED")
    return pipeline, result


def candidate_name(feature: str) -> str:
    return f"RAW_MINUS_{feature.upper()}_REFIT_V1"


def artifact_hash(value: dict[str, Any]) -> str:
    copy = dict(value); copy.pop("canonical_candidate_hash", None)
    return stage1.canonical_hash(copy)


def fit_candidates(training: pd.DataFrame, artifact: dict[str, Any], output_dir: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    features = artifact["feature_order"]
    manifest = []
    for index, omitted in enumerate(features):
        retained = [feature for feature in features if feature != omitted]
        manifest.append({
            "candidate_number": index + 1, "candidate_identity": candidate_name(omitted), "omitted_direct_feature": omitted,
            "retained_feature_count": len(retained), "retained_feature_order_pipe": "|".join(retained),
            "training_rows": len(training), "training_rows_identical_to_champion": True,
            "model_class_hyperparameters_identical": True, "scaler_refit_on_training_only": True,
            "intercept_refit": True, "dispersion_refit_by_original_contract": True,
            "upstream_gating_support_shrinkage_fallback_preserved": True,
            "evaluation_rows_or_outcomes_used_for_fit_selection_tuning": False,
            "predeclared_interpretation_rule": "independent RAW-minus-exactly-one-feature refit; no recursive removal, tuning, added proxy, group test, or evaluation-guided selection",
        })
    write_csv(output_dir / "stage3_22_refit_manifest.csv", manifest)
    frozen_manifest_sha = sha256(output_dir / "stage3_22_refit_manifest.csv")

    artifacts_dir = output_dir / "candidate_artifacts"; artifacts_dir.mkdir(exist_ok=True)
    candidates = {}; hash_rows = []
    actual = training.final_total.to_numpy(float)
    for manifest_row in manifest:
        omitted = manifest_row["omitted_direct_feature"]
        retained = manifest_row["retained_feature_order_pipe"].split("|")
        pipeline = fit_pipeline(training, retained, artifact)
        forecast = pipeline.predict(training[retained]); fitted_dispersion = dispersion(actual, forecast)
        candidate: dict[str, Any] = {
            "candidate_identity": manifest_row["candidate_identity"], "designation": "RESEARCH_REPLACEABILITY_ONLY_NOT_PROMOTED",
            "source_task": TASK_ID, "champion_model_hash": stage1.MODEL_HASH,
            "omitted_direct_feature": omitted, "upstream_role_preserved": True,
            "model_family": artifact["model_family"], "location_regularization_alpha": artifact["location_regularization_alpha"],
            "location_max_iter": artifact["location_max_iter"], "solver": "lbfgs", "solver_tolerance": 1e-4,
            "random_seed": "NOT_APPLICABLE_DETERMINISTIC_LBFGS", "sklearn_version": sklearn.__version__,
            "development_population": artifact["development_population"], "development_games": len(training),
            "development_date_min": str(training.game_date.min().date()), "development_date_max": str(training.game_date.max().date()),
            "training_row_identity_target_hash": frame_hash(training, ["game_pk", "game_date", "final_total"]),
            "training_matrix_hash": frame_hash(training, ["game_pk", "final_total", *retained]),
            "feature_order": retained, "scaler_mean": pipeline["scaler"].mean_.tolist(),
            "scaler_scale": pipeline["scaler"].scale_.tolist(), "intercept": float(pipeline["location"].intercept_),
            "coefficients": pipeline["location"].coef_.tolist(), "dispersion_alpha": fitted_dispersion,
            "dispersion_construction": "max(0,sum(((y-mu)^2-y))/sum(mu^2)) on exact development rows",
            "normalization": artifact["normalization"], "distribution_support": artifact["distribution_support"],
            "outcome_target": artifact["outcome_target"], "fit_count": 1,
            "evaluation_rows_used_for_fit_selection_or_tuning": 0, "public_status": "RESEARCH_ONLY_NOT_AUTHORIZED",
            "frozen_pre_fit_manifest_sha256": frozen_manifest_sha,
        }
        candidate["canonical_candidate_hash"] = artifact_hash(candidate)
        path = artifacts_dir / f"{candidate['candidate_identity']}.json"
        write_json(path, candidate); candidates[omitted] = candidate
        hash_rows.append({"omitted_feature": omitted, "candidate_identity": candidate["candidate_identity"],
                          "canonical_candidate_hash": candidate["canonical_candidate_hash"],
                          "artifact_relative_path": str(path.relative_to(output_dir)), "artifact_sha256": sha256(path),
                          "frozen_manifest_sha256": frozen_manifest_sha, "fit_count": 1})
    write_csv(output_dir / "stage3_candidate_artifact_hashes.csv", hash_rows)
    return candidates, manifest


def score_candidate(frame: pd.DataFrame, candidate: dict[str, Any]) -> np.ndarray:
    values = frame[candidate["feature_order"]].astype(float).to_numpy()
    z = (values - np.asarray(candidate["scaler_mean"])) / np.asarray(candidate["scaler_scale"])
    return np.exp(float(candidate["intercept"]) + z @ np.asarray(candidate["coefficients"]))


def loss_arrays(frame: pd.DataFrame, mu: np.ndarray, alpha: float) -> dict[str, np.ndarray]:
    actual = frame.final_total.to_numpy(float)
    distribution_loss = stage1.row_distribution_losses(mu, actual, alpha)
    return {"mae": abs(actual - mu), "squared_error": (actual - mu) ** 2,
            "actual_minus_forecast_bias": actual - mu, "crps": distribution_loss["crps"],
            "brier": distribution_loss["brier"], "log_loss": distribution_loss["log_loss"],
            "event_probability": distribution_loss["event_probability"], "event_outcome": distribution_loss["event_outcome"]}


def metrics(frame: pd.DataFrame, mu: np.ndarray, loss: dict[str, np.ndarray]) -> dict[str, float]:
    return {"games": len(frame), "mae": float(loss["mae"].mean()), "rmse": float(np.sqrt(loss["squared_error"].mean())),
            "actual_minus_forecast_bias": float(loss["actual_minus_forecast_bias"].mean()),
            "crps": float(loss["crps"].mean()), "brier": float(loss["brier"].mean()),
            "log_loss": float(loss["log_loss"].mean()),
            "ece": stage1.ece(loss["event_probability"], loss["event_outcome"])}


def period_status(rows: list[dict[str, Any]]) -> str:
    states = []
    for row in rows:
        mae, crps = row["stage3_refit_mae_delta"], row["stage3_refit_crps_delta"]
        if mae > MAE_MATERIAL and crps > CRPS_MATERIAL: state = "WORSE"
        elif mae < -MAE_MATERIAL and crps < -CRPS_MATERIAL: state = "BETTER"
        elif abs(mae) <= MAE_MATERIAL and abs(crps) <= CRPS_MATERIAL: state = "RECOVERED_OR_NEUTRAL"
        else: state = "MIXED"
        states.append(state)
    if len(set(states)) == 1: return "STABLE"
    common, count = Counter(states).most_common(1)[0]
    if count >= 3 and states[-1] == common: return "MOSTLY_STABLE"
    if states[-1] not in set(states[:-1]) or ("WORSE" in states and "BETTER" in states): return "REGIME_DEPENDENT"
    if len(set(states)) >= 3: return "UNSTABLE"
    return "MIXED"


def recovery(frozen_delta: float, refit_delta: float) -> float | None:
    return float((frozen_delta - refit_delta) / frozen_delta) if frozen_delta > 0 else None


def run(output_dir: Path = OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    prior_checks = verify_prior_packages()
    artifact = stage1.load_artifact(); features = artifact["feature_order"]
    historical = stage1.raw.load_historical(artifact)
    training = historical.loc[historical.period.eq("DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE")].copy().reset_index(drop=True)
    _, reproduction = reproduce_training(training, artifact, prior_checks)
    write_json(output_dir / "stage3_raw_training_reproduction.json", reproduction)

    training_contract = f"""# RAW training contract

- Population: exact 2023–2024 governed development spine, {len(training):,} rows, {training.game_date.min().date()} through {training.game_date.max().date()}.
- Target: official final total runs (`OFFICIAL_FINAL_TOTAL_RUNS`).
- Location model: `sklearn.linear_model.PoissonRegressor`; Poisson deviance objective with L2 alpha `{artifact['location_regularization_alpha']}`; fitted intercept; `lbfgs`; max iterations `{artifact['location_max_iter']}`; tolerance `1e-4`; deterministic/no random seed.
- Preprocessing: `StandardScaler` fit on development rows only. Governed upstream fallbacks are preserved; the authoritative spine loader replaces non-finite direct inputs and fills remaining missing values with zero before fitting.
- Dispersion: refit after location by `max(0, sum(((y-mu)^2-y)) / sum(mu^2))`; NB support 0..30 with 30-plus tail folded into 30.
- Every Stage-3 model omits exactly one direct feature, refits the other 21 terms/intercept/scaler/dispersion once, and uses no evaluation row for fitting, tuning, scaling, thresholding, or selection.
- Count/support fields remain available to upstream gating, history sufficiency, fallback, and park shrinkage even when omitted from the direct location matrix.
"""
    (output_dir / "stage3_training_contract.md").write_text(training_contract)
    candidates, manifest = fit_candidates(training, artifact, output_dir)

    frames, _ = stage1.load_populations(artifact)
    population_rows = []
    for period, frame in frames.items():
        champion_mu = stage1.score_components(frame, artifact)[0]
        candidate_counts = {feature: len(score_candidate(frame, candidates[feature])) for feature in features}
        population_rows.append({"population": period, "date_min": str(frame.game_date.min().date()),
                                "date_max": str(frame.game_date.max().date()), "champion_rows": len(frame),
                                "all_22_candidate_rows": min(candidate_counts.values()),
                                "row_count_parity": len(set(candidate_counts.values()) | {len(frame)}) == 1,
                                "game_identity_unique": not frame.game_pk.duplicated().any(),
                                "outcomes_complete": bool(frame.final_total.notna().all()),
                                "feature_state_authority": "ORIGINAL_IMMUTABLE_PROSPECTIVE_CONTEXT" if period == stage1.PERIODS[-1] else "FROZEN_GOVERNED_FEATURE_SPINE",
                                "row_identity_outcome_hash": frame_hash(frame, ["game_pk", "game_date", "final_total"]),
                                "champion_mu_hash": hashlib.sha256(champion_mu.tobytes()).hexdigest()})
    if not all(row["row_count_parity"] for row in population_rows): raise RuntimeError("EVALUATION_ROW_PARITY_FAILED")
    write_csv(output_dir / "stage3_evaluation_population_parity.csv", population_rows)

    stage1_class = {row["feature"]: row for row in read_csv(STAGE1_OUTPUT / "raw_champion_stage1_classification.csv")}
    stage1_point = {(row["feature"], row["population"]): row for row in read_csv(STAGE1_OUTPUT / "raw_champion_feature_point_deltas.csv")
                    if row["feature"] != "UNALTERED_CHAMPION"}
    stage1_dist = {(row["feature"], row["population"]): row for row in read_csv(STAGE1_OUTPUT / "raw_champion_feature_distribution_deltas.csv")
                   if row["feature"] != "UNALTERED_CHAMPION"}
    stage2_labels = {row["feature"]: row for row in read_csv(STAGE2_OUTPUT / "stage2_feature_structural_classification.csv")}
    stage2_pair = {(row["feature_a"], row["feature_b"]): row for row in read_csv(STAGE2_OUTPUT / "stage2_pair_diagnostic_matrix.csv")}

    point_rows, dist_rows, compare_rows, recovery_rows = [], [], [], []
    cache: dict[tuple[str, str], dict[str, Any]] = {}
    for period, frame in frames.items():
        champion_mu = stage1.score_components(frame, artifact)[0]
        champion_loss = loss_arrays(frame, champion_mu, float(artifact["dispersion_alpha"])); champion_metrics = metrics(frame, champion_mu, champion_loss)
        cache[("CHAMPION", period)] = {"mu": champion_mu, "loss": champion_loss, "metrics": champion_metrics}
        point_rows.append({"population": period, "variant": "CHAMPION", "omitted_feature": "NONE", **{k: champion_metrics[k] for k in POINT_METRICS},
                           **{f"{k}_delta": 0.0 for k in POINT_METRICS}})
        dist_rows.append({"population": period, "variant": "CHAMPION", "omitted_feature": "NONE", **{k: champion_metrics[k] for k in DIST_METRICS},
                          **{f"{k}_delta": 0.0 for k in DIST_METRICS}})
        for omitted in features:
            candidate = candidates[omitted]; mu = score_candidate(frame, candidate)
            loss = loss_arrays(frame, mu, float(candidate["dispersion_alpha"])); values = metrics(frame, mu, loss)
            cache[(omitted, period)] = {"mu": mu, "loss": loss, "metrics": values}
            point_delta = {f"{key}_delta": values[key] - champion_metrics[key] for key in POINT_METRICS}
            dist_delta = {f"{key}_delta": values[key] - champion_metrics[key] for key in DIST_METRICS}
            point_rows.append({"population": period, "variant": candidate["candidate_identity"], "omitted_feature": omitted,
                               **{k: values[k] for k in POINT_METRICS}, **{f"champion_{k}": champion_metrics[k] for k in POINT_METRICS}, **point_delta})
            dist_rows.append({"population": period, "variant": candidate["candidate_identity"], "omitted_feature": omitted,
                              **{k: values[k] for k in DIST_METRICS}, **{f"champion_{k}": champion_metrics[k] for k in DIST_METRICS}, **dist_delta})
            frozen_point = stage1_point[(omitted, period)]; frozen_dist = stage1_dist[(omitted, period)]
            row = {"feature": omitted, "population": period,
                   "stage1_frozen_mae_delta": float(frozen_point["mae_delta"]), "stage3_refit_mae_delta": point_delta["mae_delta"],
                   "stage1_frozen_rmse_delta": float(frozen_point["rmse_delta"]), "stage3_refit_rmse_delta": point_delta["rmse_delta"],
                   "stage1_frozen_crps_delta": float(frozen_dist["crps_delta"]), "stage3_refit_crps_delta": dist_delta["crps_delta"],
                   "stage1_frozen_brier_delta": float(frozen_dist["brier_delta"]), "stage3_refit_brier_delta": dist_delta["brier_delta"],
                   "stage1_frozen_log_loss_delta": float(frozen_dist["log_loss_delta"]), "stage3_refit_log_loss_delta": dist_delta["log_loss_delta"],
                   "stage1_frozen_absolute_bias_delta": float(frozen_point["absolute_bias_delta"]),
                   "stage3_refit_actual_minus_forecast_bias_delta": point_delta["actual_minus_forecast_bias_delta"],
                   "upstream_role_preserved": True}
            compare_rows.append(row)
            for metric, frozen_key, refit_key in (("mae", "stage1_frozen_mae_delta", "stage3_refit_mae_delta"),
                                                   ("rmse", "stage1_frozen_rmse_delta", "stage3_refit_rmse_delta"),
                                                   ("crps", "stage1_frozen_crps_delta", "stage3_refit_crps_delta"),
                                                   ("brier", "stage1_frozen_brier_delta", "stage3_refit_brier_delta"),
                                                   ("log_loss", "stage1_frozen_log_loss_delta", "stage3_refit_log_loss_delta")):
                recovered = recovery(row[frozen_key], row[refit_key])
                recovery_rows.append({"feature": omitted, "population": period, "metric": metric,
                                      "stage1_frozen_delta": row[frozen_key], "stage3_refit_delta": row[refit_key],
                                      "fraction_stage1_loss_recovered": recovered,
                                      "recovery_defined": recovered is not None,
                                      "interpretation": "positive frozen loss basis only" if recovered is not None else "not defined because frozen removal did not worsen metric"})

    # Aggregate metrics are recomputed row-wise, never averaged across periods.
    all_frame = pd.concat([frames[period] for period in stage1.PERIODS], ignore_index=True)
    champion_mu = np.concatenate([cache[("CHAMPION", period)]["mu"] for period in stage1.PERIODS])
    champion_loss = loss_arrays(all_frame, champion_mu, float(artifact["dispersion_alpha"])); champion_metrics = metrics(all_frame, champion_mu, champion_loss)
    point_rows.append({"population": AGGREGATE, "variant": "CHAMPION", "omitted_feature": "NONE", **{k: champion_metrics[k] for k in POINT_METRICS}, **{f"{k}_delta": 0.0 for k in POINT_METRICS}})
    dist_rows.append({"population": AGGREGATE, "variant": "CHAMPION", "omitted_feature": "NONE", **{k: champion_metrics[k] for k in DIST_METRICS}, **{f"{k}_delta": 0.0 for k in DIST_METRICS}})
    aggregate_compare = {}
    for omitted in features:
        candidate = candidates[omitted]; mu = np.concatenate([cache[(omitted, period)]["mu"] for period in stage1.PERIODS])
        loss = loss_arrays(all_frame, mu, float(candidate["dispersion_alpha"])); values = metrics(all_frame, mu, loss)
        point_delta = {f"{key}_delta": values[key] - champion_metrics[key] for key in POINT_METRICS}
        dist_delta = {f"{key}_delta": values[key] - champion_metrics[key] for key in DIST_METRICS}
        cache[(omitted, AGGREGATE)] = {"mu": mu, "loss": loss, "metrics": values}
        point_rows.append({"population": AGGREGATE, "variant": candidate["candidate_identity"], "omitted_feature": omitted,
                           **{k: values[k] for k in POINT_METRICS}, **{f"champion_{k}": champion_metrics[k] for k in POINT_METRICS}, **point_delta})
        dist_rows.append({"population": AGGREGATE, "variant": candidate["candidate_identity"], "omitted_feature": omitted,
                          **{k: values[k] for k in DIST_METRICS}, **{f"champion_{k}": champion_metrics[k] for k in DIST_METRICS}, **dist_delta})
        frozen = stage1_class[omitted]
        row = {"feature": omitted, "population": AGGREGATE,
               "stage1_frozen_mae_delta": float(frozen["mae_delta"]), "stage3_refit_mae_delta": point_delta["mae_delta"],
               "stage1_frozen_rmse_delta": float(frozen["rmse_delta"]), "stage3_refit_rmse_delta": point_delta["rmse_delta"],
               "stage1_frozen_crps_delta": float(frozen["crps_delta"]), "stage3_refit_crps_delta": dist_delta["crps_delta"],
               "stage1_frozen_brier_delta": float(frozen["brier_delta"]), "stage3_refit_brier_delta": dist_delta["brier_delta"],
               "stage1_frozen_log_loss_delta": float(frozen["log_loss_delta"]), "stage3_refit_log_loss_delta": dist_delta["log_loss_delta"],
               "stage1_frozen_absolute_bias_delta": float(frozen["absolute_bias_delta"]),
               "stage3_refit_actual_minus_forecast_bias_delta": point_delta["actual_minus_forecast_bias_delta"],
               "upstream_role_preserved": True}
        compare_rows.append(row); aggregate_compare[omitted] = row
        for metric, frozen_key, refit_key in (("mae", "stage1_frozen_mae_delta", "stage3_refit_mae_delta"),
                                               ("rmse", "stage1_frozen_rmse_delta", "stage3_refit_rmse_delta"),
                                               ("crps", "stage1_frozen_crps_delta", "stage3_refit_crps_delta"),
                                               ("brier", "stage1_frozen_brier_delta", "stage3_refit_brier_delta"),
                                               ("log_loss", "stage1_frozen_log_loss_delta", "stage3_refit_log_loss_delta")):
            recovered = recovery(row[frozen_key], row[refit_key])
            recovery_rows.append({"feature": omitted, "population": AGGREGATE, "metric": metric,
                                  "stage1_frozen_delta": row[frozen_key], "stage3_refit_delta": row[refit_key],
                                  "fraction_stage1_loss_recovered": recovered, "recovery_defined": recovered is not None,
                                  "interpretation": "positive frozen loss basis only" if recovered is not None else "not defined because frozen removal did not worsen metric"})
    write_csv(output_dir / "stage3_refit_point_metrics.csv", point_rows)
    write_csv(output_dir / "stage3_refit_distribution_metrics.csv", dist_rows)
    write_csv(output_dir / "stage3_frozen_vs_refit_effects.csv", compare_rows)
    write_csv(output_dir / "stage3_loss_recovery.csv", recovery_rows)

    # Coefficient redistribution and descriptive absorption rankings.
    champion_coefficient = dict(zip(features, artifact["coefficients"])); redistribution = []
    absorption_rows = []
    combined_frame = all_frame
    champion_all_mu, _, champion_contributions = stage1.score_components(combined_frame, artifact)
    stage1_delta_response = {feature: champion_all_mu * np.exp(-champion_contributions[:, features.index(feature)]) - champion_all_mu for feature in features}
    for omitted in features:
        candidate = candidates[omitted]; refit_map = dict(zip(candidate["feature_order"], candidate["coefficients"]))
        original_vector = np.asarray([champion_coefficient[name] for name in candidate["feature_order"]])
        refit_vector = np.asarray([refit_map[name] for name in candidate["feature_order"]])
        vector_corr = float(np.corrcoef(original_vector, refit_vector)[0, 1]); vector_distance = float(np.linalg.norm(refit_vector - original_vector))
        shifts = []
        for retained in candidate["feature_order"]:
            original, refit = champion_coefficient[retained], refit_map[retained]; change = refit - original
            redistribution.append({"omitted_feature": omitted, "term": retained, "term_type": "COEFFICIENT",
                                   "original_value": original, "refit_value": refit, "absolute_change": abs(change),
                                   "percentage_change": 100 * change / original if abs(original) > 1e-12 else None,
                                   "sign_change": np.sign(original) != np.sign(refit),
                                   "coefficient_vector_correlation": vector_corr, "coefficient_vector_l2_distance": vector_distance,
                                   "intercept_change": candidate["intercept"] - artifact["intercept"],
                                   "dispersion_change": candidate["dispersion_alpha"] - artifact["dispersion_alpha"]})
            key = (omitted, retained) if (omitted, retained) in stage2_pair else (retained, omitted)
            diagnostic = stage2_pair[key]
            response_similarity = float(np.corrcoef(stage1_delta_response[omitted], stage1_delta_response[retained])[0, 1])
            overlap_bonus = 1.0 if diagnostic["overlapping_source_lineage"] != "DISTINCT_PRIMARY_STATE_FAMILIES" else 0.0
            shifts.append({"retained": retained, "absolute_coefficient_change": abs(change),
                           "direct_contribution_correlation": float(diagnostic["direct_contribution_correlation"]),
                           "source_concept_overlap": diagnostic["overlapping_source_lineage"],
                           "forecast_response_similarity": response_similarity, "overlap_bonus": overlap_bonus})
        redistribution.append({"omitted_feature": omitted, "term": "INTERCEPT", "term_type": "INTERCEPT",
                               "original_value": artifact["intercept"], "refit_value": candidate["intercept"],
                               "absolute_change": abs(candidate["intercept"] - artifact["intercept"]),
                               "percentage_change": 100 * (candidate["intercept"] - artifact["intercept"]) / artifact["intercept"],
                               "sign_change": False, "coefficient_vector_correlation": vector_corr,
                               "coefficient_vector_l2_distance": vector_distance,
                               "intercept_change": candidate["intercept"] - artifact["intercept"],
                               "dispersion_change": candidate["dispersion_alpha"] - artifact["dispersion_alpha"]})
        redistribution.append({"omitted_feature": omitted, "term": "DISPERSION_ALPHA", "term_type": "DISPERSION",
                               "original_value": artifact["dispersion_alpha"], "refit_value": candidate["dispersion_alpha"],
                               "absolute_change": abs(candidate["dispersion_alpha"] - artifact["dispersion_alpha"]),
                               "percentage_change": 100 * (candidate["dispersion_alpha"] - artifact["dispersion_alpha"]) / artifact["dispersion_alpha"],
                               "sign_change": False, "coefficient_vector_correlation": vector_corr,
                               "coefficient_vector_l2_distance": vector_distance,
                               "intercept_change": candidate["intercept"] - artifact["intercept"],
                               "dispersion_change": candidate["dispersion_alpha"] - artifact["dispersion_alpha"]})
        max_shift = max(item["absolute_coefficient_change"] for item in shifts)
        for item in shifts:
            item["absorption_score"] = ((item["absolute_coefficient_change"] / max(max_shift, 1e-15)) * .55 +
                                        abs(item["direct_contribution_correlation"]) * .20 +
                                        abs(item["forecast_response_similarity"]) * .15 + item["overlap_bonus"] * .10)
        for rank, item in enumerate(sorted(shifts, key=lambda row: row["absorption_score"], reverse=True)[:5], 1):
            strength = "CLEAR" if item["absorption_score"] >= .75 else "MODERATE" if item["absorption_score"] >= .55 else "WEAK" if item["absorption_score"] >= .35 else "NONE"
            absorption_rows.append({"omitted_feature": omitted, "absorber_rank": rank, "candidate_absorber": item["retained"],
                                    **item, "absorption_evidence": strength,
                                    "causal_claim": False, "map": f"{omitted} -> {item['retained']}"})
    write_csv(output_dir / "stage3_coefficient_redistribution.csv", redistribution)
    write_csv(output_dir / "stage3_absorption_map.csv", absorption_rows)

    temporal_rows = []
    for omitted in features:
        period_rows = [next(row for row in compare_rows if row["feature"] == omitted and row["population"] == period) for period in stage1.PERIODS]
        status = period_status(period_rows); aggregate = aggregate_compare[omitted]
        temporal_rows.append({"feature": omitted, "replaceability_temporal_status": status,
                              **{f"{period}_mae_delta": row["stage3_refit_mae_delta"] for period, row in zip(stage1.PERIODS, period_rows)},
                              **{f"{period}_crps_delta": row["stage3_refit_crps_delta"] for period, row in zip(stage1.PERIODS, period_rows)},
                              "aggregate_mae_delta": aggregate["stage3_refit_mae_delta"],
                              "aggregate_crps_delta": aggregate["stage3_refit_crps_delta"],
                              "aggregate_mae_loss_recovery": recovery(aggregate["stage1_frozen_mae_delta"], aggregate["stage3_refit_mae_delta"]),
                              "aggregate_crps_loss_recovery": recovery(aggregate["stage1_frozen_crps_delta"], aggregate["stage3_refit_crps_delta"])})
    write_csv(output_dir / "stage3_temporal_replaceability.csv", temporal_rows)

    # Date-cluster bootstrap across the same four populations.
    rng = np.random.default_rng(BOOTSTRAP_SEED); uncertainty = []
    for period, frame in frames.items():
        dates = pd.to_datetime(frame.game_date).dt.date.astype(str).to_numpy(); unique = np.unique(dates)
        draw = rng.integers(0, len(unique), size=(BOOTSTRAP_DRAWS, len(unique)))
        day_n = np.asarray([(dates == day).sum() for day in unique], float); denominator = day_n[draw].sum(axis=1)
        champion_loss = cache[("CHAMPION", period)]["loss"]
        for omitted in features:
            candidate_loss = cache[(omitted, period)]["loss"]
            for metric in BOOTSTRAP_METRICS:
                difference = candidate_loss[metric] - champion_loss[metric]
                day_sum = np.asarray([difference[dates == day].sum() for day in unique])
                sampled = day_sum[draw].sum(axis=1) / denominator
                favor_candidate = float(np.mean(sampled < 0)); favor_champion = float(np.mean(sampled > 0))
                p = min(1.0, 2 * min(float(np.mean(sampled <= 0)), float(np.mean(sampled >= 0))))
                uncertainty.append({"population": period, "omitted_feature": omitted, "metric": metric,
                                    "date_clusters": len(unique), "bootstrap_draws": BOOTSTRAP_DRAWS,
                                    "point_delta_candidate_minus_champion": float(difference.mean()),
                                    "ci95_low": float(np.quantile(sampled, .025)), "ci95_high": float(np.quantile(sampled, .975)),
                                    "fraction_favoring_candidate": favor_candidate, "fraction_favoring_champion": favor_champion,
                                    "unadjusted_two_sided_bootstrap_p": p, "seed": BOOTSTRAP_SEED})
    uncertainty_frame = pd.DataFrame(uncertainty); sensitivity = []
    for (period, metric), group in uncertainty_frame.groupby(["population", "metric"], sort=False):
        adjusted = stage1.holm(group.unadjusted_two_sided_bootstrap_p.tolist())
        for (_, row), value in zip(group.iterrows(), adjusted):
            sensitivity.append({"population": period, "metric": metric, "omitted_feature": row.omitted_feature,
                                "family_size": len(group), "unadjusted_two_sided_bootstrap_p": row.unadjusted_two_sided_bootstrap_p,
                                "holm_adjusted_p": value, "holm_significant_0_05": value < .05,
                                "fraction_favoring_candidate": row.fraction_favoring_candidate,
                                "fraction_favoring_champion": row.fraction_favoring_champion,
                                "interpretation": "FWER sensitivity only; not sole replaceability interpretation"})
    write_csv(output_dir / "stage3_clustered_uncertainty.csv", uncertainty)
    write_csv(output_dir / "stage3_multiple_comparison_sensitivity.csv", sensitivity)

    # Formal classifications after all 22 independent candidates have been fit and scored.
    temporal_map = {row["feature"]: row for row in temporal_rows}
    classification_rows = []
    for omitted in features:
        agg = aggregate_compare[omitted]; temporal = temporal_map[omitted]["replaceability_temporal_status"]
        frozen_mae, frozen_crps = agg["stage1_frozen_mae_delta"], agg["stage1_frozen_crps_delta"]
        refit_mae, refit_crps = agg["stage3_refit_mae_delta"], agg["stage3_refit_crps_delta"]
        period_rows = [next(row for row in compare_rows if row["feature"] == omitted and row["population"] == period) for period in stage1.PERIODS]
        worse_periods = sum(row["stage3_refit_mae_delta"] > 0 and row["stage3_refit_crps_delta"] > 0 for row in period_rows)
        not_worse_periods = sum(row["stage3_refit_mae_delta"] <= MAE_MATERIAL and row["stage3_refit_crps_delta"] <= CRPS_MATERIAL for row in period_rows)
        mae_recovery, crps_recovery = recovery(frozen_mae, refit_mae), recovery(frozen_crps, refit_crps)
        stage2_label = stage2_labels[omitted]["stage2_primary_structural_label"]
        if (omitted == "strict_prior_total_run_factor" and stage2_label == "POSSIBLE_FOUNDATION" and
            refit_mae > MAE_MATERIAL and refit_crps > CRPS_MATERIAL and worse_periods >= 3 and temporal in ("STABLE", "MOSTLY_STABLE")):
            label, reason = "HIGH_CONFIDENCE_FOUNDATION_CANDIDATE", "meaningful frozen and refit loss, temporal coherence, and Stage-2 overlap survival"
        elif refit_mae > MAE_MATERIAL and refit_crps > CRPS_MATERIAL and worse_periods >= 3:
            label, reason = "UNIQUE_INFORMATION_CANDIDATE", "material unrecovered point and distribution loss in at least three periods"
        elif (frozen_mae > MAE_MATERIAL or frozen_crps > CRPS_MATERIAL) and abs(refit_mae) <= MAE_MATERIAL and abs(refit_crps) <= CRPS_MATERIAL:
            label, reason = "FULLY_REPLACEABLE", "meaningful frozen dependence disappears inside neutral refit thresholds"
        elif abs(frozen_mae) <= MAE_MATERIAL and abs(frozen_crps) <= CRPS_MATERIAL and abs(refit_mae) <= MAE_MATERIAL and abs(refit_crps) <= CRPS_MATERIAL:
            label, reason = "REDUNDANT_OR_NEAR_REDUNDANT", "both frozen and refit effects remain below material thresholds"
        elif refit_mae < -MAE_MATERIAL and refit_crps < -CRPS_MATERIAL and not_worse_periods >= 3:
            label, reason = "POTENTIALLY_REMOVABLE", "refit improves aggregate point and distribution scores without broad temporal harm"
        elif temporal in ("REGIME_DEPENDENT", "UNSTABLE"):
            label, reason = "REGIME_DEPENDENT", "refit direction or recovery changes materially by evaluation period"
        elif ((frozen_mae < -MAE_MATERIAL and refit_mae > MAE_MATERIAL) or
              (frozen_crps < -CRPS_MATERIAL and refit_crps > CRPS_MATERIAL) or stage2_label == "COMPENSATING_DEPENDENCE"):
            label, reason = "COMPENSATION_DEPENDENT", "frozen/refit reversal or Stage-2 compensation remains structurally relevant"
        elif ((mae_recovery is not None and 0.25 <= mae_recovery < .95) or
              (crps_recovery is not None and 0.25 <= crps_recovery < .95)):
            label, reason = "PARTIALLY_REPLACEABLE", "refit recovers a material but incomplete fraction of frozen loss"
        else:
            label, reason = "UNRESOLVED", "point, distribution, temporal, or recovery evidence does not satisfy one directional gate"
        classification_rows.append({"feature": omitted, "stage1_classification": stage1_class[omitted]["stage1_classification"],
                                    "stage2_structural_label": stage2_label, "stage3_primary_label": label,
                                    "aggregate_frozen_mae_delta": frozen_mae, "aggregate_refit_mae_delta": refit_mae,
                                    "aggregate_frozen_crps_delta": frozen_crps, "aggregate_refit_crps_delta": refit_crps,
                                    "mae_loss_recovery": mae_recovery, "crps_loss_recovery": crps_recovery,
                                    "worse_periods": worse_periods, "temporal_status": temporal,
                                    "classification_reason": reason, "irreducible_claimed": False})
    write_csv(output_dir / "stage3_feature_replaceability_classification.csv", classification_rows)
    class_map = {row["feature"]: row for row in classification_rows}; class_counts = Counter(row["stage3_primary_label"] for row in classification_rows)

    # Count/confidence and Stage-2 harmful-term reviews.
    count_rows = []
    for feature in ("home_starter_prior_starts", "away_starter_prior_starts", "park_history_depth"):
        absorbers = [row for row in absorption_rows if row["omitted_feature"] == feature][:5]
        for period in (*stage1.PERIODS, AGGREGATE):
            row = next(item for item in compare_rows if item["feature"] == feature and item["population"] == period)
            count_rows.append({**row, "candidate_identity": candidates[feature]["candidate_identity"],
                               "candidate_dispersion_alpha": candidates[feature]["dispersion_alpha"],
                               "temporal_status": temporal_map[feature]["replaceability_temporal_status"],
                               "stage3_primary_label": class_map[feature]["stage3_primary_label"],
                               "top_absorbers": "|".join(item["candidate_absorber"] for item in absorbers),
                               "upstream_support_role_preserved": True})
    write_csv(output_dir / "stage3_count_confidence_review.csv", count_rows)

    harmful_features = [feature for feature in features if stage2_labels[feature]["stage2_primary_structural_label"] == "POTENTIALLY_HARMFUL"]
    harmful_rows = []
    for feature in harmful_features:
        aggregate = aggregate_compare[feature]
        period_rows = [next(row for row in compare_rows if row["feature"] == feature and row["population"] == period) for period in stage1.PERIODS]
        better = sum(row["stage3_refit_mae_delta"] <= 0 and row["stage3_refit_crps_delta"] <= 0 for row in period_rows)
        worse = sum(row["stage3_refit_mae_delta"] > 0 and row["stage3_refit_crps_delta"] > 0 for row in period_rows)
        if aggregate["stage3_refit_mae_delta"] < -MAE_MATERIAL and aggregate["stage3_refit_crps_delta"] < -CRPS_MATERIAL and better >= 3:
            status = "SUPPORTED"
        elif aggregate["stage3_refit_mae_delta"] <= 0 and aggregate["stage3_refit_crps_delta"] <= 0 and better >= 2:
            status = "WEAKLY_SUPPORTED"
        elif worse >= 3: status = "REVERSED"
        elif better and worse: status = "MIXED"
        else: status = "NOT_SUPPORTED"
        for period in (*stage1.PERIODS, AGGREGATE):
            row = next(item for item in compare_rows if item["feature"] == feature and item["population"] == period)
            harmful_rows.append({"feature": feature, "population": period,
                                 "stage3_refit_mae_delta": row["stage3_refit_mae_delta"],
                                 "stage3_refit_crps_delta": row["stage3_refit_crps_delta"],
                                 "harmfulness_after_refit": status,
                                 "automatic_deletion_recommendation": False})
    write_csv(output_dir / "stage3_harmful_feature_recheck.csv", harmful_rows)

    strict = class_map["strict_prior_total_run_factor"]; strict_agg = aggregate_compare["strict_prior_total_run_factor"]
    strict_period = [next(row for row in compare_rows if row["feature"] == "strict_prior_total_run_factor" and row["population"] == period) for period in stage1.PERIODS]
    retained_ratio = max(strict_agg["stage3_refit_mae_delta"] / strict_agg["stage1_frozen_mae_delta"],
                         strict_agg["stage3_refit_crps_delta"] / strict_agg["stage1_frozen_crps_delta"])
    if temporal_map["strict_prior_total_run_factor"]["replaceability_temporal_status"] in ("REGIME_DEPENDENT", "UNSTABLE"):
        strict_replaceability = "MIXED"
    elif retained_ratio >= .75: strict_replaceability = "NOT_REPLACED"
    elif retained_ratio >= .25: strict_replaceability = "PARTIALLY_REPLACED"
    elif strict_agg["stage3_refit_mae_delta"] > 0 or strict_agg["stage3_refit_crps_delta"] > 0: strict_replaceability = "LARGELY_REPLACED"
    else: strict_replaceability = "FULLY_REPLACED"
    strict_absorbers = [row for row in absorption_rows if row["omitted_feature"] == "strict_prior_total_run_factor"][:5]
    strict_review = f"""# Strict-prior total-run-factor replaceability

`STRICT_PRIOR_FACTOR_REPLACEABILITY = {strict_replaceability}`

- Frozen removal: MAE {strict_agg['stage1_frozen_mae_delta']:+.6f}; CRPS {strict_agg['stage1_frozen_crps_delta']:+.6f}.
- Refit removal: MAE {strict_agg['stage3_refit_mae_delta']:+.6f}; CRPS {strict_agg['stage3_refit_crps_delta']:+.6f}.
- Temporal status: `{temporal_map['strict_prior_total_run_factor']['replaceability_temporal_status']}`; periods with joint MAE/CRPS degradation: {sum(row['stage3_refit_mae_delta'] > 0 and row['stage3_refit_crps_delta'] > 0 for row in strict_period)}/4.
- Stage-3 label: `{strict['stage3_primary_label']}`. This is not an irreducibility claim.
- Strongest descriptive absorbers: {', '.join(row['candidate_absorber'] for row in strict_absorbers)}.
- Dispersion: {artifact['dispersion_alpha']:.12f} -> {candidates['strict_prior_total_run_factor']['dispersion_alpha']:.12f}; intercept change {candidates['strict_prior_total_run_factor']['intercept'] - artifact['intercept']:+.12f}.
"""
    (output_dir / "stage3_strict_prior_factor_review.md").write_text(strict_review)

    foundation = [row["feature"] for row in classification_rows if row["stage3_primary_label"] == "HIGH_CONFIDENCE_FOUNDATION_CANDIDATE"]
    unique = [row["feature"] for row in classification_rows if row["stage3_primary_label"] == "UNIQUE_INFORMATION_CANDIDATE"]
    replaceable = [row["feature"] for row in classification_rows if row["stage3_primary_label"] in ("FULLY_REPLACEABLE", "PARTIALLY_REPLACEABLE")]
    removable = [row["feature"] for row in classification_rows if row["stage3_primary_label"] in ("REDUNDANT_OR_NEAR_REDUNDANT", "POTENTIALLY_REMOVABLE")]
    unresolved = [row["feature"] for row in classification_rows if row["stage3_primary_label"] in ("COMPENSATION_DEPENDENT", "REGIME_DEPENDENT", "UNRESOLVED")]
    decision = ("REDUCED_FOUNDATION_BUILD_JUSTIFIED" if foundation and len(replaceable) + len(removable) >= 5 else
                "FULL_CHAMPION_COMPLEXITY_STILL_JUSTIFIED" if len(foundation) + len(unique) >= 16 else "MORE_STRUCTURAL_WORK_REQUIRED")
    lower = max(1, len(foundation) + int(np.ceil(len(unique) / 2)))
    upper = min(22, len(foundation) + len(unique) + len([x for x in replaceable if class_map[x]["stage3_primary_label"] == "PARTIALLY_REPLACEABLE"]))
    plausible_range = f"{lower}–{upper} terms/concepts" if decision == "REDUCED_FOUNDATION_BUILD_JUSTIFIED" else "NOT_YET_SUPPORTED"
    (output_dir / "stage3_foundation_size_preview.md").write_text(
        "# Stage-3 foundation-size preview\n\n" + f"`{decision}`\n\n" +
        f"- High-confidence foundation candidates: {', '.join(foundation) if foundation else 'none'}\n"
        f"- Unique-information candidates: {', '.join(unique) if unique else 'none'}\n"
        f"- Fully/partially replaceable: {', '.join(replaceable) if replaceable else 'none'}\n"
        f"- Redundant/removable: {', '.join(removable) if removable else 'none'}\n"
        f"- Unresolved/compensation/regime-dependent: {', '.join(unresolved) if unresolved else 'none'}\n"
        f"- Plausible foundation range: `{plausible_range}`\n\nNo reduced model was built.\n"
    )
    (output_dir / "stage3_nhl_transfer_lessons.md").write_text(
        "# Conceptual NHL transfer lessons\n\nNo NHL asset was inspected. Frozen dependence and information uniqueness must be separated: refitting can transfer signal into correlated terms, while some frozen effects survive redistribution. Plausible features should earn admission through incremental out-of-time evidence; support/count variables should remain upstream absent direct scoring proof; and single-removal refits should precede any reduced-core or composite build. A composite also needs an explicit equivalence contract rather than merely similar aggregate performance.\n"
    )

    largest_worse = max(classification_rows, key=lambda row: row["aggregate_refit_mae_delta"])
    largest_better = min(classification_rows, key=lambda row: row["aggregate_refit_mae_delta"])
    disappeared = [row["feature"] for row in classification_rows
                   if row["stage3_primary_label"] in ("FULLY_REPLACEABLE", "REDUNDANT_OR_NEAR_REDUNDANT") and
                   (row["aggregate_frozen_mae_delta"] > MAE_MATERIAL or row["aggregate_frozen_crps_delta"] > CRPS_MATERIAL or
                    row["stage1_classification"] in ("STRONGLY_REQUIRED_IN_FROZEN_CHAMPION", "MODERATELY_REQUIRED_IN_FROZEN_CHAMPION"))]
    survived = foundation + unique
    harmful_status = {row["feature"]: row["harmfulness_after_refit"] for row in harmful_rows if row["population"] == AGGREGATE}
    report = f"""# MLB Totals RAW champion single-feature replaceability refit v1

## Result

`RAW_REFIT_REPRODUCTION = {reproduction['RAW_REFIT_REPRODUCTION']}` on {len(training):,} development games ({training.game_date.min().date()} through {training.game_date.max().date()}). All 22 predeclared RAW-minus-one candidates were fit exactly once on those rows; evaluation data was never used for fitting, scaling, dispersion selection, tuning, or model choice.

- Largest aggregate MAE degradation after refit: `{largest_worse['feature']}` {largest_worse['aggregate_refit_mae_delta']:+.6f}.
- Largest aggregate MAE improvement after refit: `{largest_better['feature']}` {largest_better['aggregate_refit_mae_delta']:+.6f}.
- Frozen importance disappearing after refit: {', '.join(disappeared) if disappeared else 'none under the formal thresholds'}.
- Importance surviving refit: {', '.join(survived) if survived else 'none'}.
- `STRICT_PRIOR_FACTOR_REPLACEABILITY = {strict_replaceability}`; Stage-3 label `{strict['stage3_primary_label']}`.
- Stage-2 harmful-term recheck: `{json.dumps(harmful_status, sort_keys=True)}`.
- Stage-3 classification counts: `{json.dumps(dict(sorted(class_counts.items())), sort_keys=True)}`.

`{decision}`. `PLAUSIBLE_FOUNDATION_RANGE = {plausible_range}`. This package is a replaceability map only: no greedy elimination, group removal, reduced model, tuning, RAW/C change, or production modification occurred.
"""
    (output_dir / "concise_mlb_totals_raw_champion_single_feature_replaceability_refit_v1.md").write_text(report)

    outputs = sorted(path for path in output_dir.rglob("*") if path.is_file() and path.name != "reproducibility_hashes.json")
    hashes = {"task_id": TASK_ID, "created_at_utc": datetime.now(timezone.utc).isoformat(),
              "model_artifact_sha256": sha256(stage1.raw.CONFIG), "stage1_package_hash_manifest_sha256": sha256(STAGE1_OUTPUT / "reproducibility_hashes.json"),
              "stage2_package_hash_manifest_sha256": sha256(STAGE2_OUTPUT / "reproducibility_hashes.json"),
              "stage2_manifest_sha256": sha256(STAGE2_OUTPUT / "stage2_joint_ablation_manifest.csv"),
              "stage3_refit_manifest_sha256": sha256(output_dir / "stage3_22_refit_manifest.csv"),
              "analysis_utility_sha256": sha256(Path(__file__)),
              "outputs": {str(path.relative_to(output_dir)): sha256(path) for path in outputs},
              "bootstrap_seed": BOOTSTRAP_SEED, "bootstrap_draws": BOOTSTRAP_DRAWS,
              "candidate_fit_count": 22, "evaluation_rows_used_for_fit_or_selection": 0}
    write_json(output_dir / "reproducibility_hashes.json", hashes)
    return {"status": "PASS", "raw_refit_reproduction": reproduction["RAW_REFIT_REPRODUCTION"],
            "training_rows": len(training), "training_window": [str(training.game_date.min().date()), str(training.game_date.max().date())],
            "refits_completed": len(candidates), "largest_unrecovered_mae": largest_worse["feature"],
            "largest_improvement_mae": largest_better["feature"], "importance_disappeared": disappeared,
            "importance_survived": survived, "strict_prior_replaceability": strict_replaceability,
            "classification_counts": dict(class_counts), "foundation": foundation, "unique": unique,
            "replaceable": replaceable, "removable": removable, "unresolved": unresolved,
            "decision": decision, "plausible_foundation_range": plausible_range, "output": str(output_dir)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

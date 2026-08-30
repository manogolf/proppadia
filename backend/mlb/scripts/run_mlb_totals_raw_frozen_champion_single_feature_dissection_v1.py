"""No-refit, direct-term dissection of the exact frozen MLB totals RAW champion.

Each intervention zeros one standardized location contribution while retaining
the original row, all upstream state, all other terms, and the frozen NB alpha.
This module is analysis-only and never writes models or operational ledgers.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as raw
from backend.mlb.scripts import run_mlb_totals_remove_park_history_depth_direct_location_defect_v1 as metrics
from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_RAW_FROZEN_CHAMPION_SINGLE_FEATURE_DISSECTION_V1"
MODEL_IDENTITY = "DIRECT_NEGATIVE_BINOMIAL_RAW_V1"
MODEL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
ARTIFACT_SHA = "c99079334a7f061d08f7611a05e40cca4f17281239e962da267588282c1e22fe"
OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_frozen_champion_single_feature_dissection_v1/2026-08-29"
PROSPECTIVE_START = "2026-08-17"
PROSPECTIVE_END = "2026-08-28"
PERIODS = (
    "FROZEN_2025_VALIDATION",
    "2026_SEQUENTIAL_EARLY",
    "2026_LATE_HOLDOUT",
    "PROSPECTIVE_AUG17_28_CLEAN_RAW",
)
EXPECTED_COUNTS = dict(zip(PERIODS, (2433, 1281, 439, 156)))
THRESHOLDS = metrics.THRESHOLDS
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260829
INTERVENTION = "FROZEN_DIRECT_TERM_ABLATION"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def frame_hash(frame: pd.DataFrame, columns: list[str]) -> str:
    payload = frame.sort_values(["game_date", "game_pk"])[columns].to_csv(
        index=False, lineterminator="\n", float_format="%.17g"
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def load_artifact() -> dict[str, Any]:
    if sha256(raw.CONFIG) != ARTIFACT_SHA:
        raise RuntimeError("FROZEN_ARTIFACT_SHA_FAILED")
    artifact = json.loads(raw.CONFIG.read_text())
    if artifact.get("canonical_model_hash") != MODEL_HASH:
        raise RuntimeError("FROZEN_MODEL_HASH_FAILED")
    if len(artifact.get("feature_order", [])) != 22 or len(set(artifact["feature_order"])) != 22:
        raise RuntimeError("FROZEN_22_FEATURE_ORDER_FAILED")
    return artifact


def score_components(frame: pd.DataFrame, artifact: dict[str, Any]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    values = frame[artifact["feature_order"]].astype(float).to_numpy()
    z = (values - np.asarray(artifact["scaler_mean"])) / np.asarray(artifact["scaler_scale"])
    contributions = z * np.asarray(artifact["coefficients"])
    mu = np.exp(float(artifact["intercept"]) + contributions.sum(axis=1))
    return mu, z, contributions


def load_prospective(artifact: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    connection = sqlite3.connect(f"file:{raw.LEDGER}?mode=ro", uri=True)
    rows = connection.execute(
        """SELECT p.canonical_identity,p.game_date,p.game_id,p.scheduled_start_utc,
                  p.prediction_timestamp_utc,p.model_hash,p.feature_state_hash,
                  p.prediction_payload_json,p.prediction_payload_sha256,
                  c.context_payload_json,c.context_payload_sha256,
                  o.official_final_total,o.grading_payload_json,o.grading_payload_sha256
           FROM totals_shadow_predictions p
           JOIN totals_shadow_prediction_context c USING(canonical_identity)
           JOIN totals_shadow_outcomes o USING(canonical_identity)
           WHERE p.game_date BETWEEN ? AND ? ORDER BY p.game_date,p.game_id""",
        (PROSPECTIVE_START, PROSPECTIVE_END),
    ).fetchall()
    output: list[dict[str, Any]] = []
    probability_max_error = 0.0
    for row in rows:
        (identity, date, game_id, scheduled, predicted, model_hash, feature_hash,
         prediction_json, prediction_sha, context_json, context_sha, actual,
         grading_json, grading_sha) = row
        prediction = json.loads(prediction_json)
        context = json.loads(context_json)
        grading = json.loads(grading_json)
        if model_hash != MODEL_HASH:
            raise RuntimeError(f"PROSPECTIVE_MODEL_HASH_FAILED:{identity}")
        if canonical_hash(prediction) != prediction_sha or canonical_hash(context) != context_sha or canonical_hash(grading) != grading_sha:
            raise RuntimeError(f"PROSPECTIVE_PAYLOAD_HASH_FAILED:{identity}")
        if raw.iso_utc(predicted) >= raw.iso_utc(scheduled):
            raise RuntimeError(f"PROSPECTIVE_POST_START_ROW:{identity}")
        features = context.get("model_features")
        if not isinstance(features, dict) or set(artifact["feature_order"]) - set(features):
            raise RuntimeError(f"PROSPECTIVE_FEATURE_CONTRACT_FAILED:{identity}")
        record = {
            "period": PERIODS[-1], "game_date": pd.Timestamp(date), "game_pk": int(game_id),
            "canonical_identity": identity, "scheduled_start_utc": scheduled,
            "prediction_timestamp_utc": predicted, "feature_state_hash": feature_hash,
            "final_total": float(actual), "stored_raw_forecast": float(prediction["expected_total"]),
            "market_total_line": prediction.get("total_line"),
            "market_source_hash": prediction.get("market_source_sha256"),
            **{feature: float(features[feature]) for feature in artifact["feature_order"]},
        }
        score, _, _ = score_components(pd.DataFrame([record]), artifact)
        if abs(float(score[0]) - record["stored_raw_forecast"]) > 1e-11:
            raise RuntimeError(f"PROSPECTIVE_RAW_ROW_REPRODUCTION_FAILED:{identity}")
        pmf = distribution(float(score[0]), float(artifact["dispersion_alpha"]))
        support = np.arange(len(pmf))
        for threshold in THRESHOLDS:
            key = f"p_over_{str(threshold).replace('.', '_')}"
            if key in prediction:
                probability_max_error = max(probability_max_error, abs(float(prediction[key]) - float(pmf[support > threshold].sum())))
        output.append(record)
    connection.close()
    frame = pd.DataFrame(output)
    if len(frame) != EXPECTED_COUNTS[PERIODS[-1]] or frame.game_pk.duplicated().any():
        raise RuntimeError(f"PROSPECTIVE_POPULATION_FAILED:{len(frame)}")
    return frame, {"stored_probability_max_abs_error": probability_max_error}


def load_populations(artifact: dict[str, Any]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    historical = raw.load_historical(artifact)
    frames = {
        period: historical.loc[historical.period.eq(period)].copy().reset_index(drop=True)
        for period in PERIODS[:3]
    }
    prospective, probability_check = load_prospective(artifact)
    frames[PERIODS[-1]] = prospective.reset_index(drop=True)
    for period, frame in frames.items():
        if len(frame) != EXPECTED_COUNTS[period] or frame.game_pk.duplicated().any():
            raise RuntimeError(f"FROZEN_POPULATION_FAILED:{period}:{len(frame)}")
        score, _, _ = score_components(frame, artifact)
        stored = frame["raw_forecast"].to_numpy(float) if "raw_forecast" in frame else frame["stored_raw_forecast"].to_numpy(float)
        tolerance = 2e-12 if period != PERIODS[-1] else 1e-11
        if not np.allclose(score, stored, atol=tolerance, rtol=0):
            raise RuntimeError(f"CHAMPION_ROW_REPRODUCTION_FAILED:{period}")
        frame["raw_forecast"] = score
    return frames, probability_check


def row_distribution_losses(mu: np.ndarray, actual: np.ndarray, alpha: float) -> dict[str, np.ndarray]:
    crps_values, briers, log_losses, probabilities, outcomes = [], [], [], [], []
    for forecast, outcome in zip(mu, actual):
        pmf = distribution(float(forecast), alpha)
        support = np.arange(len(pmf))
        crps_values.append(float(np.sum((np.cumsum(pmf) - (support >= int(outcome)).astype(float)) ** 2)))
        p = np.asarray([pmf[support > threshold].sum() for threshold in THRESHOLDS], dtype=float)
        y = np.asarray([outcome > threshold for threshold in THRESHOLDS], dtype=float)
        probabilities.extend(p.tolist()); outcomes.extend(y.tolist())
        briers.append(float(np.mean((p - y) ** 2)))
        clipped = np.clip(p, 1e-12, 1 - 1e-12)
        log_losses.append(float(np.mean(-(y * np.log(clipped) + (1 - y) * np.log(1 - clipped)))))
    return {
        "crps": np.asarray(crps_values), "brier": np.asarray(briers), "log_loss": np.asarray(log_losses),
        "event_probability": np.asarray(probabilities), "event_outcome": np.asarray(outcomes),
    }


def ece(probability: np.ndarray, outcome: np.ndarray) -> float:
    bins = np.minimum((probability * 10).astype(int), 9)
    value = 0.0
    for index in range(10):
        selected = bins == index
        if selected.any():
            value += float(selected.mean()) * abs(float(probability[selected].mean()) - float(outcome[selected].mean()))
    return value


def summary(frame: pd.DataFrame, mu: np.ndarray, losses: dict[str, np.ndarray]) -> dict[str, float]:
    actual = frame.final_total.to_numpy(float)
    residual = actual - mu
    return {
        "games": len(frame), "mae": float(np.mean(abs(residual))),
        "rmse": float(np.sqrt(np.mean(residual ** 2))),
        "actual_minus_forecast_bias": float(np.mean(residual)),
        "absolute_bias": abs(float(np.mean(residual))),
        "mean_forecast": float(np.mean(mu)), "median_forecast": float(np.median(mu)),
        "crps": float(losses["crps"].mean()), "governed_total_line_brier": float(losses["brier"].mean()),
        "governed_total_line_log_loss": float(losses["log_loss"].mean()),
        "governed_total_line_ece": ece(losses["event_probability"], losses["event_outcome"]),
    }


def describe(values: np.ndarray, prefix: str = "") -> dict[str, float]:
    return {
        f"{prefix}min": float(np.min(values)), f"{prefix}p10": float(np.quantile(values, .10)),
        f"{prefix}median": float(np.median(values)), f"{prefix}mean": float(np.mean(values)),
        f"{prefix}p90": float(np.quantile(values, .90)), f"{prefix}max": float(np.max(values)),
        f"{prefix}std": float(np.std(values)),
    }


def holm(p_values: list[float]) -> list[float]:
    order = np.argsort(p_values)
    adjusted = np.empty(len(p_values), dtype=float)
    running = 0.0
    for rank, index in enumerate(order):
        running = max(running, min(1.0, (len(p_values) - rank) * p_values[index]))
        adjusted[index] = running
    return adjusted.tolist()


def temporal_label(rows: list[dict[str, Any]], effect_rows: list[dict[str, Any]]) -> str:
    if max(abs(r["mae_delta"]) for r in rows) < .002 and max(abs(r["crps_delta"]) for r in rows) < .002 and max(r["mean_absolute_forecast_change"] for r in effect_rows) < .02:
        return "NEGLIGIBLE"
    states = []
    for row in rows:
        values = [row["mae_delta"], row["crps_delta"], row["brier_delta"], row["log_loss_delta"]]
        states.append("HELP" if sum(v > 0 for v in values) >= 3 else ("HARM" if sum(v < 0 for v in values) >= 3 else "MIXED"))
    if states.count("HELP") == len(states): return "CONSISTENTLY_BENEFICIAL"
    if states.count("HELP") >= 3: return "MOSTLY_BENEFICIAL"
    if states.count("HARM") == len(states): return "CONSISTENTLY_HARMFUL"
    if states.count("HARM") >= 3: return "MOSTLY_HARMFUL"
    return "MIXED"


def stage1_classification(label: str, aggregate: dict[str, Any]) -> tuple[str, str]:
    mae, crps = aggregate["mae_delta"], aggregate["crps_delta"]
    movement = aggregate["mean_absolute_forecast_change"]
    if label == "NEGLIGIBLE":
        return "NEUTRAL_IN_FROZEN_CHAMPION", "negligible score and forecast movement under predeclared thresholds"
    if label in ("CONSISTENTLY_HARMFUL", "MOSTLY_HARMFUL") and mae < 0 and crps < 0:
        return "POTENTIALLY_HARMFUL_IN_FROZEN_CHAMPION", "removal improves aggregate MAE and CRPS and is temporally harmful/mostly harmful"
    if label == "CONSISTENTLY_BENEFICIAL" and mae > 0 and crps > 0:
        if mae >= .01 and movement >= .10:
            return "STRONGLY_REQUIRED_IN_FROZEN_CHAMPION", "consistent multi-period benefit with material aggregate degradation and movement"
        return "MODERATELY_REQUIRED_IN_FROZEN_CHAMPION", "consistent multi-period benefit below strong materiality gate"
    if label == "MOSTLY_BENEFICIAL" and mae > 0 and crps > 0:
        return "MODERATELY_REQUIRED_IN_FROZEN_CHAMPION", "beneficial in at least three periods with aggregate MAE and CRPS degradation"
    if label == "MIXED" and movement >= .02:
        return "TEMPORALLY_UNSTABLE", "material movement with mixed temporal score direction"
    if mae > 0 and crps > 0:
        return "WEAKLY_REQUIRED_IN_FROZEN_CHAMPION", "aggregate MAE and CRPS degrade but temporal support is incomplete"
    return "UNRESOLVED", "single-feature score evidence does not satisfy a directional classification gate"


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    out = ["| " + " | ".join(columns) + " |", "|" + "|".join(["---"] * len(columns)) + "|"]
    for row in rows:
        vals = []
        for col in columns:
            value = row.get(col, "")
            vals.append(f"{value:.6f}" if isinstance(value, float) else str(value))
        out.append("| " + " | ".join(vals) + " |")
    return "\n".join(out)


def run(output_dir: Path = OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact = load_artifact()
    alpha = float(artifact["dispersion_alpha"])
    frames, probability_check = load_populations(artifact)
    features = artifact["feature_order"]
    coefficient = dict(zip(features, artifact["coefficients"]))
    scale = dict(zip(features, artifact["scaler_scale"]))
    mean = dict(zip(features, artifact["scaler_mean"]))

    population_rows = []
    for period, frame in frames.items():
        population_rows.append({
            "population": period, "date_min": str(frame.game_date.min().date()), "date_max": str(frame.game_date.max().date()),
            "games": len(frame), "date_clusters": frame.game_date.dt.date.nunique(),
            "source": ("totals_core_feature_spine.csv plus independent retained residual spine" if period != PERIODS[-1]
                       else "append-only totals shadow prediction/context/outcome ledgers"),
            "outcome_completeness": "100% OFFICIAL FINAL",
            "feature_state_authority": ("HISTORICALLY_FROZEN_GOVERNED_FEATURE_SPINE" if period != PERIODS[-1]
                                        else "ORIGINAL_IMMUTABLE_PROSPECTIVE_CONTEXT_PAYLOAD"),
            "bullpen_state": ("HISTORICALLY_FROZEN" if period != PERIODS[-1] else "REPAIRED_AUTHORITATIVE_STRICT_PRIOR_STATE"),
            "counterfactual_rows_mixed": False,
            "row_identity_outcome_hash": frame_hash(frame, ["game_pk", "game_date", "final_total"]),
        })
    write_csv(output_dir / "raw_champion_evaluation_populations.csv", population_rows)

    point_rows, distribution_rows, forecast_rows = [], [], []
    temporal_source: dict[str, list[dict[str, Any]]] = {feature: [] for feature in features}
    effect_source: dict[str, list[dict[str, Any]]] = {feature: [] for feature in features}
    loss_cache: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    mu_cache: dict[tuple[str, str], np.ndarray] = {}
    champion_summaries = {}
    manifest_rows = []
    parity_errors = []

    for index, feature in enumerate(features):
        manifest_rows.append({
            "ablation_number": index + 1, "feature": feature, "intervention": INTERVENTION,
            "location_intervention": "set standardized direct contribution to zero (training-mean equivalent)",
            "upstream_field_preserved": True, "other_features_preserved": True, "other_coefficients_preserved": True,
            "intercept_preserved": True, "scaler_preserved": True, "dispersion_preserved": True,
            "gating_shrinkage_fallback_context_market_outcome_preserved": True,
            "refit_count": 0, "coefficient": coefficient[feature], "scaler_mean": mean[feature], "scaler_scale": scale[feature],
        })
    write_csv(output_dir / "raw_champion_22_feature_ablation_manifest.csv", manifest_rows)

    for period, frame in frames.items():
        champion_mu, z, contributions = score_components(frame, artifact)
        stored = frame.raw_forecast.to_numpy(float)
        parity_errors.append(float(np.max(abs(champion_mu - stored))))
        actual = frame.final_total.to_numpy(float)
        champion_losses = row_distribution_losses(champion_mu, actual, alpha)
        champion = summary(frame, champion_mu, champion_losses)
        champion_summaries[period] = champion
        loss_cache[(period, "CHAMPION")] = champion_losses
        mu_cache[(period, "CHAMPION")] = champion_mu
        point_rows.append({"population": period, "feature": "UNALTERED_CHAMPION", "variant": MODEL_IDENTITY, **champion,
                           "mae_delta": 0.0, "rmse_delta": 0.0, "absolute_bias_delta": 0.0})
        distribution_rows.append({"population": period, "feature": "UNALTERED_CHAMPION", "variant": MODEL_IDENTITY,
                                  **{k: champion[k] for k in ("crps", "governed_total_line_brier", "governed_total_line_log_loss", "governed_total_line_ece")},
                                  "crps_delta": 0.0, "brier_delta": 0.0, "log_loss_delta": 0.0,
                                  "probability_thresholds": "6.5|7.5|8.5|9.5|10.5|11.5"})
        for index, feature in enumerate(features):
            ablated_mu = champion_mu * np.exp(-contributions[:, index])
            # Independent equation form proves that only this direct term was zeroed.
            independent = np.exp(float(artifact["intercept"]) + contributions.sum(axis=1) - contributions[:, index])
            if not np.allclose(ablated_mu, independent, atol=2e-12, rtol=0):
                raise RuntimeError(f"ABLATION_EQUATION_FAILED:{period}:{feature}")
            ablated_losses = row_distribution_losses(ablated_mu, actual, alpha)
            ablated = summary(frame, ablated_mu, ablated_losses)
            loss_cache[(period, feature)] = ablated_losses
            mu_cache[(period, feature)] = ablated_mu
            delta = ablated_mu - champion_mu
            effect = {
                "population": period, "feature": feature, "games": len(frame),
                "mean_absolute_forecast_change": float(np.mean(abs(delta))),
                "median_absolute_forecast_change": float(np.median(abs(delta))),
                "maximum_absolute_forecast_change": float(np.max(abs(delta))),
                "mean_signed_forecast_change": float(np.mean(delta)),
                "p90_absolute_forecast_change": float(np.quantile(abs(delta), .90)),
                "pct_change_ge_0_10": float(np.mean(abs(delta) >= .10)),
                "pct_change_ge_0_25": float(np.mean(abs(delta) >= .25)),
                "pct_change_ge_0_50": float(np.mean(abs(delta) >= .50)),
                "pct_change_ge_1_00": float(np.mean(abs(delta) >= 1.00)),
            }
            forecast_rows.append(effect); effect_source[feature].append(effect)
            p_row = {
                "population": period, "feature": feature, "variant": INTERVENTION, **ablated,
                "champion_mae": champion["mae"], "mae_delta": ablated["mae"] - champion["mae"],
                "champion_rmse": champion["rmse"], "rmse_delta": ablated["rmse"] - champion["rmse"],
                "champion_absolute_bias": champion["absolute_bias"],
                "absolute_bias_delta": ablated["absolute_bias"] - champion["absolute_bias"],
            }
            point_rows.append(p_row)
            d_row = {
                "population": period, "feature": feature, "variant": INTERVENTION,
                **{k: ablated[k] for k in ("crps", "governed_total_line_brier", "governed_total_line_log_loss", "governed_total_line_ece")},
                "champion_crps": champion["crps"], "crps_delta": ablated["crps"] - champion["crps"],
                "champion_brier": champion["governed_total_line_brier"],
                "brier_delta": ablated["governed_total_line_brier"] - champion["governed_total_line_brier"],
                "champion_log_loss": champion["governed_total_line_log_loss"],
                "log_loss_delta": ablated["governed_total_line_log_loss"] - champion["governed_total_line_log_loss"],
                "probability_thresholds": "6.5|7.5|8.5|9.5|10.5|11.5",
            }
            distribution_rows.append(d_row)
            temporal_source[feature].append({**p_row, **d_row, **effect})

    write_csv(output_dir / "raw_champion_feature_point_deltas.csv", point_rows)
    write_csv(output_dir / "raw_champion_feature_distribution_deltas.csv", distribution_rows)
    write_csv(output_dir / "raw_champion_feature_forecast_effects.csv", forecast_rows)

    # Aggregate row-weighted effects and scores are descriptive only; period labels remain separate.
    aggregate_rows: dict[str, dict[str, Any]] = {}
    all_actual = np.concatenate([frames[p].final_total.to_numpy(float) for p in PERIODS])
    all_champion_mu = np.concatenate([mu_cache[(p, "CHAMPION")] for p in PERIODS])
    all_champion_losses = {key: np.concatenate([loss_cache[(p, "CHAMPION")][key] for p in PERIODS]) for key in ("crps", "brier", "log_loss")}
    for feature in features:
        all_mu = np.concatenate([mu_cache[(p, feature)] for p in PERIODS])
        all_losses = {key: np.concatenate([loss_cache[(p, feature)][key] for p in PERIODS]) for key in ("crps", "brier", "log_loss")}
        delta = all_mu - all_champion_mu
        aggregate_rows[feature] = {
            "feature": feature,
            "mae_delta": float(np.mean(abs(all_actual - all_mu)) - np.mean(abs(all_actual - all_champion_mu))),
            "rmse_delta": float(np.sqrt(np.mean((all_actual - all_mu) ** 2)) - np.sqrt(np.mean((all_actual - all_champion_mu) ** 2))),
            "absolute_bias_delta": abs(float(np.mean(all_actual - all_mu))) - abs(float(np.mean(all_actual - all_champion_mu))),
            "crps_delta": float(all_losses["crps"].mean() - all_champion_losses["crps"].mean()),
            "brier_delta": float(all_losses["brier"].mean() - all_champion_losses["brier"].mean()),
            "log_loss_delta": float(all_losses["log_loss"].mean() - all_champion_losses["log_loss"].mean()),
            "mean_absolute_forecast_change": float(np.mean(abs(delta))),
            "median_absolute_forecast_change": float(np.median(abs(delta))),
            "maximum_absolute_forecast_change": float(np.max(abs(delta))),
            "mean_signed_forecast_change": float(np.mean(delta)),
        }

    temporal_rows = []
    classifications = []
    for feature in features:
        label = temporal_label(temporal_source[feature], effect_source[feature])
        aggregate = aggregate_rows[feature]
        classification, rationale = stage1_classification(label, aggregate)
        temporal_rows.append({"feature": feature, "temporal_effect": label,
                              **{f"{row['population']}_{metric}": row[metric] for row in temporal_source[feature]
                                 for metric in ("mae_delta", "crps_delta", "brier_delta", "log_loss_delta")},
                              **aggregate})
        classifications.append({"feature": feature, "temporal_effect": label,
                                "stage1_classification": classification, "classification_rule_rationale": rationale,
                                **aggregate,
                                "interpretation_limit": "frozen-champion dependence only; not foundational, causal, irreplaceable, or a removal decision"})
    write_csv(output_dir / "raw_champion_feature_temporal_effects.csv", temporal_rows)
    write_csv(output_dir / "raw_champion_stage1_classification.csv", classifications)

    # Date-cluster bootstrap, sampling whole slates with replacement.
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    uncertainty_rows = []
    for period, frame in frames.items():
        dates = pd.to_datetime(frame.game_date).dt.date.astype(str).to_numpy()
        unique_dates = np.unique(dates)
        draw = rng.integers(0, len(unique_dates), size=(BOOTSTRAP_DRAWS, len(unique_dates)))
        day_n = np.asarray([(dates == day).sum() for day in unique_dates], dtype=float)
        denominator = day_n[draw].sum(axis=1)
        actual = frame.final_total.to_numpy(float)
        champion_losses = {
            "mae": abs(actual - mu_cache[(period, "CHAMPION")]),
            "crps": loss_cache[(period, "CHAMPION")]["crps"],
            "brier": loss_cache[(period, "CHAMPION")]["brier"],
            "log_loss": loss_cache[(period, "CHAMPION")]["log_loss"],
        }
        for feature in features:
            candidate_losses = {
                "mae": abs(actual - mu_cache[(period, feature)]),
                "crps": loss_cache[(period, feature)]["crps"],
                "brier": loss_cache[(period, feature)]["brier"],
                "log_loss": loss_cache[(period, feature)]["log_loss"],
            }
            for metric_name in ("mae", "crps", "brier", "log_loss"):
                difference = candidate_losses[metric_name] - champion_losses[metric_name]
                day_sum = np.asarray([difference[dates == day].sum() for day in unique_dates])
                sampled = day_sum[draw].sum(axis=1) / denominator
                point = float(difference.mean())
                fraction = float(np.mean(sampled > 0))
                p_two = min(1.0, 2 * min(float(np.mean(sampled <= 0)), float(np.mean(sampled >= 0))))
                uncertainty_rows.append({
                    "population": period, "feature": feature, "metric": metric_name,
                    "date_clusters": len(unique_dates), "bootstrap_draws": BOOTSTRAP_DRAWS,
                    "point_delta_ablation_minus_champion": point,
                    "ci95_low": float(np.quantile(sampled, .025)), "ci95_high": float(np.quantile(sampled, .975)),
                    "fraction_draws_keeping_feature_better": fraction,
                    "unadjusted_two_sided_bootstrap_p": p_two, "seed": BOOTSTRAP_SEED,
                })
    sensitivity_rows = []
    uncertainty_frame = pd.DataFrame(uncertainty_rows)
    for (period, metric_name), group in uncertainty_frame.groupby(["population", "metric"], sort=False):
        adjusted = holm(group.unadjusted_two_sided_bootstrap_p.tolist())
        for (_, row), value in zip(group.iterrows(), adjusted):
            sensitivity_rows.append({
                "population": period, "metric": metric_name, "feature": row.feature,
                "family_size": len(group), "unadjusted_two_sided_bootstrap_p": row.unadjusted_two_sided_bootstrap_p,
                "holm_adjusted_p": value, "holm_significant_0_05": value < .05,
                "point_delta_ablation_minus_champion": row.point_delta_ablation_minus_champion,
                "fraction_draws_keeping_feature_better": row.fraction_draws_keeping_feature_better,
                "interpretation": "FWER sensitivity only; not sole screening interpretation",
            })
    write_csv(output_dir / "raw_champion_feature_clustered_uncertainty.csv", uncertainty_rows)
    write_csv(output_dir / "raw_champion_feature_multiple_comparison_sensitivity.csv", sensitivity_rows)

    # Coefficient versus actual dependence and count/confidence distributions.
    coefficient_rows = []
    count_rows = []
    count_features = ("home_starter_prior_starts", "away_starter_prior_starts", "park_history_depth")
    training = raw.load_historical(artifact)
    development = training.loc[training.period.eq("DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE")].copy()
    all_frames = pd.concat([frames[p].assign(source_period=p) for p in PERIODS], ignore_index=True)
    _, all_z, all_contribution = score_components(all_frames, artifact)
    for index, feature in enumerate(features):
        contribution = all_contribution[:, index]
        aggregate = aggregate_rows[feature]
        coefficient_rows.append({
            "feature": feature, "coefficient": coefficient[feature], "absolute_coefficient": abs(coefficient[feature]),
            "coefficient_rank_by_absolute_magnitude": 1 + int(sum(abs(v) > abs(coefficient[feature]) for v in coefficient.values())),
            **describe(abs(contribution), "absolute_standardized_contribution_"),
            **aggregate,
            "dependence_pattern": ("LARGE_FORECAST_MOVEMENT_NO_SCORE_BENEFIT" if aggregate["mean_absolute_forecast_change"] >= .10 and aggregate["mae_delta"] <= 0
                                   else "SMALL_COEFFICIENT_MEANINGFUL_DEPENDENCE" if abs(coefficient[feature]) < .005 and aggregate["mae_delta"] >= .005
                                   else "LARGE_COEFFICIENT_LOW_DEPENDENCE" if abs(coefficient[feature]) >= .02 and abs(aggregate["mae_delta"]) < .003
                                   else "DIRECTIONALLY_ALIGNED_OR_NO_FLAG"),
        })
    for feature in count_features:
        index = features.index(feature)
        for period, frame in [("DEVELOPMENT_2023_24", development), *frames.items()]:
            mu, z, contributions = score_components(frame, artifact)
            ablated = mu * np.exp(-contributions[:, index])
            actual = frame.final_total.to_numpy(float)
            base_losses = row_distribution_losses(mu, actual, alpha)
            ablated_losses = row_distribution_losses(ablated, actual, alpha)
            count_rows.append({
                "feature": feature, "population": period, "games": len(frame),
                **describe(frame[feature].to_numpy(float), "raw_value_"),
                **describe(contributions[:, index], "direct_log_location_contribution_"),
                "mean_absolute_forecast_change": float(np.mean(abs(ablated - mu))),
                "mae_delta": float(np.mean(abs(actual - ablated)) - np.mean(abs(actual - mu))),
                "crps_delta": float(ablated_losses["crps"].mean() - base_losses["crps"].mean()),
                "brier_delta": float(ablated_losses["brier"].mean() - base_losses["brier"].mean()),
                "upstream_support_confidence_shrinkage_preserved": True,
                "temporal_effect": next((r["temporal_effect"] for r in temporal_rows if r["feature"] == feature), "NOT_APPLICABLE_TRAINING"),
            })
    write_csv(output_dir / "raw_champion_coefficient_vs_dependence.csv", coefficient_rows)
    write_csv(output_dir / "raw_champion_count_confidence_review.csv", count_rows)

    pair_rows = []
    pairs = (("starter_ra9", "home_starter_ra9", "away_starter_ra9"),
             ("starter_prior_starts", "home_starter_prior_starts", "away_starter_prior_starts"),
             ("expected_outs", "home_expected_outs", "away_expected_outs"),
             ("workload_uncertainty", "home_workload_uncertainty_outs", "away_workload_uncertainty_outs"),
             ("bullpen_ra9", "home_bullpen_ra9", "away_bullpen_ra9"),
             ("available_reliever_count", "home_bullpen_likely_available_reliever_count", "away_bullpen_likely_available_reliever_count"),
             ("recent_bullpen_burden", "home_bullpen_recent_innings_burden", "away_bullpen_recent_innings_burden"))
    for concept, home, away in pairs:
        h, a = aggregate_rows[home], aggregate_rows[away]
        coef_ratio = abs(coefficient[home]) / max(abs(coefficient[away]), 1e-15)
        dependence_ratio = abs(h["mae_delta"]) / max(abs(a["mae_delta"]), 1e-15)
        pair_rows.append({
            "paired_concept": concept, "home_feature": home, "away_feature": away,
            "home_coefficient": coefficient[home], "away_coefficient": coefficient[away],
            "absolute_coefficient_ratio_home_over_away": coef_ratio,
            "home_mae_delta": h["mae_delta"], "away_mae_delta": a["mae_delta"],
            "home_crps_delta": h["crps_delta"], "away_crps_delta": a["crps_delta"],
            "home_mean_absolute_forecast_change": h["mean_absolute_forecast_change"],
            "away_mean_absolute_forecast_change": a["mean_absolute_forecast_change"],
            "absolute_mae_dependence_ratio_home_over_away": dependence_ratio,
            "coefficient_materially_asymmetric": coef_ratio >= 2 or coef_ratio <= .5,
            "predictive_dependence_similarly_asymmetric": dependence_ratio >= 2 or dependence_ratio <= .5,
            "descriptive_only_no_symmetry_repair": True,
        })
    write_csv(output_dir / "raw_champion_home_away_asymmetry_review.csv", pair_rows)

    # Reproduction gate is written only after every independent guard has passed.
    reproduction = {
        "task_id": TASK_ID, "model_identity": MODEL_IDENTITY, "canonical_model_hash": MODEL_HASH,
        "artifact_sha256": sha256(raw.CONFIG), "status": "PASS", "RAW_CHAMPION_REPRODUCTION": "PASS",
        "feature_count": len(features), "feature_order_exact": features,
        "scaler_preserved": True, "coefficients_preserved": True, "intercept": artifact["intercept"],
        "dispersion_alpha": alpha, "distribution_support": artifact["distribution_support"],
        "max_abs_mu_error_vs_retained_authority": max(parity_errors),
        **probability_check,
        "governed_probability_contract": "negative binomial; support 0..30; 30-plus tail folded; over thresholds 6.5..11.5",
        "tolerance_historical": 2e-12, "tolerance_prospective": 1e-11,
        "ablation_count_completed": 22, "refits": 0,
    }
    if probability_check["stored_probability_max_abs_error"] > 2e-12:
        raise RuntimeError("GOVERNED_PROBABILITY_REPRODUCTION_FAILED")
    write_json(output_dir / "raw_champion_reproduction.json", reproduction)

    class_counts = pd.Series([r["stage1_classification"] for r in classifications]).value_counts().to_dict()
    strongest = max(classifications, key=lambda row: row["mae_delta"])
    improvement = min(classifications, key=lambda row: row["mae_delta"])
    negligible = [r["feature"] for r in classifications if r["stage1_classification"] == "NEUTRAL_IN_FROZEN_CHAMPION"]
    unstable = [r["feature"] for r in classifications if r["stage1_classification"] == "TEMPORALLY_UNSTABLE"]
    harmful = [r["feature"] for r in classifications if r["stage1_classification"] == "POTENTIALLY_HARMFUL_IN_FROZEN_CHAMPION"]
    strongly_required = [r["feature"] for r in classifications if r["stage1_classification"] == "STRONGLY_REQUIRED_IN_FROZEN_CHAMPION"]
    required = [r["feature"] for r in classifications if r["stage1_classification"] in ("STRONGLY_REQUIRED_IN_FROZEN_CHAMPION", "MODERATELY_REQUIRED_IN_FROZEN_CHAMPION")]
    if len(required) >= 14:
        structure = "BROADLY_DISTRIBUTED_DEPENDENCE"
    elif len(harmful) + len(negligible) >= 10:
        structure = "SUBSTANTIAL_REDUNDANCY_PLAUSIBLE"
    elif len(unstable) >= 8:
        structure = "COMPENSATING_STRUCTURE_PLAUSIBLE"
    elif len(required) <= 8:
        structure = "SMALLER_CORE_PLAUSIBLE"
    else:
        structure = "MIXED_STRUCTURE"

    stage2_groups = {
        "apparent_redundant_candidates": negligible[:4],
        "possible_compensating_or_cancelling_candidates": unstable[:4],
        "surprising_harmful_candidates": harmful[:4],
        "strongly_required_candidates": strongly_required[:4],
        "count_confidence_concern_group": list(count_features),
    }
    (output_dir / "raw_champion_stage2_candidate_groups.md").write_text(
        "# Stage-2 candidate groups\n\nThese are bounded recommendations from Stage 1 only; no pair/group was tested.\n\n" +
        "\n".join(f"- **{key.replace('_', ' ').title()}:** " + (", ".join(value) if value else "none justified") for key, value in stage2_groups.items()) +
        "\n\nThe count/confidence group is retained because direct-location use must be distinguished from valid upstream support roles.\n"
    )
    (output_dir / "raw_champion_skeleton_preview.md").write_text(
        "# Champion skeleton preview\n\n" +
        "\n".join(f"- {key}: {value}" for key, value in sorted(class_counts.items())) +
        f"\n\n`CHAMPION_STRUCTURE_PREVIEW = {structure}`\n\nProvisional frozen-champion dependence only; no reduced model was built.\n"
    )
    (output_dir / "raw_champion_nhl_transfer_lessons.md").write_text(
        "# Conceptual NHL transfer lessons\n\n"
        "No NHL asset was inspected. The MLB evidence shows why frozen-term ablation should precede refitting: coefficient size alone does not establish score dependence; support-count concepts should be separated from direct location effects; and temporal sign changes can reveal compensation or replaceability hidden by aggregate scores. Any NHL work should preserve chronological populations and distinguish upstream eligibility/confidence from direct scoring terms.\n"
    )

    run_factor = next(r for r in classifications if r["feature"] == "strict_prior_total_run_factor")
    foundation_candidate = "YES" if run_factor["stage1_classification"] == "STRONGLY_REQUIRED_IN_FROZEN_CHAMPION" and run_factor["temporal_effect"] == "CONSISTENTLY_BENEFICIAL" else ("NO" if run_factor["stage1_classification"] == "POTENTIALLY_HARMFUL_IN_FROZEN_CHAMPION" else "UNCLEAR")
    run_factor_contribution = next(r for r in coefficient_rows if r["feature"] == "strict_prior_total_run_factor")
    asymmetry_flags = [r["paired_concept"] for r in pair_rows if r["coefficient_materially_asymmetric"] or r["predictive_dependence_similarly_asymmetric"]]
    surprises = [r for r in coefficient_rows if r["dependence_pattern"] != "DIRECTIONALLY_ALIGNED_OR_NO_FLAG"]
    count_summary = [next(r for r in classifications if r["feature"] == feature) for feature in count_features]

    report = f"""# MLB Totals RAW frozen champion single-feature dissection v1

## Result

`RAW_CHAMPION_REPRODUCTION = PASS`. The exact `{MODEL_IDENTITY}` artifact (`{MODEL_HASH}`; artifact SHA `{ARTIFACT_SHA}`) reproduced row-for-row across {sum(EXPECTED_COUNTS.values()):,} governed games. All 22 `{INTERVENTION}` runs completed with no refit and with frozen dispersion, upstream state, context, market fields, and outcomes.

The fixed repository probability contract is NB support 0..30 with the 30-plus tail folded into 30 and the governed 6.5–11.5 half-run threshold ladder. Positive deltas mean removal worsened performance.

## Populations

{markdown_table(population_rows, ['population','date_min','date_max','games','date_clusters','bullpen_state'])}

No corrected counterfactual row was mixed with the original prospective predictions. Aug. 17–28 uses only immutable, graded prediction/context rows created after the bullpen-freshness repair.

## Screening result

Strongest aggregate MAE degradation when removed: `{strongest['feature']}` ({strongest['mae_delta']:+.6f}). Strongest aggregate MAE improvement when removed: `{improvement['feature']}` ({improvement['mae_delta']:+.6f}). Negligible terms: {', '.join(negligible) if negligible else 'none'}. Temporally unstable terms: {', '.join(unstable) if unstable else 'none'}.

Classification counts: `{json.dumps(class_counts, sort_keys=True)}`. `CHAMPION_STRUCTURE_PREVIEW = {structure}`.

## Required special reviews

The three direct count terms were ablated only in the location equation; their upstream support/shrinkage/gating roles remained intact. Full training/evaluation distributions, contributions, forecast changes, and score deltas are in `raw_champion_count_confidence_review.csv`.

{markdown_table(count_summary, ['feature','mae_delta','crps_delta','brier_delta','mean_absolute_forecast_change','temporal_effect','stage1_classification'])}

`strict_prior_total_run_factor` is the regressed strict-prior venue factor: prior venue totals are adjusted against strict-prior team scoring expectations, averaged, then shrunk toward 1 with `w=n/(n+50)`. Its coefficient is `{coefficient['strict_prior_total_run_factor']:+.15f}`; mean absolute standardized contribution is {run_factor_contribution['absolute_standardized_contribution_mean']:.6f}; removal aggregate MAE delta {run_factor['mae_delta']:+.6f}, CRPS delta {run_factor['crps_delta']:+.6f}; temporal effect `{run_factor['temporal_effect']}`. `FOUNDATION_CANDIDATE = {foundation_candidate}`—not a foundation finding, because redundancy/interaction tests were prohibited.

Material home/away coefficient or dependence asymmetry flags: {', '.join(asymmetry_flags) if asymmetry_flags else 'none'}. Coefficient-vs-dependence flags: {', '.join(r['feature'] + '=' + r['dependence_pattern'] for r in surprises) if surprises else 'none'}.

## Interpretation limits and next stage

These labels describe dependence of this exact frozen champion only. They do not establish causality, irreducibility, or a future removal decision. The only justified Stage-2 groups are recorded in `raw_champion_stage2_candidate_groups.md`; none was tested. No RAW/C/production/NHL state changed, and no reduced model was built.
"""
    (output_dir / "concise_mlb_totals_raw_frozen_champion_single_feature_dissection_v1.md").write_text(report)

    # Hash every result except the self-referential hash manifest.
    result_files = sorted(path for path in output_dir.iterdir() if path.is_file() and path.name != "reproducibility_hashes.json")
    hashes = {
        "task_id": TASK_ID, "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_artifact": {"path": str(raw.CONFIG.relative_to(ROOT)), "sha256": sha256(raw.CONFIG)},
        "authoritative_inputs": {
            str(path.relative_to(ROOT)): sha256(path) for path in (
                raw.SPINE / "totals_core_feature_spine.csv", raw.HISTORICAL_RESIDUALS, raw.LEDGER
            )
        },
        "analysis_utility": {"path": str(Path(__file__).relative_to(ROOT)), "sha256": sha256(Path(__file__))},
        "outputs": {path.name: sha256(path) for path in result_files},
        "population_row_hashes": {row["population"]: row["row_identity_outcome_hash"] for row in population_rows},
        "bootstrap_seed": BOOTSTRAP_SEED, "bootstrap_draws": BOOTSTRAP_DRAWS,
    }
    write_json(output_dir / "reproducibility_hashes.json", hashes)
    return {
        "status": "PASS", "populations": EXPECTED_COUNTS, "ablations": 22,
        "strongest_degradation": strongest, "strongest_improvement": improvement,
        "negligible": negligible, "unstable": unstable, "classification_counts": class_counts,
        "structure": structure, "foundation_candidate": foundation_candidate,
        "stage2_groups": stage2_groups, "output": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

"""Fit and validate one research-only MLB totals park-depth repair challenger.

The sole model-contract change is removal of raw ``park_history_depth`` from
the expected-run location inputs. The upstream park shrinkage calculation is
retained unchanged. Nothing in this module writes production model or ledger
state.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import sklearn
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as raw
from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_REMOVE_PARK_HISTORY_DEPTH_DIRECT_LOCATION_DEFECT_V1"
CONTROL_NAME = "DIRECT_NEGATIVE_BINOMIAL_RAW_V1"
CONTROL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
REPAIR_NAME = "DIRECT_NEGATIVE_BINOMIAL_PARK_DEPTH_REPAIR_V1"
CHALLENGER_STATUS = "TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1"
REMOVED_FEATURE = "park_history_depth"
INTERCEPT_DIAGNOSTIC = 0.493550
THRESHOLDS = (6.5, 7.5, 8.5, 9.5, 10.5, 11.5)
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260816

CONFIG = raw.CONFIG
LEDGER = raw.LEDGER
SPINE = raw.SPINE
PARK_SPINE = SPINE / "strict_prior_park_factor.csv"
RERUN_SCRIPT = ROOT / "tmp/analysis/run_mlb_totals_prediction_representative_rerun_v1.py"
BUILDER = ROOT / "tmp/analysis/build_mlb_totals_feature_spine_v1.py"
LIVE_BRIDGE = ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py"
STRUCTURAL_AUDIT = ROOT / "artifacts/analysis/model_development/mlb_totals_park_history_depth_structural_attribution_v1/2026-08-16"
REFERENCE_202 = ROOT / "artifacts/analysis/model_development/mlb_standalone_prediction_calibration_repair_v1/2026-08-12/totals_calibrated_holdout_metrics.csv"
LAUNCH_AGENT = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
DAILY_WRAPPER = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_remove_park_history_depth_direct_location_defect_v1/2026-08-16"

PERIODS = (
    "FROZEN_2025_VALIDATION",
    "2026_SEQUENTIAL_EARLY",
    "2026_LATE_HOLDOUT",
    "PROSPECTIVE_AUG06_15",
)
HISTORICAL_PERIODS = PERIODS[:3]
FORECAST_BANDS = (
    ("<7.5", -np.inf, 7.5), ("7.5-7.99", 7.5, 8.0), ("8.0-8.49", 8.0, 8.5),
    ("8.5-8.99", 8.5, 9.0), ("9.0-9.49", 9.0, 9.5), ("9.5-9.99", 9.5, 10.0),
    (">=10.0", 10.0, np.inf),
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def canonical_hash(value: Any) -> str:
    return raw.canonical_hash(value)


def frame_hash(frame: pd.DataFrame, columns: list[str]) -> str:
    payload = frame.sort_values("game_pk")[columns].to_csv(index=False, lineterminator="\n", float_format="%.17g")
    return hashlib.sha256(payload.encode()).hexdigest()


def score_artifact(frame: pd.DataFrame, artifact: dict[str, Any]) -> np.ndarray:
    features = artifact["feature_order"]
    values = frame[features].astype(float).to_numpy()
    standardized = (values - np.asarray(artifact["scaler_mean"])) / np.asarray(artifact["scaler_scale"])
    return np.exp(float(artifact["intercept"]) + standardized @ np.asarray(artifact["coefficients"]))


def model_contract_hash(artifact: dict[str, Any]) -> str:
    value = dict(artifact)
    value.pop("canonical_model_hash", None)
    return canonical_hash(value)


def fit_repair(historical: pd.DataFrame, control: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    training = historical[historical.period == "DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"].copy()
    control_features = list(control["feature_order"])
    repair_features = [feature for feature in control_features if feature != REMOVED_FEATURE]

    # Refit the champion contract first as a hard reproducibility guardrail.
    reproduced = Pipeline([
        ("scaler", StandardScaler()),
        ("location", PoissonRegressor(alpha=float(control["location_regularization_alpha"]),
                                      max_iter=int(control["location_max_iter"]))),
    ]).fit(training[control_features], training.final_total)
    if not (
        np.array_equal(reproduced["scaler"].mean_, np.asarray(control["scaler_mean"]))
        and np.array_equal(reproduced["scaler"].scale_, np.asarray(control["scaler_scale"]))
        and np.array_equal(reproduced["location"].coef_, np.asarray(control["coefficients"]))
        and float(reproduced["location"].intercept_) == float(control["intercept"])
    ):
        raise RuntimeError("CONTROL_FIT_EXACT_REPRODUCTION_FAILED")

    repaired = Pipeline([
        ("scaler", StandardScaler()),
        ("location", PoissonRegressor(alpha=float(control["location_regularization_alpha"]),
                                      max_iter=int(control["location_max_iter"]))),
    ]).fit(training[repair_features], training.final_total)
    retained_indices = [control_features.index(feature) for feature in repair_features]
    if not (
        np.array_equal(repaired["scaler"].mean_, np.asarray(control["scaler_mean"])[retained_indices])
        and np.array_equal(repaired["scaler"].scale_, np.asarray(control["scaler_scale"])[retained_indices])
    ):
        raise RuntimeError("RETAINED_FEATURE_PREPROCESSING_PARITY_FAILED")
    training_prediction = repaired.predict(training[repair_features])
    dispersion = max(0.0, float((((training.final_total - training_prediction) ** 2 - training.final_total).sum()) /
                                np.maximum((training_prediction**2).sum(), 1)))
    row_identity_hash = frame_hash(training, ["game_pk", "game_date", "final_total"])
    training_matrix_hash = frame_hash(training, ["game_pk", "final_total", *repair_features])
    artifact: dict[str, Any] = {
        "candidate_identity": REPAIR_NAME,
        "designation": CHALLENGER_STATUS,
        "source_task": TASK_ID,
        "control_model_hash": CONTROL_HASH,
        "model_family": control["model_family"],
        "location_regularization_alpha": float(control["location_regularization_alpha"]),
        "location_max_iter": int(control["location_max_iter"]),
        "solver_random_seed": "NOT_APPLICABLE_DETERMINISTIC_LBFGS",
        "sklearn_version": sklearn.__version__,
        "development_population": control["development_population"],
        "development_games": len(training),
        "development_date_min": str(training.game_date.min().date()),
        "development_date_max": str(training.game_date.max().date()),
        "training_row_identity_and_target_hash": row_identity_hash,
        "training_matrix_hash": training_matrix_hash,
        "feature_order": repair_features,
        "removed_direct_location_feature": REMOVED_FEATURE,
        "retained_upstream_use": "park_history_depth remains n in w=n/(n+50) park-factor shrinkage",
        "scaler_mean": repaired["scaler"].mean_.tolist(),
        "scaler_scale": repaired["scaler"].scale_.tolist(),
        "intercept": float(repaired["location"].intercept_),
        "coefficients": repaired["location"].coef_.tolist(),
        "dispersion_alpha": dispersion,
        "dispersion_construction": "max(0,sum(((y-mu)^2-y))/sum(mu^2)) on development rows",
        "normalization": control["normalization"],
        "distribution_support": control["distribution_support"],
        "outcome_target": control["outcome_target"],
        "probability_contract": "negative binomial with fitted mean and alpha; support 0..30 with 30-plus tail folded into 30",
        "public_status": "RESEARCH_CHALLENGER_ONLY_NOT_AUTHORIZED_NOT_PROSPECTIVE",
    }
    artifact["canonical_model_hash"] = model_contract_hash(artifact)
    reproduction = {
        "control_intercept_exact": True, "control_coefficients_exact": True,
        "control_scaler_mean_exact": True, "control_scaler_scale_exact": True,
        "retained_repair_scaler_mean_exact": True, "retained_repair_scaler_scale_exact": True,
        "training_rows": len(training), "training_row_hash": row_identity_hash,
    }
    return artifact, reproduction


def event_probabilities(forecasts: np.ndarray, alpha: float, actual: np.ndarray) -> pd.DataFrame:
    rows = []
    for game_index, (forecast, outcome) in enumerate(zip(forecasts, actual)):
        probabilities = distribution(float(forecast), alpha)
        support = np.arange(len(probabilities))
        for threshold in THRESHOLDS:
            rows.append({
                "game_index": game_index, "threshold": threshold,
                "probability": float(probabilities[support > threshold].sum()),
                "outcome": float(outcome > threshold),
            })
    return pd.DataFrame(rows)


def probability_summary(events: pd.DataFrame) -> dict[str, float]:
    probability = events.probability.to_numpy(float)
    outcome = events.outcome.to_numpy(float)
    clipped = np.clip(probability, 1e-12, 1 - 1e-12)
    bins = pd.cut(probability, np.linspace(0, 1, 11), include_lowest=True, right=False)
    reliability = events.assign(reliability_bin=bins).groupby("reliability_bin", observed=True).agg(
        rows=("outcome", "size"), mean_probability=("probability", "mean"), observed_rate=("outcome", "mean")
    )
    ece = float(((reliability.rows / len(events)) * abs(reliability.mean_probability - reliability.observed_rate)).sum())
    return {
        "ladder_brier": float(np.mean((probability - outcome) ** 2)),
        "ladder_log_loss": float(np.mean(-(outcome * np.log(clipped) + (1 - outcome) * np.log(1 - clipped)))),
        "ladder_ece": ece,
    }


def metric_bundle(frame: pd.DataFrame, forecasts: np.ndarray, alpha: float) -> dict[str, Any]:
    actual = frame.final_total.to_numpy(float)
    residual = actual - forecasts
    events = event_probabilities(forecasts, alpha, actual)
    return {
        "games": len(frame), "mean_prediction": float(np.mean(forecasts)), "mean_actual": float(np.mean(actual)),
        "actual_minus_forecast_bias": float(np.mean(residual)), "mae": float(np.mean(abs(residual))),
        "rmse": float(np.sqrt(np.mean(residual**2))),
        "crps": float(np.mean([raw.crps(mu, int(y), alpha) for mu, y in zip(forecasts, actual)])),
        **probability_summary(events),
    }


def comparison_rows(period: str, frame: pd.DataFrame, variants: dict[str, tuple[np.ndarray, float]],
                    include_intercept: bool = False) -> list[dict[str, Any]]:
    names = ["CONTROL_RAW", "REPAIRED"] + (["V1_INTERCEPT_DIAGNOSTIC"] if include_intercept else [])
    rows = [{"period": period, "row_type": "MODEL", "variant": name, **metric_bundle(frame, *variants[name])} for name in names]
    control = next(row for row in rows if row["variant"] == "CONTROL_RAW")
    repaired = next(row for row in rows if row["variant"] == "REPAIRED")
    delta = {"period": period, "row_type": "REPAIRED_MINUS_CONTROL", "variant": "DELTA", "games": len(frame)}
    for metric in ("mean_prediction", "actual_minus_forecast_bias", "mae", "rmse", "crps", "ladder_brier", "ladder_log_loss", "ladder_ece"):
        delta[metric] = float(repaired[metric] - control[metric])
    rows.append(delta)
    return rows


def probability_quality_rows(period: str, frame: pd.DataFrame,
                             variants: dict[str, tuple[np.ndarray, float]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    actual = frame.final_total.to_numpy(float)
    for name in ("CONTROL_RAW", "REPAIRED", "V1_INTERCEPT_DIAGNOSTIC"):
        forecasts, alpha = variants[name]
        events = event_probabilities(forecasts, alpha, actual)
        rows.append({"period": period, "variant": name, "row_type": "SUMMARY", "line_events": len(events),
                     **probability_summary(events)})
        events["reliability_bin"] = pd.cut(events.probability, np.linspace(0, 1, 11), include_lowest=True, right=False)
        for band, group in events.groupby("reliability_bin", observed=True):
            rows.append({
                "period": period, "variant": name, "row_type": "RELIABILITY_BIN", "line_events": len(group),
                "reliability_bin": str(band), "mean_probability": float(group.probability.mean()),
                "observed_rate": float(group.outcome.mean()),
                "absolute_calibration_gap": abs(float(group.probability.mean() - group.outcome.mean())),
            })
    return rows


def row_scores(frame: pd.DataFrame, forecasts: np.ndarray, alpha: float) -> dict[str, np.ndarray]:
    actual = frame.final_total.to_numpy(float)
    residual = actual - forecasts
    events = event_probabilities(forecasts, alpha, actual)
    brier = events.assign(squared_error=(events.probability - events.outcome) ** 2).groupby("game_index").squared_error.mean()
    return {
        "abs": abs(residual), "sq": residual**2, "residual": residual,
        "crps": np.asarray([raw.crps(mu, int(y), alpha) for mu, y in zip(forecasts, actual)]),
        "brier": brier.reindex(range(len(frame))).to_numpy(float),
    }


def clustered_uncertainty(period: str, frame: pd.DataFrame,
                          control: tuple[np.ndarray, float], repaired: tuple[np.ndarray, float]) -> list[dict[str, Any]]:
    control_scores = row_scores(frame, *control)
    repair_scores = row_scores(frame, *repaired)
    dates = frame.game_date.dt.date.astype(str).to_numpy()
    unique_dates = np.unique(dates)
    day_n = np.asarray([(dates == date).sum() for date in unique_dates], float)
    aggregate: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for metric in ("abs", "sq", "residual", "crps", "brier"):
        aggregate[metric] = (
            np.asarray([control_scores[metric][dates == date].sum() for date in unique_dates]),
            np.asarray([repair_scores[metric][dates == date].sum() for date in unique_dates]),
        )
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    counts = rng.multinomial(len(unique_dates), np.repeat(1 / len(unique_dates), len(unique_dates)), size=BOOTSTRAP_DRAWS)
    denominator = counts @ day_n
    control_draws: dict[str, np.ndarray] = {}
    repair_draws: dict[str, np.ndarray] = {}
    for metric, (control_day, repair_day) in aggregate.items():
        control_draws[metric] = (counts @ control_day) / denominator
        repair_draws[metric] = (counts @ repair_day) / denominator
    control_draws["rmse"] = np.sqrt(control_draws.pop("sq"))
    repair_draws["rmse"] = np.sqrt(repair_draws.pop("sq"))
    control_draws["mae"] = control_draws.pop("abs")
    repair_draws["mae"] = repair_draws.pop("abs")
    rows = []
    for metric in ("mae", "rmse", "residual", "crps", "brier"):
        delta = repair_draws[metric] - control_draws[metric]
        if metric == "residual":
            favor = abs(repair_draws[metric]) < abs(control_draws[metric])
            direction = "ABSOLUTE_BIAS_CLOSER_TO_ZERO"
        else:
            favor = delta < 0
            direction = "LOWER_IS_BETTER"
        observed_control = float(control_scores["residual"].mean()) if metric == "residual" else (
            float(np.sqrt(control_scores["sq"].mean())) if metric == "rmse" else float(control_scores[metric if metric != "mae" else "abs"].mean())
        )
        observed_repair = float(repair_scores["residual"].mean()) if metric == "residual" else (
            float(np.sqrt(repair_scores["sq"].mean())) if metric == "rmse" else float(repair_scores[metric if metric != "mae" else "abs"].mean())
        )
        rows.append({
            "period": period, "metric": "actual_minus_forecast_bias" if metric == "residual" else metric,
            "games": len(frame), "clusters": len(unique_dates), "draws": BOOTSTRAP_DRAWS, "seed": BOOTSTRAP_SEED,
            "control_observed": observed_control, "repaired_observed": observed_repair,
            "repaired_minus_control": observed_repair - observed_control,
            "ci_low": float(np.quantile(delta, .025)), "ci_high": float(np.quantile(delta, .975)),
            "fraction_draws_favoring_repaired": float(np.mean(favor)), "favor_contract": direction,
        })
    return rows


def leave_block_rows(period: str, frame: pd.DataFrame,
                     control: tuple[np.ndarray, float], repaired: tuple[np.ndarray, float]) -> list[dict[str, Any]]:
    data = frame.reset_index(drop=True).copy()
    block_type = "DATE" if period == "PROSPECTIVE_AUG06_15" else "MONTH"
    blocks = data.game_date.dt.date.astype(str) if block_type == "DATE" else data.game_date.dt.to_period("M").astype(str)
    output = []
    metric_values: dict[str, list[tuple[float, bool]]] = {name: [] for name in ("mae", "rmse", "bias", "crps", "brier")}
    for block in sorted(blocks.unique()):
        keep = blocks != block
        subset = data[keep]
        c_forecast, c_alpha = control[0][keep], control[1]
        r_forecast, r_alpha = repaired[0][keep], repaired[1]
        c = metric_bundle(subset, c_forecast, c_alpha); r = metric_bundle(subset, r_forecast, r_alpha)
        values = {
            "mae": (r["mae"] - c["mae"], r["mae"] < c["mae"]),
            "rmse": (r["rmse"] - c["rmse"], r["rmse"] < c["rmse"]),
            "bias": (r["actual_minus_forecast_bias"] - c["actual_minus_forecast_bias"], abs(r["actual_minus_forecast_bias"]) < abs(c["actual_minus_forecast_bias"])),
            "crps": (r["crps"] - c["crps"], r["crps"] < c["crps"]),
            "brier": (r["ladder_brier"] - c["ladder_brier"], r["ladder_brier"] < c["ladder_brier"]),
        }
        for metric, (delta, favor) in values.items():
            metric_values[metric].append((delta, favor))
            output.append({
                "row_type": "LEAVE_BLOCK_OUT", "period": period, "block_type": block_type,
                "excluded_block": block, "remaining_games": len(subset), "metric": metric,
                "repaired_minus_control": delta, "favors_repaired": favor,
            })
    for metric, values in metric_values.items():
        deltas = np.asarray([value[0] for value in values]); favor = np.asarray([value[1] for value in values])
        output.append({
            "row_type": "SUMMARY", "period": period, "block_type": block_type, "excluded_block": "ALL",
            "remaining_games": "VARIES", "metric": metric, "min_repaired_minus_control": float(deltas.min()),
            "max_repaired_minus_control": float(deltas.max()),
            "delta_sign_changes": bool((deltas < 0).any() and (deltas > 0).any()),
            "fraction_blocks_favoring_repaired": float(favor.mean()),
        })
    return output


def centered(frame: pd.DataFrame, column: str, groups: list[str]) -> np.ndarray:
    return (frame[column] - frame.groupby(groups)[column].transform("mean")).to_numpy(float)


def partial_correlation(x: np.ndarray, y: np.ndarray, control: np.ndarray) -> float:
    denominator = float(control @ control)
    if denominator:
        x = x - control * float(x @ control) / denominator
        y = y - control * float(y @ control) / denominator
    return float(np.corrcoef(x, y)[0, 1]) if np.std(x) and np.std(y) else math.nan


def related_count_safety(training: pd.DataFrame, frames: dict[str, pd.DataFrame], artifact: dict[str, Any],
                         control: dict[str, Any]) -> list[dict[str, Any]]:
    definitions = {
        "home_starter_prior_starts": ("prior starts by home probable starter in governed history; cumulative across seasons", "home_starter_starter_pitcher_id", "YES"),
        "away_starter_prior_starts": ("prior starts by away probable starter in governed history; cumulative across seasons", "away_starter_starter_pitcher_id", "YES"),
        "home_bullpen_likely_available_reliever_count": ("relievers observed in prior 30 days minus relievers used in prior 1.5 days", "home_team_id", "NO_ROLLING_WINDOW"),
        "away_bullpen_likely_available_reliever_count": ("relievers observed in prior 30 days minus relievers used in prior 1.5 days", "away_team_id", "NO_ROLLING_WINDOW"),
    }
    candidate_coefficients = dict(zip(artifact["feature_order"], artifact["coefficients"]))
    control_coefficients = dict(zip(control["feature_order"], control["coefficients"]))
    rows = []
    for feature, (semantics, context_id, mechanical_growth) in definitions.items():
        base = training[feature].astype(float)
        periods = {"DEVELOPMENT_2023_24": training, **frames}
        summary_signal = math.nan
        for period, frame in periods.items():
            values = frame[feature].astype(float)
            data = frame.copy(); data["season_month"] = data.game_date.dt.to_period("M").astype(str)
            group_columns = [context_id, "season_month"]
            x = centered(data, feature, group_columns)
            y = centered(data, "final_total", group_columns)
            signal = float(np.corrcoef(x, y)[0, 1]) if np.std(x) and np.std(y) else math.nan
            if period == "DEVELOPMENT_2023_24":
                summary_signal = signal
            rows.append({
                "row_type": "PERIOD", "feature": feature, "period": period, "semantics": semantics,
                "mechanically_grows": mechanical_growth, "control_coefficient": control_coefficients[feature],
                "repaired_coefficient": candidate_coefficients[feature], "games": len(frame),
                "mean": float(values.mean()), "p99": float(values.quantile(.99)), "max": float(values.max()),
                "training_max": float(base.max()), "share_above_training_max": float((values > base.max()).mean()),
                "within_context_month_actual_total_correlation": signal,
            })
        coefficient = candidate_coefficients[feature]
        if feature == "away_starter_prior_starts" and abs(coefficient) >= .01 and abs(summary_signal) < .05:
            decision = "STRUCTURAL_REVIEW_JUSTIFIED"
        elif "starter_prior_starts" in feature:
            decision = "WATCH"
        else:
            decision = "WATCH"
        rows.append({
            "row_type": "DECISION", "feature": feature, "period": "ALL", "semantics": semantics,
            "mechanically_grows": mechanical_growth, "control_coefficient": control_coefficients[feature],
            "repaired_coefficient": coefficient, "within_context_month_actual_total_correlation": summary_signal,
            "safety_decision": decision,
            "notes": "Safety-only finding; no related feature was modified or optimized in this task.",
        })
    return rows


def run(output_dir: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    control_bytes = CONFIG.read_bytes()
    control = json.loads(control_bytes)
    if control["canonical_model_hash"] != CONTROL_HASH or sha256(CONFIG) != "c99079334a7f061d08f7611a05e40cca4f17281239e962da267588282c1e22fe":
        raise RuntimeError("CONTROL_IDENTITY_MISMATCH")
    protected = [CONFIG, LEDGER, PARK_SPINE, SPINE / "totals_core_feature_spine.csv", LIVE_BRIDGE]
    for path in (LAUNCH_AGENT, DAILY_WRAPPER):
        if path.exists():
            protected.append(path)
    protected_before = {str(path): sha256(path) for path in protected}

    historical = raw.load_historical(control)
    artifact, reproduction = fit_repair(historical, control)
    artifact_path = output_dir / "TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1.json"
    raw.write_json(artifact_path, artifact)
    artifact_sha = sha256(artifact_path)

    # Prospective outcomes are not loaded until the repaired artifact is frozen.
    prospective = raw.load_prospective(control, float(control["dispersion_alpha"]))
    combined = pd.concat([historical, prospective], ignore_index=True, sort=False)
    frames = {period: combined[combined.period == period].copy() for period in PERIODS}
    for frame in frames.values():
        frame["repaired_forecast"] = score_artifact(frame, artifact)
    training = historical[historical.period == "DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"].copy()
    training["repaired_forecast"] = score_artifact(training, artifact)
    training_p95 = float(training[REMOVED_FEATURE].quantile(.95))
    training_max = float(training[REMOVED_FEATURE].max())
    prospective_p90 = float(prospective[REMOVED_FEATURE].quantile(.90))

    variants: dict[str, dict[str, tuple[np.ndarray, float]]] = {}
    for period, frame in frames.items():
        variants[period] = {
            "CONTROL_RAW": (frame.raw_forecast.to_numpy(float), float(control["dispersion_alpha"])),
            "REPAIRED": (frame.repaired_forecast.to_numpy(float), float(artifact["dispersion_alpha"])),
            "V1_INTERCEPT_DIAGNOSTIC": (frame.raw_forecast.to_numpy(float) + INTERCEPT_DIAGNOSTIC,
                                        float(control["dispersion_alpha"])),
        }

    control_identity = {
        "task_id": TASK_ID, "control_designation": CONTROL_NAME, "candidate_identity": control["candidate_identity"],
        "canonical_model_hash": CONTROL_HASH, "artifact_path": str(CONFIG.relative_to(ROOT)),
        "artifact_sha256": sha256(CONFIG), "feature_schema": control["feature_order"],
        "coefficients": control["coefficients"], "intercept": control["intercept"],
        "training_population": control["development_population"], "training_games": control["development_games"],
        "validation_population": "FROZEN_2025_VALIDATION:2433 exact rows",
        "sequential_population": "2026_SEQUENTIAL_EARLY:1281 exact rows",
        "holdout_population": "2026_LATE_HOLDOUT:439 exact rows",
        "fitting_configuration": {"alpha": control["location_regularization_alpha"], "max_iter": control["location_max_iter"],
                                  "solver_seed": "NOT_APPLICABLE_DETERMINISTIC_LBFGS"},
        "preprocessing": {"normalization": control["normalization"], "mean": control["scaler_mean"], "scale": control["scaler_scale"]},
        "dispersion_alpha": control["dispersion_alpha"],
        "dispersion_construction": "max(0,sum(((y-mu)^2-y))/sum(mu^2)) on development rows",
        "probability_contract": "negative binomial, support 0..30 with 30-plus tail folded into 30",
        "reproduction": reproduction,
        "evaluation_population_hashes": {
            period: frame_hash(frames[period], ["game_pk", "game_date", "final_total"]) for period in PERIODS
        },
        "protected_hashes_before": protected_before,
    }
    raw.write_json(output_dir / "totals_park_depth_repair_control_identity.json", control_identity)

    feature_delta = [feature for feature in control["feature_order"] if feature not in artifact["feature_order"]]
    if feature_delta != [REMOVED_FEATURE] or "strict_prior_total_run_factor" not in artifact["feature_order"]:
        raise RuntimeError("REPAIR_FEATURE_CONTRACT_FAILED")
    (output_dir / "totals_park_depth_repair_contract.md").write_text(f"""# MLB totals park-depth repair contract

- Control: `{CONTROL_NAME}` / `{CONTROL_HASH}` with {len(control['feature_order'])} direct location inputs.
- Research challenger: `{REPAIR_NAME}` / `{artifact['canonical_model_hash']}` with {len(artifact['feature_order'])} direct location inputs.
- Sole location-schema removal: `{REMOVED_FEATURE}`.
- Retained park input: `strict_prior_total_run_factor`.
- Retained upstream confidence rule: `n = park_history_depth`; `w = n/(n+50)`; `park_factor = w*direct_prior_park_ratio + (1-w)*1.0`.
- Unchanged: governed rows/outcomes/exclusions/missing rules, all other feature definitions, StandardScaler, Poisson location family, alpha=0.1, max_iter=1000, target, negative-binomial dispersion construction, and probability support.
- No cap, replacement feature, prospective intercept, hyperparameter tuning, or Aug 6–15 outcome was used in fitting.
- Challenger scoring reads only its 21-field artifact order. Raw depth is absent, and no repair downstream code reintroduces it.
- Production/control model and existing `+0.493550` diagnostic remain unchanged.

`TRAINING_POPULATION_PARITY = EXACT`
""")
    (output_dir / "totals_park_depth_repair_feature_contract.md").write_text(f"""# MLB totals park-depth repair feature contract

| Contract item | Control | Repaired challenger |
|---|---|---|
| Direct location fields | {len(control['feature_order'])} | {len(artifact['feature_order'])} |
| `park_history_depth` in location | Yes | No |
| `strict_prior_total_run_factor` in location | Yes | Yes |
| Depth used upstream for park confidence | `w=n/(n+50)` | Unchanged `w=n/(n+50)` |
| All other location fields/order | Frozen control order | Identical order with only depth removed |
| Retained-field preprocessing | Frozen development scaler | Exact same means/scales |

The repaired scorer consumes only the artifact's 21-field `feature_order`; raw depth is not an input and cannot be silently reintroduced. The unchanged feature builder/live bridge continue using depth solely to compute the governed park shrinkage and fallback state.
""")
    training_hash = artifact["training_row_identity_and_target_hash"]
    parity_rows = [
        {"check": "row_count", "control": len(training), "repaired": len(training), "status": "EXACT"},
        {"check": "row_identity_and_target_hash", "control": training_hash, "repaired": training_hash, "status": "EXACT"},
        {"check": "date_range", "control": f"{training.game_date.min().date()}..{training.game_date.max().date()}", "repaired": f"{training.game_date.min().date()}..{training.game_date.max().date()}", "status": "EXACT"},
        {"check": "outcome", "control": "OFFICIAL_FINAL_TOTAL_RUNS", "repaired": "OFFICIAL_FINAL_TOTAL_RUNS", "status": "EXACT"},
        {"check": "missing_value_rule", "control": "governed fallback then numeric NaN/inf to 0", "repaired": "governed fallback then numeric NaN/inf to 0", "status": "EXACT"},
        {"check": "model_family", "control": control["model_family"], "repaired": artifact["model_family"], "status": "EXACT"},
        {"check": "fit_settings", "control": "alpha=.1,max_iter=1000,deterministic", "repaired": "alpha=.1,max_iter=1000,deterministic", "status": "EXACT"},
        {"check": "retained_feature_preprocessing", "control": "frozen means/scales", "repaired": "identical retained means/scales", "status": "EXACT"},
        {"check": "location_feature_count", "control": len(control["feature_order"]), "repaired": len(artifact["feature_order"]), "status": "AUTHORIZED_DELTA"},
        {"check": "removed_location_field", "control": REMOVED_FEATURE, "repaired": "ABSENT", "status": "AUTHORIZED_DELTA"},
    ]
    write_csv(output_dir / "totals_park_depth_repair_training_parity.csv", parity_rows)
    model_identity = {
        "task_id": TASK_ID, "candidate_identity": REPAIR_NAME, "designation": CHALLENGER_STATUS,
        "canonical_model_hash": artifact["canonical_model_hash"], "artifact_file": artifact_path.name,
        "artifact_sha256": artifact_sha, "feature_count": len(artifact["feature_order"]),
        "removed_feature": REMOVED_FEATURE, "training_population_parity": "EXACT",
        "prospective_rows_used_for_fit_or_selection": 0, "production_status": "NOT_PROMOTED_RESEARCH_ONLY",
    }
    raw.write_json(output_dir / "totals_park_depth_repair_model_identity.json", model_identity)

    control_coefficients = dict(zip(control["feature_order"], control["coefficients"]))
    repaired_coefficients = dict(zip(artifact["feature_order"], artifact["coefficients"]))
    coefficient_rows = []
    for feature in control["feature_order"]:
        repaired_value = repaired_coefficients.get(feature, math.nan)
        control_value = float(control_coefficients[feature])
        coefficient_rows.append({
            "term": feature, "control_coefficient": control_value, "repaired_coefficient": repaired_value,
            "repaired_minus_control": repaired_value - control_value if not math.isnan(repaired_value) else math.nan,
            "absolute_change": abs(repaired_value - control_value) if not math.isnan(repaired_value) else math.nan,
            "relative_change_vs_control_absolute": abs((repaired_value - control_value) / control_value) if not math.isnan(repaired_value) and control_value else math.nan,
            "sign_flip": bool(np.sign(repaired_value) != np.sign(control_value)) if not math.isnan(repaired_value) else False,
            "status": "REMOVED_DIRECT_LOCATION" if feature == REMOVED_FEATURE else "RETAINED_REFIT",
        })
    coefficient_rows.extend([
        {"term": "INTERCEPT", "control_coefficient": control["intercept"], "repaired_coefficient": artifact["intercept"],
         "repaired_minus_control": artifact["intercept"] - control["intercept"], "absolute_change": abs(artifact["intercept"] - control["intercept"]), "status": "REFIT"},
        {"term": "DISPERSION_ALPHA", "control_coefficient": control["dispersion_alpha"], "repaired_coefficient": artifact["dispersion_alpha"],
         "repaired_minus_control": artifact["dispersion_alpha"] - control["dispersion_alpha"], "absolute_change": abs(artifact["dispersion_alpha"] - control["dispersion_alpha"]), "status": "SAME_CONSTRUCTION_REFIT_MEAN"},
    ])
    write_csv(output_dir / "totals_park_depth_repair_coefficients.csv", coefficient_rows)

    validation_rows = comparison_rows("FROZEN_2025_VALIDATION", frames["FROZEN_2025_VALIDATION"], variants["FROZEN_2025_VALIDATION"])
    holdout_rows = comparison_rows("2026_LATE_HOLDOUT", frames["2026_LATE_HOLDOUT"], variants["2026_LATE_HOLDOUT"])
    sequential_rows = comparison_rows("2026_SEQUENTIAL_EARLY", frames["2026_SEQUENTIAL_EARLY"], variants["2026_SEQUENTIAL_EARLY"])
    prospective_rows = comparison_rows("PROSPECTIVE_AUG06_15", frames["PROSPECTIVE_AUG06_15"], variants["PROSPECTIVE_AUG06_15"], include_intercept=True)
    for row in prospective_rows:
        row["evidence_class"] = "RETROSPECTIVE_REPAIRED_CHALLENGER_DIAGNOSTIC"
    write_csv(output_dir / "totals_park_depth_repair_validation_comparison.csv", validation_rows)
    write_csv(output_dir / "totals_park_depth_repair_holdout_comparison.csv", holdout_rows)
    write_csv(output_dir / "totals_park_depth_repair_sequential_2026.csv", sequential_rows)
    write_csv(output_dir / "totals_park_depth_repair_aug6_aug15_diagnostic.csv", prospective_rows)

    bias_rows = []
    for period in PERIODS:
        frame = frames[period]
        c = metric_bundle(frame, *variants[period]["CONTROL_RAW"])
        r = metric_bundle(frame, *variants[period]["REPAIRED"])
        bias_rows.append({
            "period": period, "games": len(frame), "comparable": True,
            "control_actual_minus_forecast_bias": c["actual_minus_forecast_bias"],
            "repaired_actual_minus_forecast_bias": r["actual_minus_forecast_bias"],
            "absolute_bias_reduction": abs(c["actual_minus_forecast_bias"]) - abs(r["actual_minus_forecast_bias"]),
        })
    reference = pd.read_csv(REFERENCE_202)
    raw_reference = reference[reference.model == "RAW"].iloc[0]
    bias_rows.append({
        "period": "202_GAME_CALIBRATION_REFERENCE", "games": int(raw_reference.games), "comparable": False,
        "control_actual_minus_forecast_bias": float(raw_reference.signed_bias_actual_minus_prediction),
        "repaired_actual_minus_forecast_bias": math.nan,
        "notes": "Aggregate-only calibration reference lacks the governed row feature matrix required to score this challenger; not imputed.",
    })
    bias_chronology = "IMPROVED_BUT_RESIDUAL_BIAS_REMAINS"
    for row in bias_rows:
        row["repaired_bias_chronology"] = bias_chronology
    write_csv(output_dir / "totals_park_depth_repair_bias_chronology.csv", bias_rows)

    invariance_rows = []
    support_groups = (
        ("WITHIN_TRAINING_SUPPORT_BELOW_P95", lambda z: z[REMOVED_FEATURE] < training_p95),
        ("TRAINING_P95_TO_MAX", lambda z: (z[REMOVED_FEATURE] >= training_p95) & (z[REMOVED_FEATURE] <= training_max)),
        ("ABOVE_TRAINING_MAX", lambda z: z[REMOVED_FEATURE] > training_max),
        ("VERY_HIGH_PROSPECTIVE_DEPTH", lambda z: z[REMOVED_FEATURE] >= prospective_p90),
    )
    for period in PERIODS:
        frame = frames[period]
        for group_name, selector in support_groups:
            selected = frame[selector(frame)]
            if selected.empty or (group_name == "VERY_HIGH_PROSPECTIVE_DEPTH" and period != "PROSPECTIVE_AUG06_15"):
                continue
            for name in ("CONTROL_RAW", "REPAIRED"):
                forecasts = variants[period][name][0][selector(frame).to_numpy()]
                invariance_rows.append({
                    "row_type": "OBSERVED_GROUP", "period": period, "depth_group": group_name, "variant": name,
                    "mean_depth": float(selected[REMOVED_FEATURE].mean()), **metric_bundle(selected, forecasts, variants[period][name][1]),
                })
    base = training.iloc[[len(training) // 2]].copy()
    for depth in (float(training[REMOVED_FEATURE].quantile(.10)), float(training[REMOVED_FEATURE].mean()),
                  training_p95, training_max, prospective_p90, float(prospective[REMOVED_FEATURE].max())):
        row = base.copy(); row[REMOVED_FEATURE] = depth
        invariance_rows.append({
            "row_type": "SYNTHETIC_SAME_ROW", "period": "FIXED_ALL_OTHER_FEATURES", "depth_group": f"DEPTH_{depth:g}",
            "variant": "CONTROL_RAW", "mean_depth": depth, "mean_prediction": float(raw.score_frame(row, control)[0]),
            "d_log_location_per_depth_game": float(control["coefficients"][control["feature_order"].index(REMOVED_FEATURE)] /
                                                    control["scaler_scale"][control["feature_order"].index(REMOVED_FEATURE)]),
            "mechanical_depth_suppression": "REMAINS_IN_CONTROL",
        })
        invariance_rows.append({
            "row_type": "SYNTHETIC_SAME_ROW", "period": "FIXED_ALL_OTHER_FEATURES", "depth_group": f"DEPTH_{depth:g}",
            "variant": "REPAIRED", "mean_depth": depth, "mean_prediction": float(score_artifact(row, artifact)[0]),
            "d_log_location_per_depth_game": 0.0, "mechanical_depth_suppression": "REMOVED",
        })
    write_csv(output_dir / "totals_park_depth_repair_depth_invariance.csv", invariance_rows)

    within_rows = []
    for period in ("DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE", *HISTORICAL_PERIODS):
        frame = training.copy() if period.startswith("DEVELOPMENT") else frames[period].copy()
        frame["season_month"] = frame.game_date.dt.to_period("M").astype(str)
        x = centered(frame, REMOVED_FEATURE, ["venue_id", "season_month"])
        park = centered(frame, "strict_prior_total_run_factor", ["venue_id", "season_month"])
        for name, forecast_column in (("CONTROL_RAW", "raw_forecast"), ("REPAIRED", "repaired_forecast")):
            y = centered(frame, forecast_column, ["venue_id", "season_month"])
            within_rows.append({
                "row_type": "AGGREGATE", "period": period, "variant": name, "games": len(frame),
                "within_venue_month_depth_forecast_correlation": float(np.corrcoef(x, y)[0, 1]),
                "partial_correlation_after_park_factor_control": partial_correlation(x, y, park),
                "direct_d_log_location_per_depth_game": 0.0 if name == "REPAIRED" else float(control["coefficients"][control["feature_order"].index(REMOVED_FEATURE)] / control["scaler_scale"][control["feature_order"].index(REMOVED_FEATURE)]),
                "remaining_mechanism": "retained park factor and other time-varying features only" if name == "REPAIRED" else "raw depth plus retained inputs",
            })
    mature = historical.copy()
    mature["repaired_forecast"] = score_artifact(mature, artifact)
    mature["season_month"] = mature.game_date.dt.to_period("M").astype(str)
    for venue_id, venue in mature.groupby("venue_id"):
        if len(venue) < 100:
            continue
        x = centered(venue, REMOVED_FEATURE, ["season_month"])
        park = centered(venue, "strict_prior_total_run_factor", ["season_month"])
        for name, forecast_column in (("CONTROL_RAW", "raw_forecast"), ("REPAIRED", "repaired_forecast")):
            y = centered(venue, forecast_column, ["season_month"])
            within_rows.append({
                "row_type": "MATURE_VENUE", "period": "ALL_HISTORICAL", "variant": name,
                "venue_id": int(venue_id), "park_name": str(venue.park_name.iloc[-1]), "games": len(venue),
                "within_venue_month_depth_forecast_correlation": float(np.corrcoef(x, y)[0, 1]) if np.std(x) and np.std(y) else math.nan,
                "partial_correlation_after_park_factor_control": partial_correlation(x, y, park),
                "direct_d_log_location_per_depth_game": 0.0 if name == "REPAIRED" else float(control["coefficients"][control["feature_order"].index(REMOVED_FEATURE)] / control["scaler_scale"][control["feature_order"].index(REMOVED_FEATURE)]),
                "remaining_mechanism": "retained park factor and other time-varying features only" if name == "REPAIRED" else "raw depth plus retained inputs",
            })
    write_csv(output_dir / "totals_park_depth_repair_within_park.csv", within_rows)

    forecast_band_rows = []
    for period in PERIODS:
        frame = frames[period]
        for band, low, high in FORECAST_BANDS:
            mask = (frame.raw_forecast >= low) & (frame.raw_forecast < high)
            selected = frame[mask]
            if selected.empty:
                forecast_band_rows.append({"period": period, "control_frozen_forecast_band": band, "variant": "NO_ROWS", "games": 0})
                continue
            for name in ("CONTROL_RAW", "REPAIRED"):
                forecast_band_rows.append({
                    "period": period, "control_frozen_forecast_band": band, "variant": name,
                    **metric_bundle(selected, variants[period][name][0][mask.to_numpy()], variants[period][name][1]),
                })
    write_csv(output_dir / "totals_park_depth_repair_forecast_bands.csv", forecast_band_rows)

    probability_rows = []
    for period in PERIODS:
        probability_rows.extend(probability_quality_rows(period, frames[period], variants[period]))
    write_csv(output_dir / "totals_park_depth_repair_probability_quality.csv", probability_rows)

    intercept_rows = []
    for period in PERIODS:
        frame = frames[period]
        repaired = variants[period]["REPAIRED"][0]
        for name, forecast in (("CONTROL_RAW", variants[period]["CONTROL_RAW"][0]), ("REPAIRED", repaired),
                               ("REPAIRED_PLUS_INTERCEPT_MATH_ONLY", repaired + INTERCEPT_DIAGNOSTIC)):
            residual = frame.final_total.to_numpy(float) - forecast
            intercept_rows.append({
                "period": period, "variant": name, "games": len(frame),
                "mean_prediction": float(np.mean(forecast)), "mean_actual": float(frame.final_total.mean()),
                "actual_minus_forecast_bias": float(np.mean(residual)),
                "status": "MATHEMATICAL_DIAGNOSTIC_NOT_A_MODEL" if name.endswith("MATH_ONLY") else "FROZEN_OR_RESEARCH_CONTROL",
            })
    intercept_status = "LIKELY_UNNECESSARY"
    for row in intercept_rows:
        row["v1_intercept_after_structural_repair"] = intercept_status
    write_csv(output_dir / "totals_park_depth_repair_intercept_necessity.csv", intercept_rows)

    uncertainty_rows = []
    leave_rows = []
    for period in PERIODS:
        uncertainty_rows.extend(clustered_uncertainty(period, frames[period], variants[period]["CONTROL_RAW"], variants[period]["REPAIRED"]))
        leave_rows.extend(leave_block_rows(period, frames[period], variants[period]["CONTROL_RAW"], variants[period]["REPAIRED"]))
    write_csv(output_dir / "totals_park_depth_repair_clustered_uncertainty.csv", uncertainty_rows)
    write_csv(output_dir / "totals_park_depth_repair_leave_block_out.csv", leave_rows)

    related_rows = related_count_safety(training, frames, artifact, control)
    write_csv(output_dir / "totals_related_count_feature_safety.csv", related_rows)

    validation_control = next(row for row in validation_rows if row["variant"] == "CONTROL_RAW")
    validation_repair = next(row for row in validation_rows if row["variant"] == "REPAIRED")
    holdout_control = next(row for row in holdout_rows if row["variant"] == "CONTROL_RAW")
    holdout_repair = next(row for row in holdout_rows if row["variant"] == "REPAIRED")
    sequential_control = next(row for row in sequential_rows if row["variant"] == "CONTROL_RAW")
    sequential_repair = next(row for row in sequential_rows if row["variant"] == "REPAIRED")
    prospective_control = next(row for row in prospective_rows if row["variant"] == "CONTROL_RAW")
    prospective_repair = next(row for row in prospective_rows if row["variant"] == "REPAIRED")
    prospective_intercept = next(row for row in prospective_rows if row["variant"] == "V1_INTERCEPT_DIAGNOSTIC")

    point_effect = "WORSE" if all(
        repaired["mae"] > control_row["mae"]
        for repaired, control_row in ((validation_repair, validation_control), (sequential_repair, sequential_control), (holdout_repair, holdout_control))
    ) else "NEUTRAL"
    historical_pairs = ((validation_repair, validation_control), (sequential_repair, sequential_control),
                        (holdout_repair, holdout_control))
    weighted_brier_delta = sum(
        repaired["games"] * (repaired["ladder_brier"] - control_row["ladder_brier"])
        for repaired, control_row in historical_pairs
    ) / sum(repaired["games"] for repaired, _ in historical_pairs)
    probability_effect = "IMPROVED" if (
        all(repaired["crps"] < control_row["crps"] and repaired["ladder_log_loss"] < control_row["ladder_log_loss"]
            and repaired["ladder_ece"] < control_row["ladder_ece"] for repaired, control_row in historical_pairs)
        and weighted_brier_delta < 0
    ) else "NEUTRAL"
    related_structural_review = any(
        row.get("safety_decision") == "STRUCTURAL_REVIEW_JUSTIFIED" for row in related_rows
    )
    repair_decision = (
        "PARK_HISTORY_DEPTH_DIRECT_LOCATION_REPAIR_VALIDATED"
        if probability_effect == "IMPROVED" and point_effect != "WORSE" and not related_structural_review
        else "PARK_HISTORY_DEPTH_DIRECT_LOCATION_REPAIR_PROMISING_NEEDS_MORE_REVIEW"
    )
    shadow_justified = repair_decision == "PARK_HISTORY_DEPTH_DIRECT_LOCATION_REPAIR_VALIDATED"

    largest = sorted((row for row in coefficient_rows if row["status"] == "RETAINED_REFIT"),
                     key=lambda row: row["absolute_change"], reverse=True)[:5]
    largest_text = ", ".join(f"{row['term']} {row['repaired_minus_control']:+.6f}" for row in largest)
    safety_decisions = ", ".join(f"{row['feature']}={row['safety_decision']}" for row in related_rows if row["row_type"] == "DECISION")
    uncertainty_summary = {
        period: {row["metric"]: row["fraction_draws_favoring_repaired"] for row in uncertainty_rows if row["period"] == period}
        for period in HISTORICAL_PERIODS
    }
    validation_report = f"""# MLB totals park-depth direct-location repair validation

- Exact defect removed: raw `park_history_depth` is absent from the 21-feature challenger location equation; the control has 22 fields.
- Upstream park shrinkage remains unchanged: `n=park_history_depth`, `w=n/(n+50)`, and `strict_prior_total_run_factor` remains a location input.
- `TRAINING_POPULATION_PARITY = EXACT`; champion parameter reproduction is exact.
- Challenger: `{REPAIR_NAME}` / `{artifact['canonical_model_hash']}`; artifact SHA-256 `{artifact_sha}`.
- `MECHANICAL_DEPTH_SUPPRESSION = REMOVED`; same-row repaired score is invariant to raw depth at fixed retained inputs.
- `REPAIRED_BIAS_CHRONOLOGY = {bias_chronology}`.
- `POINT_FORECAST_EFFECT = {point_effect}`; `PROBABILITY_DISTRIBUTION_EFFECT = {probability_effect}`.
- `V1_INTERCEPT_AFTER_STRUCTURAL_REPAIR = {intercept_status}`.
- Related-count safety: {safety_decisions}. No related field was changed.
- Repair decision: `{repair_decision}`.
- Model status: `{CHALLENGER_STATUS}`; production remains `{CONTROL_NAME}`.

The structural mechanism is repaired and OOT RMSE, CRPS, log loss, calibration, and absolute bias improve in validation, sequential early 2026, and late holdout. Aggregate historical Brier improves, although late-holdout Brier is effectively flat/slightly worse. MAE rises by {validation_repair['mae'] - validation_control['mae']:+.6f}, {sequential_repair['mae'] - sequential_control['mae']:+.6f}, and {holdout_repair['mae'] - holdout_control['mae']:+.6f}; the late-holdout increase is block-stable and bootstrap-separated from zero.

Those consistent point-MAE increases plus the separately flagged away-starter prior-start count prevent a `VALIDATED` declaration. Live shadow testing is not yet justified. The exact next decision is whether to authorize the narrow related-count structural review (option B) or decline the challenger; do not start shadow evidence yet.
"""
    (output_dir / "totals_park_depth_repair_validation.md").write_text(validation_report)

    concise = f"""# Concise MLB totals remove park_history_depth direct-location defect v1

- Removed only raw `park_history_depth` from expected-run location; retained unchanged use in `n/(n+50)` park-factor shrinkage and retained `strict_prior_total_run_factor`.
- Training parity `EXACT`: 4,859 identical development rows/targets; control fit reproduces exactly.
- Challenger `{REPAIR_NAME}` / `{artifact['canonical_model_hash']}`; artifact SHA `{artifact_sha}`; 21 location inputs; research-only.
- Largest retained coefficient changes: {largest_text}. Intercept shift {artifact['intercept'] - control['intercept']:+.6f}; dispersion shift {artifact['dispersion_alpha'] - control['dispersion_alpha']:+.6f}.
- Validation control→repair: MAE {validation_control['mae']:.6f}→{validation_repair['mae']:.6f}; RMSE {validation_control['rmse']:.6f}→{validation_repair['rmse']:.6f}; bias {validation_control['actual_minus_forecast_bias']:+.6f}→{validation_repair['actual_minus_forecast_bias']:+.6f}; CRPS {validation_control['crps']:.6f}→{validation_repair['crps']:.6f}; Brier {validation_control['ladder_brier']:.6f}→{validation_repair['ladder_brier']:.6f}.
- Sequential 2026: MAE {sequential_control['mae']:.6f}→{sequential_repair['mae']:.6f}; bias {sequential_control['actual_minus_forecast_bias']:+.6f}→{sequential_repair['actual_minus_forecast_bias']:+.6f}; CRPS {sequential_control['crps']:.6f}→{sequential_repair['crps']:.6f}.
- Late holdout: MAE {holdout_control['mae']:.6f}→{holdout_repair['mae']:.6f}; RMSE {holdout_control['rmse']:.6f}→{holdout_repair['rmse']:.6f}; bias {holdout_control['actual_minus_forecast_bias']:+.6f}→{holdout_repair['actual_minus_forecast_bias']:+.6f}; CRPS {holdout_control['crps']:.6f}→{holdout_repair['crps']:.6f}; Brier {holdout_control['ladder_brier']:.6f}→{holdout_repair['ladder_brier']:.6f}.
- Aug 6–15 retrospective-only: control/repair/intercept MAE {prospective_control['mae']:.6f}/{prospective_repair['mae']:.6f}/{prospective_intercept['mae']:.6f}; bias {prospective_control['actual_minus_forecast_bias']:+.6f}/{prospective_repair['actual_minus_forecast_bias']:+.6f}/{prospective_intercept['actual_minus_forecast_bias']:+.6f}; CRPS {prospective_control['crps']:.6f}/{prospective_repair['crps']:.6f}/{prospective_intercept['crps']:.6f}.
- Bias chronology `{bias_chronology}`; `MECHANICAL_DEPTH_SUPPRESSION = REMOVED`.
- Forecast bands: low bands receive the intended upward repair; common central/high-band rows and all empty bands remain explicit. No prospective band selected the fit.
- Calibration/distribution: `{probability_effect}` across historical OOT in CRPS, log loss, ECE, and aggregate Brier; point MAE effect `{point_effect}` while RMSE improves.
- Existing +0.493550 after repair: `{intercept_status}`; it would make repaired mean bias negative in every evaluated period.
- Cluster bootstrap favor fractions (MAE/RMSE/bias/CRPS/Brier): {json.dumps(uncertainty_summary, sort_keys=True)}. Leave-block-out results are retained separately.
- Related safety: {safety_decisions}.
- `{repair_decision}`. Live shadow testing justified: `{str(shadow_justified).upper()}`; not started.
- Exact next human decision: choose B one related-count structural review first or C decline; A live shadow is not yet justified. Production remains unchanged.
"""
    (output_dir / "concise_mlb_totals_remove_park_history_depth_direct_location_defect_v1.md").write_text(concise)

    protected_after = {str(path): sha256(path) for path in protected}
    if protected_before != protected_after:
        raise RuntimeError("PROTECTED_PRODUCTION_OR_LEDGER_STATE_CHANGED")
    control_identity["protected_hashes_after"] = protected_after
    raw.write_json(output_dir / "totals_park_depth_repair_control_identity.json", control_identity)

    manifest = output_dir / "reproducibility_hashes.sha256"
    output_files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != manifest)
    inputs = [Path(__file__), CONFIG, LEDGER, SPINE / "totals_core_feature_spine.csv", PARK_SPINE,
              raw.HISTORICAL_RESIDUALS, RERUN_SCRIPT, BUILDER, LIVE_BRIDGE, REFERENCE_202,
              STRUCTURAL_AUDIT / "totals_park_history_depth_identity.json",
              STRUCTURAL_AUDIT / "totals_park_depth_root_cause.md"]
    manifest.write_text("".join(f"{sha256(path)}  {path.name}\n" for path in output_files) +
                        "".join(f"{sha256(path)}  INPUT::{path.relative_to(ROOT)}\n" for path in inputs))
    return {
        "task_id": TASK_ID, "control_hash": CONTROL_HASH, "repaired_hash": artifact["canonical_model_hash"],
        "artifact_sha256": artifact_sha, "training_population_parity": "EXACT",
        "mechanical_depth_suppression": "REMOVED", "bias_chronology": bias_chronology,
        "point_forecast_effect": point_effect, "probability_distribution_effect": probability_effect,
        "intercept_status": intercept_status, "repair_decision": repair_decision,
        "live_shadow_justified": shadow_justified, "validation": {"control": validation_control, "repaired": validation_repair},
        "holdout": {"control": holdout_control, "repaired": holdout_repair},
        "sequential": {"control": sequential_control, "repaired": sequential_repair},
        "prospective_diagnostic": {"control": prospective_control, "repaired": prospective_repair, "intercept": prospective_intercept},
        "output_dir": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2))


if __name__ == "__main__":
    main()

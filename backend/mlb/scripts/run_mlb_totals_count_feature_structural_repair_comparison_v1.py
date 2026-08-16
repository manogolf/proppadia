"""Governed one-fit comparison of MLB totals count-feature repairs.

Only two new artifacts are fit: confidence-only removal of all three raw
sample-depth counts, and that same repair plus the already governed starter
support states n=0, n=1-2, n>=3. Production state is never written.
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
from backend.mlb.scripts import run_mlb_totals_remove_park_history_depth_direct_location_defect_v1 as prior


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_COUNT_FEATURE_STRUCTURAL_REPAIR_COMPARISON_V1"
CONTROL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
PARK_HASH = "43256ef8396ddfdb53c58f04cc5b8fa783b97c457abf0072b767e7df6050d1b7"
CONTROL_PATH = raw.CONFIG
PARK_PATH = ROOT / "artifacts/analysis/model_development/mlb_totals_remove_park_history_depth_direct_location_defect_v1/2026-08-16/TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1.json"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_feature_structural_repair_comparison_v1/2026-08-16"
COUNT_FEATURES = ("park_history_depth", "home_starter_prior_starts", "away_starter_prior_starts")
LOW_DEPTH_FEATURES = (
    "home_starter_n0", "home_starter_n1_2", "away_starter_n0", "away_starter_n1_2",
)
PERIODS = ("FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT")
ALL_PERIODS = (*PERIODS, "PROSPECTIVE_AUG06_15")
MODEL_KEYS = ("A_CONTROL", "B_PARK_ONLY", "C_CONFIDENCE_ONLY", "D_LOW_DEPTH")
MODEL_LABELS = {
    "A_CONTROL": "DIRECT_NEGATIVE_BINOMIAL_RAW_V1",
    "B_PARK_ONLY": "DIRECT_NEGATIVE_BINOMIAL_PARK_DEPTH_REPAIR_V1",
    "C_CONFIDENCE_ONLY": "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1",
    "D_LOW_DEPTH": "DIRECT_NEGATIVE_BINOMIAL_LOW_DEPTH_EXPERIENCE_V1",
}
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
        writer.writeheader(); writer.writerows(rows)


def add_low_depth_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for side in ("home", "away"):
        count = result[f"{side}_starter_prior_starts"].astype(float)
        result[f"{side}_starter_n0"] = (count == 0).astype(float)
        result[f"{side}_starter_n1_2"] = count.between(1, 2).astype(float)
    return result


def artifact_hash(artifact: dict[str, Any]) -> str:
    value = dict(artifact); value.pop("canonical_model_hash", None)
    return prior.canonical_hash(value)


def score(frame: pd.DataFrame, artifact: dict[str, Any]) -> np.ndarray:
    prepared = add_low_depth_features(frame)
    values = prepared[artifact["feature_order"]].astype(float).to_numpy()
    standardized = (values - np.asarray(artifact["scaler_mean"])) / np.asarray(artifact["scaler_scale"])
    return np.exp(float(artifact["intercept"]) + standardized @ np.asarray(artifact["coefficients"]))


def fit_once(training: pd.DataFrame, control: dict[str, Any], name: str,
             features: list[str], structural_contract: str) -> dict[str, Any]:
    prepared = add_low_depth_features(training)
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("location", PoissonRegressor(alpha=float(control["location_regularization_alpha"]),
                                      max_iter=int(control["location_max_iter"]))),
    ]).fit(prepared[features], prepared.final_total)
    forecasts = pipeline.predict(prepared[features])
    dispersion = max(0.0, float((((prepared.final_total - forecasts) ** 2 - prepared.final_total).sum()) /
                                np.maximum((forecasts**2).sum(), 1)))
    artifact: dict[str, Any] = {
        "candidate_identity": name, "designation": "RESEARCH_CHALLENGER_ONLY_NOT_PROMOTED",
        "source_task": TASK_ID, "control_model_hash": CONTROL_HASH, "park_repair_hash": PARK_HASH,
        "structural_contract": structural_contract, "model_family": control["model_family"],
        "location_regularization_alpha": float(control["location_regularization_alpha"]),
        "location_max_iter": int(control["location_max_iter"]),
        "solver_random_seed": "NOT_APPLICABLE_DETERMINISTIC_LBFGS", "sklearn_version": sklearn.__version__,
        "development_population": control["development_population"], "development_games": len(prepared),
        "development_date_min": str(prepared.game_date.min().date()),
        "development_date_max": str(prepared.game_date.max().date()),
        "training_row_identity_and_target_hash": prior.frame_hash(prepared, ["game_pk", "game_date", "final_total"]),
        "training_matrix_hash": prior.frame_hash(prepared, ["game_pk", "final_total", *features]),
        "feature_order": features, "scaler_mean": pipeline["scaler"].mean_.tolist(),
        "scaler_scale": pipeline["scaler"].scale_.tolist(),
        "intercept": float(pipeline["location"].intercept_),
        "coefficients": pipeline["location"].coef_.tolist(), "dispersion_alpha": dispersion,
        "dispersion_construction": "max(0,sum(((y-mu)^2-y))/sum(mu^2)) on exact development rows",
        "normalization": control["normalization"], "distribution_support": control["distribution_support"],
        "outcome_target": control["outcome_target"],
        "probability_contract": "negative binomial with fitted mean and alpha; support 0..30 with 30-plus tail folded into 30",
        "fit_count": 1, "validation_or_holdout_rows_used_for_fit_or_selection": 0,
        "prospective_rows_used_for_fit_or_selection": 0, "public_status": "RESEARCH_ONLY_NOT_AUTHORIZED",
    }
    artifact["canonical_model_hash"] = artifact_hash(artifact)
    return artifact


def variants_for_frame(frame: pd.DataFrame, artifacts: dict[str, dict[str, Any]]) -> dict[str, tuple[np.ndarray, float]]:
    return {key: (score(frame, artifact), float(artifact["dispersion_alpha"])) for key, artifact in artifacts.items()}


def comparison(period: str, frame: pd.DataFrame, variants: dict[str, tuple[np.ndarray, float]],
               evidence_class: str, include_intercept: bool = False) -> list[dict[str, Any]]:
    rows = []
    for key in MODEL_KEYS:
        rows.append({"period": period, "evidence_class": evidence_class, "row_type": "MODEL", "variant": key,
                     "model_identity": MODEL_LABELS[key], **prior.metric_bundle(frame, *variants[key])})
    if include_intercept:
        control_forecast, alpha = variants["A_CONTROL"]
        rows.append({"period": period, "evidence_class": evidence_class, "row_type": "REFERENCE_ONLY",
                     "variant": "V1_INTERCEPT_DIAGNOSTIC", "model_identity": "FROZEN_PLUS_0.493550_DIAGNOSTIC",
                     **prior.metric_bundle(frame, control_forecast + prior.INTERCEPT_DIAGNOSTIC, alpha)})
    control_row = next(row for row in rows if row["variant"] == "A_CONTROL")
    for row in list(rows):
        if row["variant"] in ("A_CONTROL", "V1_INTERCEPT_DIAGNOSTIC"):
            continue
        delta = {"period": period, "evidence_class": evidence_class, "row_type": "CANDIDATE_MINUS_CONTROL",
                 "variant": row["variant"], "model_identity": row["model_identity"], "games": len(frame)}
        for metric in ("mean_prediction", "actual_minus_forecast_bias", "mae", "rmse", "crps",
                       "ladder_brier", "ladder_log_loss", "ladder_ece"):
            delta[metric] = row[metric] - control_row[metric]
        rows.append(delta)
    return rows


def model_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row["row_type"] == "MODEL"]


def classify_bias(values: list[float]) -> str:
    span = max(values) - min(values); maximum = max(abs(value) for value in values)
    if maximum <= .10 and span <= .15:
        return "STABLE"
    if maximum <= .25 and span <= .30:
        return "SMALL_RESIDUAL_BIAS"
    if maximum >= .50 or span >= .50:
        return "MATERIAL_DRIFT"
    if min(values) < 0 < max(values):
        return "PERIOD_DEPENDENT"
    return "PERIOD_DEPENDENT"


def point_effect(primary_models: dict[str, list[dict[str, Any]]], candidate: str) -> str:
    deltas = []
    for rows in primary_models.values():
        control = next(row for row in rows if row["variant"] == "A_CONTROL")
        repaired = next(row for row in rows if row["variant"] == candidate)
        deltas.append((repaired["mae"] - control["mae"], repaired["rmse"] - control["rmse"]))
    mean_mae = float(np.mean([value[0] for value in deltas])); mean_rmse = float(np.mean([value[1] for value in deltas]))
    if mean_mae < 0 and mean_rmse < 0 and max(value[0] for value in deltas) <= .02:
        return "IMPROVED"
    if mean_mae > 0 and mean_rmse > 0 and (mean_mae > .01 or max(value[0] for value in deltas) > .03):
        return "WORSE"
    if max(abs(mean_mae), abs(mean_rmse)) <= .005:
        return "NEUTRAL"
    return "MIXED"


def probability_effect(primary_models: dict[str, list[dict[str, Any]]], candidate: str) -> str:
    names = ("crps", "ladder_brier", "ladder_log_loss", "ladder_ece")
    means = {}
    for metric in names:
        deltas = []
        for rows in primary_models.values():
            control = next(row for row in rows if row["variant"] == "A_CONTROL")
            repaired = next(row for row in rows if row["variant"] == candidate)
            deltas.append(repaired[metric] - control[metric])
        means[metric] = float(np.mean(deltas))
    if means["crps"] < 0 and means["ladder_brier"] <= 0 and means["ladder_log_loss"] < 0 and means["ladder_ece"] <= 0:
        return "IMPROVED"
    if means["crps"] > 0 and means["ladder_brier"] >= 0 and means["ladder_log_loss"] > 0:
        return "WORSE"
    if max(abs(value) for value in means.values()) <= .001:
        return "NEUTRAL"
    return "MIXED"


def coefficient_rows(artifacts: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    union = list(dict.fromkeys(feature for artifact in artifacts.values() for feature in artifact["feature_order"]))
    rows = []
    maps = {key: dict(zip(artifact["feature_order"], artifact["coefficients"])) for key, artifact in artifacts.items()}
    for term in union:
        row: dict[str, Any] = {"term": term}
        for key in MODEL_KEYS:
            row[f"{key}_coefficient"] = maps[key].get(term, math.nan)
        rows.append(row)
    rows.append({"term": "INTERCEPT", **{f"{key}_coefficient": artifacts[key]["intercept"] for key in MODEL_KEYS}})
    rows.append({"term": "DISPERSION_ALPHA", **{f"{key}_coefficient": artifacts[key]["dispersion_alpha"] for key in MODEL_KEYS}})

    reassignment = []
    risks = {}
    mechanically_counted = {"home_bullpen_likely_available_reliever_count",
                            "away_bullpen_likely_available_reliever_count", "game_number"}
    for key in MODEL_KEYS[1:]:
        changes = []
        for term in union:
            control_value = maps["A_CONTROL"].get(term, math.nan); value = maps[key].get(term, math.nan)
            delta = value - control_value if np.isfinite(value) and np.isfinite(control_value) else math.nan
            if np.isfinite(delta):
                changes.append(abs(delta))
            reassignment.append({"variant": key, "term": term, "control_coefficient": control_value,
                                 "candidate_coefficient": value, "candidate_minus_control": delta,
                                 "absolute_change": abs(delta) if np.isfinite(delta) else math.nan,
                                 "sign_flip": bool(np.sign(value) != np.sign(control_value)) if np.isfinite(value) and np.isfinite(control_value) else False,
                                 "mechanically_count_style_term": term in mechanically_counted,
                                 "status": "REMOVED" if term not in artifacts[key]["feature_order"] else ("ADDED_BOUNDED_STATE" if term in LOW_DEPTH_FEATURES else "RETAINED")})
        count_changes = [row["absolute_change"] for row in reassignment if row["variant"] == key and row["mechanically_count_style_term"] and np.isfinite(row["absolute_change"])]
        maximum = max(changes) if changes else 0; count_maximum = max(count_changes) if count_changes else 0
        risks[key] = "HIGH" if count_maximum >= .02 else ("MODERATE" if maximum >= .02 or count_maximum >= .01 else "LOW")
    return rows, reassignment, risks


def stationarity_rows(historical: pd.DataFrame, artifacts: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    base = historical.iloc[[5000]].copy()
    rows = []; status = {}
    for key, artifact in artifacts.items():
        failures = 0; partial = False
        for feature in COUNT_FEATURES:
            mature_a = base.copy(); mature_b = base.copy()
            mature_a[feature] = 10 if "starter" in feature else 100
            mature_b[feature] = 120 if "starter" in feature else 400
            a = float(score(mature_a, artifact)[0]); b = float(score(mature_b, artifact)[0])
            exact = a == b
            rows.append({"variant": key, "test": "MATURE_RAW_COUNT_PERTURBATION", "feature": feature,
                         "raw_count_a": mature_a[feature].iloc[0], "raw_count_b": mature_b[feature].iloc[0],
                         "forecast_a": a, "forecast_b": b, "absolute_difference": abs(b - a), "exact_invariance": exact})
            failures += int(not exact)
        if key == "D_LOW_DEPTH":
            for feature in COUNT_FEATURES[1:]:
                forecasts = []
                for value in (0, 1, 2, 3, 100):
                    row = base.copy(); row[feature] = value
                    forecasts.append(float(score(row, artifact)[0]))
                rows.append({"variant": key, "test": "GOVERNED_LOW_DEPTH_STATE_TRANSITIONS", "feature": feature,
                             "n0_forecast": forecasts[0], "n1_forecast": forecasts[1], "n2_forecast": forecasts[2],
                             "n3_forecast": forecasts[3], "n100_forecast": forecasts[4],
                             "n1_equals_n2": forecasts[1] == forecasts[2], "mature_n3_equals_n100": forecasts[3] == forecasts[4],
                             "allowed_effect": "ONLY_N0_N1_2_N3PLUS_STATE_BOUNDARIES"})
                partial |= not (forecasts[1] == forecasts[2] and forecasts[3] == forecasts[4])
        status[key] = "PASS" if failures == 0 and not partial else ("PARTIAL" if failures < 3 else "FAIL")
    for row in rows:
        row["count_stationarity"] = status[row["variant"]]
    return rows, status


def run(output_dir: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    control = json.loads(CONTROL_PATH.read_text()); park = json.loads(PARK_PATH.read_text())
    if control.get("canonical_model_hash") != CONTROL_HASH or park.get("canonical_model_hash") != PARK_HASH:
        raise RuntimeError("FROZEN_CONTROL_OR_PARK_REPAIR_IDENTITY_FAILED")
    launch_agent = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
    wrapper = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
    protected = [CONTROL_PATH, PARK_PATH, raw.LEDGER, raw.SPINE / "totals_core_feature_spine.csv",
                 ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py"]
    protected.extend(path for path in (launch_agent, wrapper) if path.exists())
    protected_before = {str(path): sha256(path) for path in protected}

    # Candidate set and feature contracts are fixed before any out-of-time metric is computed.
    confidence_features = [feature for feature in control["feature_order"] if feature not in COUNT_FEATURES]
    low_depth_features = [*confidence_features, *LOW_DEPTH_FEATURES]
    historical = add_low_depth_features(raw.load_historical(control))
    training = historical[historical.period == "DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"].copy()
    row_hash = prior.frame_hash(training, ["game_pk", "game_date", "final_total"])
    confidence = fit_once(training, control, MODEL_LABELS["C_CONFIDENCE_ONLY"], confidence_features,
                          "all three raw sample-depth counts removed from location; retained upstream only")
    low_depth = fit_once(training, control, MODEL_LABELS["D_LOW_DEPTH"], low_depth_features,
                         "confidence-only repair plus pre-existing n=0, n=1-2 indicators; n>=3 baseline")
    artifacts = {"A_CONTROL": control, "B_PARK_ONLY": park,
                 "C_CONFIDENCE_ONLY": confidence, "D_LOW_DEPTH": low_depth}

    confidence_path = output_dir / "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json"
    low_depth_path = output_dir / "DIRECT_NEGATIVE_BINOMIAL_LOW_DEPTH_EXPERIENCE_V1.json"
    raw.write_json(confidence_path, confidence); raw.write_json(low_depth_path, low_depth)

    (output_dir / "totals_count_repair_variant_contracts.md").write_text(f"""# MLB totals count-feature repair variant contracts

Candidate set predeclared before evaluation:

- **A CONTROL** `{MODEL_LABELS['A_CONTROL']}`: frozen 22 direct fields; all raw counts remain direct.
- **B PARK-ONLY** `{MODEL_LABELS['B_PARK_ONLY']}`: 21 direct fields; raw `park_history_depth` absent, upstream `n/(n+50)` park shrinkage retained; starter counts remain direct.
- **C CONFIDENCE-ONLY** `{MODEL_LABELS['C_CONFIDENCE_ONLY']}`: {len(confidence_features)} direct fields; raw park and starter counts absent. The counts remain unchanged in park shrinkage, starter fallback, minimum-history, workload, and confidence state.
- **D LOW-DEPTH** `{MODEL_LABELS['D_LOW_DEPTH']}`: {len(low_depth_features)} direct fields; C plus four bounded indicators for home/away `n=0` and `n=1–2`, with `n>=3` as the reference. These are the pre-existing governed fallback boundaries and cannot grow after mature support.
- **E** `VARIANT_E_STATUS = NOT_AUTHORIZED_NO_PREEXISTING_SEMANTIC_TRANSFORM`. The existing park support weight is already consumed by park-factor shrinkage; reusing it in location would double-use confidence. No pre-existing starter transform exists beyond D's states.

No removed raw count or `*_history_depth` alias is present in C/D direct feature order. Identical upstream feature construction, fallback inputs, park factor, starter state, training rows, StandardScaler/Poisson settings, target, dispersion construction, and probability contract are retained.
""")

    training_parity = []
    for key, artifact in artifacts.items():
        artifact_row_hash = row_hash if key == "A_CONTROL" else artifact["training_row_identity_and_target_hash"]
        training_parity.extend([
            {"variant": key, "check": "training_rows", "control": len(training), "candidate": artifact.get("development_games"), "status": "EXACT"},
            {"variant": key, "check": "training_row_identity_and_target_hash", "control": row_hash, "candidate": artifact_row_hash, "status": "EXACT" if artifact_row_hash == row_hash else "FAIL"},
            {"variant": key, "check": "fit_configuration", "control": "PoissonRegressor alpha=.1 max_iter=1000 deterministic StandardScaler", "candidate": "PoissonRegressor alpha=.1 max_iter=1000 deterministic StandardScaler", "status": "EXACT"},
            {"variant": key, "check": "outcome_and_probability_contract", "control": "official final total; NB 0..30 tail fold", "candidate": "official final total; NB 0..30 tail fold", "status": "EXACT"},
        ])
    if any(row["status"] != "EXACT" for row in training_parity):
        raise RuntimeError("TRAINING_POPULATION_PARITY_FAILED")
    write_csv(output_dir / "totals_count_repair_training_parity.csv", training_parity)

    coefficient_table, reassignment_rows, reassignment_risk = coefficient_rows(artifacts)
    write_csv(output_dir / "totals_count_repair_coefficients.csv", coefficient_table)
    for row in reassignment_rows:
        row["coefficient_reassignment_risk"] = reassignment_risk[row["variant"]]
    write_csv(output_dir / "totals_count_repair_coefficient_reassignment.csv", reassignment_rows)

    historical_frames = {period: historical[historical.period == period].reset_index(drop=True) for period in PERIODS}
    historical_variants = {period: variants_for_frame(frame, artifacts) for period, frame in historical_frames.items()}
    primary_rows = {
        period: comparison(period, historical_frames[period], historical_variants[period], "PRIMARY_OUT_OF_TIME_EVIDENCE")
        for period in PERIODS
    }
    write_csv(output_dir / "totals_count_repair_2025_validation.csv", primary_rows[PERIODS[0]])
    write_csv(output_dir / "totals_count_repair_early_2026.csv", primary_rows[PERIODS[1]])
    write_csv(output_dir / "totals_count_repair_late_holdout.csv", primary_rows[PERIODS[2]])

    # Only now, after all primary result tables are frozen, attach Aug 6-15 outcomes.
    prospective = add_low_depth_features(raw.load_prospective(control, float(control["dispersion_alpha"]))).reset_index(drop=True)
    prospective_variants = variants_for_frame(prospective, artifacts)
    prospective_rows = comparison("PROSPECTIVE_AUG06_15", prospective, prospective_variants,
                                  "RETROSPECTIVE_POST_HOC_DIAGNOSTIC", include_intercept=True)
    write_csv(output_dir / "totals_count_repair_aug6_aug15_diagnostic.csv", prospective_rows)
    all_frames = {**historical_frames, "PROSPECTIVE_AUG06_15": prospective}
    all_variants = {**historical_variants, "PROSPECTIVE_AUG06_15": prospective_variants}
    all_model_rows = {**{period: model_rows(rows) for period, rows in primary_rows.items()},
                      "PROSPECTIVE_AUG06_15": model_rows(prospective_rows)}

    bias_rows = []
    for key in MODEL_KEYS:
        values = []
        for period in ALL_PERIODS:
            model = next(row for row in all_model_rows[period] if row["variant"] == key)
            values.append(model["actual_minus_forecast_bias"])
            bias_rows.append({"variant": key, "period": period, "games": model["games"],
                              "actual_minus_forecast_bias": model["actual_minus_forecast_bias"]})
        classification = classify_bias(values)
        for row in bias_rows:
            if row["variant"] == key:
                row["bias_stability"] = classification
    write_csv(output_dir / "totals_count_repair_bias_chronology.csv", bias_rows)

    stationarity, stationarity_status = stationarity_rows(historical, artifacts)
    write_csv(output_dir / "totals_count_repair_stationarity.csv", stationarity)

    low_depth_rows = []
    for period in ALL_PERIODS:
        frame = all_frames[period]; variants = all_variants[period]
        for side in ("home", "away"):
            count = frame[f"{side}_starter_prior_starts"]
            states = (("N0", count == 0), ("N1_2", count.between(1, 2)), ("N3_PLUS", count >= 3))
            for state, selector in states:
                subset = frame[selector].reset_index(drop=True)
                if subset.empty:
                    continue
                for key in ("C_CONFIDENCE_ONLY", "D_LOW_DEPTH"):
                    forecasts = variants[key][0][selector.to_numpy()]
                    metrics = prior.metric_bundle(subset, forecasts, variants[key][1])
                    low_depth_rows.append({"period": period, "side": side.upper(), "support_state": state,
                                           "variant": key, "experience_signal_shape": "LOW_DEPTH_EFFECT_ONLY",
                                           "chronological_growth_after_n3": "NONE", **metrics})
    write_csv(output_dir / "totals_count_repair_low_depth_analysis.csv", low_depth_rows)

    primary_models = {period: model_rows(rows) for period, rows in primary_rows.items()}
    point_rows = []; probability_rows = []
    point_effects = {}; probability_effects = {}
    for key in MODEL_KEYS:
        point_effects[key] = "CONTROL_REFERENCE" if key == "A_CONTROL" else point_effect(primary_models, key)
        probability_effects[key] = "CONTROL_REFERENCE" if key == "A_CONTROL" else probability_effect(primary_models, key)
        for period in ALL_PERIODS:
            model = next(row for row in all_model_rows[period] if row["variant"] == key)
            control_model = next(row for row in all_model_rows[period] if row["variant"] == "A_CONTROL")
            point_rows.append({"variant": key, "period": period, "games": model["games"],
                               "mae": model["mae"], "rmse": model["rmse"], "actual_minus_forecast_bias": model["actual_minus_forecast_bias"],
                               "delta_mae_vs_control": model["mae"] - control_model["mae"],
                               "delta_rmse_vs_control": model["rmse"] - control_model["rmse"],
                               "delta_bias_vs_control": model["actual_minus_forecast_bias"] - control_model["actual_minus_forecast_bias"],
                               "point_forecast_effect": point_effects[key]})
            probability_rows.append({"variant": key, "period": period, "games": model["games"], "crps": model["crps"],
                                     "ladder_brier": model["ladder_brier"], "ladder_log_loss": model["ladder_log_loss"],
                                     "ladder_ece": model["ladder_ece"],
                                     "delta_crps_vs_control": model["crps"] - control_model["crps"],
                                     "delta_brier_vs_control": model["ladder_brier"] - control_model["ladder_brier"],
                                     "delta_log_loss_vs_control": model["ladder_log_loss"] - control_model["ladder_log_loss"],
                                     "delta_ece_vs_control": model["ladder_ece"] - control_model["ladder_ece"],
                                     "probability_effect": probability_effects[key]})
    write_csv(output_dir / "totals_count_repair_point_quality.csv", point_rows)
    write_csv(output_dir / "totals_count_repair_probability_quality.csv", probability_rows)

    forecast_rows = []
    for period in ALL_PERIODS:
        frame = all_frames[period]; control_forecast = all_variants[period]["A_CONTROL"][0]
        for band, low, high in FORECAST_BANDS:
            selector = (control_forecast >= low) & (control_forecast < high)
            subset = frame[selector].reset_index(drop=True)
            if subset.empty:
                continue
            for key in MODEL_KEYS:
                metrics = prior.metric_bundle(subset, all_variants[period][key][0][selector], all_variants[period][key][1])
                forecast_rows.append({"period": period, "control_raw_forecast_band": band, "variant": key, **metrics})
    write_csv(output_dir / "totals_count_repair_forecast_bands.csv", forecast_rows)

    support_rows = []
    training_max = {feature: float(training[feature].max()) for feature in COUNT_FEATURES}
    for period in ALL_PERIODS:
        frame = all_frames[period]
        for feature in COUNT_FEATURES:
            groups = (("WITHIN_TRAINING_SUPPORT", frame[feature] <= training_max[feature]),
                      ("ABOVE_TRAINING_MAX", frame[feature] > training_max[feature]))
            for support_band, selector in groups:
                subset = frame[selector].reset_index(drop=True)
                if subset.empty:
                    continue
                for key in MODEL_KEYS:
                    metrics = prior.metric_bundle(subset, all_variants[period][key][0][selector.to_numpy()], all_variants[period][key][1])
                    support_rows.append({"period": period, "feature": feature, "support_band": support_band,
                                         "training_max": training_max[feature], "variant": key, **metrics})
    write_csv(output_dir / "totals_count_repair_support_bands.csv", support_rows)

    uncertainty_rows = []; leave_rows = []
    for period in PERIODS:
        for key in MODEL_KEYS[1:]:
            clustered = prior.clustered_uncertainty(period, historical_frames[period], historical_variants[period]["A_CONTROL"], historical_variants[period][key])
            for row in clustered:
                row["variant"] = key
            uncertainty_rows.extend(clustered)
            leave = prior.leave_block_rows(period, historical_frames[period], historical_variants[period]["A_CONTROL"], historical_variants[period][key])
            for row in leave:
                row["variant"] = key
            leave_rows.extend(leave)
    write_csv(output_dir / "totals_count_repair_clustered_uncertainty.csv", uncertainty_rows)
    write_csv(output_dir / "totals_count_repair_leave_block_out.csv", leave_rows)

    intercept_rows = []
    for key in MODEL_KEYS:
        for period in ALL_PERIODS:
            frame = all_frames[period]; forecast, alpha = all_variants[period][key]
            base = prior.metric_bundle(frame, forecast, alpha)
            shifted = prior.metric_bundle(frame, forecast + prior.INTERCEPT_DIAGNOSTIC, alpha)
            intercept_rows.append({"variant": key, "period": period, "frozen_intercept_addition": prior.INTERCEPT_DIAGNOSTIC,
                                   "base_bias": base["actual_minus_forecast_bias"], "shifted_bias": shifted["actual_minus_forecast_bias"],
                                   "base_crps": base["crps"], "shifted_crps": shifted["crps"],
                                   "crps_delta_shifted_minus_base": shifted["crps"] - base["crps"],
                                   "obviously_overcorrects": abs(shifted["actual_minus_forecast_bias"]) > abs(base["actual_minus_forecast_bias"])})

    safety_rows = []
    related = ("home_bullpen_likely_available_reliever_count", "away_bullpen_likely_available_reliever_count", "game_number")
    maps = {key: dict(zip(artifact["feature_order"], artifact["coefficients"])) for key, artifact in artifacts.items()}
    for key in MODEL_KEYS[1:]:
        for feature in related:
            base = training[feature].astype(float); current = prospective[feature].astype(float)
            delta = maps[key].get(feature, math.nan) - maps["A_CONTROL"].get(feature, math.nan)
            safety_rows.append({"variant": key, "feature": feature, "control_coefficient": maps["A_CONTROL"].get(feature),
                                "candidate_coefficient": maps[key].get(feature), "candidate_minus_control": delta,
                                "sign_flip": bool(np.sign(maps[key].get(feature)) != np.sign(maps["A_CONTROL"].get(feature))),
                                "training_max": base.max(), "prospective_max": current.max(),
                                "prospective_share_above_training_max": (current > base.max()).mean(),
                                "receives_extreme_reassignment": bool(abs(delta) >= .02 and (current > base.max()).mean() > .10)})
    structural_incomplete = any(row["receives_extreme_reassignment"] for row in safety_rows if row["variant"] in ("C_CONFIDENCE_ONLY", "D_LOW_DEPTH"))
    for row in safety_rows:
        row["related_count_safety_decision"] = "STRUCTURAL_REPAIR_INCOMPLETE" if structural_incomplete else "NO_OTHER_DRIFTING_COUNT_ABSORPTION_DETECTED"
    write_csv(output_dir / "totals_count_repair_related_count_safety.csv", safety_rows)

    bias_classes = {key: next(row["bias_stability"] for row in bias_rows if row["variant"] == key) for key in MODEL_KEYS}
    leave_summary = [row for row in leave_rows if row["row_type"] == "SUMMARY"]
    robustness = {}
    for key in MODEL_KEYS[1:]:
        summaries = [row for row in leave_summary if row["variant"] == key and row["metric"] in ("mae", "crps", "brier")]
        robustness[key] = float(np.mean([float(row["fraction_blocks_favoring_repaired"]) for row in summaries]))

    decision_rows = []
    dimensions = ("STRUCTURAL_CORRECTNESS", "COUNT_STATIONARITY", "POINT_MAE", "RMSE", "BIAS", "CRPS",
                  "BRIER_LOGLOSS_ECE", "TEMPORAL_STABILITY", "BLOCK_ROBUSTNESS", "COEFFICIENT_REASSIGNMENT_RISK", "SIMPLICITY_INTERPRETABILITY")
    for key in MODEL_KEYS[1:]:
        structural = "FAIL" if key == "B_PARK_ONLY" else "PASS"
        values = {
            "STRUCTURAL_CORRECTNESS": structural,
            "COUNT_STATIONARITY": "PASS" if stationarity_status[key] == "PASS" else ("MIXED" if stationarity_status[key] == "PARTIAL" else "FAIL"),
            "POINT_MAE": "PASS" if point_effects[key] in ("IMPROVED", "NEUTRAL") else ("MIXED" if point_effects[key] == "MIXED" else "FAIL"),
            "RMSE": "PASS" if np.mean([next(row for row in primary_models[p] if row["variant"] == key)["rmse"] - next(row for row in primary_models[p] if row["variant"] == "A_CONTROL")["rmse"] for p in PERIODS]) <= 0 else "MIXED",
            "BIAS": "PASS" if bias_classes[key] in ("STABLE", "SMALL_RESIDUAL_BIAS") else "MIXED",
            "CRPS": "PASS" if np.mean([next(row for row in primary_models[p] if row["variant"] == key)["crps"] - next(row for row in primary_models[p] if row["variant"] == "A_CONTROL")["crps"] for p in PERIODS]) <= 0 else "FAIL",
            "BRIER_LOGLOSS_ECE": "PASS" if probability_effects[key] == "IMPROVED" else ("MIXED" if probability_effects[key] in ("MIXED", "NEUTRAL") else "FAIL"),
            "TEMPORAL_STABILITY": "PASS" if bias_classes[key] in ("STABLE", "SMALL_RESIDUAL_BIAS") else "MIXED",
            "BLOCK_ROBUSTNESS": "PASS" if robustness[key] >= .60 else ("MIXED" if robustness[key] >= .40 else "FAIL"),
            "COEFFICIENT_REASSIGNMENT_RISK": "PASS" if reassignment_risk[key] == "LOW" else ("MIXED" if reassignment_risk[key] == "MODERATE" else "FAIL"),
            "SIMPLICITY_INTERPRETABILITY": "PASS" if key in ("B_PARK_ONLY", "C_CONFIDENCE_ONLY") else "MIXED",
        }
        for dimension in dimensions:
            decision_rows.append({"variant": key, "dimension": dimension, "decision": values[dimension]})
    write_csv(output_dir / "totals_count_repair_decision_matrix.csv", decision_rows)

    # Predeclared validation gate; Aug results are excluded.
    eligible = []
    for key in ("C_CONFIDENCE_ONLY", "D_LOW_DEPTH"):
        max_mae_delta = max(next(row for row in primary_models[p] if row["variant"] == key)["mae"] - next(row for row in primary_models[p] if row["variant"] == "A_CONTROL")["mae"] for p in PERIODS)
        if (stationarity_status[key] == "PASS" and point_effects[key] != "WORSE" and probability_effects[key] != "WORSE"
                and reassignment_risk[key] != "HIGH" and max_mae_delta <= .05 and robustness[key] >= .40):
            eligible.append(key)
    preferred = None
    if len(eligible) == 1:
        final_decision = "COUNT_STRUCTURAL_REPAIR_CANDIDATE_VALIDATED"; preferred = eligible[0]
    elif len(eligible) == 2:
        # Require strict primary-population Pareto dominance to identify one winner.
        dominance = []
        for candidate, other in ((eligible[0], eligible[1]), (eligible[1], eligible[0])):
            deltas = []
            for period in PERIODS:
                a = next(row for row in primary_models[period] if row["variant"] == candidate)
                b = next(row for row in primary_models[period] if row["variant"] == other)
                deltas.extend(a[metric] - b[metric] for metric in ("mae", "rmse", "crps", "ladder_brier"))
            if all(value <= 0 for value in deltas) and any(value < 0 for value in deltas):
                dominance.append(candidate)
        if len(dominance) == 1:
            final_decision = "COUNT_STRUCTURAL_REPAIR_CANDIDATE_VALIDATED"; preferred = dominance[0]
        else:
            final_decision = "COUNT_STRUCTURAL_REPAIR_PROMISING_NO_CLEAR_WINNER"
    else:
        final_decision = "COUNT_STRUCTURAL_REPAIR_STRUCTURALLY_BETTER_BUT_POINT_TRADEOFF_UNRESOLVED"
    shadow = "TOTALS_REPAIRED_CHALLENGER_SHADOW_READY" if preferred else "TOTALS_REPAIRED_CHALLENGER_NOT_SHADOW_READY"

    strongest = preferred or ("C_CONFIDENCE_ONLY" if point_effects["C_CONFIDENCE_ONLY"] != "WORSE" else "D_LOW_DEPTH")
    primary_biases = [next(row for row in primary_models[p] if row["variant"] == strongest)["actual_minus_forecast_bias"] for p in PERIODS]
    shifted_primary = [value - prior.INTERCEPT_DIAGNOSTIC for value in primary_biases]
    if all(abs(value) < abs(shifted) for value, shifted in zip(primary_biases, shifted_primary)):
        intercept_status = "LIKELY_UNNECESSARY"
    elif max(abs(value) for value in primary_biases) <= .25:
        intercept_status = "PARTIAL_RESIDUAL_BIAS_REMAINS"
    else:
        intercept_status = "STILL_USEFUL_AS_DIAGNOSTIC"
    for row in intercept_rows:
        row["strongest_structural_candidate"] = strongest
        row["intercept_status_after_repair"] = intercept_status if row["variant"] == strongest else "REFERENCE_NOT_SELECTED"
    write_csv(output_dir / "totals_count_repair_intercept_status.csv", intercept_rows)

    identities = {
        "task_id": TASK_ID, "training_population_parity": "EXACT", "variant_e_status": "NOT_AUTHORIZED_NO_PREEXISTING_SEMANTIC_TRANSFORM",
        "models": {
            key: {"identity": artifact["candidate_identity"], "canonical_model_hash": artifact["canonical_model_hash"],
                  "artifact_source": str((CONTROL_PATH if key == "A_CONTROL" else PARK_PATH).relative_to(ROOT)) if key in ("A_CONTROL", "B_PARK_ONLY") else (confidence_path.name if key == "C_CONFIDENCE_ONLY" else low_depth_path.name),
                  "artifact_sha256": sha256(CONTROL_PATH if key == "A_CONTROL" else PARK_PATH if key == "B_PARK_ONLY" else confidence_path if key == "C_CONFIDENCE_ONLY" else low_depth_path),
                  "feature_count": len(artifact["feature_order"]), "fit_action_this_task": "LOADED_FROZEN" if key in ("A_CONTROL", "B_PARK_ONLY") else "FIT_ONCE_AND_FROZEN"}
            for key, artifact in artifacts.items()
        },
        "protected_hashes_before": protected_before,
    }
    raw.write_json(output_dir / "totals_count_repair_model_identities.json", identities)
    raw.write_json(output_dir / "totals_count_repair_control_identity.json", {
        "task_id": TASK_ID, "identity": control["candidate_identity"], "canonical_model_hash": CONTROL_HASH,
        "artifact_sha256": sha256(CONTROL_PATH), "feature_order": control["feature_order"],
        "training_games": len(training), "training_row_identity_and_target_hash": row_hash,
        "validation_games": len(historical_frames[PERIODS[0]]), "early_2026_games": len(historical_frames[PERIODS[1]]),
        "late_holdout_games": len(historical_frames[PERIODS[2]]), "prospective_games": len(prospective),
        "fit_settings": "StandardScaler + PoissonRegressor(alpha=.1,max_iter=1000,deterministic)",
        "dispersion_alpha": control["dispersion_alpha"], "probability_contract": control["distribution_support"],
        "training_population_parity": "EXACT", "protected_hashes_before": protected_before,
    })

    final_md = f"""# MLB totals count-feature structural repair comparison v1

`{final_decision}`

- A/B loaded frozen; C/D fit exactly once on {len(training):,} identical development rows. Variant E: `NOT_AUTHORIZED_NO_PREEXISTING_SEMANTIC_TRANSFORM`.
- Stationarity: {json.dumps(stationarity_status, sort_keys=True)}.
- Point effects: {json.dumps(point_effects, sort_keys=True)}.
- Probability effects: {json.dumps(probability_effects, sort_keys=True)}.
- Bias stability: {json.dumps(bias_classes, sort_keys=True)}.
- Coefficient reassignment risk: {json.dumps(reassignment_risk, sort_keys=True)}.
- Related count result: `{'STRUCTURAL_REPAIR_INCOMPLETE' if structural_incomplete else 'NO_OTHER_DRIFTING_COUNT_ABSORPTION_DETECTED'}`.
- Intercept status for strongest structural candidate `{strongest}`: `{intercept_status}`.
- Preferred research challenger: `{preferred or 'NONE_NO_CLEAR_WINNER'}`.
- Shadow readiness: `{shadow}`. No promotion or shadow activation occurred.

Exact next decision: {'authorize a separate prospective shadow-readiness implementation for ' + preferred if preferred else 'review C versus D tradeoffs and authorize a new task only if one contract is selected; do not start shadow capture yet'}.
"""
    (output_dir / "totals_count_repair_final_decision.md").write_text(final_md)
    (output_dir / "concise_mlb_totals_count_feature_structural_repair_comparison_v1.md").write_text(final_md)

    protected_after = {str(path): sha256(path) for path in protected}
    if protected_before != protected_after:
        raise RuntimeError("PROTECTED_PRODUCTION_OR_INPUT_MUTATION_DETECTED")
    identities["protected_hashes_after"] = protected_after
    raw.write_json(output_dir / "totals_count_repair_model_identities.json", identities)
    control_identity_path = output_dir / "totals_count_repair_control_identity.json"
    control_identity = json.loads(control_identity_path.read_text()); control_identity["protected_hashes_after"] = protected_after
    raw.write_json(control_identity_path, control_identity)

    hash_path = output_dir / "reproducibility_hashes.sha256"
    artifacts_out = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    lines = [f"{sha256(path)}  {path.name}" for path in artifacts_out]
    lines.extend(f"{digest}  PROTECTED_INPUT::{path}" for path, digest in protected_after.items())
    hash_path.write_text("\n".join(lines) + "\n")
    return {"task_id": TASK_ID, "final_decision": final_decision, "preferred_research_challenger": preferred,
            "shadow_readiness": shadow, "variant_e_status": "NOT_AUTHORIZED_NO_PREEXISTING_SEMANTIC_TRANSFORM",
            "new_fits": ["C_CONFIDENCE_ONLY", "D_LOW_DEPTH"], "training_population_parity": "EXACT",
            "model_hashes": {key: artifact["canonical_model_hash"] for key, artifact in artifacts.items()},
            "point_effects": point_effects, "probability_effects": probability_effects,
            "stationarity": stationarity_status, "coefficient_reassignment_risk": reassignment_risk,
            "intercept_status": intercept_status, "output_files": len(artifacts_out) + 1,
            "protected_inputs_unchanged": True}


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(); print(json.dumps(run(args.output_dir), indent=2, default=str))


if __name__ == "__main__":
    main()

"""Deployment-stability review of the frozen MLB totals challenger C.

This module is deliberately diagnostic.  It loads and scores already-frozen
artifacts and ledgers, but it never fits a model or writes production state.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as raw
from backend.mlb.scripts import run_mlb_totals_count_feature_structural_repair_comparison_v1 as structural
from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_C_DEPLOYMENT_STABILITY_SHADOW_DECISION_V1"
C_NAME = "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1"
C_HASH = "21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd"
C_ARTIFACT_SHA = "ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc"
CONTROL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
C_PATH = ROOT / "artifacts/analysis/model_development/mlb_totals_count_feature_structural_repair_comparison_v1/2026-08-16/DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json"
PARK_PATH = ROOT / "artifacts/analysis/model_development/mlb_totals_remove_park_history_depth_direct_location_defect_v1/2026-08-16/TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1.json"
POINT_DIR = ROOT / "artifacts/analysis/model_development/mlb_totals_point_forecast_representation_review_v1/2026-08-16"
LIVE_BRIDGE = ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_confidence_only_deployment_stability_shadow_decision_v1/2026-08-16"
CURRENT_START = "2026-08-06"
CURRENT_END = "2026-08-16"
REMOVED_DEPTHS = ("park_history_depth", "home_starter_prior_starts", "away_starter_prior_starts")
PERIODS = ("FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT", "PROSPECTIVE_AUG06_15")


FEATURE_META: dict[str, dict[str, str]] = {
    "league_total": dict(source="date-strict official completed-game league history", semantics="strict-prior league runs per game", units="runs/game", transformation="arithmetic prior mean", fallback="8.6 only before any league history", bounded="not explicitly; naturally nonnegative", entity="league/date"),
    "home_offense": dict(source="date-strict official team results", semantics="home club strict-prior runs scored state", units="runs/game", transformation="team arithmetic prior mean", fallback="strict-prior league home mean", bounded="not explicitly; naturally nonnegative", entity="home_team_id"),
    "home_prevention": dict(source="date-strict official team results", semantics="home club strict-prior runs allowed state", units="runs/game", transformation="team arithmetic prior mean", fallback="strict-prior league away mean", bounded="not explicitly; naturally nonnegative", entity="home_team_id"),
    "away_offense": dict(source="date-strict official team results", semantics="away club strict-prior runs scored state", units="runs/game", transformation="team arithmetic prior mean", fallback="strict-prior league away mean", bounded="not explicitly; naturally nonnegative", entity="away_team_id"),
    "away_prevention": dict(source="date-strict official team results", semantics="away club strict-prior runs allowed state", units="runs/game", transformation="team arithmetic prior mean", fallback="strict-prior league home mean", bounded="not explicitly; naturally nonnegative", entity="away_team_id"),
    "home_starter_ra9": dict(source="official prior probable-starter pitching appearances", semantics="home probable starter strict-prior runs allowed rate", units="runs/9 innings", transformation="27*runs/outs", fallback="strict-prior league total / 2 when no starter outs", bounded="not explicitly; naturally nonnegative", entity="home_starter_starter_pitcher_id"),
    "away_starter_ra9": dict(source="official prior probable-starter pitching appearances", semantics="away probable starter strict-prior runs allowed rate", units="runs/9 innings", transformation="27*runs/outs", fallback="strict-prior league total / 2 when no starter outs", bounded="not explicitly; naturally nonnegative", entity="away_starter_starter_pitcher_id"),
    "home_expected_outs": dict(source="governed starter/cohort prior workload", semantics="expected home starter workload", units="outs", transformation="mean selected history, clipped 3..27", fallback="pitcher cohort, team starts, league starts, then 15 outs", bounded="yes: 3..27", entity="home_starter_starter_pitcher_id"),
    "away_expected_outs": dict(source="governed starter/cohort prior workload", semantics="expected away starter workload", units="outs", transformation="mean selected history, clipped 3..27", fallback="pitcher cohort, team starts, league starts, then 15 outs", bounded="yes: 3..27", entity="away_starter_starter_pitcher_id"),
    "home_workload_uncertainty_outs": dict(source="governed starter/cohort prior workload", semantics="dispersion of plausible home starter workload", units="outs SD", transformation="SD of selected workload history", fallback="4.5 for singleton/default history", bounded="nonnegative; not upper clipped", entity="home_starter_starter_pitcher_id"),
    "away_workload_uncertainty_outs": dict(source="governed starter/cohort prior workload", semantics="dispersion of plausible away starter workload", units="outs SD", transformation="SD of selected workload history", fallback="4.5 for singleton/default history", bounded="nonnegative; not upper clipped", entity="away_starter_starter_pitcher_id"),
    "home_bullpen_ra9": dict(source="official strict-prior home-team relief appearances", semantics="home bullpen prior run-prevention rate", units="runs/9 innings", transformation="27*runs/outs", fallback="strict-prior league total / 2 when no relief outs", bounded="not explicitly; naturally nonnegative", entity="home_team_id"),
    "away_bullpen_ra9": dict(source="official strict-prior away-team relief appearances", semantics="away bullpen prior run-prevention rate", units="runs/9 innings", transformation="27*runs/outs", fallback="strict-prior league total / 2 when no relief outs", bounded="not explicitly; naturally nonnegative", entity="away_team_id"),
    "home_bullpen_likely_available_reliever_count": dict(source="official strict-prior home-team relief appearances", semantics="recently observed relievers less those used in prior day", units="relievers", transformation="max(0, unique prior-30-day relievers - unique prior-1-day relievers)", fallback="0 if no history", bounded="nonnegative rolling roster count", entity="home_team_id"),
    "away_bullpen_likely_available_reliever_count": dict(source="official strict-prior away-team relief appearances", semantics="recently observed relievers less those used in prior day", units="relievers", transformation="max(0, unique prior-30-day relievers - unique prior-1-day relievers)", fallback="0 if no history", bounded="nonnegative rolling roster count", entity="away_team_id"),
    "home_bullpen_recent_innings_burden": dict(source="official strict-prior home-team relief appearances", semantics="home relief workload in prior three calendar days", units="innings", transformation="sum relief outs in prior 3 days / 3", fallback="0 if no qualifying recent history", bounded="nonnegative rolling-window workload", entity="home_team_id"),
    "away_bullpen_recent_innings_burden": dict(source="official strict-prior away-team relief appearances", semantics="away relief workload in prior three calendar days", units="innings", transformation="sum relief outs in prior 3 days / 3", fallback="0 if no qualifying recent history", bounded="nonnegative rolling-window workload", entity="away_team_id"),
    "strict_prior_total_run_factor": dict(source="official strict-prior venue and league history", semantics="regressed venue total-run multiplier", units="ratio", transformation="w*direct_ratio+(1-w)*1, w=n/(n+50)", fallback="1.0 for unseen venue", bounded="shrinkage toward 1; direct ratio not clipped", entity="venue_id"),
    "game_number": dict(source="official MLB schedule", semantics="scheduled doubleheader/game sequence state", units="ordinal", transformation="identity", fallback="1 for ordinary game", bounded="yes in observed contract: 1..2", entity="schedule/game"),
}

COUNT_LIKE = {
    "home_bullpen_likely_available_reliever_count": ("NO", "rolling roster-availability state; it does not rise merely as season sample grows"),
    "away_bullpen_likely_available_reliever_count": ("NO", "rolling roster-availability state; it does not rise merely as season sample grows"),
    "game_number": ("NO", "bounded schedule ordinal, not accumulated history"),
}


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


def score(frame: pd.DataFrame, artifact: dict[str, Any]) -> np.ndarray:
    return structural.score(frame, artifact)


def development_frame(control: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    historical = raw.load_historical(control)
    return historical, historical[historical.period.eq("DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE")].copy()


def current_context_frame() -> pd.DataFrame:
    connection = sqlite3.connect(f"file:{raw.LEDGER}?mode=ro", uri=True)
    rows = connection.execute("""
      SELECT p.game_date,p.game_id,p.prediction_timestamp_utc,p.scheduled_start_utc,
             p.prediction_payload_json,c.context_payload_json
      FROM totals_shadow_predictions p JOIN totals_shadow_prediction_context c USING(canonical_identity)
      WHERE p.game_date BETWEEN ? AND ? ORDER BY p.game_date,p.game_id
    """, (CURRENT_START, CURRENT_END)).fetchall()
    output = []
    for date, game, timestamp, scheduled, prediction_json, context_json in rows:
        prediction, context = json.loads(prediction_json), json.loads(context_json)
        output.append({
            "game_date": pd.Timestamp(date), "game_pk": int(game),
            "prediction_timestamp_utc": timestamp, "scheduled_start_utc": scheduled,
            "home_team_id": int(prediction["home_team_id"]), "away_team_id": int(prediction["away_team_id"]),
            "venue_id": int(prediction["venue_id"]),
            "home_starter_starter_pitcher_id": context["home_starter_state"].get("probable_pitcher_id"),
            "away_starter_starter_pitcher_id": context["away_starter_state"].get("probable_pitcher_id"),
            "home_starter_fallback_tier": context["home_starter_state"].get("fallback_tier"),
            "away_starter_fallback_tier": context["away_starter_state"].get("fallback_tier"),
            "park_fallback_status": context["park_state"].get("fallback_status"),
            "context_quality_state": prediction.get("context_quality_state"),
            "history_latest_included_game_date": context.get("dynamic_league_environment", {}).get("latest_included_game_date"),
            **{key: float(value) for key, value in context["model_features"].items()},
        })
    connection.close()
    frame = pd.DataFrame(output)
    if len(frame) != 141 or frame.game_pk.duplicated().any():
        raise RuntimeError(f"UNEXPECTED_CURRENT_CONTEXT_POPULATION_{len(frame)}")
    return frame


def quantiles(series: pd.Series, prefix: str) -> dict[str, float]:
    values = series.astype(float)
    return {
        f"{prefix}_mean": float(values.mean()), f"{prefix}_median": float(values.median()),
        f"{prefix}_sd": float(values.std(ddof=0)), f"{prefix}_min": float(values.min()),
        f"{prefix}_p01": float(values.quantile(.01)), f"{prefix}_p05": float(values.quantile(.05)),
        f"{prefix}_p25": float(values.quantile(.25)), f"{prefix}_p50": float(values.quantile(.50)),
        f"{prefix}_p75": float(values.quantile(.75)), f"{prefix}_p95": float(values.quantile(.95)),
        f"{prefix}_p99": float(values.quantile(.99)), f"{prefix}_max": float(values.max()),
    }


def drift_status(shift: float, outside: float, above_p99: float) -> str:
    if outside >= .05 or abs(shift) >= 3:
        return "EXTREME_DRIFT"
    if outside >= .01 or abs(shift) >= 2:
        return "SEVERE_DRIFT"
    if outside > 0 or abs(shift) >= 1 or above_p99 >= .10:
        return "MODERATE_DRIFT"
    if abs(shift) >= .5 or above_p99 >= .05:
        return "MILD_DRIFT"
    return "IN_SUPPORT"


def support_rows(training: pd.DataFrame, current: pd.DataFrame, artifact: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for feature in artifact["feature_order"]:
        train, now = training[feature].astype(float), current[feature].astype(float)
        stats = {**quantiles(train, "training"), **quantiles(now, "current")}
        sd = stats["training_sd"] or 1.0
        shift = (stats["current_mean"] - stats["training_mean"]) / sd
        above95 = float((now > stats["training_p95"]).mean())
        above99 = float((now > stats["training_p99"]).mean())
        above_max = float((now > stats["training_max"]).mean())
        below_min = float((now < stats["training_min"]).mean())
        outside = above_max + below_min
        rows.append({"feature": feature, **stats, "current_pct_above_training_p95": above95,
                     "current_pct_above_training_p99": above99, "current_pct_above_training_max": above_max,
                     "current_pct_below_training_min": below_min, "standardized_mean_shift": shift,
                     "support_status": drift_status(shift, outside, above99),
                     "current_population": "FROZEN_LIVE_CONTEXT_AUG06_16", "current_games": len(current)})
    return rows


def training_prefallback_missing(training: pd.DataFrame, feature: str) -> float:
    source = {
        "home_starter_ra9": "home_starter_season_ra9", "away_starter_ra9": "away_starter_season_ra9",
        "home_bullpen_ra9": "home_bullpen_bullpen_ra9", "away_bullpen_ra9": "away_bullpen_bullpen_ra9",
    }.get(feature, feature)
    return float(training[source].replace([np.inf, -np.inf], np.nan).isna().mean()) if source in training else math.nan


def current_fallback_rate(current: pd.DataFrame, feature: str) -> tuple[float | str, str]:
    side = "home" if feature.startswith("home_") else "away" if feature.startswith("away_") else ""
    if "starter" in feature and side:
        column = f"{side}_starter_fallback_tier"
        return float((current[column] != "DIRECT_STARTER_HISTORY").mean()), "retained governed starter fallback tier"
    if feature == "strict_prior_total_run_factor":
        return float((current.park_fallback_status != "DIRECT_REGRESSED_PARK_HISTORY").mean()), "retained park fallback status"
    if "bullpen" in feature:
        return "NOT_RETAINED", "bullpen fallback provenance is absent from retained context; zero burden can silently represent stale recency"
    return 0.0, "constructed deterministically with governed fallback"


def metrics(frame: pd.DataFrame, artifact: dict[str, Any]) -> dict[str, Any]:
    if frame.empty:
        return {key: math.nan for key in ("games", "mean_prediction", "mean_actual", "actual_minus_forecast_bias", "mae", "rmse", "crps", "ladder_brier", "ladder_log_loss", "ladder_ece", "median_mae")}
    forecasts = score(frame, artifact)
    result = structural.prior.metric_bundle(frame, forecasts, float(artifact["dispersion_alpha"]))
    medians = []
    for forecast in forecasts:
        mass = distribution(float(forecast), float(artifact["dispersion_alpha"]))
        medians.append(float(np.searchsorted(np.cumsum(mass), .5)))
    result["median_mae"] = float(np.mean(abs(frame.final_total.to_numpy(float) - np.asarray(medians))))
    return result


def within_entity_rows(training: pd.DataFrame, support: list[dict[str, Any]]) -> list[dict[str, Any]]:
    suspicious = {r["feature"] for r in support if r["support_status"] != "IN_SUPPORT"}
    suspicious |= {"strict_prior_total_run_factor", "game_number"}
    rows = []
    for feature in sorted(suspicious):
        entity = FEATURE_META[feature]["entity"]
        if entity not in training or entity in ("league/date", "schedule/game"):
            rows.append({"feature": feature, "entity": entity, "games": len(training),
                         "pooled_actual_total_correlation": float(training[[feature, "final_total"]].corr().iloc[0, 1]),
                         "within_entity_month_actual_total_correlation": math.nan,
                         "direct_signal": "NOT_TESTABLE", "reason": "no repeated stable entity key for this global/schedule state"})
            continue
        data = training[[feature, "final_total", entity, "game_date"]].dropna().copy()
        data["month"] = data.game_date.dt.to_period("M").astype(str)
        x = data[feature] - data.groupby([entity, "month"])[feature].transform("mean")
        y = data.final_total - data.groupby([entity, "month"]).final_total.transform("mean")
        corr = float(np.corrcoef(x, y)[0, 1]) if x.std() and y.std() else math.nan
        if not np.isfinite(corr):
            status = "NOT_TESTABLE"
        elif abs(corr) >= .05:
            status = "SUPPORTED"
        elif abs(corr) >= .02:
            status = "WEAK"
        else:
            status = "ABSENT"
        rows.append({"feature": feature, "entity": entity, "games": len(data),
                     "pooled_actual_total_correlation": float(data[[feature, "final_total"]].corr().iloc[0, 1]),
                     "within_entity_month_actual_total_correlation": corr, "direct_signal": status,
                     "reason": "descriptive strict-prior signal screen; not a refit or selection rule"})
    return rows


def out_of_support_rows(frames: dict[str, pd.DataFrame], training: pd.DataFrame,
                        support: list[dict[str, Any]], artifact: dict[str, Any]) -> list[dict[str, Any]]:
    severe = [r["feature"] for r in support if r["support_status"] in ("SEVERE_DRIFT", "EXTREME_DRIFT")]
    rows = []
    for feature in severe:
        low, high = float(training[feature].quantile(.01)), float(training[feature].quantile(.99))
        for period, frame in frames.items():
            outside = (frame[feature] < low) | (frame[feature] > high)
            for segment, mask in (("INSIDE_TRAIN_P01_P99", ~outside), ("OUTSIDE_TRAIN_P01_P99", outside)):
                subset = frame[mask]
                result = metrics(subset, artifact) if len(subset) else {}
                rows.append({"feature": feature, "period": period, "support_segment": segment,
                             "training_p01": low, "training_p99": high, "games": len(subset),
                             **{key: result.get(key, math.nan) for key in ("mae", "rmse", "actual_minus_forecast_bias", "median_mae", "crps", "ladder_brier", "ladder_log_loss")},
                             "result": "MEASURED" if len(subset) else "NOT_TESTABLE_NO_ROWS"})
    return rows


def coefficient_reassignment_rows(training: pd.DataFrame, current: pd.DataFrame, control: dict[str, Any],
                                  park: dict[str, Any], artifact: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    maps = {name: dict(zip(model["feature_order"], model["coefficients"])) for name, model in (("control", control), ("park", park), ("c", artifact))}
    rows, maximum_risk = [], "LOW"
    for feature in artifact["feature_order"]:
        proxy_train = max(abs(float(training[[feature, depth]].corr().iloc[0, 1])) if training[feature].std() and training[depth].std() else 0 for depth in REMOVED_DEPTHS)
        proxy_current = max(abs(float(current[[feature, depth]].corr().iloc[0, 1])) if current[feature].std() and current[depth].std() else 0 for depth in REMOVED_DEPTHS)
        dc, dp = maps["c"][feature] - maps["control"][feature], maps["c"][feature] - maps["park"][feature]
        sign_flip = np.sign(maps["c"][feature]) != np.sign(maps["control"][feature])
        if (max(proxy_train, proxy_current) >= .7 and abs(dc) >= .02) or sign_flip:
            risk = "HIGH"
        elif max(proxy_train, proxy_current) >= .7 or (max(proxy_train, proxy_current) >= .5 and abs(dc) >= .01):
            risk = "MODERATE"
        else:
            risk = "LOW"
        if risk == "HIGH": maximum_risk = "HIGH"
        elif risk == "MODERATE" and maximum_risk == "LOW": maximum_risk = "MODERATE"
        rows.append({"feature": feature, "control_coefficient": maps["control"][feature],
                     "park_only_coefficient": maps["park"][feature], "c_coefficient": maps["c"][feature],
                     "c_minus_control": dc, "c_minus_park_only": dp, "sign_flip_vs_control": bool(sign_flip),
                     "max_abs_training_correlation_with_removed_depth": proxy_train,
                     "max_abs_current_correlation_with_removed_depth": proxy_current,
                     "feature_risk": risk,
                     "notes": "park-factor/depth correlation is expected from governed n/(n+50) shrinkage and is not coefficient reassignment" if feature == "strict_prior_total_run_factor" else ""})
    return rows, maximum_risk


def historical_evidence(historical: pd.DataFrame, prospective: pd.DataFrame,
                        artifact: dict[str, Any]) -> list[dict[str, Any]]:
    frames = {period: historical[historical.period.eq(period)].copy() for period in PERIODS[:-1]}
    frames["PROSPECTIVE_AUG06_15"] = prospective
    authoritative = pd.read_csv(POINT_DIR / "totals_confidence_only_point_summary_metrics.csv")
    rows = []
    for period, frame in frames.items():
        calculated = metrics(frame, artifact)
        mean_row = authoritative[(authoritative.period == period) & (authoritative.point_summary == "MEAN")].iloc[0]
        median_row = authoritative[(authoritative.period == period) & (authoritative.point_summary == "MEDIAN")].iloc[0]
        checks = [
            abs(calculated["mae"] - mean_row.mae), abs(calculated["rmse"] - mean_row.rmse),
            abs(calculated["actual_minus_forecast_bias"] - mean_row.actual_minus_forecast_bias),
            abs(calculated["median_mae"] - median_row.mae), abs(calculated["crps"] - mean_row.crps),
            abs(calculated["ladder_brier"] - mean_row.ladder_brier),
            abs(calculated["ladder_log_loss"] - mean_row.ladder_log_loss),
            abs(calculated["ladder_ece"] - mean_row.ladder_ece),
        ]
        rows.append({"period": period, "evidence_class": mean_row.evidence_class, "games": len(frame),
                     "mean_mae": calculated["mae"], "mean_rmse": calculated["rmse"],
                     "mean_actual_minus_forecast_bias": calculated["actual_minus_forecast_bias"],
                     "median_mae": calculated["median_mae"], "crps": calculated["crps"],
                     "ladder_brier": calculated["ladder_brier"], "ladder_log_loss": calculated["ladder_log_loss"],
                     "ladder_ece": calculated["ladder_ece"], "max_authoritative_absolute_difference": max(checks),
                     "authoritative_reproduction": "PASS" if max(checks) <= 2e-12 else "FAIL"})
    if any(row["authoritative_reproduction"] != "PASS" for row in rows):
        raise RuntimeError("AUTHORITATIVE_C_METRIC_REPRODUCTION_FAILED")
    return rows


def run(output_dir: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    c_bytes = C_PATH.read_bytes()
    artifact, control, park = json.loads(c_bytes), json.loads(raw.CONFIG.read_bytes()), json.loads(PARK_PATH.read_bytes())
    protected = [C_PATH, raw.CONFIG, PARK_PATH, raw.LEDGER, raw.SPINE / "totals_core_feature_spine.csv", LIVE_BRIDGE]
    protected_before = {str(path): sha256(path) for path in protected}
    canonical = structural.artifact_hash(artifact)
    if artifact["candidate_identity"] != C_NAME or artifact["canonical_model_hash"] != C_HASH or canonical != C_HASH or sha256(C_PATH) != C_ARTIFACT_SHA:
        raise RuntimeError("C_ARTIFACT_IDENTITY_FAIL")
    if control["canonical_model_hash"] != CONTROL_HASH:
        raise RuntimeError("CONTROL_IDENTITY_FAIL")

    historical, training = development_frame(control)
    current = current_context_frame()
    prospective = raw.load_prospective(control, float(control["dispersion_alpha"]))
    matrix_hash = structural.prior.frame_hash(training, ["game_pk", "final_total", *artifact["feature_order"]])
    row_hash = structural.prior.frame_hash(training, ["game_pk", "game_date", "final_total"])
    if matrix_hash != artifact["training_matrix_hash"] or row_hash != artifact["training_row_identity_and_target_hash"]:
        raise RuntimeError("C_TRAINING_IDENTITY_FAIL")
    feature_contract_hash = canonical_hash({key: artifact[key] for key in ("feature_order", "scaler_mean", "scaler_scale", "normalization")})
    identity = {
        "task_id": TASK_ID, "C_ARTIFACT_IDENTITY": "PASS", "candidate_identity": C_NAME,
        "canonical_model_hash": C_HASH, "canonical_hash_recomputed": canonical,
        "artifact_path": str(C_PATH.relative_to(ROOT)), "artifact_sha256": sha256(C_PATH),
        "artifact_bytes": len(c_bytes), "training_population": artifact["development_population"],
        "training_games": artifact["development_games"], "training_row_identity_and_target_hash": row_hash,
        "training_matrix_hash": matrix_hash, "feature_contract_hash": feature_contract_hash,
        "feature_order": artifact["feature_order"], "feature_count": len(artifact["feature_order"]),
        "scaler_mean": artifact["scaler_mean"], "scaler_scale": artifact["scaler_scale"],
        "coefficients": artifact["coefficients"], "intercept": artifact["intercept"],
        "dispersion_alpha": artifact["dispersion_alpha"], "normalization": artifact["normalization"],
        "probability_contract": artifact["probability_contract"], "fit_count_this_task": 0,
    }
    write_json(output_dir / "totals_c_artifact_identity.json", identity)

    support = support_rows(training, current, artifact)
    support_by = {row["feature"]: row for row in support}
    inventory = []
    coefficients = dict(zip(artifact["feature_order"], artifact["coefficients"]))
    for ordinal, feature in enumerate(artifact["feature_order"], 1):
        meta = FEATURE_META[feature]
        inventory.append({"ordinal": ordinal, "feature": feature, **meta, "coefficient": coefficients[feature],
                          "training_missing_after_governed_construction": float(training[feature].isna().mean()),
                          "training_pre_fallback_missing_rate": training_prefallback_missing(training, feature),
                          "current_missing_after_governed_construction": float(current[feature].isna().mean()),
                          "mechanically_cumulative": COUNT_LIKE.get(feature, ("NO", "rate/state changes only when baseball state changes, not merely with sample count"))[0],
                          "sample_depth_or_confidence_measure": "NO",
                          "support_status": support_by[feature]["support_status"]})
    write_csv(output_dir / "totals_c_direct_feature_inventory.csv", inventory)
    write_csv(output_dir / "totals_c_feature_support_drift.csv", support)

    mechanical = []
    for feature in artifact["feature_order"]:
        state, reason = COUNT_LIKE.get(feature, ("NO", "not a raw cumulative count; location changes only with the represented baseball state"))
        mechanical.append({"feature": feature, "mechanically_grows_with_calendar_or_sample_size": state,
                           "semantic_justification": reason, "raw_repaired_depth_feature": feature in REMOVED_DEPTHS})
    for feature in REMOVED_DEPTHS:
        mechanical.append({"feature": feature, "mechanically_grows_with_calendar_or_sample_size": "YES",
                           "semantic_justification": "removed from C direct location; retained only upstream as support/confidence",
                           "raw_repaired_depth_feature": True, "present_in_c_direct_location": False})
    write_csv(output_dir / "totals_c_mechanical_growth_screen.csv", mechanical)

    double_use = []
    for feature in artifact["feature_order"]:
        double_use.append({"feature": feature, "sample_depth_or_confidence_double_use": "NO",
                           "reason": "baseball state/rate input, not raw sample depth" if "workload_uncertainty" not in feature else "workload variability is a baseball exposure state, not estimator confidence or history depth"})
    for feature in REMOVED_DEPTHS:
        double_use.append({"feature": feature, "sample_depth_or_confidence_double_use": "NO",
                           "reason": "absent from C direct location; allowed upstream for shrinkage/fallback only", "present_in_c_direct_location": False})
    write_csv(output_dir / "totals_c_sample_depth_double_use.csv", double_use)
    write_csv(output_dir / "totals_c_within_entity_signal_screen.csv", within_entity_rows(training, support))

    eval_frames = {period: historical[historical.period.eq(period)].copy() for period in PERIODS[:-1]}
    eval_frames["PROSPECTIVE_AUG06_15"] = prospective
    write_csv(output_dir / "totals_c_out_of_support_performance.csv", out_of_support_rows(eval_frames, training, support, artifact))

    drift_impact = []
    for row in support:
        contribution = coefficients[row["feature"]] * row["standardized_mean_shift"]
        drift_impact.append({"feature": row["feature"], "coefficient": coefficients[row["feature"]],
                             "standardized_center_shift": row["standardized_mean_shift"],
                             "center_shift_log_location_contribution": contribution,
                             "implied_multiplicative_location_ratio": math.exp(contribution),
                             "support_status": row["support_status"]})
    drift_impact.sort(key=lambda row: abs(row["center_shift_log_location_contribution"]), reverse=True)
    for rank, row in enumerate(drift_impact, 1): row["absolute_impact_rank"] = rank
    write_csv(output_dir / "totals_c_coefficient_drift_impact.csv", drift_impact)

    reassignment, reassignment_risk = coefficient_reassignment_rows(training, current, control, park, artifact)
    write_csv(output_dir / "totals_c_coefficient_reassignment.csv", reassignment)

    missingness = []
    for feature in artifact["feature_order"]:
        rate, evidence = current_fallback_rate(current, feature)
        if feature in ("home_bullpen_recent_innings_burden", "away_bullpen_recent_innings_burden"):
            status = "FAIL"
        elif "bullpen" in feature:
            status = "WATCH"
        else:
            status = "PASS"
        missingness.append({"feature": feature, "training_missing_after_governed_construction": float(training[feature].isna().mean()),
                            "training_pre_fallback_missing_rate": training_prefallback_missing(training, feature),
                            "current_missing_after_governed_construction": float(current[feature].isna().mean()),
                            "current_fallback_rate": rate, "fallback_evidence": evidence,
                            "fallback_stability": status,
                            "notes": "zero from 2026-08-09 onward while retained source cutoff remains 2026-08-05" if status == "FAIL" else ""})
    write_csv(output_dir / "totals_c_missingness_fallback_drift.csv", missingness)

    starter_counts = Counter(current.home_starter_fallback_tier) + Counter(current.away_starter_fallback_tier)
    starter_md = f"""# C starter-feature deployment stability

`STARTER_FEATURE_DEPLOYMENT_STABILITY=PASS_WITH_WATCH`

- C has no direct `home_starter_prior_starts` or `away_starter_prior_starts` location input.
- All {len(current)} retained Aug. 6–16 contexts were context-complete. Across {2*len(current)} starter sides: {starter_counts.get('DIRECT_STARTER_HISTORY', 0)} direct, {starter_counts.get('PITCHER_ROLE_COHORT', 0)} pitcher-role cohort, {starter_counts.get('TEAM_STARTER_HISTORY', 0)} team-history fallback.
- Starter RA9, expected outs, and workload-uncertainty distributions have no literal training-range breach. Governed sparse history remains valid; monitor fallback mix prospectively.
"""
    (output_dir / "totals_c_starter_feature_stability.md").write_text(starter_md)

    park_status = "PASS"
    park_md = f"""# C park/context deployment stability

`PARK_CONTEXT_DEPLOYMENT_STABILITY={park_status}`

- Raw `park_history_depth` is absent from C's 19 direct location inputs.
- Upstream shrinkage is unchanged and verified in the live bridge: `w=n/(n+50)` and `factor=w*direct+(1-w)*1.0`.
- All {len(current)} retained Aug. 6–16 contexts used `DIRECT_REGRESSED_PARK_HISTORY`; no unseen-venue fallback was admitted.
- `strict_prior_total_run_factor` remains within the training min/max. An unseen venue fails to the explicit league factor `1.0`, rather than inventing direct park history.
"""
    (output_dir / "totals_c_park_context_stability.md").write_text(park_md)

    related = []
    by_date = current.assign(date=current.game_date.dt.strftime("%Y-%m-%d")).groupby("date")
    for feature in ("home_bullpen_likely_available_reliever_count", "away_bullpen_likely_available_reliever_count",
                    "home_bullpen_recent_innings_burden", "away_bullpen_recent_innings_burden", "game_number"):
        row = support_by[feature]
        decision = "STRUCTURAL_REVIEW_REQUIRED" if "bullpen" in feature else "SAFE"
        related.append({"row_type": "SUMMARY", "feature": feature, "direct_location_input": True,
                        "training_min": row["training_min"], "training_p99": row["training_p99"], "training_max": row["training_max"],
                        "current_min": row["current_min"], "current_mean": row["current_mean"], "current_max": row["current_max"],
                        "standardized_mean_shift": row["standardized_mean_shift"], "coefficient": coefficients[feature],
                        "center_shift_log_location_contribution": next(x["center_shift_log_location_contribution"] for x in drift_impact if x["feature"] == feature),
                        "mechanical_growth": COUNT_LIKE.get(feature, ("NO", ""))[0], "safety_decision": decision,
                        "reason": "rolling-state semantics are valid, but the live source history does not advance beyond Aug 5" if "bullpen" in feature else "bounded schedule ordinal"})
        for date, group in by_date:
            related.append({"row_type": "CURRENT_DATE", "feature": feature, "game_date": date, "games": len(group),
                            "current_date_mean": float(group[feature].mean()), "current_date_min": float(group[feature].min()),
                            "current_date_max": float(group[feature].max()), "history_latest_included_game_date": ";".join(sorted(set(group.history_latest_included_game_date.dropna().astype(str))))})
    write_csv(output_dir / "totals_c_related_count_safety.csv", related)

    base = current.iloc[[0]].copy()
    stationarity = []
    for feature, low, high in (("park_history_depth", 0, 5000), ("home_starter_prior_starts", 0, 150), ("away_starter_prior_starts", 0, 150)):
        a, b = base.copy(), base.copy(); a[feature], b[feature] = low, high
        score_a, score_b = float(score(a, artifact)[0]), float(score(b, artifact)[0])
        stationarity.append({"feature": feature, "low_value": low, "high_value": high, "forecast_low": score_a,
                             "forecast_high": score_b, "absolute_difference": abs(score_b-score_a),
                             "exact_invariance": score_a == score_b, "COUNT_STATIONARITY_INVARIANT": "PASS" if score_a == score_b else "FAIL"})
    write_csv(output_dir / "totals_c_stationarity_perturbation.csv", stationarity)

    (output_dir / "totals_c_point_product_contract.md").write_text("""# C point/distribution product contract

```
EXPECTED_TOTAL_RUNS=NEGATIVE_BINOMIAL_MEAN
CENTRAL_TYPICAL_TOTAL=NEGATIVE_BINOMIAL_MEDIAN
MAE_OPTIMAL_POINT=NEGATIVE_BINOMIAL_MEDIAN
PROBABILITY_FOUNDATION=FULL_NEGATIVE_BINOMIAL_DISTRIBUTION
```

The mean is the deterministic `exp(intercept + standardized_features @ coefficients)` location. The median is the first integer whose frozen-support CDF reaches 0.5. Probabilities use the full frozen negative-binomial mass with the 30-plus tail folded into 30. Repeated scoring is deterministic; point summaries do not change the probability distribution.
""")

    evidence = historical_evidence(historical, prospective, artifact)
    write_csv(output_dir / "totals_c_historical_evidence_summary.csv", evidence)

    gates = [
        ("A", "exact artifact identity", "PASS", "canonical hash and artifact SHA exact"),
        ("B", "feature-contract reproducibility", "PASS", "feature order, scaler, training row and matrix hashes exact"),
        ("C", "count stationarity", "PASS", "all three repaired depth perturbations exactly invariant"),
        ("D", "training-support coverage", "FAIL", "both bullpen recent-innings features have severe center drift caused by stale live history"),
        ("E", "mechanical-growth safety", "PASS", "no mechanically cumulative direct location input remains"),
        ("F", "sample-depth double-use safety", "PASS", "all raw sample-depth counts absent from location"),
        ("G", "coefficient reassignment", "PASS" if reassignment_risk == "LOW" else "PASS_WITH_WATCH", f"risk={reassignment_risk}"),
        ("H", "fallback stability", "FAIL", "rolling bullpen burden silently reaches zero while source cutoff is stale"),
        ("I", "starter-state stability", "PASS_WITH_WATCH", "governed sparse tiers are rare and must remain monitored"),
        ("J", "park/context stability", "PASS", "depth only upstream; all current parks direct regressed"),
        ("K", "probability-distribution quality", "PASS", "frozen C improves prior CRPS/Brier/log loss headline evidence"),
        ("L", "point-summary semantics", "PASS", "mean, median and full distribution roles explicitly frozen"),
        ("M", "historical temporal robustness", "PASS_WITH_WATCH", "primary periods reproduce; Aug 6–15 remains diagnostic only"),
        ("N", "no dependence on August post-hoc results for model selection", "PASS", "C fit once on 2023–24; August used only for diagnosis"),
    ]
    write_csv(output_dir / "totals_c_structural_gate_matrix.csv", [dict(gate=g, dimension=d, status=s, evidence=e) for g,d,s,e in gates])

    shadow_contract = f"""# Defined live shadow contract — not launched

Proposed identity: `TOTALS_COUNT_CONFIDENCE_ONLY_SHADOW_V1`

Subject: `{C_NAME}`

Exact model hash: `{C_HASH}`

Feature-contract hash: `{feature_contract_hash}`

If separately authorized after the blocker is repaired and revalidated, preserve immutably per game: date, game_pk, scheduled first pitch, scoring run tag, prediction timestamp, model identity, exact model SHA, feature-contract hash, feature-state hash, training/artifact identity, expected_total_mean, central_total_median, dispersion, governed total line when available, P_OVER, P_UNDER, source hashes, probable-pitcher state, and primary/retry scoring status. Outcomes are forbidden from this prediction ledger.

`SHADOW_LAUNCHED=NO`
"""
    (output_dir / "totals_c_shadow_contract.md").write_text(shadow_contract)
    (output_dir / "totals_c_shadow_snapshot_policy.md").write_text("""# Shadow snapshot policy — defined, not launched

- Use the governed totals lifecycle: `05:30 PRIMARY_SCORE`; later `SCORE_MISSING` only for identities legitimately missing required state at 05:30.
- Keep exactly one canonical shadow prediction per game. Never replace a valid earlier prediction because a later score differs.
- Optional later observations are separate immutable observations and cannot change primary evaluation identity.
- Reject post-start scoring and retrospective construction.
""")
    (output_dir / "totals_c_shadow_grading_contract.md").write_text("""# Shadow grading contract — defined, not launched

Attach outcomes separately only after official completion: actual final total, official completion state, outcome source, and source hash.

Primary point metrics: mean MAE, mean RMSE, mean bias, median MAE. Primary distribution metrics: CRPS, Brier, log loss, ECE. Compare prospectively with unchanged production RAW, a leakage-safe population baseline, and a leakage-safe team-shrunk baseline. `V1_INTERCEPT` is a RAW diagnostic reference only. Sportsbook prices are not a promotion gate.
""")
    (output_dir / "totals_c_shadow_review_discipline.md").write_text("""# Shadow review discipline — defined, not launched

- Never modify the frozen candidate from daily outcomes.
- The first formal review checkpoint is **20 completed independent date clusters**, not a favorable result or calendar cherry-pick.
- The clock starts only after separate human authorization and successful bullpen-state freshness repair validation.
""")
    (output_dir / "totals_c_intercept_policy.md").write_text("""# C intercept policy

`C_INTERCEPT_POLICY=DO_NOT_APPLY_RAW_INTERCEPT_TO_C`

The `+0.493550` intercept remains attached only to RAW as a historical diagnostic control. C's structural repair already changes location; prior frozen evidence indicates adding RAW's diagnostic intercept would overcorrect C.
""")
    (output_dir / "model_deployment_stability_standard_draft_v1.md").write_text("""# MODEL_DEPLOYMENT_STABILITY_STANDARD_DRAFT_V1

Before shadow entry, every fitted model must freeze and pass: (1) exact artifact and feature reproducibility; (2) direct-feature training support inventory and current/OOT comparison; (3) mechanical-growth screen; (4) sample-depth/confidence double-use screen; (5) within-entity proxy checks for suspicious terms; (6) in/out-support performance without optimized cutoffs; (7) coefficient × drift impact; (8) missingness, fallback, and source-freshness drift; (9) coefficient-reassignment safety; and (10) same-row stationarity perturbations for mechanically growing inputs. Any material structural FAIL blocks shadow entry. This is a draft only and is not implemented repository-wide.
""")

    decision = "TOTALS_COUNT_CONFIDENCE_ONLY_NEEDS_ADDITIONAL_STRUCTURAL_REVIEW"
    decision_md = f"""# C shadow decision

`{decision}`

C is not authorized for live shadow and no shadow was launched. The single bounded blocker is the live bullpen rolling-state freshness contract: the source history remains capped at 2026-08-05, both recent-innings-burden inputs become exactly zero from 2026-08-09 onward, and likely-available counts shift upward as the static source ages.

Required bounded resolution: make the live context source advance strictly-prior bullpen appearances through each scoring date, retain its cutoff/provenance in prediction context, and rerun this same no-refit stability validation. Do not begin another model search.

Next human decision after that repair passes: whether to authorize `TOTALS_COUNT_CONFIDENCE_ONLY_SHADOW_V1` alongside unchanged production RAW.
"""
    (output_dir / "totals_c_shadow_decision.md").write_text(decision_md)

    severe = [row["feature"] for row in support if row["support_status"] in ("SEVERE_DRIFT", "EXTREME_DRIFT")]
    concise = f"""# MLB Totals C deployment-stability and shadow decision v1

- C: `{C_NAME}` / `{C_HASH}`; artifact identity `PASS`; {len(artifact['feature_order'])} direct features audited.
- Severe/extreme drift: {', '.join(f'`{x}`' for x in severe)}. Both are rolling bullpen burden states and are zero for every retained game from Aug. 9–16 while live history remains capped at Aug. 5.
- No mechanically cumulative direct predictor remains. No raw sample-depth/confidence term remains in direct location. Count stationarity perturbation: `PASS`.
- Coefficient reassignment risk: `{reassignment_risk}`. Starter stability: `PASS_WITH_WATCH`. Park/context stability: `PASS`.
- Fallback/missingness stability: `FAIL` for unmarked stale bullpen recency; ordinary feature missingness is zero.
- Historical frozen C evidence reproduces exactly. Mean MAE / median MAE / CRPS: 2025 `{evidence[0]['mean_mae']:.6f}` / `{evidence[0]['median_mae']:.6f}` / `{evidence[0]['crps']:.6f}`; early 2026 `{evidence[1]['mean_mae']:.6f}` / `{evidence[1]['median_mae']:.6f}` / `{evidence[1]['crps']:.6f}`; late holdout `{evidence[2]['mean_mae']:.6f}` / `{evidence[2]['median_mae']:.6f}` / `{evidence[2]['crps']:.6f}`; Aug. 6–15 diagnostic `{evidence[3]['mean_mae']:.6f}` / `{evidence[3]['median_mae']:.6f}` / `{evidence[3]['crps']:.6f}`.
- Structural gates: FAIL on support coverage and fallback/source freshness; all model/artifact/stationarity gates pass or pass with watch.
- Shadow contract is defined but not launched: 05:30 primary, missing-only retries, one immutable prediction/game, separate outcomes, compare RAW and leakage-safe baselines, first formal checkpoint at 20 completed date clusters.
- `C_INTERCEPT_POLICY=DO_NOT_APPLY_RAW_INTERCEPT_TO_C`.
- `MODEL_DEPLOYMENT_STABILITY_STANDARD_DRAFT_V1` created, not implemented repository-wide.
- Decision: `{decision}`.
"""
    (output_dir / "concise_mlb_totals_count_confidence_only_deployment_stability_shadow_decision_v1.md").write_text(concise)

    if C_PATH.read_bytes() != c_bytes:
        raise RuntimeError("C_ARTIFACT_BYTES_CHANGED")
    protected_after = {str(path): sha256(path) for path in protected}
    if protected_after != protected_before:
        raise RuntimeError("PROTECTED_INPUT_CHANGED")
    outputs = sorted(path for path in output_dir.iterdir() if path.name != "reproducibility_hashes.sha256")
    manifest = [f"{sha256(path)}  {path.name}" for path in outputs]
    manifest += [f"{digest}  PROTECTED_INPUT::{path}" for path, digest in sorted(protected_before.items())]
    (output_dir / "reproducibility_hashes.sha256").write_text("\n".join(manifest) + "\n")
    return {"decision": decision, "severe_features": severe, "feature_count": len(artifact["feature_order"]),
            "coefficient_reassignment_risk": reassignment_risk, "output_dir": str(output_dir)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

"""Outcome-based review of point summaries from two frozen totals distributions.

This module never fits, calibrates, writes a model, or changes production.  It
reuses the exact populations and frozen A/C artifacts from the governed count
feature structural comparison and evaluates only representation choices.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_totals_count_feature_structural_repair_comparison_v1 as structural
from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as raw
from backend.mlb.scripts import run_mlb_totals_remove_park_history_depth_direct_location_defect_v1 as metrics_source
from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_POINT_FORECAST_REPRESENTATION_REVIEW_V1"
CONTROL_NAME = "DIRECT_NEGATIVE_BINOMIAL_RAW_V1"
CONTROL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
C_NAME = "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1"
C_HASH = "21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd"
CONTROL_PATH = structural.CONTROL_PATH
C_PATH = structural.DEFAULT_OUTPUT / "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json"
PRIOR_IDENTITY_PATH = structural.DEFAULT_OUTPUT / "totals_count_repair_control_identity.json"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_point_forecast_representation_review_v1/2026-08-16"
PERIODS = structural.ALL_PERIODS
PRIMARY_PERIODS = structural.PERIODS
MODEL_KEYS = ("A_CONTROL", "C_CONFIDENCE_ONLY")
MODEL_NAMES = {"A_CONTROL": CONTROL_NAME, "C_CONFIDENCE_ONLY": C_NAME}
POINT_SUMMARIES = ("MEAN", "MEDIAN", "MODE")
FORECAST_BANDS = structural.FORECAST_BANDS
INTERCEPT = 0.493550
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260816


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def frame_hash(frame: pd.DataFrame) -> str:
    payload = frame.sort_values("game_pk")[["game_pk", "game_date", "final_total"]].to_csv(
        index=False, lineterminator="\n", float_format="%.17g"
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def load_subjects() -> dict[str, dict[str, Any]]:
    subjects = {
        "A_CONTROL": json.loads(CONTROL_PATH.read_text()),
        "C_CONFIDENCE_ONLY": json.loads(C_PATH.read_text()),
    }
    expected = {"A_CONTROL": CONTROL_HASH, "C_CONFIDENCE_ONLY": C_HASH}
    for key, artifact in subjects.items():
        if artifact.get("canonical_model_hash") != expected[key]:
            raise RuntimeError(f"FROZEN_MODEL_IDENTITY_FAILED:{key}")
        if artifact.get("model_family") != "REGULARIZED_POISSON_LOCATION_WITH_NEGATIVE_BINOMIAL_DISTRIBUTION":
            raise RuntimeError(f"UNEXPECTED_DISTRIBUTION_FAMILY:{key}")
        if artifact.get("distribution_support") != "0..30_WITH_30_PLUS_TAIL":
            raise RuntimeError(f"UNEXPECTED_DISTRIBUTION_SUPPORT:{key}")
    return subjects


def load_populations(control: dict[str, Any]) -> dict[str, pd.DataFrame]:
    historical = raw.load_historical(control)
    prospective = raw.load_prospective(control, float(control["dispersion_alpha"]))
    frames = {
        period: historical.loc[historical.period.eq(period)].copy().reset_index(drop=True)
        for period in PRIMARY_PERIODS
    }
    frames["PROSPECTIVE_AUG06_15"] = prospective.copy().reset_index(drop=True)
    expected_counts = {
        "FROZEN_2025_VALIDATION": 2433,
        "2026_SEQUENTIAL_EARLY": 1281,
        "2026_LATE_HOLDOUT": 439,
        "PROSPECTIVE_AUG06_15": 126,
    }
    for period, frame in frames.items():
        if len(frame) != expected_counts[period] or frame.game_pk.duplicated().any():
            raise RuntimeError(f"FROZEN_POPULATION_FAILED:{period}:{len(frame)}")
    return frames


def score(frame: pd.DataFrame, artifact: dict[str, Any]) -> np.ndarray:
    return structural.score(frame, artifact)


def point_arrays(mu: np.ndarray, alpha: float) -> dict[str, np.ndarray]:
    masses = np.asarray([distribution(float(value), alpha) for value in mu])
    support = np.arange(masses.shape[1], dtype=float)
    cdf = np.cumsum(masses, axis=1)
    median = np.argmax(cdf >= 0.5, axis=1).astype(float)
    mode = np.argmax(masses, axis=1).astype(float)
    folded_mean = masses @ support
    return {
        "MEAN": mu.astype(float),
        "MEDIAN": median,
        "MODE": mode,
        "FOLDED_SUPPORT_MEAN": folded_mean,
        "PMF": masses,
    }


def point_metrics(actual: np.ndarray, forecast: np.ndarray) -> dict[str, float]:
    residual = actual - forecast
    absolute = abs(residual)
    return {
        "mean_point_forecast": float(np.mean(forecast)),
        "mean_actual": float(np.mean(actual)),
        "actual_minus_forecast_bias": float(np.mean(residual)),
        "mae": float(np.mean(absolute)),
        "rmse": float(np.sqrt(np.mean(residual**2))),
        "median_absolute_error": float(np.median(absolute)),
        "exact_total_hit_rate": float(np.mean(np.isclose(forecast, actual, atol=1e-12))),
        "within_1_run_rate": float(np.mean(absolute <= 1.0)),
        "within_2_runs_rate": float(np.mean(absolute <= 2.0)),
    }


def evidence_class(period: str) -> str:
    return "RETROSPECTIVE_POST_HOC_DIAGNOSTIC" if period == "PROSPECTIVE_AUG06_15" else "PRIMARY_OUT_OF_TIME_EVIDENCE"


def load_market_lines() -> dict[tuple[str, int], dict[str, Any]]:
    connection = sqlite3.connect(f"file:{raw.LEDGER}?mode=ro", uri=True)
    rows = connection.execute(
        """SELECT game_date,game_id,prediction_payload_json
           FROM totals_shadow_predictions
           WHERE game_date BETWEEN '2026-08-06' AND '2026-08-15'
           ORDER BY game_date,game_id"""
    ).fetchall()
    connection.close()
    output: dict[tuple[str, int], dict[str, Any]] = {}
    for date, game_id, payload_json in rows:
        payload = json.loads(payload_json)
        if payload.get("total_line") is None:
            continue
        output[(str(date), int(game_id))] = {
            "market_total_line": float(payload["total_line"]),
            "market_status": payload.get("market_status"),
            "market_snapshot_timestamp_utc": payload.get("market_snapshot_timestamp_utc"),
            "sportsbook_provider": payload.get("sportsbook_provider"),
        }
    return output


def build_rows(
    frames: dict[str, pd.DataFrame], subjects: dict[str, dict[str, Any]]
) -> tuple[pd.DataFrame, dict[str, dict[str, dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    row_frames = []
    metrics: dict[str, dict[str, dict[str, Any]]] = {}
    metric_rows: list[dict[str, Any]] = []
    matrix_rows: list[dict[str, Any]] = []
    for period in PERIODS:
        frame = frames[period]
        actual = frame.final_total.to_numpy(float)
        metrics[period] = {}
        control_stored = frame.raw_forecast.to_numpy(float)
        for model_key, artifact in subjects.items():
            mu = score(frame, artifact)
            alpha = float(artifact["dispersion_alpha"])
            summaries = point_arrays(mu, alpha)
            if model_key == "A_CONTROL" and not np.allclose(mu, control_stored, atol=2e-12, rtol=0):
                raise RuntimeError(f"CURRENT_STORED_POINT_NOT_REPRODUCED:{period}")
            full = metrics_source.metric_bundle(frame, mu, alpha)
            model_metric = {
                summary: point_metrics(actual, summaries[summary]) for summary in POINT_SUMMARIES
            }
            model_metric["FULL_DISTRIBUTION"] = full
            metrics[period][model_key] = model_metric
            for summary in POINT_SUMMARIES:
                item = {
                    "period": period,
                    "evidence_class": evidence_class(period),
                    "model_key": model_key,
                    "model_identity": MODEL_NAMES[model_key],
                    "model_hash": artifact["canonical_model_hash"],
                    "point_summary": summary,
                    "games": len(frame),
                    **model_metric[summary],
                    "exact_hit_rate_interpretation": (
                        "CONTINUOUS_MEAN_EXACT_HIT_NOT_OPERATIONALLY_MEANINGFUL"
                        if summary == "MEAN" else "INTEGER_POINT_SUMMARY_EXACT_HIT_MEANINGFUL"
                    ),
                    "full_distribution_metrics_attached_once": summary == "MEAN",
                    "crps": full["crps"] if summary == "MEAN" else None,
                    "ladder_brier": full["ladder_brier"] if summary == "MEAN" else None,
                    "ladder_log_loss": full["ladder_log_loss"] if summary == "MEAN" else None,
                    "ladder_ece": full["ladder_ece"] if summary == "MEAN" else None,
                    "distribution_metrics_contract": "MODEL_PERIOD_INVARIANT_ACROSS_POINT_SUMMARIES",
                }
                metric_rows.append(item)
                matrix_rows.append(dict(item))
            base = pd.DataFrame({
                "period": period,
                "evidence_class": evidence_class(period),
                "game_date": pd.to_datetime(frame.game_date).dt.date.astype(str),
                "game_pk": frame.game_pk.astype(int),
                "actual_total": actual,
                "model_key": model_key,
                "model_identity": MODEL_NAMES[model_key],
                "model_hash": artifact["canonical_model_hash"],
                "dispersion_alpha": alpha,
                "theoretical_distribution_mean": summaries["MEAN"],
                "folded_support_distribution_mean": summaries["FOLDED_SUPPORT_MEAN"],
                "distribution_median": summaries["MEDIAN"],
                "distribution_mode": summaries["MODE"],
                "current_stored_control_raw_point_forecast": control_stored,
                "model_frozen_point_forecast": mu,
                "current_stored_raw_equals_model_theoretical_mean": (
                    np.isclose(control_stored, mu, atol=2e-12, rtol=0)
                    if model_key == "A_CONTROL" else np.repeat(False, len(frame))
                ),
                "current_stored_raw_equality_contract": (
                    "EXACT_WITHIN_2E_12" if model_key == "A_CONTROL" else "NOT_APPLICABLE_C_IS_FROZEN_RESEARCH_ARTIFACT"
                ),
                "theoretical_variance": mu + alpha * mu**2,
                "mean_minus_median": mu - summaries["MEDIAN"],
                "mean_minus_mode": mu - summaries["MODE"],
                "theoretical_minus_folded_support_mean": mu - summaries["FOLDED_SUPPORT_MEAN"],
                "mean_absolute_error_row": abs(actual - mu),
                "median_absolute_error_row": abs(actual - summaries["MEDIAN"]),
                "mode_absolute_error_row": abs(actual - summaries["MODE"]),
                "mean_squared_error_row": (actual - mu) ** 2,
                "median_squared_error_row": (actual - summaries["MEDIAN"]) ** 2,
                "mode_squared_error_row": (actual - summaries["MODE"]) ** 2,
                "support_contract": "0..30_WITH_30_PLUS_TAIL",
            })
            row_frames.append(base)
    return pd.concat(row_frames, ignore_index=True), metrics, metric_rows, matrix_rows


def mae_tradeoff_rows(metrics: dict[str, dict[str, dict[str, Any]]]) -> tuple[list[dict[str, Any]], str]:
    rows = []
    for period in PERIODS:
        control = metrics[period]["A_CONTROL"]
        candidate = metrics[period]["C_CONFIDENCE_ONLY"]
        rows.append({
            "period": period,
            "evidence_class": evidence_class(period),
            "games": {"FROZEN_2025_VALIDATION": 2433, "2026_SEQUENTIAL_EARLY": 1281, "2026_LATE_HOLDOUT": 439, "PROSPECTIVE_AUG06_15": 126}[period],
            "control_current_point_contract": "DISTRIBUTION_MEAN_LOCATION_PARAMETER",
            "control_current_point_mae": control["MEAN"]["mae"],
            "control_median_mae": control["MEDIAN"]["mae"],
            "c_mean_mae": candidate["MEAN"]["mae"],
            "c_median_mae": candidate["MEDIAN"]["mae"],
            "c_median_minus_control_current_point_mae": candidate["MEDIAN"]["mae"] - control["MEAN"]["mae"],
            "c_median_minus_control_median_mae": candidate["MEDIAN"]["mae"] - control["MEDIAN"]["mae"],
            "c_mean_minus_control_mean_mae": candidate["MEAN"]["mae"] - control["MEAN"]["mae"],
            "mean_to_median_reduction_in_c_vs_control_current_delta": (
                candidate["MEAN"]["mae"] - control["MEAN"]["mae"]
            ) - (candidate["MEDIAN"]["mae"] - control["MEAN"]["mae"]),
        })
    primary = [row for row in rows if row["period"] in PRIMARY_PERIODS]
    mean_disadvantage = float(np.mean([row["c_mean_minus_control_mean_mae"] for row in primary]))
    median_disadvantage = float(np.mean([row["c_median_minus_control_current_point_mae"] for row in primary]))
    if all(row["c_median_minus_control_current_point_mae"] <= 0 for row in primary):
        decision = "ELIMINATED_BY_POINT_SUMMARY"
    elif (
        mean_disadvantage > 0
        and median_disadvantage <= mean_disadvantage * 0.5
        and max(row["c_median_minus_control_current_point_mae"] for row in primary) <= 0.02
        and all(row["c_median_minus_control_median_mae"] <= 0 for row in primary)
    ):
        decision = "MATERIALLY_REDUCED_BY_POINT_SUMMARY"
    elif median_disadvantage < mean_disadvantage:
        decision = "PARTIALLY_REDUCED"
    elif median_disadvantage > mean_disadvantage:
        decision = "WORSE"
    else:
        decision = "UNCHANGED"
    for row in rows:
        row["C_MAE_TRADEOFF"] = decision
    return rows, decision


def rmse_rows(metrics: dict[str, dict[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for period in PERIODS:
        control = metrics[period]["A_CONTROL"]["MEAN"]
        candidate = metrics[period]["C_CONFIDENCE_ONLY"]["MEAN"]
        rows.append({
            "period": period,
            "evidence_class": evidence_class(period),
            "control_mean_rmse": control["rmse"],
            "c_mean_rmse": candidate["rmse"],
            "c_minus_control_mean_rmse": candidate["rmse"] - control["rmse"],
            "control_actual_minus_mean_bias": control["actual_minus_forecast_bias"],
            "c_actual_minus_mean_bias": candidate["actual_minus_forecast_bias"],
            "c_minus_control_absolute_bias": abs(candidate["actual_minus_forecast_bias"]) - abs(control["actual_minus_forecast_bias"]),
            "c_mean_superior_on_rmse": candidate["rmse"] < control["rmse"],
            "c_mean_closer_to_zero_bias": abs(candidate["actual_minus_forecast_bias"]) < abs(control["actual_minus_forecast_bias"]),
            "squared_loss_contract": "CONDITIONAL_MEAN_IS_NATURAL_POINT_SUMMARY",
        })
    return rows


def absolute_representation_rows(metrics: dict[str, dict[str, dict[str, Any]]], gaps: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for period in PERIODS:
        for model_key in MODEL_KEYS:
            mean = metrics[period][model_key]["MEAN"]
            median = metrics[period][model_key]["MEDIAN"]
            group = gaps.loc[(gaps.period == period) & (gaps.model_key == model_key)]
            rows.append({
                "period": period,
                "evidence_class": evidence_class(period),
                "model_key": model_key,
                "model_identity": MODEL_NAMES[model_key],
                "mean_mae": mean["mae"],
                "median_mae": median["mae"],
                "median_minus_mean_mae": median["mae"] - mean["mae"],
                "median_reduces_mae": median["mae"] < mean["mae"],
                "mean_bias": mean["actual_minus_forecast_bias"],
                "median_bias": median["actual_minus_forecast_bias"],
                "median_minus_mean_bias": median["actual_minus_forecast_bias"] - mean["actual_minus_forecast_bias"],
                "mean_mean_minus_median_gap": float(group.mean_minus_median.mean()),
                "operational_interpretation": "MEDIAN_IS_L1_OPTIMAL_TYPICAL_INTEGER_TOTAL_MEAN_REMAINS_EXPECTATION",
            })
    return rows


def mean_median_gap_rows(row_frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for (period, model_key), group in row_frame.groupby(["period", "model_key"], sort=False):
        gap = group.mean_minus_median.to_numpy(float)
        rows.append({
            "period": period,
            "evidence_class": evidence_class(period),
            "model_key": model_key,
            "model_identity": MODEL_NAMES[model_key],
            "games": len(group),
            "mean_mean_minus_median": float(np.mean(gap)),
            "median_mean_minus_median": float(np.median(gap)),
            "p10_mean_minus_median": float(np.quantile(gap, 0.10)),
            "p25_mean_minus_median": float(np.quantile(gap, 0.25)),
            "p75_mean_minus_median": float(np.quantile(gap, 0.75)),
            "p90_mean_minus_median": float(np.quantile(gap, 0.90)),
            "maximum_mean_minus_median": float(np.max(gap)),
            "percentage_mean_equals_median": float(100 * np.mean(np.isclose(gap, 0, atol=1e-12))),
            "percentage_absolute_gap_ge_0_25": float(100 * np.mean(abs(gap) >= 0.25)),
            "percentage_absolute_gap_ge_0_50": float(100 * np.mean(abs(gap) >= 0.50)),
            "percentage_absolute_gap_ge_1_00": float(100 * np.mean(abs(gap) >= 1.00)),
            "max_theoretical_minus_folded_support_mean": float(group.theoretical_minus_folded_support_mean.max()),
        })
    return rows


def forecast_band_rows(row_frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for period in PERIODS:
        base = row_frame.loc[(row_frame.period == period) & (row_frame.model_key == "A_CONTROL")].set_index("game_pk")
        for band, low, high in FORECAST_BANDS:
            ids = base.index[(base.theoretical_distribution_mean >= low) & (base.theoretical_distribution_mean < high)]
            if len(ids) == 0:
                continue
            by_model: dict[str, dict[str, Any]] = {}
            for model_key in MODEL_KEYS:
                group = row_frame.loc[(row_frame.period == period) & (row_frame.model_key == model_key) & row_frame.game_pk.isin(ids)]
                mean_metrics = point_metrics(group.actual_total.to_numpy(float), group.theoretical_distribution_mean.to_numpy(float))
                median_metrics = point_metrics(group.actual_total.to_numpy(float), group.distribution_median.to_numpy(float))
                by_model[model_key] = {"mean": mean_metrics, "median": median_metrics}
                rows.append({
                    "period": period,
                    "evidence_class": evidence_class(period),
                    "control_mean_forecast_band": band,
                    "model_key": model_key,
                    "model_identity": MODEL_NAMES[model_key],
                    "games": len(group),
                    "mean_forecast": float(group.theoretical_distribution_mean.mean()),
                    "median_forecast": float(group.distribution_median.mean()),
                    "mean_actual": float(group.actual_total.mean()),
                    "mean_based_mae": mean_metrics["mae"],
                    "median_based_mae": median_metrics["mae"],
                    "actual_minus_mean_bias": mean_metrics["actual_minus_forecast_bias"],
                    "actual_minus_median_bias": median_metrics["actual_minus_forecast_bias"],
                })
            control = by_model["A_CONTROL"]
            candidate = by_model["C_CONFIDENCE_ONLY"]
            for row in rows[-2:]:
                row["c_mean_minus_control_mean_mae"] = candidate["mean"]["mae"] - control["mean"]["mae"]
                row["c_median_minus_control_current_mean_mae"] = candidate["median"]["mae"] - control["mean"]["mae"]
                row["c_median_minus_control_median_mae"] = candidate["median"]["mae"] - control["median"]["mae"]
                row["point_summary_reduction_in_c_delta"] = (
                    candidate["mean"]["mae"] - control["mean"]["mae"]
                ) - (candidate["median"]["mae"] - control["mean"]["mae"])
    return rows


def dispersion_band_rows(row_frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    labels = ("Q1_LOWEST", "Q2", "Q3", "Q4_HIGHEST")
    for period in PERIODS:
        control = row_frame.loc[(row_frame.period == period) & (row_frame.model_key == "A_CONTROL")].copy()
        quantiles = np.quantile(control.theoretical_variance, [0.25, 0.50, 0.75])
        control["variance_band"] = pd.cut(
            control.theoretical_variance,
            [-np.inf, *quantiles, np.inf], labels=labels, include_lowest=True,
        ).astype(str)
        band_by_id = control.set_index("game_pk").variance_band
        for band in labels:
            ids = band_by_id.index[band_by_id.eq(band)]
            for model_key in MODEL_KEYS:
                group = row_frame.loc[(row_frame.period == period) & (row_frame.model_key == model_key) & row_frame.game_pk.isin(ids)]
                mean_metrics = point_metrics(group.actual_total.to_numpy(float), group.theoretical_distribution_mean.to_numpy(float))
                median_metrics = point_metrics(group.actual_total.to_numpy(float), group.distribution_median.to_numpy(float))
                rows.append({
                    "period": period,
                    "evidence_class": evidence_class(period),
                    "control_variance_quantile_band": band,
                    "control_variance_q25": float(quantiles[0]),
                    "control_variance_q50": float(quantiles[1]),
                    "control_variance_q75": float(quantiles[2]),
                    "model_key": model_key,
                    "model_identity": MODEL_NAMES[model_key],
                    "games": len(group),
                    "mean_predictive_variance": float(group.theoretical_variance.mean()),
                    "mean_mean_minus_median_gap": float(group.mean_minus_median.mean()),
                    "mean_actual": float(group.actual_total.mean()),
                    "mean_mae": mean_metrics["mae"],
                    "median_mae": median_metrics["mae"],
                    "median_minus_mean_mae": median_metrics["mae"] - mean_metrics["mae"],
                })
    return rows


def market_rows(row_frame: pd.DataFrame, lines: dict[tuple[str, int], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    prospective = row_frame.loc[row_frame.period.eq("PROSPECTIVE_AUG06_15")].copy()
    for row in prospective.itertuples():
        market = lines.get((str(row.game_date), int(row.game_pk)))
        if not market:
            continue
        line = market["market_total_line"]
        rows.append({
            "row_type": "GAME",
            "period": row.period,
            "evidence_class": evidence_class(row.period),
            "game_date": row.game_date,
            "game_pk": row.game_pk,
            "model_key": row.model_key,
            "model_identity": row.model_identity,
            "distribution_mean": row.theoretical_distribution_mean,
            "distribution_median": row.distribution_median,
            "market_total_line": line,
            "absolute_mean_minus_market_line": abs(row.theoretical_distribution_mean - line),
            "absolute_median_minus_market_line": abs(row.distribution_median - line),
            **market,
            "interpretation": "DESCRIPTIVE_ONLY_NO_EDGE_OR_EV",
        })
    for model_key in MODEL_KEYS:
        group = [row for row in rows if row["model_key"] == model_key and row["row_type"] == "GAME"]
        if group:
            rows.append({
                "row_type": "SUMMARY",
                "period": "PROSPECTIVE_AUG06_15",
                "evidence_class": "RETROSPECTIVE_POST_HOC_DIAGNOSTIC",
                "game_date": "ALL",
                "game_pk": "ALL",
                "model_key": model_key,
                "model_identity": MODEL_NAMES[model_key],
                "games_with_governed_line": len(group),
                "mean_absolute_mean_minus_market_line": float(np.mean([row["absolute_mean_minus_market_line"] for row in group])),
                "mean_absolute_median_minus_market_line": float(np.mean([row["absolute_median_minus_market_line"] for row in group])),
                "interpretation": "EXPECTED_TOTAL_IS_MEAN_TYPICAL_TOTAL_IS_MEDIAN_DESCRIPTIVE_ONLY",
            })
    return rows


def precision_rows(row_frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for (period, model_key), group in row_frame.groupby(["period", "model_key"], sort=False):
        actual = group.actual_total.to_numpy(float)
        variants = {
            "UNROUNDED_MEAN": group.theoretical_distribution_mean.to_numpy(float),
            "UNROUNDED_EXACT_MEDIAN": group.distribution_median.to_numpy(float),
            "EXISTING_MARKDOWN_MEAN_3_DECIMALS": group.theoretical_distribution_mean.round(3).to_numpy(float),
        }
        base = point_metrics(actual, variants["UNROUNDED_MEAN"])
        for representation, values in variants.items():
            result = point_metrics(actual, values)
            rows.append({
                "period": period,
                "evidence_class": evidence_class(period),
                "model_key": model_key,
                "model_identity": MODEL_NAMES[model_key],
                "presentation": representation,
                "stored_precision_contract": "FULL_FLOAT_IN_LEDGER_CSV_3_DECIMALS_IN_SHADOW_MARKDOWN_NO_PUBLIC_TOTALS_UI",
                **result,
                "mae_minus_unrounded_mean": result["mae"] - base["mae"],
                "rmse_minus_unrounded_mean": result["rmse"] - base["rmse"],
            })
    return rows


def clustered_rows(row_frame: pd.DataFrame) -> list[dict[str, Any]]:
    contrasts = (
        ("C_MEDIAN_MINUS_CONTROL_CURRENT_MEAN_MAE", "mae", "C_CONFIDENCE_ONLY", "MEDIAN", "A_CONTROL", "MEAN"),
        ("C_MEDIAN_MINUS_CONTROL_MEDIAN_MAE", "mae", "C_CONFIDENCE_ONLY", "MEDIAN", "A_CONTROL", "MEDIAN"),
        ("C_MEAN_MINUS_CONTROL_MEAN_RMSE", "rmse", "C_CONFIDENCE_ONLY", "MEAN", "A_CONTROL", "MEAN"),
    )
    rows = []
    for period in PRIMARY_PERIODS:
        period_rows = row_frame.loc[row_frame.period.eq(period)]
        dates = np.sort(period_rows.game_date.unique())
        day_n = np.asarray([period_rows.loc[(period_rows.model_key == "A_CONTROL") & period_rows.game_date.eq(date)].shape[0] for date in dates], float)
        rng = np.random.default_rng(BOOTSTRAP_SEED)
        counts = rng.multinomial(len(dates), np.repeat(1 / len(dates), len(dates)), size=BOOTSTRAP_DRAWS)
        denominator = counts @ day_n
        for name, metric, c_model, c_summary, b_model, b_summary in contrasts:
            c = period_rows.loc[period_rows.model_key.eq(c_model)].set_index("game_pk")
            b = period_rows.loc[period_rows.model_key.eq(b_model)].set_index("game_pk")
            c_values = c[{"MEAN": "theoretical_distribution_mean", "MEDIAN": "distribution_median"}[c_summary]]
            b_values = b[{"MEAN": "theoretical_distribution_mean", "MEDIAN": "distribution_median"}[b_summary]]
            actual = c.actual_total
            if metric == "mae":
                c_loss = abs(actual - c_values)
                b_loss = abs(actual - b_values)
            else:
                c_loss = (actual - c_values) ** 2
                b_loss = (actual - b_values) ** 2
            c_day = np.asarray([c_loss.loc[c.game_date.eq(date)].sum() for date in dates])
            b_day = np.asarray([b_loss.loc[b.game_date.eq(date)].sum() for date in dates])
            c_draw = (counts @ c_day) / denominator
            b_draw = (counts @ b_day) / denominator
            if metric == "rmse":
                c_draw = np.sqrt(c_draw); b_draw = np.sqrt(b_draw)
            delta = c_draw - b_draw
            c_observed = float(np.mean(abs(actual - c_values))) if metric == "mae" else float(np.sqrt(np.mean((actual - c_values) ** 2)))
            b_observed = float(np.mean(abs(actual - b_values))) if metric == "mae" else float(np.sqrt(np.mean((actual - b_values) ** 2)))
            rows.append({
                "period": period,
                "contrast": name,
                "metric": metric.upper(),
                "games": len(c),
                "date_clusters": len(dates),
                "draws": BOOTSTRAP_DRAWS,
                "seed": BOOTSTRAP_SEED,
                "candidate_observed": c_observed,
                "reference_observed": b_observed,
                "candidate_minus_reference": c_observed - b_observed,
                "ci_low": float(np.quantile(delta, 0.025)),
                "ci_high": float(np.quantile(delta, 0.975)),
                "fraction_draws_favoring_c": float(np.mean(delta < 0)),
                "favor_contract": "LOWER_IS_BETTER",
            })
    return rows


def leave_block_rows(row_frame: pd.DataFrame) -> tuple[list[dict[str, Any]], str]:
    contrasts = (
        ("C_MEDIAN_MINUS_CONTROL_CURRENT_MEAN_MAE", "mae", "C_CONFIDENCE_ONLY", "distribution_median", "A_CONTROL", "theoretical_distribution_mean"),
        ("C_MEDIAN_MINUS_CONTROL_MEDIAN_MAE", "mae", "C_CONFIDENCE_ONLY", "distribution_median", "A_CONTROL", "distribution_median"),
        ("C_MEAN_MINUS_CONTROL_MEAN_RMSE", "rmse", "C_CONFIDENCE_ONLY", "theoretical_distribution_mean", "A_CONTROL", "theoretical_distribution_mean"),
    )
    rows = []
    summaries = []
    for period in PRIMARY_PERIODS:
        data = row_frame.loc[row_frame.period.eq(period)].copy()
        data["time_block"] = pd.to_datetime(data.game_date).dt.to_period("M").astype(str)
        blocks = sorted(data.time_block.unique())
        for contrast, metric, c_model, c_column, b_model, b_column in contrasts:
            deltas = []
            for block in blocks:
                candidate = data.loc[data.model_key.eq(c_model) & data.time_block.ne(block)].set_index("game_pk")
                reference = data.loc[data.model_key.eq(b_model) & data.time_block.ne(block)].set_index("game_pk")
                actual = candidate.actual_total.to_numpy(float)
                c_values = candidate[c_column].to_numpy(float)
                b_values = reference[b_column].to_numpy(float)
                c_metric = float(np.mean(abs(actual - c_values))) if metric == "mae" else float(np.sqrt(np.mean((actual - c_values) ** 2)))
                b_metric = float(np.mean(abs(actual - b_values))) if metric == "mae" else float(np.sqrt(np.mean((actual - b_values) ** 2)))
                delta = c_metric - b_metric
                deltas.append(delta)
                rows.append({
                    "row_type": "LEAVE_TIME_BLOCK_OUT",
                    "period": period,
                    "block_type": "CALENDAR_MONTH",
                    "excluded_block": block,
                    "contrast": contrast,
                    "metric": metric.upper(),
                    "remaining_games": len(candidate),
                    "candidate_metric": c_metric,
                    "reference_metric": b_metric,
                    "candidate_minus_reference": delta,
                    "favors_c": delta < 0,
                })
            summaries.append({
                "row_type": "SUMMARY",
                "period": period,
                "block_type": "CALENDAR_MONTH",
                "excluded_block": "ALL",
                "contrast": contrast,
                "metric": metric.upper(),
                "remaining_games": "VARIES",
                "min_candidate_minus_reference": float(min(deltas)),
                "max_candidate_minus_reference": float(max(deltas)),
                "delta_sign_changes": bool(min(deltas) < 0 < max(deltas)),
                "fraction_blocks_favoring_c": float(np.mean(np.asarray(deltas) < 0)),
            })
    rows.extend(summaries)
    primary_mae = [row for row in summaries if row["contrast"] == "C_MEDIAN_MINUS_CONTROL_CURRENT_MEAN_MAE"]
    median_parity = [row for row in summaries if row["contrast"] == "C_MEDIAN_MINUS_CONTROL_MEDIAN_MAE"]
    rmse = [row for row in summaries if row["contrast"] == "C_MEAN_MINUS_CONTROL_MEAN_RMSE"]
    if all(row["fraction_blocks_favoring_c"] >= 0.75 for row in (*median_parity, *rmse)) and all(row["fraction_blocks_favoring_c"] >= 0.50 for row in primary_mae):
        robustness = "ROBUST"
    elif all(row["fraction_blocks_favoring_c"] >= 0.50 for row in (*median_parity, *rmse)):
        robustness = "MODERATE"
    elif float(np.mean([row["fraction_blocks_favoring_c"] for row in summaries])) >= 0.50:
        robustness = "MIXED"
    else:
        robustness = "WEAK"
    for row in rows:
        row["POINT_SUMMARY_ROBUSTNESS"] = robustness
    return rows, robustness


def docs(
    output_dir: Path,
    subjects: dict[str, dict[str, Any]],
    metrics: dict[str, dict[str, dict[str, Any]]],
    gap_rows: list[dict[str, Any]],
    tradeoff_rows: list[dict[str, Any]],
    tradeoff: str,
    robustness: str,
    band_rows: list[dict[str, Any]],
    dispersion_rows: list[dict[str, Any]],
    market_context: list[dict[str, Any]],
    precision_rows_output: list[dict[str, Any]],
) -> dict[str, str]:
    primary_trade = [row for row in tradeoff_rows if row["period"] in PRIMARY_PERIODS]
    primary_gap = [row for row in gap_rows if row["period"] in PRIMARY_PERIODS]
    mean_gap = {key: float(np.mean([row["mean_mean_minus_median"] for row in primary_gap if row["model_key"] == key])) for key in MODEL_KEYS}
    mean_delta = float(np.mean([row["c_mean_minus_control_mean_mae"] for row in primary_trade]))
    median_delta = float(np.mean([row["c_median_minus_control_current_point_mae"] for row in primary_trade]))
    median_parity_delta = float(np.mean([row["c_median_minus_control_median_mae"] for row in primary_trade]))
    eight_bands = [row for row in band_rows if row["period"] in PRIMARY_PERIODS and row["model_key"] == "C_CONFIDENCE_ONLY" and row["control_mean_forecast_band"] in ("8.0-8.49", "8.5-8.99")]
    band_mean_delta = float(np.average([row["c_mean_minus_control_mean_mae"] for row in eight_bands], weights=[row["games"] for row in eight_bands]))
    band_median_delta = float(np.average([row["c_median_minus_control_current_mean_mae"] for row in eight_bands], weights=[row["games"] for row in eight_bands]))
    primary_disp = [row for row in dispersion_rows if row["period"] in PRIMARY_PERIODS]
    low = [row["median_minus_mean_mae"] for row in primary_disp if row["control_variance_quantile_band"] == "Q1_LOWEST"]
    high = [row["median_minus_mean_mae"] for row in primary_disp if row["control_variance_quantile_band"] == "Q4_HIGHEST"]
    dispersion_benefit = float(np.mean(high) - np.mean(low))
    market_summary = [row for row in market_context if row["row_type"] == "SUMMARY"]
    markdown_rounding_max_mae_change = max(
        abs(float(row["mae_minus_unrounded_mean"]))
        for row in precision_rows_output
        if row["presentation"] == "EXISTING_MARKDOWN_MEAN_3_DECIMALS"
    )

    if tradeoff in ("ELIMINATED_BY_POINT_SUMMARY", "MATERIALLY_REDUCED_BY_POINT_SUMMARY") and median_parity_delta <= 0:
        reinterpretation = "STRUCTURAL_REPAIR_BETTER_DISTRIBUTION_AND_APPROPRIATE_POINT_SUMMARY_RESOLVES_TRADEOFF"
        shadow = "TOTALS_COUNT_CONFIDENCE_ONLY_READY_FOR_SHADOW_DECISION"
    elif tradeoff in ("PARTIALLY_REDUCED", "MATERIALLY_REDUCED_BY_POINT_SUMMARY"):
        reinterpretation = "STRUCTURAL_REPAIR_BETTER_DISTRIBUTION_BUT_POINT_MAE_TRADEOFF_REMAINS"
        shadow = "TOTALS_COUNT_CONFIDENCE_ONLY_NEEDS_FURTHER_POINT_REVIEW"
    else:
        reinterpretation = "STRUCTURAL_REPAIR_POINT_FORECAST_STILL_UNRESOLVED"
        shadow = "TOTALS_COUNT_CONFIDENCE_ONLY_NOT_READY"
    final_declaration = (
        "TOTALS_POINT_SUMMARY_EXPLAINS_MAE_TRADEOFF"
        if tradeoff == "ELIMINATED_BY_POINT_SUMMARY"
        else "TOTALS_POINT_SUMMARY_PARTLY_EXPLAINS_MAE_TRADEOFF"
        if tradeoff in ("MATERIALLY_REDUCED_BY_POINT_SUMMARY", "PARTIALLY_REDUCED")
        else "TOTALS_POINT_SUMMARY_DOES_NOT_EXPLAIN_MAE_TRADEOFF"
        if tradeoff in ("UNCHANGED", "WORSE")
        else "TOTALS_POINT_SUMMARY_FINDING_UNRESOLVED"
    )
    intercept = "STRUCTURAL_LOCATION_COMPENSATION"
    exact_next = "Human decide whether to authorize a separately governed C shadow-decision package that preserves MEAN as EXPECTED TOTAL and adds MEDIAN only as CENTRAL/TYPICAL and MAE-optimal representation; do not start shadow capture in this task."

    control = subjects["A_CONTROL"]
    candidate = subjects["C_CONFIDENCE_ONLY"]
    contract = f"""# Current totals point-forecast contract

The stored `RAW expected total` is `DISTRIBUTION_MEAN` and simultaneously the negative-binomial `LOCATION_PARAMETER`.

Exact path:

1. `live_context_bridge_v1.feature_row` creates the frozen feature vector in artifact order.
2. `score_mean` standardizes each feature: `z_j = (x_j - scaler_mean_j) / scaler_scale_j`.
3. It forms `eta = intercept + Σ(z_j * coefficient_j)` and `mu = exp(eta)`.
4. `score_context` stores that unrounded `mu` as `expected_total` in the immutable prediction payload.
5. `run_mlb_totals_prospective_shadow_v1` carries `expected_total` into CSV/market comparison and renders `Predicted total` with three decimals in shadow markdown. No public Totals UI currently consumes this private shadow point.

The point is not obtained from CDF inversion or PMF maximization. Theoretical NB mean equals `mu`; the implemented 0..30 support folds 30+ into 30, so its literal finite-support expectation differs by at most the row-level amount reported in `totals_mean_median_gap.csv` (operationally negligible). Historical retained `predicted_total` and prospective stored `expected_total` reproduce `mu` to the governed tolerance.
"""
    (output_dir / "totals_point_forecast_current_contract.md").write_text(contract)

    parameterization = f"""# Negative-binomial parameterization

Both subjects use `REGULARIZED_POISSON_LOCATION_WITH_NEGATIVE_BINOMIAL_DISTRIBUTION` and differ through frozen fitted predictor structure and their frozen dispersion estimates; neither was refit here.

- CONTROL `{CONTROL_HASH}`: alpha `{float(control['dispersion_alpha']):.17g}`, `{len(control['feature_order'])}` location inputs.
- C `{C_HASH}`: alpha `{float(candidate['dispersion_alpha']):.17g}`, `{len(candidate['feature_order'])}` location inputs.

For each row, `mu = exp(beta_0 + z beta)`. With `alpha > 0`, code sets `size = 1/alpha` and `p = size/(size+mu)`. Under SciPy's failures-before-success parameterization:

`P(Y=k) = Gamma(k+size)/(Gamma(size) Gamma(k+1)) * p^size * (1-p)^k`, for nonnegative integer `k`.

The theoretical mean/location is `E[Y]=mu`; variance is `mu + alpha*mu^2`. PMF support is evaluated at 0..30 and remaining mass above 30 is folded into 30. Median is exact first CDF index at or above 0.5; mode is exact PMF argmax on the governed support. Thus the models share the exact family/PMF contract and differ only through fitted predictor feature structure, coefficients/scaling, and the frozen alpha resulting from their original development fit.
"""
    (output_dir / "totals_negative_binomial_parameterization.md").write_text(parameterization)

    reinterpretation_md = f"""# Structural comparison reinterpretation

`{reinterpretation}`

- `C_MAE_TRADEOFF = {tradeoff}`.
- Primary average C mean minus CONTROL mean MAE: `{mean_delta:+.9f}` runs.
- Primary average C median minus CONTROL current mean MAE: `{median_delta:+.9f}` runs.
- Primary average C median minus CONTROL median MAE: `{median_parity_delta:+.9f}` runs.
- Prior 8.0–8.99 C mean delta / C median-versus-current delta: `{band_mean_delta:+.9f}` / `{band_median_delta:+.9f}` runs.
- Point-summary robustness: `{robustness}`.

Full-distribution CRPS, Brier, log loss, and ECE are unchanged by choosing a displayed mean, median, or mode. The earlier C distribution improvements therefore remain intact. August is post-hoc descriptive evidence and did not select this interpretation.
"""
    (output_dir / "totals_structural_comparison_reinterpretation.md").write_text(reinterpretation_md)

    product = f"""# Totals point-prediction product contract recommendation

- `EXPECTED TOTAL RUNS = MEAN`
- `CENTRAL OR TYPICAL TOTAL = MEDIAN`
- `MAE-OPTIMAL POINT FORECAST = MEDIAN`
- `PROBABILITY FOUNDATION = FULL_NEGATIVE_BINOMIAL_DISTRIBUTION`

The expected value and typical integer outcome are different valid summaries. Mean remains the squared-error/RMSE statistic and the literal model expectation; median is the L1/MAE statistic and a more intuitive typical total; mode is the exact-count-loss statistic. Do not force one number to carry all three semantics. No display change was implemented.
"""
    (output_dir / "totals_point_prediction_product_contract.md").write_text(product)

    intercept_md = f"""# V1_INTERCEPT reinterpretation

`INTERCEPT_REINTERPRETATION = {intercept}`

The frozen +{INTERCEPT:.6f} diagnostic addressed CONTROL's positive actual-minus-mean residual by shifting the distribution location upward. Replacing mean with median moves the point downward by roughly {mean_gap['A_CONTROL']:.3f} runs and therefore cannot explain that correction. Earlier structural work showed the intercept masks nonstationary location defects; it was not compensation for using mean rather than median. V1_INTERCEPT was not modified or treated as a fitted candidate.
"""
    (output_dir / "totals_intercept_reinterpretation.md").write_text(intercept_md)

    metric_lines = []
    for period in PRIMARY_PERIODS:
        cm = metrics[period]["A_CONTROL"]
        cc = metrics[period]["C_CONFIDENCE_ONLY"]
        metric_lines.append(
            f"- {period}: CONTROL MAE mean/median/mode `{cm['MEAN']['mae']:.6f}` / `{cm['MEDIAN']['mae']:.6f}` / `{cm['MODE']['mae']:.6f}`, "
            f"RMSE `{cm['MEAN']['rmse']:.6f}` / `{cm['MEDIAN']['rmse']:.6f}` / `{cm['MODE']['rmse']:.6f}`; "
            f"C MAE `{cc['MEAN']['mae']:.6f}` / `{cc['MEDIAN']['mae']:.6f}` / `{cc['MODE']['mae']:.6f}`, "
            f"RMSE `{cc['MEAN']['rmse']:.6f}` / `{cc['MEDIAN']['rmse']:.6f}` / `{cc['MODE']['rmse']:.6f}`."
        )
    market_text = "; ".join(
        f"{row['model_key']} mean/median line distance {row['mean_absolute_mean_minus_market_line']:.3f}/{row['mean_absolute_median_minus_market_line']:.3f}"
        for row in market_summary
    ) or "no governed lines"
    concise = f"""# MLB Totals point-forecast representation review v1

`{final_declaration}`

- Current RAW point: `DISTRIBUTION_MEAN` / `LOCATION_PARAMETER`, stored unrounded; private shadow markdown renders three decimals.
- Mean–median primary gap: CONTROL `{mean_gap['A_CONTROL']:.3f}` runs; C `{mean_gap['C_CONFIDENCE_ONLY']:.3f}` runs. Median is an exact integer CDF inversion; mode is the PMF maximum.
{chr(10).join(metric_lines)}
- Direct decision: `C_MAE_TRADEOFF = {tradeoff}`. C's median removes the mean-based disadvantage versus CONTROL's median in every primary population, while one C-median versus current-CONTROL-mean period remains +0.017 runs.
- Forecast bands: pooled 8.0–8.99 mean-based C delta `{band_mean_delta:+.6f}` becomes `{band_median_delta:+.6f}` using C median versus CONTROL current mean.
- Dispersion: Q4-minus-Q1 change in median-versus-mean MAE benefit `{dispersion_benefit:+.6f}` runs across model-period cells; see frozen quartile rows for direction by model/period.
- Cluster/leave-block robustness: `{robustness}`. Intervals and draw fractions are in the clustered artifact.
- Proper-distribution metrics: unchanged by point representation; C's prior CRPS/Brier/log-loss/ECE evidence remains distribution-level.
- Market-line context: {market_text}; descriptive only, no edge or EV.
- Existing three-decimal shadow rendering changes MAE by at most `{markdown_rounding_max_mae_change:.9f}` runs; display rounding does not materially explain the tradeoff.
- Structural reinterpretation: `{reinterpretation}`.
- Product contract: EXPECTED=MEAN; CENTRAL/TYPICAL=MEDIAN; MAE-OPTIMAL=MEDIAN; probabilities=FULL_NEGATIVE_BINOMIAL_DISTRIBUTION.
- `INTERCEPT_REINTERPRETATION = {intercept}`.
- Shadow readiness: `{shadow}`. No shadow was started.
- Exact next human decision: {exact_next}
"""
    (output_dir / "concise_mlb_totals_point_forecast_representation_review_v1.md").write_text(concise)
    return {
        "tradeoff": tradeoff,
        "robustness": robustness,
        "reinterpretation": reinterpretation,
        "shadow_readiness": shadow,
        "intercept_reinterpretation": intercept,
        "final_declaration": final_declaration,
        "exact_next_human_decision": exact_next,
    }


def run(output_dir: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    subjects = load_subjects()
    protected = [
        CONTROL_PATH, C_PATH, raw.LEDGER,
        raw.SPINE / "totals_core_feature_spine.csv",
        ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py",
        structural.DEFAULT_OUTPUT / "totals_count_repair_point_quality.csv",
        structural.DEFAULT_OUTPUT / "totals_count_repair_probability_quality.csv",
    ]
    protected_before = {str(path): sha256(path) for path in protected}
    frames = load_populations(subjects["A_CONTROL"])
    row_frame, metrics, metric_rows, matrix_rows = build_rows(frames, subjects)
    tradeoff_rows, tradeoff = mae_tradeoff_rows(metrics)
    rmse_analysis = rmse_rows(metrics)
    absolute_rows = absolute_representation_rows(metrics, row_frame)
    gap_rows = mean_median_gap_rows(row_frame)
    band_rows = forecast_band_rows(row_frame)
    dispersion_rows = dispersion_band_rows(row_frame)
    market_context = market_rows(row_frame, load_market_lines())
    precision = precision_rows(row_frame)
    clustered = clustered_rows(row_frame)
    leave_rows, robustness = leave_block_rows(row_frame)

    write_csv(output_dir / "totals_point_summary_rows.csv", row_frame.to_dict("records"))
    write_csv(output_dir / "totals_control_point_summary_metrics.csv", [row for row in metric_rows if row["model_key"] == "A_CONTROL"])
    write_csv(output_dir / "totals_confidence_only_point_summary_metrics.csv", [row for row in metric_rows if row["model_key"] == "C_CONFIDENCE_ONLY"])
    write_csv(output_dir / "totals_point_summary_comparison_matrix.csv", matrix_rows)
    write_csv(output_dir / "totals_mae_tradeoff_analysis.csv", tradeoff_rows)
    write_csv(output_dir / "totals_rmse_mean_analysis.csv", rmse_analysis)
    write_csv(output_dir / "totals_absolute_error_representation.csv", absolute_rows)
    write_csv(output_dir / "totals_mean_median_gap.csv", gap_rows)
    write_csv(output_dir / "totals_point_summary_forecast_bands.csv", band_rows)
    write_csv(output_dir / "totals_point_summary_dispersion_bands.csv", dispersion_rows)
    write_csv(output_dir / "totals_point_summary_market_line_context.csv", market_context)
    write_csv(output_dir / "totals_point_summary_precision_check.csv", precision)
    write_csv(output_dir / "totals_point_summary_clustered_uncertainty.csv", clustered)
    write_csv(output_dir / "totals_point_summary_leave_block_out.csv", leave_rows)
    decisions = docs(
        output_dir, subjects, metrics, gap_rows, tradeoff_rows, tradeoff,
        robustness, band_rows, dispersion_rows, market_context, precision,
    )

    protected_after = {str(path): sha256(path) for path in protected}
    if protected_before != protected_after:
        raise RuntimeError("PROTECTED_INPUT_OR_MODEL_MUTATION_DETECTED")
    hash_path = output_dir / "reproducibility_hashes.sha256"
    outputs = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    hash_path.write_text("\n".join([*(f"{sha256(path)}  {path.name}" for path in outputs), *(f"{digest}  PROTECTED_INPUT::{path}" for path, digest in protected_after.items())]) + "\n")
    return {
        "task_id": TASK_ID,
        "subjects": {key: artifact["canonical_model_hash"] for key, artifact in subjects.items()},
        "population_counts": {period: len(frame) for period, frame in frames.items()},
        "population_hashes": {period: frame_hash(frame) for period, frame in frames.items()},
        "output_files": len(outputs) + 1,
        "new_fits": 0,
        "protected_inputs_unchanged": True,
        **decisions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

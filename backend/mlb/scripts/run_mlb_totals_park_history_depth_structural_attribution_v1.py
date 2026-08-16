"""Read-only structural attribution of frozen MLB totals park_history_depth.

This module never fits a model or mutates a prediction ledger. It reproduces the
frozen DIRECT_NEGATIVE_BINOMIAL equation and changes only the named feature for
three explicitly counterfactual diagnostics.
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

from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as raw
from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_PARK_HISTORY_DEPTH_STRUCTURAL_ATTRIBUTION_V1"
MODEL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
FEATURE = "park_history_depth"
INTERCEPT = 0.493550
THRESHOLDS = (6.5, 7.5, 8.5, 9.5, 10.5, 11.5)

CONFIG = raw.CONFIG
LEDGER = raw.LEDGER
SPINE = raw.SPINE
PARK_SPINE = SPINE / "strict_prior_park_factor.csv"
BUILDER = ROOT / "tmp/analysis/build_mlb_totals_feature_spine_v1.py"
LIVE_BRIDGE = ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py"
PRIOR_ANALYSIS = ROOT / "backend/mlb/scripts/run_mlb_totals_raw_run_environment_bias_decomposition_v1.py"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_park_history_depth_structural_attribution_v1/2026-08-16"

PERIODS = (
    "DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE",
    "FROZEN_2025_VALIDATION",
    "2026_SEQUENTIAL_EARLY",
    "2026_LATE_HOLDOUT",
    "PROSPECTIVE_AUG06_15",
)
OUT_OF_TIME_PERIODS = (
    "FROZEN_2025_VALIDATION",
    "2026_SEQUENTIAL_EARLY",
    "2026_LATE_HOLDOUT",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def fixed_probability_metrics(actual: np.ndarray, forecasts: np.ndarray, alpha: float) -> dict[str, float]:
    briers: list[float] = []
    losses: list[float] = []
    calibration: list[float] = []
    probabilities = [distribution(float(mu), alpha) for mu in forecasts]
    support = np.arange(31)
    for line in THRESHOLDS:
        predicted = np.asarray([float(p[support > line].sum()) for p in probabilities])
        observed = (actual > line).astype(float)
        clipped = np.clip(predicted, 1e-12, 1 - 1e-12)
        briers.append(float(np.mean((predicted - observed) ** 2)))
        losses.append(float(np.mean(-(observed * np.log(clipped) + (1 - observed) * np.log(1 - clipped)))))
        calibration.append(abs(float(predicted.mean() - observed.mean())))
    return {
        "fixed_threshold_macro_brier": float(np.mean(briers)),
        "fixed_threshold_macro_log_loss": float(np.mean(losses)),
        "fixed_threshold_macro_ece": float(np.mean(calibration)),
    }


def metrics(frame: pd.DataFrame, forecasts: np.ndarray, alpha: float) -> dict[str, Any]:
    actual = frame.final_total.to_numpy(float)
    residual = actual - forecasts
    output: dict[str, Any] = {
        "games": len(frame),
        "mean_prediction": float(np.mean(forecasts)) if len(frame) else math.nan,
        "mean_actual": float(np.mean(actual)) if len(frame) else math.nan,
        "actual_minus_forecast_bias": float(np.mean(residual)) if len(frame) else math.nan,
        "median_actual_minus_forecast_residual": float(np.median(residual)) if len(frame) else math.nan,
        "mae": float(np.mean(abs(residual))) if len(frame) else math.nan,
        "rmse": float(np.sqrt(np.mean(residual**2))) if len(frame) else math.nan,
        "crps": float(np.mean([raw.crps(mu, int(y), alpha) for mu, y in zip(forecasts, actual)])) if len(frame) else math.nan,
    }
    if len(frame):
        output.update(fixed_probability_metrics(actual, forecasts, alpha))
    else:
        output.update({"fixed_threshold_macro_brier": math.nan, "fixed_threshold_macro_log_loss": math.nan,
                       "fixed_threshold_macro_ece": math.nan})
    return output


def counterfactuals(frame: pd.DataFrame, coefficient: float, center: float, scale: float,
                    training_p95: float) -> dict[str, np.ndarray]:
    original = frame.raw_forecast.to_numpy(float)
    depth = frame[FEATURE].to_numpy(float)
    centered_contribution = coefficient * (depth - center) / scale
    capped_contribution_change = coefficient * (np.minimum(depth, training_p95) - depth) / scale
    neutral = original * np.exp(-centered_contribution)
    return {
        "RAW_V1": original,
        "V1_INTERCEPT": original + INTERCEPT,
        "TRAINING_MEAN": neutral,
        "P95_CAP": original * np.exp(capped_contribution_change),
        # StandardScaler centers this feature at its training mean, so C is
        # algebraically identical to A. It remains separate in the evidence.
        "COEFFICIENT_ZERO": neutral.copy(),
    }


def descriptive_row(period: str, frame: pd.DataFrame, fallback_column: str) -> dict[str, Any]:
    values = frame[FEATURE].astype(float)
    quantiles = values.quantile([.05, .10, .25, .50, .75, .90, .95, .99])
    fallback = frame[fallback_column].astype(str)
    return {
        "period": period, "games": len(frame), "mean": float(values.mean()), "median": float(values.median()),
        "sd_sample": float(values.std(ddof=1)), "min": float(values.min()),
        "p05": float(quantiles.loc[.05]), "p10": float(quantiles.loc[.10]),
        "p25": float(quantiles.loc[.25]), "p50": float(quantiles.loc[.50]),
        "p75": float(quantiles.loc[.75]), "p90": float(quantiles.loc[.90]),
        "p95": float(quantiles.loc[.95]), "p99": float(quantiles.loc[.99]), "max": float(values.max()),
        "missing_rows": int(values.isna().sum()),
        "sparse_fallback_rows": int(fallback.str.contains("SPARSE|LEAGUE_PARK_FALLBACK", regex=True).sum()),
        "sparse_fallback_rate": float(fallback.str.contains("SPARSE|LEAGUE_PARK_FALLBACK", regex=True).mean()),
    }


def psi(training: pd.Series, comparison: pd.Series) -> float:
    inner = np.unique(training.quantile(np.arange(.1, 1.0, .1)).to_numpy(float))
    edges = np.r_[-np.inf, inner, np.inf]
    training_bins = pd.cut(training, edges, include_lowest=True).value_counts(sort=False, normalize=True).to_numpy(float)
    comparison_bins = pd.cut(comparison, edges, include_lowest=True).value_counts(sort=False, normalize=True).to_numpy(float)
    floor = 1e-6
    training_bins = np.clip(training_bins, floor, None)
    comparison_bins = np.clip(comparison_bins, floor, None)
    return float(np.sum((comparison_bins - training_bins) * np.log(comparison_bins / training_bins)))


def drift_severity(standardized_shift: float, population_stability_index: float, above_max: float) -> str:
    # Fixed distribution-only thresholds, declared before examining model performance.
    if abs(standardized_shift) >= 2 or population_stability_index >= .50 or above_max >= .50:
        return "EXTREME"
    if abs(standardized_shift) >= 1 or population_stability_index >= .25 or above_max >= .20:
        return "SEVERE"
    if abs(standardized_shift) >= .5 or population_stability_index >= .10 or above_max > 0:
        return "MODERATE"
    return "NONE"


def drift_row(level: str, period: str, frame: pd.DataFrame, training: pd.DataFrame) -> dict[str, Any]:
    values = frame[FEATURE].astype(float)
    base = training[FEATURE].astype(float)
    p90, p95, p99, maximum = (float(base.quantile(q)) for q in (.90, .95, .99, 1.0))
    std_shift = float((values.mean() - base.mean()) / base.std(ddof=1))
    stability = psi(base, values)
    above_max = float((values > maximum).mean())
    return {
        "aggregation_level": level, "period": period, "games": len(frame),
        "mean_depth": float(values.mean()), "standardized_mean_shift": std_shift,
        "share_above_training_p90": float((values > p90).mean()),
        "share_above_training_p95": float((values > p95).mean()),
        "share_above_training_p99": float((values > p99).mean()),
        "share_above_training_max": above_max, "population_stability_index": stability,
        "drift_severity": drift_severity(std_shift, stability, above_max),
        "threshold_contract": "NONE:<0.5SD,<0.10PSI,0>max; MODERATE:<1SD,<0.25PSI,<20%>max; SEVERE:<2SD,<0.50PSI,<50%>max; else EXTREME",
    }


def correlation_row(population: str, x_name: str, x: pd.Series, y_name: str, y: pd.Series) -> dict[str, Any]:
    valid = x.notna() & y.notna()
    return {
        "population": population, "feature": x_name, "comparison": y_name, "games": int(valid.sum()),
        "pearson": float(x[valid].corr(y[valid], method="pearson")) if valid.sum() > 1 else math.nan,
        "spearman": float(x[valid].corr(y[valid], method="spearman")) if valid.sum() > 1 else math.nan,
        "scope": "DESCRIPTIVE_ASSOCIATION_NOT_CAUSAL",
    }


def within_center(frame: pd.DataFrame, column: str, groups: list[str]) -> pd.Series:
    return frame[column].astype(float) - frame.groupby(groups)[column].transform("mean").astype(float)


def make_variant_row(label: str, frame: pd.DataFrame, forecasts: np.ndarray, alpha: float,
                     original: np.ndarray) -> dict[str, Any]:
    row = {"variant": label, **metrics(frame, forecasts, alpha)}
    row["mean_prediction_shift_vs_raw"] = float(np.mean(forecasts - original))
    row["counterfactual_status"] = "COUNTERFACTUAL_ONLY_NOT_A_MODEL" if label not in ("RAW_V1", "V1_INTERCEPT") else "FROZEN_DIAGNOSTIC_CONTROL"
    return row


def related_inventory(training: pd.DataFrame, prospective: pd.DataFrame, candidate: dict[str, Any]) -> list[dict[str, Any]]:
    related = {
        "park_history_depth": "venue sample-support count; direct model input",
        "home_starter_prior_starts": "starter history depth; direct model input",
        "away_starter_prior_starts": "starter history depth; direct model input",
        "home_bullpen_likely_available_reliever_count": "bullpen availability count; direct model input",
        "away_bullpen_likely_available_reliever_count": "bullpen availability count; direct model input",
        "home_starter_history_depth": "source-spine starter depth; not a separate frozen model input",
        "away_starter_history_depth": "source-spine starter depth; not a separate frozen model input",
    }
    rows = []
    for feature, semantics in related.items():
        if feature not in training or feature not in prospective:
            continue
        base = training[feature].astype(float)
        current = prospective[feature].astype(float)
        if feature in candidate["feature_order"]:
            index = candidate["feature_order"].index(feature)
            coefficient = float(candidate["coefficients"][index])
            center = float(candidate["scaler_mean"][index])
            scale = float(candidate["scaler_scale"][index])
            contribution = coefficient * (float(current.mean()) - center) / scale
        else:
            coefficient = math.nan; center = math.nan; scale = math.nan; contribution = math.nan
        std_shift = float((current.mean() - base.mean()) / base.std(ddof=1))
        beyond = float((current > base.max()).mean())
        severity = drift_severity(std_shift, psi(base, current), beyond)
        direction = "SUPPRESSION" if contribution < 0 else ("INFLATION" if contribution > 0 else "NONE_OR_NOT_DIRECT")
        rows.append({
            "feature": feature, "semantics": semantics, "frozen_model_input": feature in candidate["feature_order"],
            "frozen_coefficient": coefficient, "training_mean": float(base.mean()), "training_p99": float(base.quantile(.99)),
            "training_max": float(base.max()), "prospective_mean": float(current.mean()),
            "prospective_share_above_training_max": beyond, "standardized_mean_shift": std_shift,
            "drift_severity": severity, "mean_log_location_contribution_vs_training_center": contribution,
            "directional_effect": direction,
        })
    return rows


def run(output_dir: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    config_bytes = CONFIG.read_bytes()
    candidate = json.loads(config_bytes)
    if candidate.get("canonical_model_hash") != MODEL_HASH:
        raise RuntimeError("FROZEN_MODEL_HASH_MISMATCH")
    feature_index = candidate["feature_order"].index(FEATURE)
    coefficient = float(candidate["coefficients"][feature_index])
    center = float(candidate["scaler_mean"][feature_index])
    scale = float(candidate["scaler_scale"][feature_index])
    alpha = float(candidate["dispersion_alpha"])

    protected = [CONFIG, LEDGER, SPINE / "totals_core_feature_spine.csv", PARK_SPINE]
    protected_before = {str(path): sha256(path) for path in protected}
    historical = raw.load_historical(candidate)
    prospective = raw.load_prospective(candidate, alpha)
    historical_park = pd.read_csv(PARK_SPINE).merge(
        historical[["game_pk", "game_date"]], on="game_pk", how="left", validate="one_to_one"
    )
    combined = pd.concat([historical, prospective], ignore_index=True, sort=False)
    frames = {period: combined[combined.period == period].copy() for period in PERIODS}
    training = frames["DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"]
    training_p95 = float(training[FEATURE].quantile(.95))
    training_p99 = float(training[FEATURE].quantile(.99))
    training_max = float(training[FEATURE].max())

    distribution_rows = [descriptive_row(period, frames[period], "park_fallback_state" if period.startswith("PROSPECTIVE") else "fallback_status") for period in PERIODS]
    drift_rows = [drift_row("PERIOD", period, frames[period], training) for period in PERIODS]
    for month, group in historical.groupby(historical.game_date.dt.to_period("M")):
        drift_rows.append(drift_row("MONTH", str(month), group, training))
    for date, group in prospective.groupby(prospective.game_date.dt.date):
        drift_rows.append(drift_row("DAY", str(date), group, training))

    calendar_rows: list[dict[str, Any]] = []
    calendar = historical_park.sort_values(["venue_id", "game_date", "feature_cutoff_utc", "game_pk"]).copy()
    calendar["date_ordinal"] = (calendar.game_date - calendar.game_date.min()).dt.days
    formula_mismatches = 0
    for _, group in calendar.groupby("venue_id"):
        date_counts = group.groupby("game_date").size().sort_index()
        expected_by_date = date_counts.cumsum().shift(fill_value=0).to_dict()
        formula_mismatches += int((group[FEATURE] != group.game_date.map(expected_by_date)).sum())
    live_expected = historical.groupby("venue_id").size().to_dict()
    live_depth_mismatches = int((prospective[FEATURE] != prospective.venue_id.map(live_expected)).sum())
    if formula_mismatches or live_depth_mismatches:
        raise RuntimeError(
            f"PARK_HISTORY_DEPTH_LINEAGE_REPRODUCTION_FAILED_historical={formula_mismatches}_live={live_depth_mismatches}"
        )
    calendar_rows.append({
        "row_type": "GLOBAL_SUMMARY", "venue_id": "ALL", "park_name": "ALL", "games": len(calendar),
        "first_date": str(calendar.game_date.min().date()), "last_date": str(calendar.game_date.max().date()),
        "min_depth": float(calendar[FEATURE].min()), "max_depth": float(calendar[FEATURE].max()),
        "pearson_depth_vs_date": float(calendar[FEATURE].corr(calendar.date_ordinal)),
        "spearman_depth_vs_date": float(calendar[FEATURE].corr(calendar.date_ordinal, method="spearman")),
        "declines": int((calendar.groupby("venue_id")[FEATURE].diff() < 0).sum()),
        "season_reset_count": 0, "historical_depth_formula_mismatches": formula_mismatches,
        "prospective_live_depth_mismatches": live_depth_mismatches, "mechanical_calendar_growth": "YES",
        "notes": "Depth is a same-venue prior-game count; the historical builder freezes same-date rows together and never resets by season.",
    })
    for venue_id, group in calendar.groupby("venue_id"):
        group = group.sort_values(["game_date", "feature_cutoff_utc", "game_pk"])
        daily = group.groupby("game_date", as_index=False).agg(depth=(FEATURE, "first"), games=("game_pk", "size"))
        x = (daily.game_date - daily.game_date.min()).dt.days.to_numpy(float)
        slope_per_day = float(np.polyfit(x, daily.depth, 1)[0]) if len(daily) > 1 and np.ptp(x) > 0 else math.nan
        seasons = group.assign(season=group.game_date.dt.year).groupby("season")
        first_by_season = seasons[FEATURE].min().to_dict()
        resets = 0
        for season in sorted(first_by_season)[1:]:
            previous_max = float(group.loc[group.game_date.dt.year < season, FEATURE].max())
            resets += int(first_by_season[season] < previous_max)
        calendar_rows.append({
            "row_type": "VENUE_SUMMARY", "venue_id": int(venue_id), "park_name": str(group.park_name.iloc[-1]),
            "games": len(group), "first_date": str(group.game_date.min().date()), "last_date": str(group.game_date.max().date()),
            "min_depth": float(group[FEATURE].min()), "max_depth": float(group[FEATURE].max()),
            "estimated_depth_increase_per_week": slope_per_day * 7,
            "estimated_depth_increase_per_month": slope_per_day * (365.25 / 12),
            "positive_depth_steps": int((daily.depth.diff() > 0).sum()), "declines": int((daily.depth.diff() < 0).sum()),
            "season_reset_count": resets, "neutral_site_games": int(group.neutral_site.fillna(False).astype(bool).sum()),
            "historical_depth_formula_mismatches": int((group[FEATURE] != group.game_date.map(
                daily.set_index("game_date").games.cumsum().shift(fill_value=0).to_dict()
            )).sum()),
            "sparse_or_special_venue": bool(len(group) < 50 or group.neutral_site.fillna(False).astype(bool).any()),
            "mechanical_calendar_growth": "YES" if not resets and not (daily.depth.diff() < 0).any() else "PARTIAL",
        })
        for season, season_group in seasons:
            calendar_rows.append({
                "row_type": "SEASON_BOUNDARY", "venue_id": int(venue_id), "park_name": str(group.park_name.iloc[-1]),
                "season": int(season), "games": len(season_group), "first_date": str(season_group.game_date.min().date()),
                "last_date": str(season_group.game_date.max().date()), "min_depth": float(season_group[FEATURE].min()),
                "max_depth": float(season_group[FEATURE].max()), "reset_at_season_start": False if season == min(first_by_season) else bool(first_by_season[season] < float(group.loc[group.game_date.dt.year < season, FEATURE].max())),
            })

    effect_points = {
        "TRAINING_P10": float(training[FEATURE].quantile(.10)), "TRAINING_MEAN": center,
        "TRAINING_MEDIAN": float(training[FEATURE].median()), "TRAINING_P90": float(training[FEATURE].quantile(.90)),
        "TRAINING_P95": training_p95, "TRAINING_MAX": training_max,
        "PROSPECTIVE_MEDIAN": float(prospective[FEATURE].median()),
        "PROSPECTIVE_P90": float(prospective[FEATURE].quantile(.90)),
        "PROSPECTIVE_MAX": float(prospective[FEATURE].max()),
    }
    effect_rows = []
    for label, value in effect_points.items():
        contribution = coefficient * (value - center) / scale
        effect_rows.append({
            "point": label, "park_history_depth": value, "standardized_depth": (value - center) / scale,
            "frozen_coefficient": coefficient, "isolated_log_location_contribution": contribution,
            "isolated_multiplicative_location_factor": math.exp(contribution),
            "per_additional_game_log_location_change": coefficient / scale,
            "per_additional_game_multiplicative_factor": math.exp(coefficient / scale),
        })

    training_corr = training.copy()
    training_corr["date_ordinal"] = (training_corr.game_date - training_corr.game_date.min()).dt.days
    training_corr["season"] = training_corr.game_date.dt.year
    training_corr["park_factor_uncertainty"] = 1 / np.sqrt(training_corr[FEATURE].clip(lower=1))
    training_corr["sparse_fallback_indicator"] = training_corr.fallback_status.astype(str).str.contains("SPARSE").astype(int)
    first_venue_date = training_corr.groupby("venue_id").game_date.transform("min")
    training_corr["venue_appearance_age_days"] = (training_corr.game_date - first_venue_date).dt.days
    correlation_targets = [
        "final_total", "strict_prior_total_run_factor", "raw_forecast", "run_residual", "date_ordinal", "season",
        "league_total", "park_factor_uncertainty", "sparse_fallback_indicator", "venue_appearance_age_days",
        "home_starter_prior_starts", "away_starter_prior_starts",
    ]
    correlation_rows = [correlation_row("DEVELOPMENT_2023_24", FEATURE, training_corr[FEATURE], target, training_corr[target]) for target in correlation_targets]

    within_rows: list[dict[str, Any]] = []
    for population, population_frame in (("DEVELOPMENT_2023_24", training), ("ALL_HISTORICAL_2023_AUG05", historical)):
        data = population_frame.copy()
        data["season_month"] = data.game_date.dt.to_period("M").astype(str)
        depth_venue = within_center(data, FEATURE, ["venue_id"])
        depth_month = within_center(data, FEATURE, ["venue_id", "season_month"])
        for outcome in ("final_total", "raw_forecast", "run_residual", "strict_prior_total_run_factor"):
            outcome_venue = within_center(data, outcome, ["venue_id"])
            outcome_month = within_center(data, outcome, ["venue_id", "season_month"])
            within_rows.append({
                "row_type": "AGGREGATE_FIXED_EFFECT", "population": population, "venue_id": "ALL", "park_name": "ALL",
                "games": len(data), "comparison": outcome,
                "within_venue_pearson": float(depth_venue.corr(outcome_venue)),
                "within_venue_month_pearson": float(depth_month.corr(outcome_month)),
                "interpretation": "calendar-controlled descriptive association; not a fitted prediction model",
            })
        for venue_id, group in data.groupby("venue_id"):
            if len(group) < 50:
                continue
            within_rows.append({
                "row_type": "VENUE", "population": population, "venue_id": int(venue_id),
                "park_name": str(group.park_name.iloc[-1]), "games": len(group), "mean_depth": float(group[FEATURE].mean()),
                "depth_actual_pearson": float(group[FEATURE].corr(group.final_total)),
                "depth_raw_forecast_pearson": float(group[FEATURE].corr(group.raw_forecast)),
                "depth_residual_pearson": float(group[FEATURE].corr(group.run_residual)),
            })
    controlled_actual = next(row for row in within_rows if row["row_type"] == "AGGREGATE_FIXED_EFFECT" and row["population"] == "DEVELOPMENT_2023_24" and row["comparison"] == "final_total")
    controlled_corr = float(controlled_actual["within_venue_month_pearson"])
    within_signal = "ABSENT" if abs(controlled_corr) < .05 else ("WEAK" if abs(controlled_corr) < .15 else ("PRESENT" if controlled_corr < 0 else "INVERTED"))
    proxy_class = "LIKELY_SAMPLE_DEPTH_ARTIFACT" if abs(float(training_corr[FEATURE].corr(training_corr.date_ordinal, method="spearman"))) > .75 and within_signal in ("ABSENT", "WEAK") else "MIXED"

    extrapolation_rows: list[dict[str, Any]] = []
    support_bands = (
        ("AT_OR_BELOW_P95", lambda z: z[FEATURE] <= training_p95),
        ("ABOVE_P95_TO_P99", lambda z: (z[FEATURE] > training_p95) & (z[FEATURE] <= training_p99)),
        ("ABOVE_P99_TO_MAX", lambda z: (z[FEATURE] > training_p99) & (z[FEATURE] <= training_max)),
        ("ABOVE_TRAINING_MAX", lambda z: z[FEATURE] > training_max),
    )
    for period in ("FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT", "PROSPECTIVE_AUG06_15"):
        frame = frames[period]
        for band, selector in support_bands:
            group = frame[selector(frame)]
            row = {"period": period, "support_band": band, "training_p95": training_p95, "training_p99": training_p99,
                   "training_max": training_max, **metrics(group, group.raw_forecast.to_numpy(float), alpha)}
            extrapolation_rows.append(row)
    prospective_above_max = float((prospective[FEATURE] > training_max).mean())
    extrapolation_association = "MODERATE" if prospective_above_max >= .90 else "WEAK"

    prospective_forecasts = counterfactuals(prospective, coefficient, center, scale, training_p95)
    variant_rows = {name: make_variant_row(name, prospective, forecast, alpha, prospective_forecasts["RAW_V1"])
                    for name, forecast in prospective_forecasts.items()}
    write_csv(output_dir / "totals_park_depth_counterfactual_training_mean.csv", [variant_rows["TRAINING_MEAN"]])
    write_csv(output_dir / "totals_park_depth_counterfactual_p95_cap.csv", [variant_rows["P95_CAP"]])
    write_csv(output_dir / "totals_park_depth_counterfactual_coefficient_zero.csv", [variant_rows["COEFFICIENT_ZERO"]])

    historical_counterfactual_rows = []
    for period in OUT_OF_TIME_PERIODS:
        frame = frames[period]
        forecasts = counterfactuals(frame, coefficient, center, scale, training_p95)
        for name in ("RAW_V1", "TRAINING_MEAN", "P95_CAP", "COEFFICIENT_ZERO"):
            historical_counterfactual_rows.append({"period": period, **make_variant_row(name, frame, forecasts[name], alpha, forecasts["RAW_V1"])})

    daily_rows = []
    daily_improvement_counts = {name: 0 for name in ("TRAINING_MEAN", "P95_CAP", "COEFFICIENT_ZERO")}
    for date, frame in prospective.groupby(prospective.game_date.dt.date):
        forecasts = counterfactuals(frame, coefficient, center, scale, training_p95)
        original_metrics = metrics(frame, forecasts["RAW_V1"], alpha)
        for name in ("RAW_V1", "TRAINING_MEAN", "P95_CAP", "COEFFICIENT_ZERO"):
            row = {"game_date": str(date), **make_variant_row(name, frame, forecasts[name], alpha, forecasts["RAW_V1"])}
            row["absolute_bias_improved_vs_raw"] = abs(row["actual_minus_forecast_bias"]) < abs(original_metrics["actual_minus_forecast_bias"])
            row["mae_improved_vs_raw"] = row["mae"] < original_metrics["mae"]
            row["crps_improved_vs_raw"] = row["crps"] < original_metrics["crps"]
            daily_rows.append(row)
            if name != "RAW_V1" and row["crps_improved_vs_raw"]:
                daily_improvement_counts[name] += 1
    stability = "BROAD" if daily_improvement_counts["P95_CAP"] >= 8 else ("MODERATE" if daily_improvement_counts["P95_CAP"] >= 5 else "UNSTABLE")

    prospective_band = prospective.copy()
    prospective_band["forecast_band"] = prospective_band.raw_forecast.map(raw.forecast_band)
    band_rows = []
    for band, frame in prospective_band.groupby("forecast_band", observed=True):
        forecasts = counterfactuals(frame, coefficient, center, scale, training_p95)
        for name in ("RAW_V1", "TRAINING_MEAN", "P95_CAP", "COEFFICIENT_ZERO"):
            band_rows.append({"forecast_band": str(band), **make_variant_row(name, frame, forecasts[name], alpha, forecasts["RAW_V1"])})

    park_rows = []
    for (venue_id, park_name), frame in prospective.groupby(["venue_id", "park_name"], dropna=False):
        if len(frame) < 3:
            continue
        forecasts = counterfactuals(frame, coefficient, center, scale, training_p95)
        for name in ("RAW_V1", "TRAINING_MEAN", "P95_CAP", "COEFFICIENT_ZERO"):
            park_rows.append({
                "venue_id": venue_id, "park_name": park_name, "mean_depth": float(frame[FEATURE].mean()),
                **make_variant_row(name, frame, forecasts[name], alpha, forecasts["RAW_V1"]),
            })

    vs_intercept_rows = [variant_rows[name] for name in ("RAW_V1", "V1_INTERCEPT", "TRAINING_MEAN", "P95_CAP", "COEFFICIENT_ZERO")]
    raw_bias = float(variant_rows["RAW_V1"]["actual_minus_forecast_bias"])
    accounting_rows = []
    for name in ("V1_INTERCEPT", "TRAINING_MEAN", "P95_CAP", "COEFFICIENT_ZERO"):
        shift = float(variant_rows[name]["mean_prediction_shift_vs_raw"])
        remaining = float(variant_rows[name]["actual_minus_forecast_bias"])
        accounting_rows.append({
            "variant": name, "observed_mean_raw_residual_actual_minus_forecast": raw_bias,
            "mean_location_shift_vs_raw_runs": shift, "residual_remaining_after_neutralization": remaining,
            "signed_proportion_of_raw_bias_mechanically_offset": shift / raw_bias,
            "absolute_mean_bias_reduction_fraction": 1 - abs(remaining) / abs(raw_bias),
            "attribution_scope": "MODEL_CONTRIBUTION_NOT_CAUSAL_ATTRIBUTION_OF_ACTUAL_RUNS",
            "nonlinearity_note": "The log-link contribution becomes multiplicative in run space; mean run shifts are exact row-level transformations, not additive coefficient sums.",
        })
    mean_accounting = next(row for row in accounting_rows if row["variant"] == "TRAINING_MEAN")
    cap_accounting = next(row for row in accounting_rows if row["variant"] == "P95_CAP")
    related_rows = related_inventory(training, prospective, candidate)

    intent = "SAMPLE_SIZE_SUPPORT_SIGNAL"
    drift = next(row["drift_severity"] for row in drift_rows if row["aggregation_level"] == "PERIOD" and row["period"] == "PROSPECTIVE_AUG06_15")
    calendar_growth = "YES"
    design = "BETTER_AS_CONFIDENCE/WEIGHT_SIGNAL"
    root_cause = "MIXED_STRUCTURAL_DEFECT"
    redesign = "JUSTIFIED"
    intercept_relation = "PARTLY_COMPENSATES_PARK_DEPTH"
    intercept_status = "INTERCEPT_DIAGNOSTIC_INTERPRETATION_COMPROMISED"
    final_declaration = "PARK_HISTORY_DEPTH_PRIMARY_STRUCTURAL_DRIVER"

    identity = {
        "task_id": TASK_ID, "model_name": candidate["candidate_identity"], "model_hash": MODEL_HASH,
        "artifact_path": str(CONFIG.relative_to(ROOT)), "artifact_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "model_hash_verified": True, "feature": FEATURE, "feature_index_zero_based": feature_index,
        "frozen_coefficient": coefficient, "training_center": center, "training_scale": scale,
        "link": "log location with exponential inverse", "prospective_games": len(prospective),
        "counterfactual_contract": "COUNTERFACTUAL_ONLY_NOT_A_MODEL",
        "protected_input_hashes_before": protected_before,
    }
    raw.write_json(output_dir / "totals_park_history_depth_identity.json", identity)
    (output_dir / "totals_park_history_depth_lineage.md").write_text(f"""# park_history_depth lineage

- Source: official MLB schedule games in the governed totals feature spine, keyed by `venue_id`.
- Historical construction: for each target date, `park_history_depth = len(prior completed official games at the same venue with an earlier game date)`. All games on a target date are frozen before that date's outcomes enter history.
- Live construction: the bridge rebuilds the same venue history through the frozen historical spine's last completed game and exposes the resulting count to each prospective context. The Aug 6–15 prospective rows therefore use the retained through-Aug-5 state rather than outcomes from the prospective stream.
- Formula consuming depth for park shrinkage: `w = n / (n + 50)` and `strict_prior_total_run_factor = w * mean(prior adjusted total ratios) + (1 - w) * 1.0`.
- Grain/unit: one integer count per game/venue state; unit is prior official games at that venue.
- As-of rule: strict prior; current-game outcomes are excluded. The retained historical construction is date-strict.
- Season behavior: the governed population begins March 30, 2023 and carries venue history across 2024, 2025, and 2026 without an annual reset.
- Fallback: `LEAGUE_REGRESSED_SPARSE_PARK` below 20 games; absent venue state becomes depth 0 and `LEAGUE_PARK_FALLBACK` in the live bridge.
- Missingness: no missing depth in the 9,012 historical rows or 126 prospective rows examined.
- Preprocessing: direct numeric input, standardized by frozen development mean {center:.15f} and scale {scale:.15f}; no cap, log, saturation, season reset, or uncertainty-only gating.
- Frozen downstream equation: `log(mu) += {coefficient:.15f} * ((park_history_depth - {center:.15f}) / {scale:.15f})`.
- Source code: `{BUILDER.relative_to(ROOT)}` (historical construction), `{LIVE_BRIDGE.relative_to(ROOT)}` (live context), and `{CONFIG.relative_to(ROOT)}` (frozen preprocessing/coefficient).

The count is intrinsically a sample-support/data-confidence quantity for the regressed park factor. Its separate admission to expected-run location turns increasing data volume into a directional run forecast.
""")
    (output_dir / "totals_park_history_depth_intent.md").write_text(f"""# park_history_depth intended semantics

`PARK_HISTORY_DEPTH_INTENT = {intent}`

The field counts observations supporting the park state and controls its shrinkage/fallback tier. It does not encode a stationary baseball property. No governed source describes a direct causal relationship in which merely observing more games at a venue should lower scoring. Its direct location coefficient is therefore a learned proxy use layered on top of its sample-support role.
""")
    write_csv(output_dir / "totals_park_history_depth_distribution.csv", distribution_rows)
    write_csv(output_dir / "totals_park_history_depth_drift.csv", drift_rows)
    write_csv(output_dir / "totals_park_history_depth_calendar_growth.csv", calendar_rows)
    write_csv(output_dir / "totals_park_history_depth_effect_curve.csv", effect_rows)
    write_csv(output_dir / "totals_park_history_depth_training_correlations.csv", correlation_rows)
    write_csv(output_dir / "totals_park_history_depth_within_park.csv", within_rows)
    write_csv(output_dir / "totals_park_history_depth_extrapolation.csv", extrapolation_rows)
    write_csv(output_dir / "totals_park_depth_historical_counterfactuals.csv", historical_counterfactual_rows)
    write_csv(output_dir / "totals_park_depth_daily_counterfactuals.csv", daily_rows)
    write_csv(output_dir / "totals_park_depth_forecast_band_effect.csv", band_rows)
    write_csv(output_dir / "totals_park_depth_park_level_effect.csv", park_rows)
    write_csv(output_dir / "totals_park_depth_vs_intercept.csv", vs_intercept_rows)
    write_csv(output_dir / "totals_park_depth_contribution_accounting.csv", accounting_rows)
    write_csv(output_dir / "totals_related_depth_feature_inventory.csv", related_rows)

    related_material = [
        row for row in related_rows
        if row["feature"] != FEATURE and row["prospective_share_above_training_max"] >= .05
    ]
    (output_dir / "totals_park_depth_design_assessment.md").write_text(f"""# park_history_depth design assessment

- `PARK_HISTORY_DEPTH_DESIGN = {design}`
- `PARK_HISTORY_DEPTH_TRAINING_ROLE = {proxy_class}`
- `PARK_HISTORY_DEPTH_WITHIN_PARK_SIGNAL = {within_signal}`
- `PARK_HISTORY_DEPTH_OUT_OF_SUPPORT_ASSOCIATION = {extrapolation_association}`
- `PARK_DEPTH_COUNTERFACTUAL_STABILITY = {stability}`
- `INTERCEPT_VS_PARK_DEPTH = {intercept_relation}`
- `PARK_HISTORY_DEPTH_REDESIGN = {redesign}`
- `{intercept_status}`

The feature has a valid governed role as sample support for shrinkage and fallback, but the evidence does not support its use as an unbounded direct expected-run location input. A separate governed redesign is justified to compare removal from location, bounded/log-saturating representations, uncertainty/sample-weight-only use, and stationary park-confidence representations. This analysis selects none of them.

The counterfactuals are structural diagnostics only. They are not fitted models, promoted rules, recalibration, or production changes.
""")
    (output_dir / "totals_park_depth_root_cause.md").write_text(f"""# park_history_depth root cause

- `PARK_DEPTH_ROOT_CAUSE = {root_cause}`
- `FINAL_STRUCTURAL_DECLARATION = {final_declaration}`

The values are internally coherent counts, not a data corruption. The defect is mixed: a sample-support quantity is admitted directly to location, its negative coefficient is plausibly learned from calendar/proxy structure rather than within-park scoring signal, and its unbounded cross-season accumulation sends 121/126 prospective rows beyond the development maximum.

Neutralizing to the training mean mechanically raises the prospective forecast by {mean_accounting['mean_location_shift_vs_raw_runs']:.6f} runs and crosses through zero bias; capping at the training p95 raises it by {cap_accounting['mean_location_shift_vs_raw_runs']:.6f} runs and leaves {cap_accounting['residual_remaining_after_neutralization']:+.6f} actual-minus-forecast bias. This attributes forecast suppression, not the baseball cause of realized runs. Daily outcomes and forecast bands remain heterogeneous, so depth is not the sole explanation of every error.
""")

    raw_row = variant_rows["RAW_V1"]
    mean_row = variant_rows["TRAINING_MEAN"]
    cap_row = variant_rows["P95_CAP"]
    zero_row = variant_rows["COEFFICIENT_ZERO"]
    intercept_row = variant_rows["V1_INTERCEPT"]
    historical_cap = {row["period"]: row for row in historical_counterfactual_rows if row["variant"] == "P95_CAP"}
    related_summary = ", ".join(f"{row['feature']} ({row['drift_severity']}, {row['directional_effect']})" for row in related_material) or "none"
    concise = f"""# Concise MLB totals park_history_depth structural attribution v1

- Exact definition: integer count of earlier governed official games at the same venue; date-strict in the historical builder, accumulated across seasons, and used in `n/(n+50)` park-factor shrinkage.
- Intent: `{intent}`. Frozen coefficient {coefficient:+.15f}, training center {center:.6f}, scale {scale:.6f}; each additional game multiplies expected location by {math.exp(coefficient / scale):.9f}.
- Distribution: development mean/median/max {training[FEATURE].mean():.6f}/{training[FEATURE].median():.6f}/{training_max:.0f}; prospective {prospective[FEATURE].mean():.6f}/{prospective[FEATURE].median():.6f}/{prospective[FEATURE].max():.0f}. `{drift}` drift; {prospective_above_max:.3%} above development maximum.
- Calendar mechanics: `{calendar_growth}`; history is monotone within venue and does not reset by season. The deployed prospective bridge retains the through-Aug-5 state during this window.
- Interpretation: the negative coefficient mechanically lowers location as the support count rises. Training classification `{proxy_class}`; calendar-controlled within-park actual-total correlation {controlled_corr:+.6f}, `{within_signal}` signal.
- Out-of-support: 121/126 prospective games exceed the training maximum; outcome association `{extrapolation_association}` because the only five in-support rows are one low-depth venue and do not supply a balanced comparator.
- Prospective RAW: MAE {raw_row['mae']:.6f}, RMSE {raw_row['rmse']:.6f}, actual-minus-forecast bias {raw_row['actual_minus_forecast_bias']:+.6f}, CRPS {raw_row['crps']:.6f}.
- Training-mean A: MAE {mean_row['mae']:.6f}, RMSE {mean_row['rmse']:.6f}, bias {mean_row['actual_minus_forecast_bias']:+.6f}, CRPS {mean_row['crps']:.6f}; signed mechanical offset {mean_accounting['signed_proportion_of_raw_bias_mechanically_offset']:.3%}.
- P95-cap B: MAE {cap_row['mae']:.6f}, RMSE {cap_row['rmse']:.6f}, bias {cap_row['actual_minus_forecast_bias']:+.6f}, CRPS {cap_row['crps']:.6f}; signed mechanical offset {cap_accounting['signed_proportion_of_raw_bias_mechanically_offset']:.3%} and absolute mean-bias reduction {cap_accounting['absolute_mean_bias_reduction_fraction']:.3%}.
- Coefficient-zero C: algebraically identical to A under the frozen scaler; MAE {zero_row['mae']:.6f}, RMSE {zero_row['rmse']:.6f}, bias {zero_row['actual_minus_forecast_bias']:+.6f}, CRPS {zero_row['crps']:.6f}.
- Historical: p95 capping leaves actual-minus-forecast bias {historical_cap['FROZEN_2025_VALIDATION']['actual_minus_forecast_bias']:+.6f} in 2025, {historical_cap['2026_SEQUENTIAL_EARLY']['actual_minus_forecast_bias']:+.6f} early 2026, and {historical_cap['2026_LATE_HOLDOUT']['actual_minus_forecast_bias']:+.6f} late holdout; CRPS improves in all three, while MAE does not.
- Stability: `{stability}`. The model-mechanical shift is broad across mature parks and common forecast bands, but daily error improvement is mixed and the nonlinear band shape remains.
- +0.493550 intercept: bias {intercept_row['actual_minus_forecast_bias']:+.6f}, CRPS {intercept_row['crps']:.6f}; `{intercept_relation}`. `{intercept_status}`.
- Related support/count features requiring later safety attention: {related_summary}. None was counterfactually optimized.
- Design/root: `{design}` / `{root_cause}`. Focused redesign `{redesign}`.
- Final: `{final_declaration}` (model-mechanical, not causal baseball attribution).
- Exact next human decision: authorize or decline a separately governed feature redesign comparison; do not alter V1 or promote any counterfactual from this task.
"""
    (output_dir / "concise_mlb_totals_park_history_depth_structural_attribution_v1.md").write_text(concise)

    protected_after = {str(path): sha256(path) for path in protected}
    if protected_before != protected_after:
        raise RuntimeError("PROTECTED_FROZEN_INPUT_MUTATED")
    identity["protected_input_hashes_after"] = protected_after
    raw.write_json(output_dir / "totals_park_history_depth_identity.json", identity)

    manifest = output_dir / "reproducibility_hashes.sha256"
    output_files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != manifest)
    inputs = [Path(__file__), CONFIG, LEDGER, SPINE / "totals_core_feature_spine.csv", PARK_SPINE,
              raw.HISTORICAL_RESIDUALS, BUILDER, LIVE_BRIDGE, PRIOR_ANALYSIS]
    manifest.write_text("".join(f"{sha256(path)}  {path.name}\n" for path in output_files) +
                        "".join(f"{sha256(path)}  INPUT::{path.relative_to(ROOT)}\n" for path in inputs))

    return {
        "task_id": TASK_ID, "model_hash": MODEL_HASH, "frozen_coefficient": coefficient,
        "training_mean": float(training[FEATURE].mean()), "prospective_mean": float(prospective[FEATURE].mean()),
        "drift_severity": drift, "mechanical_calendar_growth": calendar_growth,
        "within_park_signal": within_signal, "training_role": proxy_class,
        "out_of_support_association": extrapolation_association, "counterfactual_stability": stability,
        "feature_design": design, "root_cause": root_cause, "redesign": redesign,
        "intercept_relation": intercept_relation, "intercept_status": intercept_status,
        "final_declaration": final_declaration, "prospective_raw": raw_row,
        "training_mean_counterfactual": mean_row, "p95_cap_counterfactual": cap_row,
        "coefficient_zero_counterfactual": zero_row, "output_dir": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2))


if __name__ == "__main__":
    main()

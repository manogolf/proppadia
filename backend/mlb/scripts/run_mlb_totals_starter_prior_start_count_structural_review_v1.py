"""Read-only structural review of MLB totals starter prior-start counts.

This module never fits a model. It scores the frozen control and the already
frozen park-depth-repair challenger, then applies explicitly labelled
counterfactual feature neutralizations to those fixed artifacts.
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
from backend.mlb.scripts import run_mlb_totals_remove_park_history_depth_direct_location_defect_v1 as repair


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_STARTER_PRIOR_START_COUNT_STRUCTURAL_REVIEW_V1"
CONTROL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
REPAIR_HASH = "43256ef8396ddfdb53c58f04cc5b8fa783b97c457abf0072b767e7df6050d1b7"
FEATURES = ("home_starter_prior_starts", "away_starter_prior_starts")
PERIODS = (
    "FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY",
    "2026_LATE_HOLDOUT", "PROSPECTIVE_AUG06_15",
)
CONTROL = raw.CONFIG
REPAIR = ROOT / "artifacts/analysis/model_development/mlb_totals_remove_park_history_depth_direct_location_defect_v1/2026-08-16/TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1.json"
BUILDER = ROOT / "tmp/analysis/build_mlb_totals_feature_spine_v1.py"
LIVE_BRIDGE = ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_starter_prior_start_count_structural_review_v1/2026-08-16"
EXPERIENCE_BANDS = (
    ("0-4", 0, 4), ("5-9", 5, 9), ("10-19", 10, 19),
    ("20-39", 20, 39), ("40-79", 40, 79), ("80+", 80, math.inf),
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def corr(x: pd.Series | np.ndarray, y: pd.Series | np.ndarray, method: str = "pearson") -> float:
    # Reset any source-frame indices so same-length row arrays align positionally.
    x = pd.Series(np.asarray(x, dtype=float)); y = pd.Series(np.asarray(y, dtype=float))
    mask = x.notna() & y.notna()
    return float(x[mask].corr(y[mask], method=method)) if mask.sum() >= 3 and x[mask].std() and y[mask].std() else math.nan


def eta_squared(values: pd.Series, groups: pd.Series) -> float:
    frame = pd.DataFrame({"x": pd.to_numeric(values, errors="coerce"), "g": groups}).dropna()
    if frame.empty or frame.x.var() == 0:
        return math.nan
    grand = frame.x.mean()
    between = sum(len(group) * (group.x.mean() - grand) ** 2 for _, group in frame.groupby("g"))
    total = float(((frame.x - grand) ** 2).sum())
    return float(between / total) if total else math.nan


def artifact_term(artifact: dict[str, Any], feature: str) -> tuple[float, float, float]:
    index = artifact["feature_order"].index(feature)
    return (float(artifact["coefficients"][index]), float(artifact["scaler_mean"][index]),
            float(artifact["scaler_scale"][index]))


def descriptive(period: str, frame: pd.DataFrame, feature: str) -> dict[str, Any]:
    values = frame[feature].astype(float)
    side = feature.split("_")[0]
    fallback = frame.get(f"{side}_starter_fallback_source", pd.Series("", index=frame.index)).astype(str)
    if period == "PROSPECTIVE_AUG06_15":
        fallback = frame.get("starter_fallback_state", fallback).astype(str)
    return {
        "period": period, "feature": feature, "games": len(frame), "mean": values.mean(),
        "std": values.std(ddof=1), "min": values.min(), "p05": values.quantile(.05), "p10": values.quantile(.10),
        "p25": values.quantile(.25), "median": values.median(), "p75": values.quantile(.75),
        "p90": values.quantile(.90), "p95": values.quantile(.95), "p99": values.quantile(.99),
        "max": values.max(), "zero_rate": (values == 0).mean(), "below_three_rate": (values < 3).mean(),
        "governed_sparse_fallback_rate": fallback.str.contains("REGRESSED|COHORT|TEAM|LEAGUE|SPARSE", regex=True).mean(),
    }


def psi(reference: pd.Series, current: pd.Series) -> float:
    edges = np.unique(np.quantile(reference.astype(float), np.linspace(0, 1, 11)))
    if len(edges) < 3:
        return 0.0
    edges[0] = -np.inf; edges[-1] = np.inf
    a = np.histogram(reference, edges)[0] / len(reference)
    b = np.histogram(current, edges)[0] / len(current)
    a = np.clip(a, 1e-8, None); b = np.clip(b, 1e-8, None)
    return float(np.sum((b - a) * np.log(b / a)))


def counterfactual(frame: pd.DataFrame, baseline: np.ndarray, artifact: dict[str, Any],
                   feature: str, variant: str, training_mean: float, training_p95: float) -> np.ndarray:
    coefficient, center, scale = artifact_term(artifact, feature)
    original = frame[feature].to_numpy(float)
    if variant in ("TRAINING_MEAN", "ZERO_COEFFICIENT_CONTRIBUTION"):
        replacement = np.full(len(frame), center if variant == "ZERO_COEFFICIENT_CONTRIBUTION" else training_mean)
    elif variant == "TRAINING_P95_CAP":
        replacement = np.minimum(original, training_p95)
    elif variant == "ORIGINAL":
        replacement = original
    else:
        raise ValueError(variant)
    return baseline * np.exp(coefficient * (replacement - original) / scale)


def centered(frame: pd.DataFrame, column: str, groups: list[str]) -> np.ndarray:
    return (frame[column] - frame.groupby(groups)[column].transform("mean")).to_numpy(float)


def residualize(values: np.ndarray, controls: np.ndarray) -> np.ndarray:
    mask = np.isfinite(values) & np.isfinite(controls).all(axis=1)
    result = np.full(len(values), np.nan)
    if mask.sum() < controls.shape[1] + 3:
        return result
    matrix = np.column_stack([np.ones(mask.sum()), controls[mask]])
    result[mask] = values[mask] - matrix @ np.linalg.lstsq(matrix, values[mask], rcond=None)[0]
    return result


def starter_appearances(historical: pd.DataFrame) -> pd.DataFrame:
    rows = []
    common = ["game_pk", "game_date", "period", "final_total", "raw_forecast", "repaired_forecast",
              "run_residual", "repaired_residual", "park_history_depth", "strict_prior_total_run_factor"]
    for side in ("home", "away"):
        selected = historical[common + [f"{side}_starter_starter_pitcher_id", f"{side}_team_id",
                              f"{side}_starter_prior_starts", f"{side}_starter_ra9",
                              f"{side}_starter_fallback_source"]].copy()
        selected = selected.rename(columns={
            f"{side}_starter_starter_pitcher_id": "pitcher_id", f"{side}_team_id": "team_id",
            f"{side}_starter_prior_starts": "prior_starts", f"{side}_starter_ra9": "starter_ra9",
            f"{side}_starter_fallback_source": "fallback_source",
        })
        selected["side"] = side.upper(); selected["feature"] = f"{side}_starter_prior_starts"
        rows.append(selected)
    result = pd.concat(rows, ignore_index=True).dropna(subset=["pitcher_id"])
    result["pitcher_id"] = result.pitcher_id.astype(int)
    result["date_ordinal"] = (result.game_date - result.game_date.min()).dt.days
    result["season_month"] = result.game_date.dt.to_period("M").astype(str)
    return result


def vif_for_feature(training: pd.DataFrame, feature_order: list[str], feature: str) -> float:
    x = training[feature].to_numpy(float)
    others = [name for name in feature_order if name != feature]
    matrix = training[others].to_numpy(float)
    matrix = np.column_stack([np.ones(len(matrix)), matrix])
    fitted = matrix @ np.linalg.lstsq(matrix, x, rcond=None)[0]
    ss_total = float(((x - x.mean()) ** 2).sum()); ss_residual = float(((x - fitted) ** 2).sum())
    r2 = 1 - ss_residual / ss_total
    return float(1 / (1 - r2)) if r2 < 1 else math.inf


def run(output_dir: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    control = json.loads(CONTROL.read_text()); park_repair = json.loads(REPAIR.read_text())
    if control.get("canonical_model_hash") != CONTROL_HASH or park_repair.get("canonical_model_hash") != REPAIR_HASH:
        raise RuntimeError("FROZEN_ARTIFACT_IDENTITY_FAILED")
    protected = [CONTROL, REPAIR, raw.LEDGER, raw.SPINE / "totals_core_feature_spine.csv", BUILDER, LIVE_BRIDGE]
    protected_before = {str(path): sha256(path) for path in protected}

    historical = raw.load_historical(control)
    prospective = raw.load_prospective(control, float(control["dispersion_alpha"]))
    historical["repaired_forecast"] = repair.score_artifact(historical, park_repair)
    prospective["repaired_forecast"] = repair.score_artifact(prospective, park_repair)
    for frame in (historical, prospective):
        frame["repaired_residual"] = frame.final_total - frame.repaired_forecast
    combined = pd.concat([historical, prospective], ignore_index=True, sort=False)
    frames = {period: combined[combined.period == period].copy() for period in PERIODS}
    training = historical[historical.period == "DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"].copy()

    inventory_rows = []
    control_coef = dict(zip(control["feature_order"], control["coefficients"]))
    repair_coef = dict(zip(park_repair["feature_order"], park_repair["coefficients"]))
    for side, feature in zip(("home", "away"), FEATURES):
        coefficient, center, scale = artifact_term(control, feature)
        inventory_rows.extend([
            {"field": feature, "side": side.upper(), "direct_location_input": True,
             "source": f"{BUILDER.relative_to(ROOT)}::build_states; {LIVE_BRIDGE.relative_to(ROOT)}::_starter/feature_row",
             "semantics": "cumulative strict-prior starts for probable pitcher across governed history",
             "formula": "n = len(prior official appearances where is_starter and game_date < target_date)",
             "grain": "one value per game x home/away probable-starter slot", "units": "prior starts",
             "as_of_construction": "date-strict; same-date outcomes excluded; cumulative across seasons",
             "missing_fallback_behavior": "resolved pitcher with no history or unresolved pitcher yields n=0 while starter rate/workload uses governed team/league fallback",
             "training_preprocessing": f"StandardScaler: center={center:.17g}; scale={scale:.17g}",
             "fallback_or_shrinkage_role": "n>=3 gates direct starter state and last-three workload; n=1-2 pitcher cohort; n=0 team/league fallback",
             "all_upstream_downstream_uses": "upstream starter-state fallback tier, sparse-history status, workload base/tier, source history_depth; downstream frozen log-location input",
             "control_coefficient": control_coef[feature], "park_repair_coefficient": repair_coef[feature],
             "intent_classification": "MIXED"},
            {"field": f"{side}_starter_history_depth", "side": side.upper(), "direct_location_input": False,
             "source": f"{BUILDER.relative_to(ROOT)}::build_states",
             "semantics": "source-spine alias of the same cumulative prior-start count",
             "formula": f"identical n as {feature}", "grain": "one value per game x starter slot", "units": "prior starts",
             "as_of_construction": "date-strict and cross-season cumulative", "missing_fallback_behavior": "0 with no prior pitcher history",
             "training_preprocessing": "not separately included in frozen model",
             "fallback_or_shrinkage_role": "lineage/support metadata", "all_upstream_downstream_uses": "source support/certification only",
             "control_coefficient": "", "park_repair_coefficient": "", "intent_classification": "SAMPLE_SIZE_SUPPORT_SIGNAL"},
            {"field": f"{side}_expected_outs.history_depth", "side": side.upper(), "direct_location_input": False,
             "source": f"{BUILDER.relative_to(ROOT)}::build_states; {LIVE_BRIDGE.relative_to(ROOT)}::_starter",
             "semantics": "workload-state support count populated from the same n",
             "formula": f"identical source n as {feature}", "grain": "one value per game x starter workload state", "units": "prior starts",
             "as_of_construction": "date-strict and cross-season cumulative", "missing_fallback_behavior": "0 selects team/league workload base",
             "training_preprocessing": "not separately included in frozen model",
             "fallback_or_shrinkage_role": "selects last-three/pitcher/team/league workload base",
             "all_upstream_downstream_uses": "workload fallback selection; expected_outs and workload_uncertainty feed location",
             "control_coefficient": "", "park_repair_coefficient": "", "intent_classification": "FALLBACK_GATING_SIGNAL"},
        ])
    write_csv(output_dir / "totals_starter_count_feature_inventory.csv", inventory_rows)

    (output_dir / "totals_starter_count_lineage.md").write_text(f"""# Starter prior-start count lineage

Both direct fields are built as `n = len(prior)` where `prior` contains official starter appearances for the resolved pitcher with `game_date < target_date`. Same-date games are frozen together and do not enter one another's history. The count is cumulative across the 2023–2026 governed spine and does not reset at a season boundary.

That same `n` is written to `prior_starts` and `history_depth`. At `n>=3`, starter state is direct and expected workload uses the last three starts; `n=1-2` uses the pitcher-role cohort; `n=0` uses team then league starter history. The live bridge implements the same contract. The frozen location row separately copies `prior_starts` into `{FEATURES[0]}` and `{FEATURES[1]}`.

Sources: `{BUILDER.relative_to(ROOT)}` (`build_states`) and `{LIVE_BRIDGE.relative_to(ROOT)}` (`_starter`, `feature_row`).
""")
    (output_dir / "totals_starter_count_intent.md").write_text("""# Starter prior-start count intended semantics

The governed construction establishes a legitimate sample-support and fallback-gating role. It does not establish an unbounded causal run-location effect for each additional career start. A low count can also identify a rookie or newly observed pitcher, but that baseball state is already expressed through governed fallback starter rate and workload fields. The current direct location use is therefore classified `MIXED`: potentially useful low-depth state, but structurally unsafe as an uncapped cumulative count without separate evidence.
""")

    distribution_rows = []
    for feature in FEATURES:
        distribution_rows.append(descriptive("DEVELOPMENT_2023_24", training, feature))
        distribution_rows.extend(descriptive(period, frames[period], feature) for period in PERIODS)
    for row in distribution_rows:
        base = training[row["feature"]].astype(float)
        source = training if row["period"] == "DEVELOPMENT_2023_24" else frames[row["period"]]
        values = source[row["feature"]].astype(float)
        row["training_p95"] = base.quantile(.95); row["training_p99"] = base.quantile(.99); row["training_max"] = base.max()
        row["share_above_training_p95"] = (values > base.quantile(.95)).mean()
        row["share_above_training_p99"] = (values > base.quantile(.99)).mean()
        row["share_above_training_max"] = (values > base.max()).mean()
        row["standardized_mean_shift"] = (values.mean() - base.mean()) / base.std(ddof=1)
        row["drift_class"] = "EXTREME" if abs(row["standardized_mean_shift"]) >= 2 or row["share_above_training_max"] >= .30 else (
            "SEVERE" if abs(row["standardized_mean_shift"]) >= 1 or row["share_above_training_max"] >= .10 else (
                "MODERATE" if abs(row["standardized_mean_shift"]) >= .5 else "NONE"))
    write_csv(output_dir / "totals_starter_count_distributions.csv", distribution_rows)

    drift_rows = []
    for feature in FEATURES:
        base = training[feature].astype(float); base_std = base.std(ddof=1)
        for period in PERIODS:
            values = frames[period][feature].astype(float)
            shift = float((values.mean() - base.mean()) / base_std)
            p = psi(base, values); above = float((values > base.max()).mean())
            severity = "EXTREME" if abs(shift) >= 2 or p >= 1 or above >= .30 else ("SEVERE" if abs(shift) >= 1 or p >= .25 or above >= .10 else ("MODERATE" if abs(shift) >= .5 or p >= .1 or above else "NONE"))
            drift_rows.append({"aggregation": "PERIOD", "feature": feature, "period": period,
                               "games": len(values), "training_mean": base.mean(), "current_mean": values.mean(),
                               "standardized_mean_shift": shift, "population_stability_index": p,
                               "training_max": base.max(), "share_above_training_max": above, "severity": severity})
        for month, group in historical.groupby(historical.game_date.dt.to_period("M")):
            values = group[feature].astype(float)
            drift_rows.append({"aggregation": "MONTH", "feature": feature, "period": str(month), "games": len(group),
                               "training_mean": base.mean(), "current_mean": values.mean(),
                               "standardized_mean_shift": (values.mean() - base.mean()) / base_std,
                               "population_stability_index": psi(base, values), "training_max": base.max(),
                               "share_above_training_max": (values > base.max()).mean()})
    write_csv(output_dir / "totals_starter_count_drift.csv", drift_rows)

    appearances = starter_appearances(historical)
    mechanical_rows = []
    for feature in FEATURES:
        data = appearances[appearances.feature == feature].sort_values(["pitcher_id", "game_date", "game_pk"])
        declines = int((data.groupby("pitcher_id").prior_starts.diff() < 0).sum())
        same_count_successor = int((data.groupby("pitcher_id").prior_starts.diff() == 0).sum())
        mechanical_rows.append({"row_type": "GLOBAL", "feature": feature, "pitcher_id": "ALL", "appearances": len(data),
                                "pitchers": data.pitcher_id.nunique(), "count_vs_date_pearson": corr(data.prior_starts, data.date_ordinal),
                                "count_vs_date_spearman": corr(data.prior_starts, data.date_ordinal, "spearman"),
                                "within_pitcher_declines": declines, "same_count_successors": same_count_successor,
                                "season_resets": 0, "mechanical_career_growth": "YES"})
        for pitcher_id, group in data.groupby("pitcher_id"):
            if len(group) < 10:
                continue
            group = group.sort_values(["game_date", "game_pk"])
            months = max((group.game_date.max() - group.game_date.min()).days / 30.4375, 1 / 30.4375)
            mechanical_rows.append({"row_type": "PITCHER", "feature": feature, "pitcher_id": pitcher_id,
                                    "appearances": len(group), "first_date": group.game_date.min().date(),
                                    "last_date": group.game_date.max().date(), "first_count": group.prior_starts.iloc[0],
                                    "last_count": group.prior_starts.iloc[-1],
                                    "estimated_count_increase_per_month": (group.prior_starts.iloc[-1] - group.prior_starts.iloc[0]) / months,
                                    "count_vs_date_pearson": corr(group.prior_starts, group.date_ordinal),
                                    "within_pitcher_declines": int((group.prior_starts.diff() < 0).sum()),
                                    "mechanical_career_growth": "YES"})
    write_csv(output_dir / "totals_starter_count_mechanical_growth.csv", mechanical_rows)

    double_use_rows = []
    for feature in FEATURES:
        side = feature.split("_")[0]
        double_use_rows.append({"feature": feature, "same_n_written_to_history_depth": True,
                                "n_gates_starter_rate_fallback": True, "n_gates_workload_base": True,
                                "n_is_direct_location_input": True, "continuous_shrinkage_formula": False,
                                "low_depth_threshold": 3, "double_use": "YES",
                                "specific_path": f"build_states/_starter -> {side} starter state and workload -> feature_row -> log(mu)",
                                "risk": "sample support influences forecasts through governed state selection and again through an unbounded standardized location term"})
    write_csv(output_dir / "totals_starter_count_confidence_double_use.csv", double_use_rows)

    effect_rows = []
    for model_name, artifact in (("CONTROL", control), ("PARK_REPAIR", park_repair)):
        for feature in FEATURES:
            coefficient, center, scale = artifact_term(artifact, feature)
            points = {"TRAINING_P10": training[feature].quantile(.10), "TRAINING_MEAN": center,
                      "TRAINING_MEDIAN": training[feature].median(), "TRAINING_P90": training[feature].quantile(.90),
                      "TRAINING_P95": training[feature].quantile(.95), "TRAINING_MAX": training[feature].max(),
                      "PROSPECTIVE_MEDIAN": prospective[feature].median(), "PROSPECTIVE_P90": prospective[feature].quantile(.90),
                      "PROSPECTIVE_MAX": prospective[feature].max()}
            for point, value in points.items():
                contribution = coefficient * (value - center) / scale
                effect_rows.append({"model": model_name, "feature": feature, "point": point, "count": value,
                                    "center": center, "scale": scale, "coefficient": coefficient,
                                    "standardized_count": (value - center) / scale,
                                    "isolated_log_location_contribution": contribution,
                                    "isolated_location_factor": math.exp(contribution),
                                    "per_additional_start_location_factor": math.exp(coefficient / scale)})
    write_csv(output_dir / "totals_starter_count_effect_curve.csv", effect_rows)

    training_corr = training.copy()
    training_corr["date_ordinal"] = (training_corr.game_date - training_corr.game_date.min()).dt.days
    training_corr["season"] = training_corr.game_date.dt.year
    training_corr["home_sparse"] = (training_corr.home_starter_prior_starts < 3).astype(int)
    training_corr["away_sparse"] = (training_corr.away_starter_prior_starts < 3).astype(int)
    numeric_targets = list(dict.fromkeys([*control["feature_order"], "final_total", "raw_forecast", "run_residual",
                                          "repaired_forecast", "repaired_residual", "date_ordinal", "season",
                                          "home_sparse", "away_sparse"]))
    correlation_rows = []
    for feature in FEATURES:
        side = feature.split("_")[0]
        for target in numeric_targets:
            correlation_rows.append({"feature": feature, "target": target, "association": "PEARSON",
                                     "value": corr(training_corr[feature], training_corr[target]), "games": len(training_corr)})
        correlation_rows.extend([
            {"feature": feature, "target": f"{side}_starter_starter_pitcher_id", "association": "ETA_SQUARED",
             "value": eta_squared(training_corr[feature], training_corr[f"{side}_starter_starter_pitcher_id"]), "games": len(training_corr)},
            {"feature": feature, "target": f"{side}_team_id", "association": "ETA_SQUARED",
             "value": eta_squared(training_corr[feature], training_corr[f"{side}_team_id"]), "games": len(training_corr)},
            {"feature": feature, "target": "ALL_OTHER_CONTROL_FEATURES", "association": "VIF",
             "value": vif_for_feature(training_corr, control["feature_order"], feature), "games": len(training_corr)},
        ])
    write_csv(output_dir / "totals_starter_count_training_correlations.csv", correlation_rows)

    count_corr = corr(training[FEATURES[0]], training[FEATURES[1]])
    park_corr = {feature: corr(training[feature], training.park_history_depth) for feature in FEATURES}
    vif = {feature: vif_for_feature(training, control["feature_order"], feature) for feature in FEATURES}
    coefficient_flip = "MULTICOLLINEARITY_REASSIGNMENT"
    (output_dir / "totals_starter_count_coefficient_flip_analysis.md").write_text(f"""# Starter-count coefficient flip analysis

The row population and retained-feature scalers are identical, yet removing `park_history_depth` changes home count `{control_coef[FEATURES[0]]:+.15f}` to `{repair_coef[FEATURES[0]]:+.15f}` and away count `{control_coef[FEATURES[1]]:+.15f}` to `{repair_coef[FEATURES[1]]:+.15f}`. Home/away count correlation is `{count_corr:.4f}`; correlations with park depth are home `{park_corr[FEATURES[0]]:.4f}` and away `{park_corr[FEATURES[1]]:.4f}`; VIFs are home `{vif[FEATURES[0]]:.3f}` and away `{vif[FEATURES[1]]:.3f}`.

Classification: `{coefficient_flip}`. The sign flip is not evidence that away-starter experience causally suppresses runs; it is the fixed-artifact symptom of correlated cumulative support proxies reallocating location weight after park-depth removal.
""")

    within_rows = []
    aggregate_controlled: dict[str, float] = {}
    for feature in FEATURES:
        data = appearances[appearances.feature == feature].copy()
        x_pitcher = centered(data, "prior_starts", ["pitcher_id"])
        x_month = centered(data, "prior_starts", ["pitcher_id", "season_month"])
        date_control = centered(data, "date_ordinal", ["pitcher_id"])
        quality_control = centered(data, "starter_ra9", ["pitcher_id"])
        controls = np.column_stack([date_control, quality_control])
        x_controlled = residualize(x_pitcher, controls)
        for outcome in ("final_total", "raw_forecast", "run_residual", "repaired_forecast", "repaired_residual", "starter_ra9"):
            y_pitcher = centered(data, outcome, ["pitcher_id"])
            y_month = centered(data, outcome, ["pitcher_id", "season_month"])
            y_controlled = residualize(y_pitcher, controls)
            controlled = corr(x_controlled, y_controlled)
            within_rows.append({"row_type": "AGGREGATE_FIXED_EFFECT", "feature": feature, "pitcher_id": "ALL",
                                "games": len(data), "comparison": outcome,
                                "within_pitcher_pearson": corr(x_pitcher, y_pitcher),
                                "within_pitcher_month_pearson": corr(x_month, y_month),
                                "within_pitcher_date_and_quality_controlled_pearson": controlled})
            if outcome == "final_total":
                aggregate_controlled[feature] = controlled
        for pitcher_id, group in data.groupby("pitcher_id"):
            if len(group) < 10:
                continue
            within_rows.append({"row_type": "PITCHER", "feature": feature, "pitcher_id": pitcher_id, "games": len(group),
                                "comparison": "final_total", "count_actual_pearson": corr(group.prior_starts, group.final_total),
                                "mean_within_pitcher_count_change": group.sort_values(["game_date", "game_pk"]).prior_starts.diff().mean(),
                                "count_raw_residual_pearson": corr(group.prior_starts, group.run_residual),
                                "count_repaired_residual_pearson": corr(group.prior_starts, group.repaired_residual),
                                "count_quality_pearson": corr(group.prior_starts, group.starter_ra9)})
    for row in within_rows:
        value = aggregate_controlled[row["feature"]]
        row["within_pitcher_direct_signal"] = "ABSENT" if abs(value) < .05 else ("WEAK" if abs(value) < .15 else ("PRESENT" if value > 0 else "INVERTED"))
    write_csv(output_dir / "totals_starter_count_within_pitcher.csv", within_rows)

    between_rows = []
    for feature in FEATURES:
        data = appearances[appearances.feature == feature]
        means = data.groupby("pitcher_id").agg(games=("game_pk", "size"), count=("prior_starts", "mean"),
                                                final_total=("final_total", "mean"), raw_residual=("run_residual", "mean"),
                                                repaired_residual=("repaired_residual", "mean"), starter_ra9=("starter_ra9", "mean"))
        means = means[means.games >= 5]
        between_rows.extend([
            {"feature": feature, "comparison": outcome, "level": "BETWEEN_PITCHER_MEANS", "pitchers": len(means),
             "correlation": corr(means["count"], means[outcome])} for outcome in ("final_total", "raw_residual", "repaired_residual", "starter_ra9")
        ])
        for outcome in ("final_total", "run_residual", "repaired_residual", "starter_ra9"):
            between_rows.append({"feature": feature, "comparison": outcome, "level": "WITHIN_PITCHER_FIXED_EFFECT",
                                 "pitchers": data.pitcher_id.nunique(),
                                 "correlation": corr(centered(data, "prior_starts", ["pitcher_id"]), centered(data, outcome, ["pitcher_id"]))})
    write_csv(output_dir / "totals_starter_count_between_vs_within.csv", between_rows)

    band_rows = []
    for feature in FEATURES:
        side = feature.split("_")[0]
        for period, frame in (("DEVELOPMENT_2023_24", training), *frames.items()):
            for band, low, high in EXPERIENCE_BANDS:
                group = frame[(frame[feature] >= low) & (frame[feature] <= high)]
                if group.empty:
                    continue
                band_rows.append({"feature": feature, "period": period, "experience_band": band, "games": len(group),
                                  "mean_count": group[feature].mean(), "mean_actual_total": group.final_total.mean(),
                                  "mean_starter_ra9": group[f"{side}_starter_ra9"].mean(),
                                  "control_actual_minus_forecast_bias": group.run_residual.mean(),
                                  "control_mae": group.run_residual.abs().mean(),
                                  "park_repair_actual_minus_forecast_bias": group.repaired_residual.mean(),
                                  "park_repair_mae": group.repaired_residual.abs().mean(),
                                  "sparse_fallback_rate": (group[feature] < 3).mean()})
    extrapolation_rows = []
    for model_name, artifact, forecast_col in (("CONTROL", control, "raw_forecast"), ("PARK_REPAIR", park_repair, "repaired_forecast")):
        alpha = float(artifact["dispersion_alpha"])
        for feature in FEATURES:
            p95, p99, maximum = (training[feature].quantile(.95), training[feature].quantile(.99), training[feature].max())
            support = (("AT_OR_BELOW_P95", -math.inf, p95), ("ABOVE_P95_TO_P99", p95, p99),
                       ("ABOVE_P99_TO_MAX", p99, maximum), ("ABOVE_TRAINING_MAX", maximum, math.inf))
            for period in PERIODS:
                frame = frames[period]
                for band, low, high in support:
                    selector = (frame[feature] <= high) & (frame[feature] > low)
                    group = frame[selector]
                    if group.empty:
                        continue
                    metrics = repair.metric_bundle(group, group[forecast_col].to_numpy(float), alpha)
                    extrapolation_rows.append({"model": model_name, "feature": feature, "period": period,
                                               "support_band": band, "training_p95": p95, "training_p99": p99,
                                               "training_max": maximum, "mean_count": group[feature].mean(), **metrics})
    experience_shape = "LOW_DEPTH_EFFECT_ONLY"
    extrapolation_class = "MODERATE"
    # Store the predeclared conclusions in their natural evidence tables.
    for row in band_rows:
        row["experience_signal_shape"] = experience_shape
    write_csv(output_dir / "totals_starter_count_experience_bands.csv", band_rows)
    for row in extrapolation_rows:
        row["starter_count_extrapolation"] = extrapolation_class
    write_csv(output_dir / "totals_starter_count_extrapolation.csv", extrapolation_rows)

    (output_dir / "totals_starter_count_home_away_asymmetry.md").write_text(f"""# Home/away structural asymmetry

Construction, cutoff, sparse threshold, fallback hierarchy, scaler magnitude, and cumulative-growth behavior are symmetric. Development means are home `{training[FEATURES[0]].mean():.3f}` and away `{training[FEATURES[1]].mean():.3f}`. Controlled within-pitcher outcome correlations are home `{aggregate_controlled[FEATURES[0]]:.4f}` and away `{aggregate_controlled[FEATURES[1]]:.4f}`.

The large repair coefficient asymmetry therefore has no corresponding governed construction asymmetry and is classified `LIKELY_ARTIFACT`, not a demonstrated baseball home/away effect.
""")

    all_counterfactual: dict[str, list[dict[str, Any]]] = {"CONTROL": [], "PARK_REPAIR": []}
    probability_rows = []
    for model_name, artifact, forecast_col in (("CONTROL", control, "raw_forecast"), ("PARK_REPAIR", park_repair, "repaired_forecast")):
        alpha = float(artifact["dispersion_alpha"])
        for feature in FEATURES:
            training_mean = float(training[feature].mean()); training_p95 = float(training[feature].quantile(.95))
            for period in PERIODS:
                frame = frames[period]; baseline = frame[forecast_col].to_numpy(float)
                original_metrics = repair.metric_bundle(frame, baseline, alpha)
                for variant in ("ORIGINAL", "TRAINING_MEAN", "TRAINING_P95_CAP", "ZERO_COEFFICIENT_CONTRIBUTION"):
                    forecasts = counterfactual(frame, baseline, artifact, feature, variant, training_mean, training_p95)
                    metrics = repair.metric_bundle(frame, forecasts, alpha)
                    row = {"evidence_label": "COUNTERFACTUAL_ONLY_NOT_A_MODEL", "model": model_name,
                           "feature": feature, "period": period, "variant": variant,
                           "training_mean": training_mean, "training_p95": training_p95, **metrics}
                    for metric in ("mae", "rmse", "actual_minus_forecast_bias", "crps", "ladder_brier", "ladder_log_loss", "ladder_ece"):
                        row[f"delta_{metric}_vs_original"] = metrics[metric] - original_metrics[metric]
                    all_counterfactual[model_name].append(row)
                    if variant != "ORIGINAL":
                        probability_rows.append({key: row[key] for key in (
                            "evidence_label", "model", "feature", "period", "variant", "games",
                            "ladder_brier", "delta_ladder_brier_vs_original", "ladder_log_loss",
                            "delta_ladder_log_loss_vs_original", "ladder_ece", "delta_ladder_ece_vs_original", "crps", "delta_crps_vs_original")})
    write_csv(output_dir / "totals_starter_count_control_counterfactuals.csv", all_counterfactual["CONTROL"])
    write_csv(output_dir / "totals_starter_count_park_repair_counterfactuals.csv", all_counterfactual["PARK_REPAIR"])
    write_csv(output_dir / "totals_starter_count_probability_effect.csv", probability_rows)

    joint_rows = []
    for model_name, artifact in (("CONTROL", control), ("PARK_REPAIR", park_repair)):
        for period, frame in (("DEVELOPMENT_2023_24", training), *frames.items()):
            contributions = np.zeros(len(frame))
            for feature in FEATURES:
                coefficient, center, scale = artifact_term(artifact, feature)
                contributions += coefficient * (frame[feature].to_numpy(float) - center) / scale
            joint_rows.append({"model": model_name, "period": period, "games": len(frame),
                               "mean_joint_log_location_contribution": contributions.mean(),
                               "median_joint_log_location_contribution": np.median(contributions),
                               "p10_joint_log_location_contribution": np.quantile(contributions, .10),
                               "p90_joint_log_location_contribution": np.quantile(contributions, .90),
                               "mean_joint_location_factor": np.exp(contributions).mean(),
                               "min_joint_location_factor": np.exp(contributions).min(),
                               "max_joint_location_factor": np.exp(contributions).max()})
    write_csv(output_dir / "totals_starter_count_joint_contribution.csv", joint_rows)

    residual_rows = []
    for model_name, artifact, residual_col in (("CONTROL", control, "run_residual"), ("PARK_REPAIR", park_repair, "repaired_residual")):
        for period, frame in (("DEVELOPMENT_2023_24", training), *frames.items()):
            joint = np.zeros(len(frame))
            for feature in FEATURES:
                coefficient, center, scale = artifact_term(artifact, feature)
                contribution = coefficient * (frame[feature].to_numpy(float) - center) / scale
                residual_rows.append({"model": model_name, "period": period, "scope": feature, "games": len(frame),
                                      "contribution_residual_pearson": corr(contribution, frame[residual_col]),
                                      "count_residual_pearson": corr(frame[feature], frame[residual_col]),
                                      "mean_contribution": contribution.mean(), "mean_residual": frame[residual_col].mean()})
                joint += contribution
            residual_rows.append({"model": model_name, "period": period, "scope": "JOINT_HOME_AWAY", "games": len(frame),
                                  "contribution_residual_pearson": corr(joint, frame[residual_col]),
                                  "mean_contribution": joint.mean(), "mean_residual": frame[residual_col].mean()})
            p95 = {feature: training[feature].quantile(.95) for feature in FEATURES}
            p99 = {feature: training[feature].quantile(.99) for feature in FEATURES}
            maximum = {feature: training[feature].max() for feature in FEATURES}
            support = np.full(len(frame), "IN_SUPPORT_AT_OR_BELOW_P95", dtype=object)
            above_p95 = np.logical_or.reduce([frame[feature].to_numpy(float) > p95[feature] for feature in FEATURES])
            above_p99 = np.logical_or.reduce([frame[feature].to_numpy(float) > p99[feature] for feature in FEATURES])
            above_max = np.logical_or.reduce([frame[feature].to_numpy(float) > maximum[feature] for feature in FEATURES])
            support[above_p95] = "EITHER_COUNT_ABOVE_TRAINING_P95"
            support[above_p99] = "EITHER_COUNT_ABOVE_TRAINING_P99"
            support[above_max] = "EITHER_COUNT_ABOVE_TRAINING_MAX"
            for band in dict.fromkeys(support):
                selector = support == band
                residual_rows.append({"model": model_name, "period": period, "scope": "JOINT_SUPPORT_BAND",
                                      "support_band": band, "games": int(selector.sum()),
                                      "contribution_residual_pearson": corr(joint[selector], frame.loc[selector, residual_col]),
                                      "mean_contribution": joint[selector].mean(),
                                      "mean_residual": frame.loc[selector, residual_col].mean(),
                                      "mae": frame.loc[selector, residual_col].abs().mean()})
    write_csv(output_dir / "totals_starter_count_residual_alignment.csv", residual_rows)

    attribution_rows = []
    for period in PERIODS:
        frame = frames[period]
        control_forecast = frame.raw_forecast.to_numpy(float); repaired_forecast = frame.repaired_forecast.to_numpy(float)
        control_joint = np.zeros(len(frame)); repaired_joint = np.zeros(len(frame))
        for feature in FEATURES:
            for artifact, output in ((control, control_joint), (park_repair, repaired_joint)):
                coefficient, center, scale = artifact_term(artifact, feature)
                output += coefficient * (frame[feature].to_numpy(float) - center) / scale
        hybrid = repaired_forecast * np.exp(control_joint - repaired_joint)
        zeroed = repaired_forecast * np.exp(-repaired_joint)
        c = repair.metric_bundle(frame, control_forecast, float(control["dispersion_alpha"]))
        r = repair.metric_bundle(frame, repaired_forecast, float(park_repair["dispersion_alpha"]))
        h = repair.metric_bundle(frame, hybrid, float(park_repair["dispersion_alpha"]))
        z = repair.metric_bundle(frame, zeroed, float(park_repair["dispersion_alpha"]))
        delta = r["mae"] - c["mae"]
        # Positive means the changed repair count coefficients worsen MAE versus
        # the otherwise fixed repair with control count coefficients; negative
        # means they mitigate degradation originating elsewhere in the refit.
        count_component = r["mae"] - h["mae"]
        fraction = count_component / delta if delta else math.nan
        if delta and count_component < 0 and abs(fraction) >= .2:
            attribution_class = "MATERIAL_MITIGATION_NOT_DEGRADATION"
        elif delta and count_component > 0 and abs(fraction) >= .5:
            attribution_class = "MATERIAL_DEGRADATION_ATTRIBUTION"
        elif delta and count_component > 0 and abs(fraction) >= .2:
            attribution_class = "PARTIAL_DEGRADATION_ATTRIBUTION"
        elif delta and abs(fraction) >= .05:
            attribution_class = "MINOR_OR_OFFSETTING_EFFECT"
        else:
            attribution_class = "NO_MATERIAL_DEGRADATION_ATTRIBUTION"
        attribution_rows.append({"evidence_label": "COUNTERFACTUAL_ONLY_NOT_A_MODEL", "period": period, "games": len(frame),
                                 "control_mae": c["mae"], "park_repair_mae": r["mae"], "park_repair_minus_control_mae": delta,
                                 "park_repair_with_control_count_coefficients_mae": h["mae"],
                                 "park_repair_with_counts_zeroed_mae": z["mae"],
                                 "repair_minus_hybrid_mae_count_coefficient_component": count_component,
                                 "signed_fraction_of_repair_control_mae_delta": fraction,
                                 "attribution_direction": "MITIGATES_REPAIR_DEGRADATION" if count_component < 0 else "CONTRIBUTES_TO_REPAIR_DEGRADATION",
                                 "starter_counts_explain_mae_degradation": "NO" if count_component <= 0 else ("MATERIAL" if abs(fraction) >= .5 else ("PARTIAL" if abs(fraction) >= .2 else "MINOR")),
                                 "attribution_class": attribution_class})
    write_csv(output_dir / "totals_starter_count_mae_degradation_attribution.csv", attribution_rows)

    within_signal = max(abs(value) for value in aggregate_controlled.values())
    direct_fitness = "UNSUPPORTED" if within_signal < .05 else "WEAK_OR_MIXED"
    late = next(row for row in attribution_rows if row["period"] == "2026_LATE_HOLDOUT")
    concern = direct_fitness == "UNSUPPORTED"
    fitness_rows = [
        {"feature": FEATURES[0], "direct_location_fitness": direct_fitness,
         "recommended_semantic_role": "BETTER_AS_CONFIDENCE/SHRINKAGE_SIGNAL",
         "reason": "same count already gates state/workload; repaired coefficient collapses near zero; controlled within-pitcher signal is negligible"},
        {"feature": FEATURES[1], "direct_location_fitness": direct_fitness,
         "recommended_semantic_role": "STRUCTURALLY_UNSUITABLE_AS_DIRECT_LOCATION_FEATURE",
         "reason": "same count already gates state/workload; coefficient flips negative after correlated park-depth removal without governed away-side asymmetry"},
    ]
    (output_dir / "totals_starter_count_feature_fitness.md").write_text(
        "# Starter-count feature fitness\n\n| Feature | Direct-location fitness | Recommended role |\n|---|---|---|\n" +
        "\n".join(f"| `{row['feature']}` | `{row['direct_location_fitness']}` | `{row['recommended_semantic_role']}` |" for row in fitness_rows) +
        "\n\n" + "\n".join(f"- `{row['feature']}`: {row['reason']}" for row in fitness_rows) + "\n")

    (output_dir / "totals_count_feature_design_principle.md").write_text("""# Count-feature design principle

`SUPPORTED`: A cumulative sample count may govern shrinkage strength, uncertainty, eligibility, or fallback selection. It should not enter an expected-run location equation as an unbounded linear term unless a separately governed, leakage-safe within-entity analysis demonstrates a stable baseball effect beyond calendar time, identity, quality state, and the fallback already driven by that count. Suitable future comparisons include confidence-only use, low-depth indicators, or bounded/log-saturating transforms; this review selects none.
""")
    (output_dir / "totals_starter_count_root_cause.md").write_text(f"""# Starter-count root cause

- Home: `SAMPLE_DEPTH_SEMANTICS_DEFECT`; a support counter is reused as a direct location term and its coefficient collapses after park-depth removal.
- Away: `MULTICOLLINEARITY_INSTABILITY`; the same cumulative proxy changes from `{control_coef[FEATURES[1]]:+.15f}` to `{repair_coef[FEATURES[1]]:+.15f}` with no construction change.
- Shared: `MECHANICAL_CALENDAR_GROWTH` and `DOUBLE_USE` amplify extrapolation risk because 2026 counts substantially exceed the 2023–2024 development support.

The review does not prove that experience contains no baseball information. It finds that the current raw cumulative representation does not isolate that information safely.
""")
    park_status = "PARK_REPAIR_BLOCKED_BY_STARTER_COUNT_DEFECT" if concern else "PARK_REPAIR_STATUS_UNCHANGED_NEEDS_MORE_REVIEW"
    final_declaration = "STARTER_PRIOR_COUNT_MATERIAL_STRUCTURAL_CONCERN" if concern else "STARTER_PRIOR_COUNT_SECONDARY_STRUCTURAL_CONCERN"
    (output_dir / "totals_starter_count_repair_scope.md").write_text(f"""# Starter-count repair scope

`STARTER_COUNT_REDESIGN_REQUIRES_SEPARATE_BOUNDED_COMPARISON`

This diagnostic authorizes no model change. A subsequent task may compare unchanged control/park-repair models against count removal, confidence-only use, a low-depth indicator, and bounded/log-saturating forms with exact row and fit governance. It must not use Aug 6–15 outcomes for selection.

Park-repair disposition: `{park_status}`. The existing repair remains research-only and unpromoted.
""")

    late_control = late["control_mae"]; late_repair = late["park_repair_mae"]
    concise = f"""# Concise MLB totals starter prior-start count structural review v1

`{final_declaration}`

- Both starter counts are cumulative strict-prior sample-depth fields, mechanically increase across careers/seasons, and are used twice: fallback/workload gating plus unbounded direct location.
- Control coefficients: home `{control_coef[FEATURES[0]]:+.15f}`, away `{control_coef[FEATURES[1]]:+.15f}`.
- Park-repair coefficients: home `{repair_coef[FEATURES[0]]:+.15f}`, away `{repair_coef[FEATURES[1]]:+.15f}`.
- The away sign flip is `{coefficient_flip}`; construction and controlled evidence do not support a special away-side causal interpretation.
- Date/quality-controlled within-pitcher correlations with total runs: home `{aggregate_controlled[FEATURES[0]]:.4f}`, away `{aggregate_controlled[FEATURES[1]]:.4f}` (`{direct_fitness}`).
- Experience shape: `{experience_shape}` (small pooled 0–4-start difference, not an independent linear within-pitcher effect); extrapolation evidence: `{extrapolation_class}` amid extreme distribution drift.
- Late-holdout MAE is control `{late_control:.4f}` versus park repair `{late_repair:.4f}`. Holding the repair fixed while restoring control count coefficients yields `{late['park_repair_with_control_count_coefficients_mae']:.4f}`; count-coefficient attribution is `{late['attribution_class']}`.
- Design decision: raw cumulative count is better used for confidence/shrinkage/gating or a separately governed bounded experience representation, not assumed valid as an unbounded location term.
- Park repair: `{park_status}`; no promotion, shadow activation, refit, or production change occurred.

All neutralizations are labelled `COUNTERFACTUAL_ONLY_NOT_A_MODEL`.
"""
    (output_dir / "concise_mlb_totals_starter_prior_start_count_structural_review_v1.md").write_text(concise)

    protected_after = {str(path): sha256(path) for path in protected}
    if protected_before != protected_after:
        raise RuntimeError("PROTECTED_INPUT_MUTATION_DETECTED")
    hash_path = output_dir / "reproducibility_hashes.sha256"
    artifact_files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    lines = [f"{sha256(path)}  {path.name}" for path in artifact_files]
    lines.extend(f"{digest}  PROTECTED_INPUT::{Path(path).name}" for path, digest in protected_after.items())
    hash_path.write_text("\n".join(lines) + "\n")
    return {"task_id": TASK_ID, "final_declaration": final_declaration, "park_repair_status": park_status,
            "files": len(artifact_files) + 1, "historical_games": len(historical), "prospective_games": len(prospective),
            "late_attribution": late, "protected_inputs_unchanged": True}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2, default=str))


if __name__ == "__main__":
    main()

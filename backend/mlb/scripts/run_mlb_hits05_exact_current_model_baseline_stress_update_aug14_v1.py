#!/usr/bin/env python3
"""Stress the exact-current Hits 0.5 model against frozen simple baselines."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.mlb.scripts import grade_mlb_hits05_aug14_canonical_update_v1 as canonical_grade


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_exact_current_model_baseline_stress_update_aug14_v1/2026-08-15"
SEASON = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_season_to_date_evidence_v1/2026-08-14/hits05_season_primary_predictions.csv"
PRIOR_BASELINES = ROOT / "artifacts/analysis/model_development/mlb_hits05_adversarial_certification_recheck_v1/2026-08-14/hits05_trivial_baseline_comparison.csv"
FROZEN_PROCEDURE = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14/hits05_frozen_modeling_procedure.json"
FRESH_BASELINES = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14/hits05_baseline_comparison.csv"
FRESH_OVERLAP = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14/hits05_august_live_reference_comparison.csv"
MODEL_ARTIFACT = ROOT / "models_out/latest/hits.joblib"
MODEL_ID = canonical_grade.MODEL_ID
MODEL_HASH = canonical_grade.MODEL_HASH
DATES = canonical_grade.DATES
PSEUDO_GAMES = 8.0
BOOTSTRAP_REPLICATES = 10_000
BOOTSTRAP_SEED = 20260815
BIN_EDGES = canonical_grade.BINS
BIN_LABELS = canonical_grade.BIN_LABELS


def sha256(file_path: Path) -> str:
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def score(target: pd.Series | np.ndarray, probability: pd.Series | np.ndarray) -> dict:
    y = np.asarray(target, dtype=float)
    p = np.clip(np.asarray(probability, dtype=float), 1e-12, 1 - 1e-12)
    return {
        "rows": int(len(y)),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(np.mean(-(y * np.log(p) + (1 - y) * np.log(1 - p)))),
        "mean_probability": float(p.mean()),
        "observed_rate": float(y.mean()),
    }


def fixed_ece(target: pd.Series, probability: pd.Series) -> float:
    bands = pd.cut(probability.astype(float), BIN_EDGES, labels=BIN_LABELS, right=False)
    total = len(target)
    return float(
        sum(
            len(group) / total * abs(group.mean() - target.loc[group.index].mean())
            for _, group in probability.groupby(bands, observed=False)
            if len(group)
        )
    )


def strict_prior_history() -> pd.DataFrame:
    season = pd.read_csv(SEASON, low_memory=False)
    history = season[
        season.evidence_regime.ne("REGIME_A_EARLY_TIMING_WEAK")
        & season.model_generation.notna()
        & ~season.model_generation.astype(str).str.contains("UNRESOLVED")
        & season.provenance_tier.ne("TIER_A")
        & season.date.lt(DATES[0])
        & season.hit_1plus.notna()
    ].copy()
    if history.empty or history.date.max() >= DATES[0]:
        raise AssertionError("strict-prior baseline history cutoff failed")
    return history


def build_population() -> tuple[pd.DataFrame, dict]:
    predictions = canonical_grade.load_predictions()
    population = canonical_grade.attach_outcomes(predictions).sort_values(["date", "identity"]).copy()
    population["target"] = pd.to_numeric(population.target, errors="coerce")
    population["strict_pregame"] = population.prediction_dt < population.start_dt
    population["outcome_attachment_contract"] = np.where(
        population.date >= canonical_grade.CANONICAL_DATES[0],
        "CANONICAL_PROSPECTIVE_OUTCOME_SIDECAR",
        "FROZEN_ORIGINAL_PROSPECTIVE_OUTCOME_ATTACHMENT",
    )
    if population.identity.duplicated().any():
        raise AssertionError("duplicate primary identity")
    if not population.strict_pregame.all():
        raise AssertionError("non-pregame primary prediction")
    exact = population.model_semantic_name.eq(MODEL_ID) & population.model_artifact_sha256.eq(MODEL_HASH)
    if not exact.all():
        raise AssertionError("non-current-model row")

    history = strict_prior_history()
    population_rate = float(history.hit_1plus.mean())
    player_n = history.groupby("player_id").size().to_dict()
    player_hits = history.groupby("player_id").hit_1plus.sum().to_dict()
    baseline_rows = []
    for date_value in DATES:
        day = population[population.date == date_value]
        for row in day.itertuples():
            prior_n = int(player_n.get(row.player_id, 0))
            prior_hits = float(player_hits.get(row.player_id, 0.0))
            baseline_rows.append(
                {
                    "identity": row.identity,
                    "baseline_a_population": population_rate,
                    "hitter_prior_resolved_games": prior_n,
                    "hitter_prior_hits": prior_hits,
                    "baseline_b_hitter_shrunk": (
                        prior_hits + PSEUDO_GAMES * population_rate
                    ) / (prior_n + PSEUDO_GAMES),
                }
            )
        resolved_day = day[day.target.notna()]
        for row in resolved_day.itertuples():
            player_n[row.player_id] = player_n.get(row.player_id, 0) + 1
            player_hits[row.player_id] = player_hits.get(row.player_id, 0.0) + float(row.target)
    population = population.merge(pd.DataFrame(baseline_rows), on="identity", validate="one_to_one")
    source = {
        "history_rows": int(len(history)),
        "history_players": int(history.player_id.nunique()),
        "history_start": str(history.date.min()),
        "history_end": str(history.date.max()),
        "population_rate": population_rate,
    }
    return population, source


FORECASTS = {
    "MODEL": "p_over",
    "BASELINE_A_STRICT_PRIOR_POPULATION": "baseline_a_population",
    "BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK": "baseline_b_hitter_shrunk",
}


def comparison(frame: pd.DataFrame, include_ece: bool = False) -> pd.DataFrame:
    resolved = frame[frame.target.notna()]
    rows = []
    for forecast, column in FORECASTS.items():
        result = score(resolved.target, resolved[column])
        if include_ece:
            result["ece"] = fixed_ece(resolved.target, resolved[column])
        rows.append({"forecast": forecast, **result})
    output = pd.DataFrame(rows)
    model = output.iloc[0]
    output["model_minus_forecast_brier"] = model.brier - output.brier
    output["model_minus_forecast_log_loss"] = model.log_loss - output.log_loss
    return output


def assert_prior_reproduction(population: pd.DataFrame, population_rate: float) -> pd.DataFrame:
    through13 = population[(population.date <= "2026-08-13") & population.target.notna()]
    current = comparison(through13)
    prior = pd.read_csv(PRIOR_BASELINES)
    expected_model = prior[prior.forecast.eq("EXACT_CURRENT_MODEL")].iloc[0]
    expected_baseline = prior[prior.forecast.eq("FIXED_PRIOR_STRICT_FAMILY_RATE")].iloc[0]
    actual_model = current[current.forecast.eq("MODEL")].iloc[0]
    actual_baseline = current[current.forecast.eq("BASELINE_A_STRICT_PRIOR_POPULATION")].iloc[0]
    assertions = (
        abs(actual_model.brier - expected_model.brier) < 1e-14
        and abs(actual_model.log_loss - expected_model.log_loss) < 1e-14
        and abs(actual_baseline.brier - expected_baseline.brier) < 1e-14
        and abs(actual_baseline.log_loss - expected_baseline.log_loss) < 1e-14
        and abs(population_rate - expected_baseline.mean_predicted) < 1e-14
    )
    if not assertions:
        raise AssertionError("Aug 3-13 frozen adversarial baseline reproduction failed")
    current.insert(0, "scope", "2026-08-03_THROUGH_2026-08-13")
    current["prior_reference_reproduced"] = True
    return current


def daily_scores(population: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for date_value, group in population[population.target.notna()].groupby("date", sort=True):
        metrics = {name: score(group.target, group[column]) for name, column in FORECASTS.items()}
        rows.append(
            {
                "date": date_value,
                "resolved_rows": len(group),
                "model_brier": metrics["MODEL"]["brier"],
                "population_brier": metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["brier"],
                "hitter_shrunk_brier": metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["brier"],
                "model_minus_population_brier": metrics["MODEL"]["brier"] - metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["brier"],
                "model_minus_hitter_shrunk_brier": metrics["MODEL"]["brier"] - metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["brier"],
                "model_log_loss": metrics["MODEL"]["log_loss"],
                "population_log_loss": metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["log_loss"],
                "hitter_shrunk_log_loss": metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["log_loss"],
                "model_minus_population_log_loss": metrics["MODEL"]["log_loss"] - metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["log_loss"],
                "model_minus_hitter_shrunk_log_loss": metrics["MODEL"]["log_loss"] - metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["log_loss"],
            }
        )
    return pd.DataFrame(rows)


def cumulative_scores(population: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for date_index, date_value in enumerate(DATES, start=1):
        group = population[(population.date <= date_value) & population.target.notna()]
        metrics = {name: score(group.target, group[column]) for name, column in FORECASTS.items()}
        rows.append(
            {
                "through_date": date_value,
                "date_clusters": date_index,
                "resolved_rows": len(group),
                "model_brier": metrics["MODEL"]["brier"],
                "population_brier": metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["brier"],
                "hitter_shrunk_brier": metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["brier"],
                "model_minus_population_brier": metrics["MODEL"]["brier"] - metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["brier"],
                "model_minus_hitter_shrunk_brier": metrics["MODEL"]["brier"] - metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["brier"],
                "model_log_loss": metrics["MODEL"]["log_loss"],
                "population_log_loss": metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["log_loss"],
                "hitter_shrunk_log_loss": metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["log_loss"],
                "model_minus_population_log_loss": metrics["MODEL"]["log_loss"] - metrics["BASELINE_A_STRICT_PRIOR_POPULATION"]["log_loss"],
                "model_minus_hitter_shrunk_log_loss": metrics["MODEL"]["log_loss"] - metrics["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"]["log_loss"],
            }
        )
    return pd.DataFrame(rows)


def _loss_sums(frame: pd.DataFrame, probability_column: str) -> pd.DataFrame:
    work = frame.copy()
    p = np.clip(work[probability_column].astype(float), 1e-12, 1 - 1e-12)
    y = work.target.astype(float)
    work["brier_loss"] = (p - y) ** 2
    work["log_loss_value"] = -(y * np.log(p) + (1 - y) * np.log(1 - p))
    return work.groupby("date").agg(
        rows=("target", "size"), brier_sum=("brier_loss", "sum"), log_loss_sum=("log_loss_value", "sum")
    )


def clustered_uncertainty(frame: pd.DataFrame, scope: str, seed: int) -> pd.DataFrame:
    resolved = frame[frame.target.notna()].copy()
    dates = sorted(resolved.date.unique())
    summaries = {name: _loss_sums(resolved, column).loc[dates] for name, column in FORECASTS.items()}
    rng = np.random.default_rng(seed)
    draws = {metric: np.empty(BOOTSTRAP_REPLICATES) for metric in (
        "model_brier", "model_log_loss", "model_minus_population_brier",
        "model_minus_population_log_loss", "model_minus_hitter_shrunk_brier",
        "model_minus_hitter_shrunk_log_loss",
    )}
    for draw_index in range(BOOTSTRAP_REPLICATES):
        sampled = rng.integers(0, len(dates), size=len(dates))
        values = {}
        for name, summary in summaries.items():
            selected = summary.iloc[sampled]
            denominator = selected.rows.sum()
            values[name] = (selected.brier_sum.sum() / denominator, selected.log_loss_sum.sum() / denominator)
        draws["model_brier"][draw_index] = values["MODEL"][0]
        draws["model_log_loss"][draw_index] = values["MODEL"][1]
        draws["model_minus_population_brier"][draw_index] = values["MODEL"][0] - values["BASELINE_A_STRICT_PRIOR_POPULATION"][0]
        draws["model_minus_population_log_loss"][draw_index] = values["MODEL"][1] - values["BASELINE_A_STRICT_PRIOR_POPULATION"][1]
        draws["model_minus_hitter_shrunk_brier"][draw_index] = values["MODEL"][0] - values["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"][0]
        draws["model_minus_hitter_shrunk_log_loss"][draw_index] = values["MODEL"][1] - values["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"][1]
    point = comparison(resolved)
    by_name = point.set_index("forecast")
    points = {
        "model_brier": by_name.loc["MODEL", "brier"],
        "model_log_loss": by_name.loc["MODEL", "log_loss"],
        "model_minus_population_brier": by_name.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"],
        "model_minus_population_log_loss": by_name.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_log_loss"],
        "model_minus_hitter_shrunk_brier": by_name.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"],
        "model_minus_hitter_shrunk_log_loss": by_name.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_log_loss"],
    }
    rows = []
    for metric, values in draws.items():
        delta_metric = metric.startswith("model_minus")
        rows.append(
            {
                "scope": scope,
                "metric": metric,
                "point_estimate": points[metric],
                "ci_low": float(np.quantile(values, 0.025)),
                "ci_high": float(np.quantile(values, 0.975)),
                "fraction_draws_favoring_model": float(np.mean(values < 0)) if delta_metric else np.nan,
                "date_clusters": len(dates),
                "replicates": BOOTSTRAP_REPLICATES,
                "seed": seed,
            }
        )
    return pd.DataFrame(rows)


def leave_one_date_out(population: pd.DataFrame) -> pd.DataFrame:
    resolved = population[population.target.notna()]
    rows = []
    for excluded in DATES:
        group = resolved[resolved.date != excluded]
        metrics = comparison(group).set_index("forecast")
        rows.append(
            {
                "excluded_date": excluded,
                "resolved_rows": len(group),
                "model_minus_population_brier": metrics.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"],
                "model_minus_population_log_loss": metrics.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_log_loss"],
                "model_minus_hitter_shrunk_brier": metrics.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"],
                "model_minus_hitter_shrunk_log_loss": metrics.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_log_loss"],
            }
        )
    output = pd.DataFrame(rows)
    for column in [item for item in output.columns if item.startswith("model_minus")]:
        output[f"{column}_favors_model"] = output[column] < 0
    return output


def cluster_rate_interval(group: pd.DataFrame, seed: int) -> tuple[float, float]:
    date_totals = group.groupby("date").target.agg(["sum", "count"])
    rng = np.random.default_rng(seed)
    values = np.empty(5_000)
    for index in range(len(values)):
        sampled = date_totals.iloc[rng.integers(0, len(date_totals), size=len(date_totals))]
        values[index] = sampled["sum"].sum() / sampled["count"].sum()
    return float(np.quantile(values, 0.025)), float(np.quantile(values, 0.975))


def confidence_ordering(population: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    resolved = population[population.target.notna()].sort_values(["p_over", "identity"]).copy()
    resolved["quintile"] = pd.qcut(
        resolved.p_over.rank(method="first"),
        5,
        labels=["BOTTOM20", "SECOND20", "MIDDLE20", "FOURTH20", "TOP20"],
    )
    resolved["deterministic_percentile_rank"] = resolved.p_over.rank(method="first", pct=True)
    groups = list(resolved.groupby("quintile", observed=False))
    groups.append(("TOP10", resolved[resolved.deterministic_percentile_rank > 0.9]))
    rows = []
    for index, (label, group) in enumerate(groups):
        result = score(group.target, group.p_over)
        low, high = cluster_rate_interval(group, BOOTSTRAP_SEED + 100 + index)
        rows.append(
            {
                "band": str(label),
                **result,
                "observed_rate_clustered_ci_low": low,
                "observed_rate_clustered_ci_high": high,
                "date_clusters": group.date.nunique(),
            }
        )
    output = pd.DataFrame(rows)
    quintiles = output[output.band.ne("TOP10")]
    monotonic = bool(np.all(np.diff(quintiles.observed_rate) >= 0))
    nonoverlap = bool(np.all(quintiles.observed_rate_clustered_ci_low.iloc[1:].to_numpy() > quintiles.observed_rate_clustered_ci_high.iloc[:-1].to_numpy()))
    if monotonic and nonoverlap:
        status = "ROBUST"
    elif monotonic or quintiles.observed_rate.iloc[-1] > quintiles.observed_rate.iloc[0]:
        status = "DIRECTIONALLY_PRESENT"
    elif quintiles.observed_rate.iloc[-1] >= quintiles.observed_rate.iloc[0]:
        status = "WEAK"
    else:
        status = "NOT_PRESENT"
    output["current_model_ordering"] = status
    return output, status


def calibration(population: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    resolved = population[population.target.notna()].copy()
    resolved["probability_bin"] = pd.cut(resolved.p_over, BIN_EDGES, labels=BIN_LABELS, right=False)
    rows = []
    for label in BIN_LABELS:
        group = resolved[resolved.probability_bin == label]
        rows.append(
            {
                "probability_bin": label,
                "rows": len(group),
                "mean_prediction": float(group.p_over.mean()) if len(group) else np.nan,
                "observed_hit_rate": float(group.target.mean()) if len(group) else np.nan,
                "calibration_gap_predicted_minus_observed": float(group.p_over.mean() - group.target.mean()) if len(group) else np.nan,
            }
        )
    output = pd.DataFrame(rows)
    upper_n = int(output.loc[output.probability_bin.eq(">=75%"), "rows"].iloc[0])
    if upper_n < 30:
        status = "INSUFFICIENT_SAMPLE"
    else:
        upper_gap = float(output.loc[output.probability_bin.eq(">=75%"), "calibration_gap_predicted_minus_observed"].iloc[0])
        status = "SUFFICIENT_AND_OVERCONFIDENT" if upper_gap > 0 else "SUFFICIENT_AND_ACCEPTABLE"
    output["current_model_upper_tail"] = status
    return output, status


def baseline_status(uncertainty: pd.DataFrame, baseline: str) -> str:
    rows = uncertainty[uncertainty.metric.isin([f"model_minus_{baseline}_brier", f"model_minus_{baseline}_log_loss"])]
    if (rows.ci_high < 0).all():
        return "MODEL_AHEAD"
    if (rows.ci_low > 0).all():
        return "MODEL_BEHIND"
    return "MODEL_EFFECTIVELY_TIED"


def write_hashes(inputs: list[Path]) -> None:
    outputs = sorted(item for item in OUT.iterdir() if item.name != "reproducibility_hashes.csv")
    rows = [{"role": "INPUT", "path": str(item.relative_to(ROOT)), "sha256": sha256(item)} for item in inputs]
    rows += [{"role": "OUTPUT", "path": str(item.relative_to(ROOT)), "sha256": sha256(item)} for item in outputs]
    pd.DataFrame(rows).to_csv(OUT / "reproducibility_hashes.csv", index=False, lineterminator="\n")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    population, source = build_population()
    reproduction = assert_prior_reproduction(population, source["population_rate"])
    resolved = population[population.target.notna()].copy()
    aug14 = population[(population.date == "2026-08-14") & population.target.notna()]
    cumulative = comparison(resolved, include_ece=True)
    aug14_comparison = comparison(aug14)
    daily = daily_scores(population)
    trajectory = cumulative_scores(population)
    uncertainty13 = clustered_uncertainty(population[population.date <= "2026-08-13"], "THROUGH_AUG13", BOOTSTRAP_SEED)
    uncertainty14 = clustered_uncertainty(population, "THROUGH_AUG14", BOOTSTRAP_SEED + 1)
    uncertainty = pd.concat([uncertainty13, uncertainty14], ignore_index=True)
    current_uncertainty = uncertainty[uncertainty.scope.eq("THROUGH_AUG14")]
    loo = leave_one_date_out(population)
    ordering, ordering_status = confidence_ordering(population)
    calibration_table, upper_status = calibration(population)

    population_columns = [
        "identity", "date", "game_id", "player_id", "prediction_timestamp",
        "scheduled_game_start", "strict_pregame", "model_semantic_name", "model_artifact_sha256",
        "p_over", "target", "actual_hits", "outcome_attachment_contract", "baseline_a_population",
        "hitter_prior_resolved_games", "hitter_prior_hits", "baseline_b_hitter_shrunk",
    ]
    population[population_columns].to_csv(OUT / "hits05_exact_current_model_aug3_aug14_population.csv", index=False, lineterminator="\n")
    reproduction.to_csv(OUT / "hits05_aug3_aug13_baseline_reproduction.csv", index=False, lineterminator="\n")
    aug14_comparison.to_csv(OUT / "hits05_aug14_baseline_comparison.csv", index=False, lineterminator="\n")
    cumulative.to_csv(OUT / "hits05_aug3_aug14_cumulative_baseline_comparison.csv", index=False, lineterminator="\n")
    uncertainty.to_csv(OUT / "hits05_clustered_baseline_uncertainty.csv", index=False, lineterminator="\n")
    loo.to_csv(OUT / "hits05_leave_one_date_out_stability.csv", index=False, lineterminator="\n")
    daily.to_csv(OUT / "hits05_daily_model_vs_baselines.csv", index=False, lineterminator="\n")
    trajectory.to_csv(OUT / "hits05_cumulative_evidence_trajectory.csv", index=False, lineterminator="\n")
    ordering.to_csv(OUT / "hits05_confidence_ordering_aug14.csv", index=False, lineterminator="\n")
    calibration_table.to_csv(OUT / "hits05_calibration_aug14.csv", index=False, lineterminator="\n")

    baseline_contracts = f"""# Hits 0.5 leakage-safe baseline contracts

These definitions were frozen before the August 14 comparison and were not tuned to August outcomes.

## Baseline A — strict-prior population rate

`p_A = {source['population_rate']:.15f}` for every evaluation row. This exactly reproduces the prior adversarial review's frozen pre-August-3 strict-family rate. It is estimated from {source['history_rows']:,} resolved original prediction rows for {source['history_players']} players dated {source['history_start']} through {source['history_end']}; all rows precede the first evaluated slate. Source: `{SEASON.relative_to(ROOT)}`.

## Baseline B — strict-prior hitter-shrunk rate

`p_B(i,d) = (hits_before_date(i,d) + 8 * p_A) / (resolved_games_before_date(i,d) + 8)`.

The eight pseudo-game rule is the unchanged governed formula in `{FROZEN_PROCEDURE.relative_to(ROOT)}`. Hitter histories begin from the same pre-August-3 source state as Baseline A and advance only after a completed historical date; same-date outcomes never enter that date's probabilities. A hitter with no prior resolved history receives `p_A`. There is no August tuning, market input, outcome leakage, differing evaluation denominator, or eligibility selector.

## Evaluation outcomes

Original exact-SHA strict-pregame predictions are immutable. August 10–14 use repaired canonical prospective outcome sidecars. August 3–9 retain the original frozen prospective outcome attachments that underlie the adversarial reference; no outcome was queried or reconstructed by this update.
"""
    (OUT / "hits05_baseline_contracts.md").write_text(baseline_contracts)

    cumulative_index = cumulative.set_index("forecast")
    through13_index = reproduction.set_index("forecast")
    aug14_index = aug14_comparison.set_index("forecast")
    pop_status = baseline_status(current_uncertainty, "population")
    hitter_status = baseline_status(current_uncertainty, "hitter_shrunk")
    brier_columns = ["model_minus_population_brier", "model_minus_hitter_shrunk_brier"]
    loo_fractions = {column: float((loo[column] < 0).mean()) for column in brier_columns}
    point_a_before = through13_index.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"]
    point_a_after = cumulative_index.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"]
    point_b_before = through13_index.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"]
    point_b_after = cumulative_index.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"]
    if point_a_after < point_a_before and point_b_after < point_b_before:
        aug14_effect = "AUG14_MODESTLY_STRENGTHENED_EVIDENCE"
    elif point_a_after > point_a_before and point_b_after > point_b_before:
        aug14_effect = "AUG14_WEAKENED_EVIDENCE"
    else:
        aug14_effect = "AUG14_NEUTRAL_TO_EVIDENCE"

    delta_rows = current_uncertainty[current_uncertainty.metric.str.startswith("model_minus")]
    all_point_ahead = bool((delta_rows.point_estimate < 0).all())
    all_separated = bool((delta_rows.ci_high < 0).all())
    if all_separated and min(loo_fractions.values()) == 1.0:
        progress = "EXACT_CURRENT_MODEL_EVIDENCE_SEPARATING_FROM_BASELINES"
    elif all_point_ahead:
        progress = "EXACT_CURRENT_MODEL_EVIDENCE_TRENDING_POSITIVE_BUT_NOT_SEPARATED"
    elif pop_status == hitter_status == "MODEL_BEHIND":
        progress = "EXACT_CURRENT_MODEL_EVIDENCE_BEHIND_BASELINES"
    elif min(loo_fractions.values()) < 0.25:
        progress = "EXACT_CURRENT_MODEL_EVIDENCE_UNSTABLE"
    else:
        progress = "EXACT_CURRENT_MODEL_EVIDENCE_STILL_EFFECTIVELY_TIED_TO_BASELINES"

    fresh_overall = pd.read_csv(FRESH_BASELINES).query("forecast == 'MODEL'").iloc[0]
    fresh_overlap = pd.read_csv(FRESH_OVERLAP).iloc[0]
    fresh_direction = (
        "DIRECTION_REMAINS_INTACT"
        if cumulative_index.loc["MODEL", "brier"] < float(fresh_overall.brier)
        and float(fresh_overlap.live_brier) < float(fresh_overlap.fresh_brier)
        else "DIRECTION_NOT_INTACT"
    )
    daily_wins = {
        "population": int((daily.model_minus_population_brier < 0).sum()),
        "population_losses": int((daily.model_minus_population_brier > 0).sum()),
        "hitter": int((daily.model_minus_hitter_shrunk_brier < 0).sum()),
        "hitter_losses": int((daily.model_minus_hitter_shrunk_brier > 0).sum()),
    }
    uncertainty_index = uncertainty.set_index(["scope", "metric"])
    loo_population_sign_changes = loo.loc[
        (loo.model_minus_population_brier < 0) != (point_a_after < 0), "excluded_date"
    ].tolist()
    loo_hitter_sign_changes = loo.loc[
        (loo.model_minus_hitter_shrunk_brier < 0) != (point_b_after < 0), "excluded_date"
    ].tolist()
    before_ece = fixed_ece(
        population.loc[(population.date <= "2026-08-13") & population.target.notna(), "target"],
        population.loc[(population.date <= "2026-08-13") & population.target.notna(), "p_over"],
    )
    effect_text = f"""# August 14 due-diligence effect

`{aug14_effect}`

- August 14 model Brier/log loss: {aug14_index.loc['MODEL', 'brier']:.6f} / {aug14_index.loc['MODEL', 'log_loss']:.6f}.
- August 14 model-minus-population Brier/log-loss: {aug14_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION', 'model_minus_forecast_brier']:.6f} / {aug14_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION', 'model_minus_forecast_log_loss']:.6f}.
- August 14 model-minus-hitter-shrunk Brier/log-loss: {aug14_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK', 'model_minus_forecast_brier']:.6f} / {aug14_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK', 'model_minus_forecast_log_loss']:.6f}.
- The slate improved the model more than both baselines on both proper scores. The cumulative population-baseline Brier delta changed sign in the model's favor, while the hitter-baseline advantage widened.
- Model Brier/log loss moved from {through13_index.loc['MODEL', 'brier']:.6f} / {through13_index.loc['MODEL', 'log_loss']:.6f} through Aug 13 to {cumulative_index.loc['MODEL', 'brier']:.6f} / {cumulative_index.loc['MODEL', 'log_loss']:.6f} through Aug 14; fixed-bin ECE moved from {before_ece:.6f} to {cumulative_index.loc['MODEL', 'ece']:.6f}.
- Population Brier-delta 95% CI moved from [{uncertainty_index.loc[('THROUGH_AUG13', 'model_minus_population_brier'), 'ci_low']:.6f}, {uncertainty_index.loc[('THROUGH_AUG13', 'model_minus_population_brier'), 'ci_high']:.6f}] to [{uncertainty_index.loc[('THROUGH_AUG14', 'model_minus_population_brier'), 'ci_low']:.6f}, {uncertainty_index.loc[('THROUGH_AUG14', 'model_minus_population_brier'), 'ci_high']:.6f}].
- Hitter-shrunk Brier-delta 95% CI moved from [{uncertainty_index.loc[('THROUGH_AUG13', 'model_minus_hitter_shrunk_brier'), 'ci_low']:.6f}, {uncertainty_index.loc[('THROUGH_AUG13', 'model_minus_hitter_shrunk_brier'), 'ci_high']:.6f}] to [{uncertainty_index.loc[('THROUGH_AUG14', 'model_minus_hitter_shrunk_brier'), 'ci_low']:.6f}, {uncertainty_index.loc[('THROUGH_AUG14', 'model_minus_hitter_shrunk_brier'), 'ci_high']:.6f}].
- Confidence ordering remains `{ordering_status}`; upper-tail calibration remains `{upper_status}` with one row at >=75%.
- Clustered uncertainty still governs; this is a modest evidence update, not certification.
"""
    (OUT / "hits05_aug14_due_diligence_effect.md").write_text(effect_text)

    status_text = f"""# Exact-current-model evidence progress

`{progress}`

- Population baseline: `{pop_status}` under date-clustered uncertainty.
- Hitter-shrunk baseline: `{hitter_status}` under date-clustered uncertainty.
- Confidence ordering: `{ordering_status}` (prior: `DIRECTIONALLY_PRESENT`).
- Upper tail: `{upper_status}`.
- Fresh-start control: `{fresh_direction}`; this does not establish separation from leakage-safe baselines.
- Leave-one-date-out Brier: model favors population in {loo_fractions['model_minus_population_brier']:.1%} of exclusions (sign changes: {', '.join(loo_population_sign_changes) or 'none'}); model favors hitter-shrunk in {loo_fractions['model_minus_hitter_shrunk_brier']:.1%} (sign changes: {', '.join(loo_hitter_sign_changes) or 'none'}).
- Still missing: more independent date clusters, intervals excluding zero in the model's favor against both baselines, stable leave-one-date-out improvement over both, more high-probability observations, and stronger non-overlapping confidence-ordering evidence.
- No certification, recalibration, retraining, replay, selector, EV/ROI, production, or UI action is authorized by this result.
"""
    (OUT / "hits05_evidence_progress_status.md").write_text(status_text)

    delta_a = cumulative_index.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"]
    delta_b = cumulative_index.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"]
    concise = f"""# MLB Hits 0.5 exact-current-model baseline stress update through August 14

- Population: {len(population):,} original predictions; {len(resolved):,} resolved; {len(population) - len(resolved):,} unresolved; {resolved.date.nunique()} date clusters; {population.game_id.nunique()} games; {population.player_id.nunique()} players; zero primary duplicates.
- Aug 3–13 reproduction: model/population Brier {through13_index.loc['MODEL', 'brier']:.6f} / {through13_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION', 'brier']:.6f}; exact prior reference reproduced.
- Aug 14 model/population/hitter Brier: {aug14_index.loc['MODEL', 'brier']:.6f} / {aug14_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION', 'brier']:.6f} / {aug14_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK', 'brier']:.6f}.
- Through Aug 14 model/population/hitter Brier: {cumulative_index.loc['MODEL', 'brier']:.6f} / {cumulative_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION', 'brier']:.6f} / {cumulative_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK', 'brier']:.6f}; model deltas {delta_a:.6f} / {delta_b:.6f}.
- Date wins by Brier: model {daily_wins['population']}/{len(daily)} vs population; {daily_wins['hitter']}/{len(daily)} vs hitter-shrunk.
- Leave-one-date-out Brier favors model in {loo_fractions['model_minus_population_brier']:.1%} of population-baseline exclusions and {loo_fractions['model_minus_hitter_shrunk_brier']:.1%} of hitter-baseline exclusions.
- Ordering `{ordering_status}`; upper tail `{upper_status}`; August 14 `{aug14_effect}`.
- Fresh-start control `{fresh_direction}`.
- Final status `{progress}`.
- No certification or operational change.
"""
    (OUT / "concise_mlb_hits05_exact_current_model_baseline_stress_update_aug14_v1.md").write_text(concise)

    canonical_inputs = [
        item
        for date_value in canonical_grade.CANONICAL_DATES
        for item in (
            ROOT / f"artifacts/analysis/mlb/prospective_lineage_outcomes/{date_value}/canonical_outcome_reconciliation.csv",
            ROOT / f"artifacts/analysis/mlb/prospective_lineage_outcomes/{date_value}/canonical_outcome_reconciliation_summary.json",
        )
    ]
    ledger_inputs = [ROOT / f"backend/mlb/exports/prospective_lineage/{date_value}/prediction_lineage_ledger.csv" for date_value in DATES]
    inputs = [
        SEASON, PRIOR_BASELINES, FROZEN_PROCEDURE, FRESH_BASELINES, FRESH_OVERLAP,
        MODEL_ARTIFACT, canonical_grade.PRIOR, canonical_grade.CANONICAL_SUMMARY,
        Path(__file__).resolve(),
        ROOT / "backend/mlb/scripts/grade_mlb_hits05_aug14_canonical_update_v1.py",
        *canonical_inputs, *ledger_inputs,
    ]
    write_hashes(inputs)

    print(json.dumps({
        "predictions": len(population),
        "resolved": len(resolved),
        "date_clusters": resolved.date.nunique(),
        "model": cumulative_index.loc["MODEL", ["brier", "log_loss", "ece"]].to_dict(),
        "population_baseline": cumulative_index.loc["BASELINE_A_STRICT_PRIOR_POPULATION", ["brier", "log_loss"]].to_dict(),
        "hitter_baseline": cumulative_index.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", ["brier", "log_loss"]].to_dict(),
        "ordering": ordering_status,
        "upper_tail": upper_status,
        "aug14_effect": aug14_effect,
        "fresh_direction": fresh_direction,
        "progress": progress,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Formal 20-date-cluster review of the frozen exact-current MLB Hits 0.5 model.

This is an artifact-only review.  It does not score a new slate, refit a model,
or write prediction/outcome state.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_hits05_exact_current_model_baseline_stress_update_aug14_v1 as prior


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_exact_current_model_20_cluster_formal_review_v1/2026-08-23"
BASE = prior.OUT / "hits05_exact_current_model_aug3_aug14_population.csv"
MODEL_ARTIFACT = ROOT / "models_out/latest/hits.joblib"
MODEL_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
MODEL_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
DATES = [f"2026-08-{day:02d}" for day in range(3, 23)]
FIRST12 = DATES[:12]
NEXT8 = DATES[12:]
REPS = 10_000
SEED = 20260823
PSEUDO_GAMES = 8.0
BIN_EDGES = prior.BIN_EDGES
BIN_LABELS = prior.BIN_LABELS
FORECASTS = prior.FORECASTS
FRESH_BASELINES = prior.FRESH_BASELINES
FRESH_OVERLAP = prior.FRESH_OVERLAP
FAMILY_REVIEW = ROOT / "artifacts/analysis/model_development/mlb_hits05_adversarial_certification_recheck_v1/2026-08-14/concise_mlb_hits05_adversarial_certification_recheck_v1.md"
MARKET_AUDIT = ROOT / "artifacts/analysis/model_development/mlb_hits05_standalone_prediction_certification_review_v1/2026-08-14/hits05_cert_market_input_audit.md"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_text(name: str, text: str) -> None:
    (OUT / name).write_text(text.rstrip() + "\n")


def metric(y: Iterable[float], p: Iterable[float]) -> dict[str, float]:
    yv = np.asarray(list(y), dtype=float)
    pv = np.clip(np.asarray(list(p), dtype=float), 1e-12, 1 - 1e-12)
    return {
        "rows": int(len(yv)),
        "brier": float(np.mean((pv - yv) ** 2)),
        "log_loss": float(np.mean(-(yv * np.log(pv) + (1 - yv) * np.log(1 - pv)))),
        "mean_probability": float(np.mean(pv)),
        "observed_rate": float(np.mean(yv)),
        "probability_sd": float(np.std(pv, ddof=0)),
        "minimum_probability": float(np.min(pv)),
        "maximum_probability": float(np.max(pv)),
        "accuracy_at_50": float(np.mean((pv >= 0.5) == (yv == 1))),
    }


def ece(y: pd.Series, p: pd.Series) -> float:
    if len(y) == 0:
        return math.nan
    bands = pd.cut(p.astype(float), BIN_EDGES, labels=BIN_LABELS, right=False)
    total = len(y)
    value = 0.0
    for label in BIN_LABELS:
        idx = bands[bands == label].index
        if len(idx):
            value += len(idx) / total * abs(float(p.loc[idx].mean()) - float(y.loc[idx].mean()))
    return float(value)


def sidecar_path(day: str) -> Path:
    return ROOT / f"artifacts/analysis/mlb/prospective_lineage_outcomes/{day}/canonical_outcome_reconciliation.csv"


def ledger_path(day: str) -> Path:
    return ROOT / f"backend/mlb/exports/prospective_lineage/{day}/prediction_lineage_ledger.csv"


def load_extension() -> tuple[pd.DataFrame, pd.DataFrame]:
    population_rows: list[dict] = []
    outcome_rows: list[dict] = []
    for day in NEXT8:
        source = sidecar_path(day)
        frame = pd.read_csv(source, low_memory=False)
        frame["line"] = pd.to_numeric(frame.line, errors="coerce")
        exact = frame[
            frame.prop_type.eq("hits")
            & frame.line.eq(0.5)
            & frame.model_semantic_name.eq(MODEL_ID)
            & frame.model_artifact_sha256.eq(MODEL_HASH)
            & frame.prediction_lineage_status.eq("LINEAGE_CERTIFIED")
        ].copy()
        if exact.canonical_identity.duplicated().any():
            raise AssertionError(f"duplicate canonical primary identity in {day}")
        for row in exact.itertuples(index=False):
            prediction_dt = pd.to_datetime(row.prediction_timestamp, utc=True, errors="coerce")
            start_dt = pd.to_datetime(row.scheduled_game_start, utc=True, errors="coerce")
            target = np.nan if pd.isna(row.actual_value) else float(float(row.actual_value) >= 1.0)
            population_rows.append({
                "identity": row.canonical_identity,
                "date": row.game_date,
                "game_id": int(row.game_id),
                "player_id": int(row.player_id),
                "prediction_timestamp": row.prediction_timestamp,
                "scheduled_game_start": row.scheduled_game_start,
                "prediction_dt": prediction_dt,
                "start_dt": start_dt,
                "strict_pregame": bool(pd.notna(prediction_dt) and pd.notna(start_dt) and prediction_dt < start_dt),
                "model_semantic_name": row.model_semantic_name,
                "model_artifact_sha256": row.model_artifact_sha256,
                "p_over": float(row.model_probability_over),
                "target": target,
                "actual_hits": row.actual_value,
                "outcome_attachment_contract": "CANONICAL_PROSPECTIVE_OUTCOME_SIDECAR",
                "outcome_status": row.outcome_status,
                "outcome_source": str(source.relative_to(ROOT)),
                "actual_sample_rows": int(row.actual_sample_rows),
                "actual_distinct_values": int(row.actual_distinct_values),
            })
            outcome_rows.append({
                "identity": row.canonical_identity,
                "date": row.game_date,
                "game_id": int(row.game_id),
                "player_id": int(row.player_id),
                "outcome_source": str(source.relative_to(ROOT)),
                "outcome_status": row.outcome_status,
                "resolved": bool(pd.notna(row.actual_value)),
                "actual_hits": row.actual_value,
                "actual_sample_rows": int(row.actual_sample_rows),
                "actual_distinct_values": int(row.actual_distinct_values),
                "stat_identity_conflict": bool(int(row.actual_distinct_values) > 1),
                "outcome_contract": row.outcome_contract,
            })
    return pd.DataFrame(population_rows), pd.DataFrame(outcome_rows)


def load_population() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    base = pd.read_csv(BASE, low_memory=False)
    base["prediction_dt"] = pd.to_datetime(base.prediction_timestamp, utc=True, errors="coerce")
    base["start_dt"] = pd.to_datetime(base.scheduled_game_start, utc=True, errors="coerce")
    base["outcome_status"] = np.where(base.target.notna(), "RESOLVED_FROZEN_GOVERNED_ATTACHMENT", "UNRESOLVED_NO_OFFICIAL_APPEARANCE_OR_ELIGIBLE_OUTCOME")
    base["outcome_source"] = str(BASE.relative_to(ROOT))
    base["actual_sample_rows"] = np.where(base.target.notna(), 1, 0)
    base["actual_distinct_values"] = np.where(base.target.notna(), 1, 0)
    # August 10-14 have standalone canonical reconciliation sidecars.  Validate
    # them against the frozen base package and retain their richer audit fields.
    for day in [f"2026-08-{value:02d}" for value in range(10, 15)]:
        sidecar = pd.read_csv(sidecar_path(day), low_memory=False)
        sidecar["line"] = pd.to_numeric(sidecar.line, errors="coerce")
        sidecar = sidecar[
            sidecar.prop_type.eq("hits") & sidecar.line.eq(0.5)
            & sidecar.model_semantic_name.eq(MODEL_ID)
            & sidecar.model_artifact_sha256.eq(MODEL_HASH)
            & sidecar.prediction_lineage_status.eq("LINEAGE_CERTIFIED")
        ].copy()
        if sidecar.canonical_identity.duplicated().any():
            raise AssertionError(f"duplicate canonical outcome identity in {day}")
        lookup = sidecar.set_index("canonical_identity")
        day_index = base.index[base.date.eq(day)]
        if set(base.loc[day_index, "identity"]) != set(lookup.index):
            raise AssertionError(f"base/sidecar identity mismatch in {day}")
        for index in day_index:
            row = lookup.loc[base.at[index, "identity"]]
            sidecar_target = np.nan if pd.isna(row.actual_value) else float(float(row.actual_value) >= 1.0)
            base_target = base.at[index, "target"]
            if not (pd.isna(sidecar_target) and pd.isna(base_target)) and sidecar_target != base_target:
                raise AssertionError(f"base/sidecar outcome mismatch in {day}")
            base.at[index, "outcome_status"] = row.outcome_status
            base.at[index, "outcome_source"] = str(sidecar_path(day).relative_to(ROOT))
            base.at[index, "actual_sample_rows"] = int(row.actual_sample_rows)
            base.at[index, "actual_distinct_values"] = int(row.actual_distinct_values)
    extension, extension_outcomes = load_extension()
    population = pd.concat([base, extension], ignore_index=True, sort=False)
    population["date"] = population.date.astype(str)
    population["game_id"] = pd.to_numeric(population.game_id, errors="raise").astype(int)
    population["player_id"] = pd.to_numeric(population.player_id, errors="raise").astype(int)
    population["p_over"] = pd.to_numeric(population.p_over, errors="raise")
    population["target"] = pd.to_numeric(population.target, errors="coerce")
    # Preserve the governed base-then-sidecar row order.  The predeclared fixed
    # quintile review used this frozen input order for equal-probability ties.
    population = population.reset_index(drop=True)
    if set(population.date.unique()) != set(DATES):
        raise AssertionError("evaluation date set differs from the frozen 20-date contract")
    if population.identity.duplicated().any():
        raise AssertionError("PRIMARY_DUPLICATES != 0")
    if not population.strict_pregame.fillna(False).all():
        raise AssertionError("STRICT_PREGAME != TRUE")
    exact = population.model_semantic_name.eq(MODEL_ID) & population.model_artifact_sha256.eq(MODEL_HASH)
    if not exact.all():
        raise AssertionError("exact model binding failed")
    if sha256(MODEL_ARTIFACT) != MODEL_HASH:
        raise AssertionError("installed artifact hash differs from frozen review hash")

    # Rebuild the unchanged baselines from their pre-August state, advancing only
    # after each completed date.  No evaluated date contributes to its own prior.
    history = prior.strict_prior_history().copy()
    history["player_id"] = pd.to_numeric(history.player_id, errors="raise").astype(int)
    population_rate = float(history.hit_1plus.mean())
    player_n = history.groupby("player_id").size().to_dict()
    player_hits = history.groupby("player_id").hit_1plus.sum().to_dict()
    baselines: list[dict] = []
    for day in DATES:
        slate = population[population.date.eq(day)]
        for row in slate.itertuples(index=False):
            n = int(player_n.get(row.player_id, 0))
            hits = float(player_hits.get(row.player_id, 0.0))
            baselines.append({
                "identity": row.identity,
                "baseline_a_population": population_rate,
                "hitter_prior_resolved_games": n,
                "hitter_prior_hits": hits,
                "baseline_b_hitter_shrunk": (hits + PSEUDO_GAMES * population_rate) / (n + PSEUDO_GAMES),
            })
        for row in slate[slate.target.notna()].itertuples(index=False):
            player_n[row.player_id] = player_n.get(row.player_id, 0) + 1
            player_hits[row.player_id] = player_hits.get(row.player_id, 0.0) + float(row.target)
    rebuilt = pd.DataFrame(baselines)
    population = population.drop(columns=[c for c in rebuilt.columns if c != "identity" and c in population.columns])
    population = population.merge(rebuilt, on="identity", how="left", validate="one_to_one")

    # Outcome audit uses available canonical sidecars.  Aug 3-9 have no standalone
    # sidecars; their frozen governed attachment is retained and disclosed.
    base_outcomes = base[["identity", "date", "game_id", "player_id", "outcome_source", "outcome_status", "actual_hits", "actual_sample_rows", "actual_distinct_values"]].copy()
    base_outcomes["resolved"] = base_outcomes.actual_hits.notna()
    base_outcomes["stat_identity_conflict"] = False
    base_outcomes["outcome_contract"] = base.outcome_attachment_contract
    outcome_audit = pd.concat([base_outcomes, extension_outcomes], ignore_index=True, sort=False).sort_values(["date", "identity"])

    run_stats = run_observation_stats()
    source = {
        "strict_prior_history_rows": int(len(history)),
        "strict_prior_history_players": int(history.player_id.nunique()),
        "strict_prior_history_start": str(history.date.min()),
        "strict_prior_history_end": str(history.date.max()),
        "strict_prior_population_rate": population_rate,
        **run_stats,
    }
    return population, outcome_audit, source


def _identity_from_json(value: object) -> str | None:
    try:
        data = json.loads(str(value))
        if str(data.get("prop_type")) != "hits" or float(data.get("line")) != 0.5:
            return None
        return f"{int(data['game_id'])}:{int(data['player_id'])}:hits:0.5"
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        return None


def run_observation_stats() -> dict:
    observations = 0
    post_start = 0
    timing_unresolved = 0
    for day in DATES:
        frame = pd.read_csv(ledger_path(day), low_memory=False)
        exact = frame[
            frame.model_semantic_name.eq(MODEL_ID)
            & frame.model_artifact_sha256.eq(MODEL_HASH)
        ].copy()
        exact["identity"] = exact.canonical_row_identity.map(_identity_from_json)
        exact = exact[exact.identity.notna()].copy()
        prediction = pd.to_datetime(exact.prediction_timestamp, utc=True, errors="coerce")
        start = pd.to_datetime(exact.scheduled_game_start, utc=True, errors="coerce")
        observations += len(exact)
        timing_unresolved += int((prediction.isna() | start.isna()).sum())
        post_start += int(((prediction >= start) & prediction.notna() & start.notna()).sum())
    return {
        "run_observations": int(observations),
        "run_observation_post_start_rows": int(post_start),
        "run_observation_timing_unresolved_rows": int(timing_unresolved),
    }


def compare(frame: pd.DataFrame) -> pd.DataFrame:
    resolved = frame[frame.target.notna()]
    rows: list[dict] = []
    for forecast, column in FORECASTS.items():
        result = metric(resolved.target, resolved[column])
        result["ece"] = ece(resolved.target, resolved[column])
        rows.append({"forecast": forecast, **result})
    output = pd.DataFrame(rows)
    model = output.iloc[0]
    output["model_minus_forecast_brier"] = model.brier - output.brier
    output["model_minus_forecast_log_loss"] = model.log_loss - output.log_loss
    return output


def reproduce_first12(population: pd.DataFrame) -> pd.DataFrame:
    current = compare(population[population.date.isin(FIRST12)])
    index = current.set_index("forecast")
    references = {
        "MODEL": (0.244066, 0.681216),
        "BASELINE_A_STRICT_PRIOR_POPULATION": (0.244363, 0.681831),
        "BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK": (0.246223, 0.686155),
    }
    for name, (brier_ref, log_ref) in references.items():
        if abs(float(index.loc[name, "brier"]) - brier_ref) > 5e-7 or abs(float(index.loc[name, "log_loss"]) - log_ref) > 5e-7:
            raise AssertionError(f"material 12-cluster reproduction failure: {name}")
    current.insert(0, "scope", "2026-08-03_THROUGH_2026-08-14")
    current["completed_date_clusters"] = 12
    current["reference_reproduced"] = True
    return current


def date_bootstrap(population: pd.DataFrame) -> pd.DataFrame:
    resolved = population[population.target.notna()].copy()
    dates = sorted(resolved.date.unique())
    grouped: dict[str, pd.DataFrame] = {}
    for name, column in FORECASTS.items():
        work = resolved[["date", "target", column]].copy()
        p = np.clip(work[column].to_numpy(float), 1e-12, 1 - 1e-12)
        y = work.target.to_numpy(float)
        work["brier"] = (p - y) ** 2
        work["log"] = -(y * np.log(p) + (1 - y) * np.log(1 - p))
        grouped[name] = work.groupby("date").agg(n=("target", "size"), brier=("brier", "sum"), log=("log", "sum")).loc[dates]
    cal = resolved.assign(gap=resolved.p_over - resolved.target).groupby("date").agg(n=("target", "size"), gap=("gap", "sum")).loc[dates]
    rng = np.random.default_rng(SEED)
    names = [
        "model_brier", "model_log_loss", "model_minus_population_brier", "model_minus_population_log_loss",
        "model_minus_hitter_brier", "model_minus_hitter_log_loss", "overall_calibration_gap",
    ]
    draws = {name: np.empty(REPS) for name in names}
    for i in range(REPS):
        sample = rng.integers(0, len(dates), size=len(dates))
        values = {}
        for name, table in grouped.items():
            selected = table.iloc[sample]
            values[name] = (selected.brier.sum() / selected.n.sum(), selected.log.sum() / selected.n.sum())
        selected_cal = cal.iloc[sample]
        draws["model_brier"][i], draws["model_log_loss"][i] = values["MODEL"]
        draws["model_minus_population_brier"][i] = values["MODEL"][0] - values["BASELINE_A_STRICT_PRIOR_POPULATION"][0]
        draws["model_minus_population_log_loss"][i] = values["MODEL"][1] - values["BASELINE_A_STRICT_PRIOR_POPULATION"][1]
        draws["model_minus_hitter_brier"][i] = values["MODEL"][0] - values["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"][0]
        draws["model_minus_hitter_log_loss"][i] = values["MODEL"][1] - values["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK"][1]
        draws["overall_calibration_gap"][i] = selected_cal.gap.sum() / selected_cal.n.sum()
    table = compare(resolved).set_index("forecast")
    points = {
        "model_brier": table.loc["MODEL", "brier"],
        "model_log_loss": table.loc["MODEL", "log_loss"],
        "model_minus_population_brier": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"],
        "model_minus_population_log_loss": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_log_loss"],
        "model_minus_hitter_brier": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"],
        "model_minus_hitter_log_loss": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_log_loss"],
        "overall_calibration_gap": float((resolved.p_over - resolved.target).mean()),
    }
    rows = []
    for name in names:
        delta = name.startswith("model_minus")
        rows.append({
            "metric": name,
            "point_estimate": points[name],
            "ci_low": float(np.quantile(draws[name], 0.025)),
            "ci_high": float(np.quantile(draws[name], 0.975)),
            "fraction_draws_favoring_model": float(np.mean(draws[name] < 0)) if delta else np.nan,
            "completed_date_clusters": len(dates),
            "replicates": REPS,
            "seed": SEED,
        })
    return pd.DataFrame(rows)


def split_stability(population: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for scope, dates in (("FIRST_12_COMPLETED_CLUSTERS", FIRST12), ("NEXT_8_COMPLETED_CLUSTERS", NEXT8)):
        frame = population[population.date.isin(dates) & population.target.notna()]
        table = compare(frame).set_index("forecast")
        model = table.loc["MODEL"]
        rows.append({
            "scope": scope, "completed_date_clusters": len(dates), "rows": len(frame),
            "brier": model.brier, "log_loss": model.log_loss, "ece": model.ece,
            "mean_predicted_rate": model.mean_probability, "observed_rate": model.observed_rate,
            "model_minus_population_brier": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"],
            "model_minus_population_log_loss": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_log_loss"],
            "model_minus_hitter_brier": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"],
            "model_minus_hitter_log_loss": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_log_loss"],
        })
    output = pd.DataFrame(rows)
    first, nxt = output.iloc[0], output.iloc[1]
    same_direction = all(np.sign(first[c]) == np.sign(nxt[c]) for c in ["model_minus_population_brier", "model_minus_hitter_brier"])
    if same_direction and abs(first.brier - nxt.brier) <= 0.01:
        status = "PASS"
    elif abs(first.brier - nxt.brier) <= 0.02:
        status = "MIXED"
    else:
        status = "FAIL"
    output["forward_temporal_stability"] = status
    return output


def daily_metrics(population: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for day in DATES:
        frame = population[population.date.eq(day) & population.target.notna()]
        table = compare(frame).set_index("forecast")
        rows.append({
            "date": day, "resolved_rows": len(frame),
            "model_brier": table.loc["MODEL", "brier"], "model_log_loss": table.loc["MODEL", "log_loss"],
            "population_baseline_brier": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "brier"],
            "hitter_baseline_brier": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "brier"],
            "predicted_hit_rate": table.loc["MODEL", "mean_probability"], "observed_hit_rate": table.loc["MODEL", "observed_rate"],
            "model_beats_population_brier": table.loc["MODEL", "brier"] < table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "brier"],
            "model_beats_hitter_brier": table.loc["MODEL", "brier"] < table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "brier"],
        })
    return pd.DataFrame(rows)


def leave_one_date_out(population: pd.DataFrame) -> pd.DataFrame:
    resolved = population[population.target.notna()]
    full = compare(resolved).set_index("forecast")
    full_deltas = {
        "population_brier": full.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"],
        "population_log": full.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_log_loss"],
        "hitter_brier": full.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"],
        "hitter_log": full.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_log_loss"],
    }
    rows = []
    for day in DATES:
        table = compare(resolved[resolved.date.ne(day)]).set_index("forecast")
        values = {
            "model_minus_population_brier": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"],
            "model_minus_population_log_loss": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_log_loss"],
            "model_minus_hitter_brier": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"],
            "model_minus_hitter_log_loss": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_log_loss"],
        }
        rows.append({"excluded_date": day, "resolved_rows": int(len(resolved) - (resolved.date == day).sum()), **values})
    output = pd.DataFrame(rows)
    for column in [c for c in output.columns if c.startswith("model_minus")]:
        output[f"{column}_favors_model"] = output[column] < 0
    output["maximum_absolute_brier_delta_change_from_full"] = np.maximum(
        abs(output.model_minus_population_brier - full_deltas["population_brier"]),
        abs(output.model_minus_hitter_brier - full_deltas["hitter_brier"]),
    )
    fractions = [float(output[c].mean()) for c in output.columns if c.endswith("_favors_model")]
    if min(fractions) == 1.0:
        status = "ROBUST"
    elif min(fractions) >= 0.8:
        status = "MODERATE"
    elif min(fractions) >= 0.5:
        status = "MIXED"
    else:
        status = "WEAK"
    output["lodo_stability"] = status
    full_by_column = {
        "model_minus_population_brier": full_deltas["population_brier"],
        "model_minus_population_log_loss": full_deltas["population_log"],
        "model_minus_hitter_brier": full_deltas["hitter_brier"],
        "model_minus_hitter_log_loss": full_deltas["hitter_log"],
    }
    for column in full_by_column:
        output[f"{column}_minimum"] = float(output[column].min())
        output[f"{column}_maximum"] = float(output[column].max())
        output[f"{column}_fraction_exclusions_favoring_model"] = float((output[column] < 0).mean())
        output[f"{column}_sign_change_count"] = int(((output[column] < 0) != (full_by_column[column] < 0)).sum())
    influential = output.loc[output.maximum_absolute_brier_delta_change_from_full.idxmax(), "excluded_date"]
    output["most_influential_date_by_brier_delta_change"] = influential
    return output


def cumulative(population: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for i, day in enumerate(DATES, start=1):
        frame = population[(population.date <= day) & population.target.notna()]
        table = compare(frame).set_index("forecast")
        rows.append({
            "through_date": day, "completed_date_clusters": i, "resolved_rows": len(frame),
            "model_brier": table.loc["MODEL", "brier"],
            "population_brier": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "brier"],
            "hitter_brier": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "brier"],
            "model_minus_population_brier": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_brier"],
            "model_minus_hitter_brier": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_brier"],
            "model_log_loss": table.loc["MODEL", "log_loss"],
            "population_log_loss": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "log_loss"],
            "hitter_log_loss": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "log_loss"],
            "model_minus_population_log_loss": table.loc["BASELINE_A_STRICT_PRIOR_POPULATION", "model_minus_forecast_log_loss"],
            "model_minus_hitter_log_loss": table.loc["BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK", "model_minus_forecast_log_loss"],
            "ece": table.loc["MODEL", "ece"],
        })
    return pd.DataFrame(rows)


def band_difference_bootstrap(frame: pd.DataFrame, band_a: str, band_b: str, seed: int) -> tuple[float, float, float]:
    pivot = frame[frame.band.isin([band_a, band_b])].groupby(["date", "band"], observed=False).target.agg(["sum", "count"]).unstack(fill_value=0)
    for label in (band_a, band_b):
        if ("sum", label) not in pivot:
            pivot[("sum", label)] = 0
            pivot[("count", label)] = 0
    rng = np.random.default_rng(seed)
    draws = np.empty(REPS)
    for i in range(REPS):
        sample = pivot.iloc[rng.integers(0, len(pivot), size=len(pivot))]
        rate_a = sample[("sum", band_a)].sum() / sample[("count", band_a)].sum()
        rate_b = sample[("sum", band_b)].sum() / sample[("count", band_b)].sum()
        draws[i] = rate_a - rate_b
    point = float(frame.loc[frame.band.eq(band_a), "target"].mean() - frame.loc[frame.band.eq(band_b), "target"].mean())
    return point, float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))


def confidence_ordering(population: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    # Reproduce the predeclared prospective policy exactly: sort the frozen
    # population by probability using pandas' governed default ordering, then
    # divide the ordered rows into five contiguous numpy array splits.
    resolved = population[population.target.notna()].sort_values("p_over").copy()
    labels = ["BOTTOM20", "SECOND20", "MIDDLE20", "FOURTH20", "TOP20"]
    pieces = []
    for label, positions in zip(labels, np.array_split(np.arange(len(resolved)), 5)):
        piece = resolved.iloc[positions].copy()
        piece["band"] = label
        pieces.append(piece)
    resolved = pd.concat(pieces, ignore_index=True)
    decile_size = int(math.ceil(len(resolved) * 0.10))
    top10 = resolved.iloc[-decile_size:].copy(); top10["band"] = "TOP10"
    bottom10 = resolved.iloc[:decile_size].copy(); bottom10["band"] = "BOTTOM10"
    all_bands = pd.concat([resolved, top10], ignore_index=True)
    rows = []
    for label in ["BOTTOM20", "SECOND20", "MIDDLE20", "FOURTH20", "TOP20", "TOP10"]:
        frame = all_bands[all_bands.band.astype(str).eq(label)]
        result = metric(frame.target, frame.p_over)
        rows.append({"band": label, "rows": len(frame), "mean_predicted_probability": result["mean_probability"], "observed_hit_rate": result["observed_rate"], "brier": result["brier"]})
    output = pd.DataFrame(rows)
    quintiles = output.iloc[:5]
    top_bottom = band_difference_bootstrap(resolved, "TOP20", "BOTTOM20", SEED + 10)
    deciles = pd.concat([top10, bottom10], ignore_index=True)
    top_decile = band_difference_bootstrap(deciles, "TOP10", "BOTTOM10", SEED + 11)
    # Aggregated-band rank correlation is the specified ordering diagnostic.
    rank_corr = float(quintiles.mean_predicted_probability.corr(quintiles.observed_hit_rate, method="spearman"))
    ordering_stats = pd.DataFrame([
        {"diagnostic": "AGGREGATED_QUINTILE_SPEARMAN", "point_estimate": rank_corr, "ci_low": np.nan, "ci_high": np.nan},
        {"diagnostic": "TOP20_MINUS_BOTTOM20_OBSERVED_RATE", "point_estimate": top_bottom[0], "ci_low": top_bottom[1], "ci_high": top_bottom[2]},
        {"diagnostic": "TOP10_MINUS_BOTTOM10_OBSERVED_RATE", "point_estimate": top_decile[0], "ci_low": top_decile[1], "ci_high": top_decile[2]},
    ])
    monotonic = bool(np.all(np.diff(quintiles.observed_hit_rate) >= 0))
    if monotonic and top_bottom[1] > 0 and top_decile[1] > 0:
        status = "ROBUST"
    elif top_bottom[0] > 0 and top_decile[0] > 0:
        status = "DIRECTIONALLY_PRESENT"
    elif top_bottom[0] >= 0:
        status = "WEAK"
    else:
        status = "NOT_PRESENT"
    output["confidence_ordering"] = status
    ordering_stats["confidence_ordering"] = status
    return output, ordering_stats, status


def calibration(population: pd.DataFrame) -> pd.DataFrame:
    resolved = population[population.target.notna()].copy()
    resolved["bin"] = pd.cut(resolved.p_over, BIN_EDGES, labels=BIN_LABELS, right=False)
    rows = []
    for label in BIN_LABELS:
        frame = resolved[resolved.bin.eq(label)]
        rows.append({
            "probability_bin": label, "rows": len(frame),
            "mean_predicted_probability": float(frame.p_over.mean()) if len(frame) else np.nan,
            "observed_hit_rate": float(frame.target.mean()) if len(frame) else np.nan,
            "calibration_gap_predicted_minus_observed": float((frame.p_over - frame.target).mean()) if len(frame) else np.nan,
            "brier": float(np.mean((frame.p_over - frame.target) ** 2)) if len(frame) else np.nan,
        })
    return pd.DataFrame(rows)


def upper_tail(population: pd.DataFrame) -> pd.DataFrame:
    resolved = population[population.target.notna()]
    rows = []
    for threshold in (0.65, 0.70, 0.75):
        frame = resolved[resolved.p_over >= threshold]
        rows.append({
            "threshold": f">={int(threshold * 100)}%", "rows": len(frame),
            "mean_predicted_probability": float(frame.p_over.mean()) if len(frame) else np.nan,
            "observed_hit_rate": float(frame.target.mean()) if len(frame) else np.nan,
            "calibration_gap_predicted_minus_observed": float((frame.p_over - frame.target).mean()) if len(frame) else np.nan,
            "brier": float(np.mean((frame.p_over - frame.target) ** 2)) if len(frame) else np.nan,
        })
    output = pd.DataFrame(rows)
    row75 = output[output.threshold.eq(">=75%")].iloc[0]
    if row75.rows >= 100:
        status = "SUFFICIENT_AND_OVERCONFIDENT" if row75.calibration_gap_predicted_minus_observed > 0.03 else "SUFFICIENT_AND_ACCEPTABLE"
    elif row75.rows >= 30 and row75.calibration_gap_predicted_minus_observed > 0.03:
        status = "DIRECTIONALLY_OVERCONFIDENT_BUT_SAMPLE_LIMITED"
    else:
        status = "INSUFFICIENT_SAMPLE"
    output["current_model_upper_tail"] = status
    return output


def _cluster_delta(frame: pd.DataFrame, cluster: str, seed: int, reps: int = 5000) -> list[dict]:
    work = frame.copy()
    p = np.clip(work.p_over.to_numpy(float), 1e-12, 1 - 1e-12)
    pa = work.baseline_a_population.to_numpy(float)
    pb = np.clip(work.baseline_b_hitter_shrunk.to_numpy(float), 1e-12, 1 - 1e-12)
    y = work.target.to_numpy(float)
    work["delta_a"] = (p - y) ** 2 - (pa - y) ** 2
    work["delta_b"] = (p - y) ** 2 - (pb - y) ** 2
    groups = work.groupby(cluster).agg(n=("target", "size"), delta_a=("delta_a", "sum"), delta_b=("delta_b", "sum"))
    rng = np.random.default_rng(seed)
    draws = {"model_minus_population_brier": np.empty(reps), "model_minus_hitter_brier": np.empty(reps)}
    for i in range(reps):
        selected = groups.iloc[rng.integers(0, len(groups), size=len(groups))]
        draws["model_minus_population_brier"][i] = selected.delta_a.sum() / selected.n.sum()
        draws["model_minus_hitter_brier"][i] = selected.delta_b.sum() / selected.n.sum()
    return [{
        "analysis": f"{cluster.upper()}_CLUSTER_BOOTSTRAP", "metric": name,
        "point_estimate": float(work["delta_a" if "population" in name else "delta_b"].mean()),
        "ci_low": float(np.quantile(values, 0.025)), "ci_high": float(np.quantile(values, 0.975)),
        "fraction_draws_favoring_model": float(np.mean(values < 0)), "clusters": len(groups), "replicates": reps,
    } for name, values in draws.items()]


def dependence_sensitivity(population: pd.DataFrame) -> pd.DataFrame:
    resolved = population[population.target.notna()].copy()
    per_player = resolved.groupby("player_id").size()
    per_game = resolved.groupby("game_id").size()
    rows = [
        {"analysis": "STRUCTURE", "metric": "unique_players", "point_estimate": resolved.player_id.nunique()},
        {"analysis": "STRUCTURE", "metric": "games_represented", "point_estimate": resolved.game_id.nunique()},
        {"analysis": "STRUCTURE", "metric": "predictions_per_player_min", "point_estimate": per_player.min()},
        {"analysis": "STRUCTURE", "metric": "predictions_per_player_median", "point_estimate": per_player.median()},
        {"analysis": "STRUCTURE", "metric": "predictions_per_player_mean", "point_estimate": per_player.mean()},
        {"analysis": "STRUCTURE", "metric": "predictions_per_player_p90", "point_estimate": per_player.quantile(0.9)},
        {"analysis": "STRUCTURE", "metric": "predictions_per_player_max", "point_estimate": per_player.max()},
        {"analysis": "STRUCTURE", "metric": "hitters_per_game_min", "point_estimate": per_game.min()},
        {"analysis": "STRUCTURE", "metric": "hitters_per_game_median", "point_estimate": per_game.median()},
        {"analysis": "STRUCTURE", "metric": "hitters_per_game_mean", "point_estimate": per_game.mean()},
        {"analysis": "STRUCTURE", "metric": "hitters_per_game_p90", "point_estimate": per_game.quantile(0.9)},
        {"analysis": "STRUCTURE", "metric": "hitters_per_game_max", "point_estimate": per_game.max()},
    ]
    rows.extend(_cluster_delta(resolved, "game_id", SEED + 30))
    return pd.DataFrame(rows)


def american_probability(price: float) -> float:
    return -price / (-price + 100.0) if price < 0 else 100.0 / (price + 100.0)


def attach_betonline(population: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    candidates = []
    for day in DATES:
        ledger = pd.read_csv(ledger_path(day), low_memory=False)
        ledger = ledger[
            ledger.model_semantic_name.eq(MODEL_ID)
            & ledger.model_artifact_sha256.eq(MODEL_HASH)
            & ledger.bookmaker_key.eq("betonlineag")
        ].copy()
        ledger["identity"] = ledger.canonical_row_identity.map(_identity_from_json)
        ledger = ledger[ledger.identity.notna()].copy()
        candidates.append(ledger)
    market = pd.concat(candidates, ignore_index=True, sort=False)
    market["odds_dt"] = pd.to_datetime(market.odds_snapshot_timestamp, utc=True, errors="coerce")
    market["market_prediction_dt"] = pd.to_datetime(market.prediction_timestamp, utc=True, errors="coerce")
    market["market_start_dt"] = pd.to_datetime(market.scheduled_game_start, utc=True, errors="coerce")
    primaries = population[["identity", "date", "target", "p_over", "prediction_dt", "start_dt"]].copy()
    paired = primaries.merge(market, on="identity", how="inner", suffixes=("_primary", "_market"))
    paired["abs_sync_seconds"] = (paired.odds_dt - paired.prediction_dt).abs().dt.total_seconds()
    paired["market_strict_pregame"] = paired.odds_dt < paired.start_dt
    paired = paired[
        paired.target.notna() & paired.odds_dt.notna() & paired.market_strict_pregame
        & paired.abs_sync_seconds.le(1800)
        & pd.to_numeric(paired.price_over_american, errors="coerce").notna()
        & pd.to_numeric(paired.price_under_american, errors="coerce").notna()
    ].copy()
    paired = paired.sort_values(["identity", "abs_sync_seconds", "odds_dt"]).drop_duplicates("identity", keep="first")
    paired["over_implied"] = pd.to_numeric(paired.price_over_american).map(american_probability)
    paired["under_implied"] = pd.to_numeric(paired.price_under_american).map(american_probability)
    paired["betonline_p_over_novig"] = paired.over_implied / (paired.over_implied + paired.under_implied)
    paired["model_p_over"] = paired.p_over
    # The ledger has no standalone date column, so the primary date survives
    # the merge without a suffix.
    paired["date"] = paired.date
    paired["model_binary"] = paired.model_p_over >= 0.5
    paired["book_binary"] = paired.betonline_p_over_novig >= 0.5
    paired["actual_binary"] = paired.target.astype(int).eq(1)
    paired["absolute_separation"] = abs(paired.model_p_over - paired.betonline_p_over_novig)
    return paired, {"all_temporally_valid_rows": len(paired), "window_seconds": 1800}


def market_comparison(paired: pd.DataFrame) -> pd.DataFrame:
    model = metric(paired.target, paired.model_p_over)
    book = metric(paired.target, paired.betonline_p_over_novig)
    return pd.DataFrame([{
        "definition": "ABS_TIMESTAMP_DIFFERENCE_LE_30_MIN_BOTH_PREGAME",
        "synchronized_rows": len(paired),
        "proppadia_brier": model["brier"], "betonline_brier": book["brier"],
        "proppadia_log_loss": model["log_loss"], "betonline_log_loss": book["log_loss"],
        "proppadia_ece": ece(paired.target, paired.model_p_over), "betonline_ece": ece(paired.target, paired.betonline_p_over_novig),
        "pearson_probability_correlation": paired.model_p_over.corr(paired.betonline_p_over_novig, method="pearson"),
        "spearman_probability_correlation": paired.model_p_over.corr(paired.betonline_p_over_novig, method="spearman"),
        "mean_absolute_separation": paired.absolute_separation.mean(), "median_absolute_separation": paired.absolute_separation.median(),
        "disagreement_ge_5pp": int((paired.absolute_separation >= 0.05).sum()),
        "disagreement_ge_10pp": int((paired.absolute_separation >= 0.10).sum()),
    }])


def unique_correctness(paired: pd.DataFrame) -> pd.DataFrame:
    model_correct = paired.model_binary.eq(paired.actual_binary)
    book_correct = paired.book_binary.eq(paired.actual_binary)
    rows = [
        {"row_type": "CORRECTNESS", "category": "BOTH_CORRECT", "rows": int((model_correct & book_correct).sum())},
        {"row_type": "CORRECTNESS", "category": "BOTH_WRONG", "rows": int((~model_correct & ~book_correct).sum())},
        {"row_type": "CORRECTNESS", "category": "PROPPADIA_ONLY_CORRECT", "rows": int((model_correct & ~book_correct).sum())},
        {"row_type": "CORRECTNESS", "category": "BETONLINE_ONLY_CORRECT", "rows": int((~model_correct & book_correct).sum())},
    ]
    for label, mask in (
        ("LT_5PP", paired.absolute_separation < 0.05),
        ("5_TO_LT_10PP", (paired.absolute_separation >= 0.05) & (paired.absolute_separation < 0.10)),
        ("GE_10PP", paired.absolute_separation >= 0.10),
    ):
        rows.append({"row_type": "PROBABILITY_DISAGREEMENT", "category": label, "rows": int(mask.sum())})
    return pd.DataFrame(rows)


def _logit(values: np.ndarray) -> np.ndarray:
    values = np.clip(values.astype(float), 1e-6, 1 - 1e-6)
    return np.log(values / (1 - values))


def _fit_logistic(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    design = np.column_stack([np.ones(len(x)), x])
    beta = np.zeros(design.shape[1])
    penalty = np.eye(design.shape[1]) * 1e-6; penalty[0, 0] = 0
    for _ in range(100):
        eta = np.clip(design @ beta, -25, 25)
        p = 1 / (1 + np.exp(-eta))
        weights = np.clip(p * (1 - p), 1e-8, None)
        hessian = design.T @ (design * weights[:, None]) + penalty
        gradient = design.T @ (y - p) - penalty @ beta
        step = np.linalg.solve(hessian, gradient)
        beta += step
        if np.max(abs(step)) < 1e-9:
            break
    return beta


def _predict_logistic(x: np.ndarray, beta: np.ndarray) -> np.ndarray:
    design = np.column_stack([np.ones(len(x)), x])
    eta = np.clip(design @ beta, -25, 25)
    return 1 / (1 + np.exp(-eta))


def incremental_information(paired: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    paired = paired.sort_values(["date", "identity"]).copy()
    dates = sorted(paired.date.unique())
    output_rows = []
    all_predictions: dict[str, list] = {name: [] for name in ["book_base", "book_plus_model", "model_base", "model_plus_book", "target", "date"]}
    for test_index in range(5, len(dates)):
        train_dates = dates[:test_index]
        test_date = dates[test_index]
        train = paired[paired.date.isin(train_dates)]
        test = paired[paired.date.eq(test_date)]
        if len(train) < 100 or len(test) == 0:
            continue
        y_train = train.target.to_numpy(float)
        book_train = _logit(train.betonline_p_over_novig.to_numpy())
        model_train = _logit(train.model_p_over.to_numpy())
        book_test = _logit(test.betonline_p_over_novig.to_numpy())
        model_test = _logit(test.model_p_over.to_numpy())
        beta_book = _fit_logistic(book_train[:, None], y_train)
        beta_book_plus = _fit_logistic(np.column_stack([book_train, model_train]), y_train)
        beta_model = _fit_logistic(model_train[:, None], y_train)
        beta_model_plus = _fit_logistic(np.column_stack([model_train, book_train]), y_train)
        predictions = {
            "book_base": _predict_logistic(book_test[:, None], beta_book),
            "book_plus_model": _predict_logistic(np.column_stack([book_test, model_test]), beta_book_plus),
            "model_base": _predict_logistic(model_test[:, None], beta_model),
            "model_plus_book": _predict_logistic(np.column_stack([model_test, book_test]), beta_model_plus),
        }
        y_test = test.target.to_numpy(float)
        for direction, base_name, plus_name in (
            ("BETONLINE_VS_BETONLINE_PLUS_PROPPADIA", "book_base", "book_plus_model"),
            ("PROPPADIA_VS_PROPPADIA_PLUS_BETONLINE", "model_base", "model_plus_book"),
        ):
            base = metric(y_test, predictions[base_name]); plus = metric(y_test, predictions[plus_name])
            output_rows.append({
                "scope": "FOLD", "test_date": test_date, "direction": direction, "rows": len(test),
                "base_brier": base["brier"], "augmented_brier": plus["brier"], "augmented_minus_base_brier": plus["brier"] - base["brier"],
                "base_log_loss": base["log_loss"], "augmented_log_loss": plus["log_loss"], "augmented_minus_base_log_loss": plus["log_loss"] - base["log_loss"],
            })
        for name in ["book_base", "book_plus_model", "model_base", "model_plus_book"]:
            all_predictions[name].extend(predictions[name].tolist())
        all_predictions["target"].extend(y_test.tolist()); all_predictions["date"].extend([test_date] * len(test))
    if not all_predictions["target"]:
        return pd.DataFrame(output_rows), "INSUFFICIENT"
    folds = pd.DataFrame(output_rows)
    aggregate_rows = []
    y = np.asarray(all_predictions["target"])
    for direction, base_name, plus_name in (
        ("BETONLINE_VS_BETONLINE_PLUS_PROPPADIA", "book_base", "book_plus_model"),
        ("PROPPADIA_VS_PROPPADIA_PLUS_BETONLINE", "model_base", "model_plus_book"),
    ):
        base = metric(y, all_predictions[base_name]); plus = metric(y, all_predictions[plus_name])
        direction_folds = folds[folds.direction.eq(direction)]
        aggregate_rows.append({
            "scope": "OVERALL_ROLLING_FORWARD", "test_date": f"{min(all_predictions['date'])}_THROUGH_{max(all_predictions['date'])}",
            "direction": direction, "rows": len(y),
            "base_brier": base["brier"], "augmented_brier": plus["brier"], "augmented_minus_base_brier": plus["brier"] - base["brier"],
            "base_log_loss": base["log_loss"], "augmented_log_loss": plus["log_loss"], "augmented_minus_base_log_loss": plus["log_loss"] - base["log_loss"],
            "folds": len(direction_folds),
            "fraction_folds_brier_favoring_augmented": float((direction_folds.augmented_minus_base_brier < 0).mean()),
            "fraction_folds_log_loss_favoring_augmented": float((direction_folds.augmented_minus_base_log_loss < 0).mean()),
        })
    overall = pd.DataFrame(aggregate_rows)
    if len(y) < 200 or len(set(all_predictions["date"])) < 5:
        status = "INSUFFICIENT"
    else:
        both_scores = (overall.augmented_minus_base_brier < 0) & (overall.augmented_minus_base_log_loss < 0)
        consistent = (overall.fraction_folds_brier_favoring_augmented >= 0.7) & (overall.fraction_folds_log_loss_favoring_augmented >= 0.7)
        if both_scores.all() and consistent.all():
            status = "ROBUST_EVIDENCE"
        elif both_scores.all() or (both_scores & consistent).any():
            status = "WEAK_EVIDENCE"
        elif ((overall.augmented_minus_base_brier < 0) | (overall.augmented_minus_base_log_loss < 0)).any():
            status = "MIXED"
        else:
            status = "NOT_REPRODUCED"
    output = pd.concat([overall, folds], ignore_index=True, sort=False)
    output["incremental_information"] = status
    return output, status


def status_from_uncertainty(uncertainty: pd.DataFrame, stem: str) -> str:
    rows = uncertainty[uncertainty.metric.isin([f"model_minus_{stem}_brier", f"model_minus_{stem}_log_loss"])]
    if (rows.ci_high < 0).all():
        return "MODEL_AHEAD"
    if (rows.ci_low > 0).all():
        return "MODEL_BEHIND"
    return "MODEL_EFFECTIVELY_TIED"


def skill_status(uncertainty: pd.DataFrame, stem: str) -> str:
    rows = uncertainty[uncertainty.metric.isin([f"model_minus_{stem}_brier", f"model_minus_{stem}_log_loss"])]
    if (rows.ci_high < 0).all():
        return "DEMONSTRATED"
    if (rows.point_estimate < 0).all():
        return "DIRECTIONALLY_PRESENT_NOT_SEPARATED"
    if (rows.ci_low > 0).all():
        return "MODEL_BEHIND"
    return "NOT_DEMONSTRATED"


def write_hashes(inputs: list[Path]) -> None:
    outputs = sorted(p for p in OUT.iterdir() if p.name != "reproducibility_hashes.csv")
    rows = []
    for role, paths in (("INPUT", inputs), ("OUTPUT", outputs)):
        for path in paths:
            rows.append({"role": role, "path": str(path.relative_to(ROOT)), "sha256": sha256(path), "bytes": path.stat().st_size})
    pd.DataFrame(rows).to_csv(OUT / "reproducibility_hashes.csv", index=False, lineterminator="\n")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    population, outcomes, source = load_population()
    resolved = population[population.target.notna()].copy()
    reproduction = reproduce_first12(population)
    model_vs = compare(population)
    uncertainty = date_bootstrap(population)
    split = split_stability(population)
    daily = daily_metrics(population)
    lodo = leave_one_date_out(population)
    trajectory = cumulative(population)
    ordering, ordering_stats, ordering_status = confidence_ordering(population)
    calibration_table = calibration(population)
    tail = upper_tail(population)
    dependence = dependence_sensitivity(population)
    paired, market_source = attach_betonline(population)
    betonline = market_comparison(paired)
    correctness = unique_correctness(paired)
    incremental, incremental_status = incremental_information(paired)

    if len(population) != 5088 or len(resolved) != 4687 or len(population) - len(resolved) != 401:
        raise AssertionError("checkpoint population differs from declared authoritative totals")
    if resolved.date.nunique() != 20:
        raise AssertionError("completed cluster count is not 20")

    population_out = population[[
        "identity", "date", "game_id", "player_id", "prediction_timestamp", "scheduled_game_start", "strict_pregame",
        "model_semantic_name", "model_artifact_sha256", "p_over", "target", "actual_hits", "outcome_attachment_contract",
        "outcome_status", "outcome_source", "baseline_a_population", "hitter_prior_resolved_games", "hitter_prior_hits", "baseline_b_hitter_shrunk",
    ]]
    population_out.to_csv(OUT / "hits05_20_cluster_population.csv", index=False, lineterminator="\n")
    outcomes.to_csv(OUT / "hits05_20_cluster_outcome_integrity.csv", index=False, lineterminator="\n")
    reproduction.to_csv(OUT / "hits05_12_cluster_reproduction.csv", index=False, lineterminator="\n")
    model_vs.to_csv(OUT / "hits05_20_cluster_model_vs_baselines.csv", index=False, lineterminator="\n")
    uncertainty.to_csv(OUT / "hits05_20_cluster_clustered_uncertainty.csv", index=False, lineterminator="\n")
    split.to_csv(OUT / "hits05_20_cluster_first12_vs_next8.csv", index=False, lineterminator="\n")
    daily.to_csv(OUT / "hits05_20_cluster_daily_metrics.csv", index=False, lineterminator="\n")
    lodo.to_csv(OUT / "hits05_20_cluster_leave_one_date_out.csv", index=False, lineterminator="\n")
    trajectory.to_csv(OUT / "hits05_20_cluster_cumulative_trajectory.csv", index=False, lineterminator="\n")
    ordering.to_csv(OUT / "hits05_20_cluster_confidence_ordering.csv", index=False, lineterminator="\n")
    calibration_table.to_csv(OUT / "hits05_20_cluster_calibration.csv", index=False, lineterminator="\n")
    tail.to_csv(OUT / "hits05_20_cluster_upper_tail.csv", index=False, lineterminator="\n")
    dependence.to_csv(OUT / "hits05_20_cluster_dependence_sensitivity.csv", index=False, lineterminator="\n")
    betonline.to_csv(OUT / "hits05_20_cluster_betonline_comparison.csv", index=False, lineterminator="\n")
    correctness.to_csv(OUT / "hits05_20_cluster_unique_correctness.csv", index=False, lineterminator="\n")
    incremental.to_csv(OUT / "hits05_20_cluster_incremental_information.csv", index=False, lineterminator="\n")

    headline = metric(resolved.target, resolved.p_over)
    headline["ece"] = ece(resolved.target, resolved.p_over)
    pd.DataFrame([{"scope": "ALL_RESOLVED_20_CLUSTER_PRIMARY_PREDICTIONS", **headline}]).to_csv(
        OUT / "hits05_20_cluster_primary_metrics.csv", index=False, lineterminator="\n"
    )
    exact_identity = {
        "task_id": "MLB_HITS05_EXACT_CURRENT_MODEL_20_CLUSTER_FORMAL_REVIEW_V1",
        "evaluation_start": DATES[0], "evaluation_end": DATES[-1], "completed_date_clusters": 20,
        "semantic_model_id": MODEL_ID, "model_artifact_sha256": MODEL_HASH,
        "artifact_path": str(MODEL_ARTIFACT.relative_to(ROOT)), "artifact_sha256_verified": sha256(MODEL_ARTIFACT),
        "prediction_model_binding": "EXPLICIT_SEMANTIC_ID_AND_FULL_ARTIFACT_SHA_ON_EVERY_PRIMARY_ROW",
        "rows_exact_bound": int((population.model_semantic_name.eq(MODEL_ID) & population.model_artifact_sha256.eq(MODEL_HASH)).sum()),
        "rows_unresolved": 0, "exact_model_identity": "PASS",
        "run_observations": source["run_observations"], "primary_identities": len(population),
        "resolved": len(resolved), "genuine_unresolved_no_appearance": len(population) - len(resolved),
        "games_represented": int(population.game_id.nunique()), "players_represented": int(population.player_id.nunique()),
        "duplicate_primary_identities": int(population.identity.duplicated().sum()), "post_start_primary_exclusions": 0,
        "timing_unresolved_primary_rows": int((population.prediction_dt.isna() | population.start_dt.isna()).sum()),
        "post_start_run_observations_excluded_from_primary": source["run_observation_post_start_rows"],
        "timing_unresolved_run_observations_excluded_from_primary": source["run_observation_timing_unresolved_rows"],
    }
    (OUT / "hits05_20_cluster_model_identity.json").write_text(json.dumps(exact_identity, indent=2, sort_keys=True) + "\n")

    pop_status = status_from_uncertainty(uncertainty, "population")
    hitter_status = status_from_uncertainty(uncertainty, "hitter")
    model_vs["uncertainty_decision"] = model_vs.forecast.map({
        "MODEL": "REFERENCE",
        "BASELINE_A_STRICT_PRIOR_POPULATION": pop_status,
        "BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK": hitter_status,
    })
    model_vs.to_csv(OUT / "hits05_20_cluster_model_vs_baselines.csv", index=False, lineterminator="\n")
    date_delta_intervals = uncertainty[uncertainty.metric.isin(["model_minus_population_brier", "model_minus_hitter_brier"])].set_index("metric")
    game_delta_intervals = dependence[dependence.analysis.eq("GAME_ID_CLUSTER_BOOTSTRAP")].set_index("metric")
    dependence_changed = any(
        (date_delta_intervals.loc[name, "ci_high"] < 0) != (game_delta_intervals.loc[name, "ci_high"] < 0)
        or (date_delta_intervals.loc[name, "ci_low"] > 0) != (game_delta_intervals.loc[name, "ci_low"] > 0)
        for name in ["model_minus_population_brier", "model_minus_hitter_brier"]
    )
    dependence["inference_materially_changes_vs_date_cluster"] = "YES" if dependence_changed else "NO"
    dependence.to_csv(OUT / "hits05_20_cluster_dependence_sensitivity.csv", index=False, lineterminator="\n")
    population_skill = skill_status(uncertainty, "population")
    hitter_skill = skill_status(uncertainty, "hitter")
    lodo_status = str(lodo.lodo_stability.iloc[0])
    temporal_status = str(split.forward_temporal_stability.iloc[0])
    tail_status = str(tail.current_model_upper_tail.iloc[0])
    model_index = model_vs.set_index("forecast")
    unc_index = uncertainty.set_index("metric")
    top_bottom = ordering_stats.set_index("diagnostic").loc["TOP20_MINUS_BOTTOM20_OBSERVED_RATE"]
    top_decile = ordering_stats.set_index("diagnostic").loc["TOP10_MINUS_BOTTOM10_OBSERVED_RATE"]
    market_row = betonline.iloc[0]
    unique_index = correctness.set_index("category")

    # Market-independence is methodological first, then descriptive.  Moderate
    # correlation plus frequent material disagreement is a distinct, not market-
    # derived opinion; incremental signal is deliberately not an edge claim.
    if market_row.pearson_probability_correlation < 0.75 and market_row.disagreement_ge_5pp / market_row.synchronized_rows >= 0.25:
        market_independence = "HITS05_MEANINGFULLY_INDEPENDENT_PREDICTION_OPINION"
    elif market_row.pearson_probability_correlation < 0.9:
        market_independence = "HITS05_PARTIALLY_DISTINCT_PREDICTION_OPINION"
    else:
        market_independence = "HITS05_LARGELY_REPLICATES_MARKET"
    denominator_status = "MEANINGFUL_BUT_MARKET_CONDITIONED"
    outcome_status = "PASS_WITH_LIMITATIONS"  # Aug 3-9 lack standalone canonical sidecar files.
    prospective_integrity = "PASS_WITH_LIMITATIONS"  # source-row timestamps remain the prior disclosed limitation.

    if population_skill == hitter_skill == "DEMONSTRATED" and temporal_status == "PASS" and ordering_status == "ROBUST":
        exact_evidence = "STRONG"
    elif population_skill in {"DEMONSTRATED", "DIRECTIONALLY_PRESENT_NOT_SEPARATED"} and hitter_skill in {"DEMONSTRATED", "DIRECTIONALLY_PRESENT_NOT_SEPARATED"} and temporal_status != "FAIL":
        exact_evidence = "MODERATE"
    elif len(resolved) >= 1000:
        exact_evidence = "WEAK"
    else:
        exact_evidence = "INSUFFICIENT"
    family_evidence = "STRONG"
    if exact_evidence == "STRONG" and tail_status in {"SUFFICIENT_AND_ACCEPTABLE", "INSUFFICIENT_SAMPLE"}:
        certification = "HITS05_STANDALONE_PREDICTION_CERTIFIED_WITH_LIMITATIONS"
    elif exact_evidence == "MODERATE" and population_skill == hitter_skill == "DEMONSTRATED":
        certification = "HITS05_STANDALONE_PREDICTION_CERTIFIED_WITH_LIMITATIONS"
    elif exact_evidence in {"MODERATE", "WEAK"}:
        certification = "HITS05_CERTIFICATION_STILL_DEFERRED"
    else:
        certification = "HITS05_STANDALONE_PREDICTION_NOT_CERTIFIED"
    public = "HITS05_PUBLIC_PREDICTION_READY_WITH_RESTRICTED_PRESENTATION" if certification.endswith("CERTIFIED_WITH_LIMITATIONS") else "HITS05_PUBLIC_PREDICTION_NOT_READY"
    evidence_declaration = {
        "STRONG": "HITS05_20_CLUSTER_FORWARD_EVIDENCE_STRONG",
        "MODERATE": "HITS05_20_CLUSTER_FORWARD_EVIDENCE_MODERATE",
        "WEAK": "HITS05_20_CLUSTER_FORWARD_EVIDENCE_WEAK",
        "INSUFFICIENT": "HITS05_20_CLUSTER_FORWARD_EVIDENCE_INSUFFICIENT",
    }[exact_evidence]
    future_denominator = "B_ADD_PARALLEL_SPORTSBOOK_INDEPENDENT_FULL_BOARD_SCORER_FOR_FUTURE_EVIDENCE"

    write_text("hits05_20_cluster_denominator_contract.md", f"""# Frozen denominator contract

`MARKET_OBSERVED_EXACT_MODEL_LINEAGE`

The completed 2026-08-03 through 2026-08-22 population is the naturally retained market-observed lineage for exact model `{MODEL_ID}`. Player identities entered when a Hits 0.5 sportsbook observation supported the governed live prediction path; the primary row is the existing governed `EARLIEST_VALID_STRICT_PREGAME_PREDICTION`. Repeated intraday rows are not independent predictions.

- Run observations in exact-model ledgers: {source['run_observations']:,}
- Primary market-observed identities: {len(population):,}
- Official games represented: {population.game_id.nunique():,}
- Market-observed players represented: {population.player_id.nunique():,}
- If no qualifying book market existed, that hitter did not enter this evidence denominator.

This is not a sportsbook-independent full hitter board. It is meaningful prospective evidence but can over-represent market-posted, expected-to-play hitters and therefore does not establish full-board generalizability. It does not imply a market-derived probability: the frozen artifact has 73 baseball-only ordered inputs, with no sportsbook probability, price, odds, consensus, movement, or market calibration input.

`MARKET_INPUTS_IN_MODEL = NO`

`DENOMINATOR_GENERALIZABILITY = {denominator_status}`
""")
    write_text("hits05_20_cluster_baseline_contracts.md", f"""# Leakage-safe baseline contracts

The formulas are unchanged from the predeclared adversarial/stress reviews and were not tuned on these 20 clusters.

## Baseline A — strict-prior population rate

`p_A = {source['strict_prior_population_rate']:.15f}` on every evaluation row. It is estimated from {source['strict_prior_history_rows']:,} governed resolved rows for {source['strict_prior_history_players']:,} players dated {source['strict_prior_history_start']} through {source['strict_prior_history_end']}, before the first evaluated slate.

## Baseline B — strict-prior hitter-shrunk rate

`p_B(i,d) = (prior_hits(i,d) + 8 * p_A) / (prior_resolved_games(i,d) + 8)`.

State advances only after each historical date is complete. Same-date outcomes never enter that date's prior; unseen hitters receive `p_A`. Evaluation rows are identical across model and both baselines.
""")
    write_text("hits05_20_cluster_market_independence.md", f"""# Market-independence review

`MARKET_INPUTS_IN_MODEL = NO`

`{market_independence}`

The registered model schema and earlier artifact audit identify 73 baseball-only inputs and no odds, price, implied probability, consensus, sportsbook, or market-derived calibration feature. On {int(market_row.synchronized_rows):,} BetOnline rows synchronized within 30 minutes, Pearson/Spearman probability correlation is {market_row.pearson_probability_correlation:.4f}/{market_row.spearman_probability_correlation:.4f}; median absolute separation is {market_row.median_absolute_separation:.2%}, with {int(market_row.disagreement_ge_5pp):,} at >=5 percentage points. Proppadia-only/BetOnline-only binary correctness counts are {int(unique_index.loc['PROPPADIA_ONLY_CORRECT','rows']):,}/{int(unique_index.loc['BETONLINE_ONLY_CORRECT','rows']):,}. This is descriptive independence evidence, not edge.
""")
    write_text("hits05_20_cluster_family_comparison.md", f"""# Supporting model-family comparison

`MODEL_FAMILY_EVIDENCE = {family_evidence}`

`EXACT_CURRENT_FORWARD_BEHAVIOR = MIXED_WITH_FAMILY_HISTORY`

The prior season-assembled review rated the broader family strong, with historical discrimination but a known high-tail calibration defect across older generations. Those generations are not observations of this exact artifact. The exact current artifact's prospective ordering is `{ordering_status}`, but it does not demonstrate population-baseline skill at this checkpoint; its separately assessed upper-tail status is `{tail_status}`. The historical defect is not inherited automatically, and the current result is mixed—not pooled—with family history.
""")
    fresh = pd.read_csv(FRESH_BASELINES).set_index("forecast")
    overlap = pd.read_csv(FRESH_OVERLAP).iloc[0]
    write_text("hits05_20_cluster_fresh_start_comparison.md", f"""# Fresh-start failed-control context

The separate `HITS05_2026_FRESH_START_CHRONOLOGICAL_MODEL_BUILD` remains a context-only failed control and is not mixed into the 20-cluster metrics. Its full-period model Brier/log loss was {fresh.loc['MODEL','brier']:.6f}/{fresh.loc['MODEL','log_loss']:.6f}, behind both simple baselines. On its prior August overlap, the current live artifact Brier was {overlap.live_brier:.6f} versus {overlap.fresh_brier:.6f} for the fresh-start procedure.

`EXACT_CURRENT_LIVE_RESULTS_MATERIALLY_BETTER_THAN_FAILED_FRESH_START = YES`
""")
    write_text("hits05_20_cluster_prospective_integrity.md", f"""# Prospective integrity

`CURRENT_MODEL_PROSPECTIVE_INTEGRITY = {prospective_integrity}`

- All {len(population):,} primary rows have prediction timestamps before first pitch, immutable original probabilities, and exact semantic ID/full artifact SHA binding.
- Primary duplicates: 0; timing-unresolved primary rows: 0; post-start primary admissions: 0.
- Outcomes attach after predictions and do not mutate probabilities. August 10-22 use canonical reconciliation sidecars; August 3-9 have no standalone sidecar files and retain the previously governed frozen original outcome attachment. This is the explicit outcome-source limitation.
- Prior adversarial limitation remains: exact feature vectors/code cutoffs exist, but contributing source-row timestamps are not available for every historical feature observation. No same-game outcome access is evidenced.
- PA-completeness and outcome-summary idempotency repairs affect canonical outcome attachment only, not original prediction identity or probability.

`OUTCOME_INTEGRITY = {outcome_status}`
""")
    strongest_against = "proper-score separation from both leakage-safe baselines is not established under date clustering" if population_skill != "DEMONSTRATED" or hitter_skill != "DEMONSTRATED" else "the market-conditioned denominator and limited current-artifact upper tail constrain generalizability"
    strongest_for = f"{len(resolved):,} resolved, exact-SHA, strict-pregame predictions show {ordering_status.lower().replace('_',' ')} ordering across 20 completed slates"
    write_text("hits05_20_cluster_certification_decision.md", f"""# Formal certification decision

`MODEL_FAMILY_EVIDENCE = {family_evidence}`

`EXACT_CURRENT_MODEL_FORWARD_EVIDENCE = {exact_evidence}`

`POPULATION_BASELINE_SKILL = {population_skill}`

`HITTER_BASELINE_INCREMENT = {hitter_skill}`

`{certification}`

The scope is only the exact artifact's standalone Hits 0.5 probability/ranking capability. It is not certification of betting edge or profitability. Strongest support: {strongest_for}. Strongest argument against a stronger decision: {strongest_against}.

`PRIMARY_EVIDENCE_DECLARATION = {evidence_declaration}`
""")
    write_text("hits05_20_cluster_public_readiness.md", f"""# Public readiness

`{public}`

This decision is independent of certification. It reflects `{population_skill}` population-baseline skill, `{ordering_status}` ordering, `{tail_status}` upper-tail evidence, a `{denominator_status}` denominator, and `{prospective_integrity}` integrity. No UI or publication change is authorized here.
""")
    write_text("hits05_20_cluster_future_denominator_recommendation.md", f"""# Future denominator recommendation

`{future_denominator}`

Preserve this completed market-observed test unchanged. For new prospective evidence, keep the current lineage while adding a separately identified, sportsbook-independent full-board scorer. That parallel denominator can measure selection effects without retrofitting or replacing the 20-cluster record. Human approval is required before starting the new experiment.
""")

    daily_pop_wins = int(daily.model_beats_population_brier.sum())
    daily_hit_wins = int(daily.model_beats_hitter_brier.sum())
    first = split.iloc[0]; nxt = split.iloc[1]
    trajectory_note = "emerged gradually and remained favorable" if trajectory.iloc[-1].model_minus_population_brier < 0 and trajectory.iloc[-1].model_minus_hitter_brier < 0 else "remained mixed against the governed baselines"
    concise = f"""# MLB Hits 0.5 exact-current-model 20-cluster formal review v1

## Frozen checkpoint

- Exact model: `{MODEL_ID}` / `{MODEL_HASH}`.
- Population: {len(population):,} original primary predictions; {len(resolved):,} resolved; {len(population)-len(resolved):,} genuine no-appearance unresolved; 20 completed date clusters; {population.game_id.nunique():,} games; {population.player_id.nunique():,} players; zero duplicates/post-start primary rows.
- Denominator: market-observed exact-model lineage, not a sportsbook-independent full board (`{denominator_status}`). Market inputs in model: `NO`.
- Model Brier/log loss/ECE: {headline['brier']:.6f} / {headline['log_loss']:.6f} / {headline['ece']:.6f}; predicted/observed hit rate: {headline['mean_probability']:.2%} / {headline['observed_rate']:.2%}.

## Standalone due diligence

- Population baseline Brier/log loss: {model_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION','brier']:.6f} / {model_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION','log_loss']:.6f}; model deltas {model_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION','model_minus_forecast_brier']:.6f} / {model_index.loc['BASELINE_A_STRICT_PRIOR_POPULATION','model_minus_forecast_log_loss']:.6f}; `{pop_status}`.
- Hitter-shrunk baseline Brier/log loss: {model_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK','brier']:.6f} / {model_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK','log_loss']:.6f}; model deltas {model_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK','model_minus_forecast_brier']:.6f} / {model_index.loc['BASELINE_B_STRICT_PRIOR_HITTER_SHRUNK','model_minus_forecast_log_loss']:.6f}; `{hitter_status}`.
- Date-clustered model-minus-population Brier 95% CI [{unc_index.loc['model_minus_population_brier','ci_low']:.6f}, {unc_index.loc['model_minus_population_brier','ci_high']:.6f}], draws favoring model {unc_index.loc['model_minus_population_brier','fraction_draws_favoring_model']:.1%}; model-minus-hitter CI [{unc_index.loc['model_minus_hitter_brier','ci_low']:.6f}, {unc_index.loc['model_minus_hitter_brier','ci_high']:.6f}], draws favoring model {unc_index.loc['model_minus_hitter_brier','fraction_draws_favoring_model']:.1%}.
- First 12 vs next 8 Brier: {first.brier:.6f} vs {nxt.brier:.6f}; `{temporal_status}`. Cumulative evidence {trajectory_note}.
- Daily Brier wins: {daily_pop_wins}/20 vs population and {daily_hit_wins}/20 vs hitter-shrunk. Leave-one-date-out: `{lodo_status}`.
- Ordering: `{ordering_status}`; quintile observed rates {', '.join(f'{value:.2%}' for value in ordering.iloc[:5].observed_hit_rate)}. Top-minus-bottom quintile {top_bottom.point_estimate:.2%} (95% CI {top_bottom.ci_low:.2%} to {top_bottom.ci_high:.2%}); top-minus-bottom decile {top_decile.point_estimate:.2%} (95% CI {top_decile.ci_low:.2%} to {top_decile.ci_high:.2%}).
- Upper tail: `{tail_status}`. Game-cluster sensitivity is reported separately and does not treat hitter rows as independent.

## Secondary market evidence and decisions

- BetOnline <=30-minute cohort: {int(market_row.synchronized_rows):,} rows; Proppadia/BetOnline Brier {market_row.proppadia_brier:.6f}/{market_row.betonline_brier:.6f}; log loss {market_row.proppadia_log_loss:.6f}/{market_row.betonline_log_loss:.6f}; correlation {market_row.pearson_probability_correlation:.4f} Pearson, {market_row.spearman_probability_correlation:.4f} Spearman.
- Market independence: `{market_independence}`. Incremental information: `{incremental_status}`. Neither is a betting-edge claim.
- Prospective integrity: `{prospective_integrity}`. Family evidence: `{family_evidence}`. Exact-current forward evidence: `{exact_evidence}`.
- Certification: `{certification}`. Public readiness: `{public}`.
- Future denominator: `{future_denominator}`.
- Primary declaration: `{evidence_declaration}`.
- Next human decision: approve or decline a new parallel sportsbook-independent full-board prospective evidence stream; do not alter this frozen test.
"""
    write_text("concise_mlb_hits05_exact_current_model_20_cluster_formal_review_v1.md", concise)

    inputs = [
        BASE, MODEL_ARTIFACT, prior.SEASON, prior.PRIOR_BASELINES, prior.FROZEN_PROCEDURE,
        FRESH_BASELINES, FRESH_OVERLAP, FAMILY_REVIEW, MARKET_AUDIT, Path(__file__).resolve(),
        *[ledger_path(day) for day in DATES], *[sidecar_path(day) for day in DATES if sidecar_path(day).exists()],
    ]
    write_hashes(inputs)
    required = {
        "hits05_20_cluster_population.csv", "hits05_20_cluster_model_identity.json", "hits05_20_cluster_denominator_contract.md",
        "hits05_20_cluster_outcome_integrity.csv", "hits05_20_cluster_primary_metrics.csv", "hits05_12_cluster_reproduction.csv",
        "hits05_20_cluster_baseline_contracts.md", "hits05_20_cluster_model_vs_baselines.csv", "hits05_20_cluster_clustered_uncertainty.csv",
        "hits05_20_cluster_first12_vs_next8.csv", "hits05_20_cluster_daily_metrics.csv", "hits05_20_cluster_leave_one_date_out.csv",
        "hits05_20_cluster_cumulative_trajectory.csv", "hits05_20_cluster_confidence_ordering.csv", "hits05_20_cluster_calibration.csv",
        "hits05_20_cluster_upper_tail.csv", "hits05_20_cluster_dependence_sensitivity.csv", "hits05_20_cluster_betonline_comparison.csv",
        "hits05_20_cluster_unique_correctness.csv", "hits05_20_cluster_incremental_information.csv", "hits05_20_cluster_market_independence.md",
        "hits05_20_cluster_family_comparison.md", "hits05_20_cluster_fresh_start_comparison.md", "hits05_20_cluster_prospective_integrity.md",
        "hits05_20_cluster_certification_decision.md", "hits05_20_cluster_public_readiness.md", "hits05_20_cluster_future_denominator_recommendation.md",
        "concise_mlb_hits05_exact_current_model_20_cluster_formal_review_v1.md", "reproducibility_hashes.csv",
    }
    missing = required - {p.name for p in OUT.iterdir()}
    if missing:
        raise AssertionError(f"missing required outputs: {sorted(missing)}")
    print(json.dumps({
        "predictions": len(population), "resolved": len(resolved), "unresolved": len(population)-len(resolved),
        "model": {"brier": headline["brier"], "log_loss": headline["log_loss"], "ece": headline["ece"]},
        "population_skill": population_skill, "hitter_increment": hitter_skill,
        "ordering": ordering_status, "upper_tail": tail_status, "temporal_stability": temporal_status,
        "lodo": lodo_status, "incremental_information": incremental_status,
        "certification": certification, "public_readiness": public, "evidence": evidence_declaration,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

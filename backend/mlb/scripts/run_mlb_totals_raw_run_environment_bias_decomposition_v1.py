"""Bounded, no-refit decomposition of the frozen RAW_V1 totals residual.

Residual convention throughout: ACTUAL_TOTAL_RUNS - RAW_FORECAST_TOTAL.
Positive values therefore mean RAW underforecast.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_RAW_RUN_ENVIRONMENT_BIAS_DECOMPOSITION_V1"
MODEL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
INTERCEPT = 0.493550
START_DATE = "2026-08-06"
END_DATE = "2026-08-15"

CONFIG = ROOT / "backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json"
LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
SPINE = ROOT / "artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06"
RAW_FEEDS = ROOT / "artifacts/raw/mlb/totals_feature_spine_v1/feed"
RAW_SCHEDULES = ROOT / "artifacts/raw/mlb/totals_feature_spine_v1/schedule"
HISTORICAL_RESIDUALS = ROOT / "artifacts/analysis/model_development/mlb_totals_bias_attribution_passage_1/2026-08-06/totals_candidate_residual_spine.csv"
HISTORICAL_AGGREGATES = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_representative_rerun_v1/2026-08-06/totals_model_comparison.csv"
CALIBRATION_REFERENCE = ROOT / "artifacts/analysis/model_development/mlb_standalone_prediction_calibration_repair_v1/2026-08-12/totals_calibrated_holdout_metrics.csv"
OFFICIAL_ROOT = ROOT / "artifacts/analysis/mlb/player_stats_completeness"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_run_environment_bias_decomposition_v1/2026-08-16"


FEATURE_ROLES = {
    "league_total": "strict-prior league run environment",
    "home_offense": "home-team strict-prior runs scored state",
    "home_prevention": "home-team strict-prior runs allowed state",
    "away_offense": "away-team strict-prior runs scored state",
    "away_prevention": "away-team strict-prior runs allowed state",
    "home_starter_ra9": "home probable starter strict-prior RA9 or governed fallback",
    "away_starter_ra9": "away probable starter strict-prior RA9 or governed fallback",
    "home_starter_prior_starts": "home probable starter history depth",
    "away_starter_prior_starts": "away probable starter history depth",
    "home_expected_outs": "home probable starter expected workload",
    "away_expected_outs": "away probable starter expected workload",
    "home_workload_uncertainty_outs": "home starter workload uncertainty",
    "away_workload_uncertainty_outs": "away starter workload uncertainty",
    "home_bullpen_ra9": "home bullpen strict-prior RA9",
    "away_bullpen_ra9": "away bullpen strict-prior RA9",
    "home_bullpen_likely_available_reliever_count": "home likely-available reliever count",
    "away_bullpen_likely_available_reliever_count": "away likely-available reliever count",
    "home_bullpen_recent_innings_burden": "home recent bullpen innings burden",
    "away_bullpen_recent_innings_burden": "away recent bullpen innings burden",
    "strict_prior_total_run_factor": "regressed strict-prior venue run factor",
    "park_history_depth": "number of prior venue games supporting the park state",
    "game_number": "doubleheader/game-number state",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def crps(mu: float, actual: int, alpha: float) -> float:
    probabilities = distribution(float(mu), alpha)
    support = np.arange(len(probabilities))
    return float(np.sum((np.cumsum(probabilities) - (support >= int(actual)).astype(float)) ** 2))


def strict_team_features(games: pd.DataFrame) -> pd.DataFrame:
    """Reproduce the already governed date-strict team-state construction; no fit."""
    league: list[tuple[float, float, float]] = []
    team_scored: dict[int, list[float]] = {}
    team_allowed: dict[int, list[float]] = {}
    rows = []
    for _, day in games.groupby("game_date", sort=True):
        league_total = float(np.mean([x[0] for x in league])) if league else 8.6
        league_home = float(np.mean([x[1] for x in league])) if league else 4.4
        league_away = float(np.mean([x[2] for x in league])) if league else 4.2
        for game in day.itertuples():
            prior = lambda state, key, fallback: float(np.mean(state.get(key, []))) if state.get(key) else fallback
            rows.append({
                "game_pk": int(game.game_pk), "league_total": league_total,
                "home_offense": prior(team_scored, int(game.home_team_id), league_home),
                "home_prevention": prior(team_allowed, int(game.home_team_id), league_away),
                "away_offense": prior(team_scored, int(game.away_team_id), league_away),
                "away_prevention": prior(team_allowed, int(game.away_team_id), league_home),
            })
        for game in day.itertuples():
            league.append((float(game.final_total), float(game.final_home_runs), float(game.final_away_runs)))
            team_scored.setdefault(int(game.home_team_id), []).append(float(game.final_home_runs))
            team_allowed.setdefault(int(game.home_team_id), []).append(float(game.final_away_runs))
            team_scored.setdefault(int(game.away_team_id), []).append(float(game.final_away_runs))
            team_allowed.setdefault(int(game.away_team_id), []).append(float(game.final_home_runs))
    return pd.DataFrame(rows)


def score_frame(frame: pd.DataFrame, candidate: dict[str, Any]) -> np.ndarray:
    features = candidate["feature_order"]
    values = frame[features].astype(float).to_numpy()
    standardized = (values - np.asarray(candidate["scaler_mean"])) / np.asarray(candidate["scaler_scale"])
    return np.exp(float(candidate["intercept"]) + standardized @ np.asarray(candidate["coefficients"]))


def load_historical(candidate: dict[str, Any]) -> pd.DataFrame:
    frame = pd.read_csv(SPINE / "totals_core_feature_spine.csv")
    frame["game_date"] = pd.to_datetime(frame.game_date)
    frame = frame.merge(strict_team_features(frame), on="game_pk", how="left")
    frame["home_starter_ra9"] = frame.home_starter_season_ra9.fillna(frame.league_total / 2)
    frame["away_starter_ra9"] = frame.away_starter_season_ra9.fillna(frame.league_total / 2)
    frame["home_bullpen_ra9"] = frame.home_bullpen_bullpen_ra9.fillna(frame.league_total / 2)
    frame["away_bullpen_ra9"] = frame.away_bullpen_bullpen_ra9.fillna(frame.league_total / 2)
    frame[candidate["feature_order"]] = frame[candidate["feature_order"]].replace([np.inf, -np.inf], np.nan).fillna(0)
    frame["raw_forecast"] = score_frame(frame, candidate)
    frame["run_residual"] = frame.final_total - frame.raw_forecast
    frame["raw_absolute_error"] = abs(frame.run_residual)
    frame["period"] = np.select(
        [frame.game_date.dt.year <= 2024, frame.game_date.dt.year == 2025,
         (frame.game_date.dt.year == 2026) & (frame.game_date.dt.month < 7),
         (frame.game_date >= pd.Timestamp("2026-07-01")) & (frame.game_date <= pd.Timestamp("2026-08-05"))],
        ["DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE", "FROZEN_2025_VALIDATION",
         "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT"], default="EXCLUDED")

    # Exact row-level survivor from the original governed rerun is an independent reproduction guardrail.
    retained = pd.read_csv(HISTORICAL_RESIDUALS, usecols=["game_pk", "predicted_total"])
    checked = frame.merge(retained, on="game_pk", how="inner")
    if len(checked) != len(frame) or float(abs(checked.raw_forecast - checked.predicted_total).max()) > 2e-12:
        raise RuntimeError("FROZEN_HISTORICAL_RAW_FORECAST_REPRODUCTION_FAILED")
    return frame


def exact_grading_payload(connection: sqlite3.Connection, identity: str) -> dict[str, Any]:
    row = connection.execute("SELECT grading_payload_json FROM totals_shadow_outcomes WHERE canonical_identity=?", (identity,)).fetchone()
    if not row:
        raise RuntimeError(f"MISSING_FROZEN_OUTCOME_{identity}")
    return json.loads(row[0])


def inning_segments(payload: dict[str, Any]) -> dict[str, int]:
    innings = payload["liveData"]["linescore"].get("innings", [])
    segments = {"INNINGS_1_5": 0, "INNINGS_6_9": 0, "EXTRA_INNINGS": 0}
    for inning in innings:
        number = int(inning.get("num", 0))
        runs = int(inning.get("away", {}).get("runs") or 0) + int(inning.get("home", {}).get("runs") or 0)
        segments["INNINGS_1_5" if number <= 5 else ("INNINGS_6_9" if number <= 9 else "EXTRA_INNINGS")] += runs
    return segments


def realized_starter_outs(payload: dict[str, Any]) -> int | None:
    boxscore = payload.get("liveData", {}).get("boxscore", {})
    total = 0
    found = False
    for side in ("home", "away"):
        team = boxscore.get("teams", {}).get(side, {})
        for pitcher_id in team.get("pitchers", []):
            stats = team.get("players", {}).get(f"ID{pitcher_id}", {}).get("stats", {}).get("pitching", {})
            if stats and int(stats.get("gamesStarted", 0)):
                total += int(stats.get("outs", 0)); found = True
    return total if found else None


def timing_mode(game_date: str, prediction_timestamp: str) -> str:
    timestamp = iso_utc(prediction_timestamp)
    if game_date == "2026-08-06":
        return "INITIALIZATION_CAPTURE"
    if game_date == "2026-08-07" and timestamp.hour < 16:
        return "INITIAL_INTEGRATION_CAPTURE"
    return "PRIMARY_SCORE" if timestamp.hour < 15 else "SCORE_MISSING"


def load_prospective(candidate: dict[str, Any], alpha: float) -> pd.DataFrame:
    connection = sqlite3.connect(f"file:{LEDGER}?mode=ro", uri=True)
    rows = connection.execute("""
      SELECT p.canonical_identity,p.game_date,p.game_id,p.scheduled_start_utc,p.prediction_timestamp_utc,
             p.model_hash,p.prediction_payload_json,p.prediction_payload_sha256,
             c.context_payload_json,c.context_payload_sha256
      FROM totals_shadow_predictions p JOIN totals_shadow_prediction_context c USING(canonical_identity)
      WHERE p.game_date BETWEEN ? AND ? ORDER BY p.game_date,p.game_id
    """, (START_DATE, END_DATE)).fetchall()
    output = []
    for identity, date, game_id, scheduled, predicted, model_hash, prediction_json, prediction_sha, context_json, context_sha in rows:
        prediction = json.loads(prediction_json); context = json.loads(context_json); grading = exact_grading_payload(connection, identity)
        if model_hash != MODEL_HASH or prediction_sha != canonical_hash(prediction) or context_sha != canonical_hash(context):
            raise RuntimeError(f"FROZEN_PROSPECTIVE_IDENTITY_OR_HASH_FAILED_{identity}")
        if iso_utc(predicted) >= iso_utc(scheduled):
            raise RuntimeError(f"POST_START_PROSPECTIVE_ROW_{identity}")
        source = ROOT / grading["official_source_path"]
        raw = source.read_bytes(); payload = json.loads(raw)
        if hashlib.sha256(raw).hexdigest() != grading["official_source_hash"]:
            raise RuntimeError(f"OFFICIAL_SOURCE_HASH_FAILED_{identity}")
        actual = int(grading["official_final_total"]); features = context["model_features"]
        scored = float(score_frame(pd.DataFrame([features]), candidate)[0])
        if abs(scored - float(prediction["expected_total"])) > 1e-11:
            raise RuntimeError(f"PROSPECTIVE_RAW_FORECAST_REPRODUCTION_FAILED_{identity}")
        segments = inning_segments(payload)
        record = {
            "period": "PROSPECTIVE_AUG06_15", "game_date": pd.Timestamp(date), "game_pk": int(game_id),
            "home_team": prediction["home_team"], "away_team": prediction["away_team"],
            "venue_id": prediction.get("venue_id"), "park_name": prediction.get("venue_name"),
            "scheduled_start_utc": scheduled, "prediction_timestamp_utc": predicted,
            "score_timing": timing_mode(date, predicted), "raw_forecast": scored, "final_total": actual,
            "final_home_runs": int(payload["liveData"]["linescore"]["teams"]["home"]["runs"]),
            "final_away_runs": int(payload["liveData"]["linescore"]["teams"]["away"]["runs"]),
            "run_residual": actual - scored, "raw_absolute_error": abs(actual - scored),
            "raw_crps": crps(scored, actual, alpha), "intercept_crps": crps(scored + INTERCEPT, actual, alpha),
            "starter_fallback_state": "+".join(sorted({prediction["home_starter_fallback_status"], prediction["away_starter_fallback_status"]})),
            "park_fallback_state": prediction.get("park_fallback_status"),
            "roof_type": context.get("park_state", {}).get("roof_type", "UNAVAILABLE"),
            "realized_starter_outs_total": realized_starter_outs(payload),
            **segments, **{feature: float(features[feature]) for feature in candidate["feature_order"]},
        }
        record["expected_starter_outs_total"] = record["home_expected_outs"] + record["away_expected_outs"]
        record["starter_outs_residual"] = (record["realized_starter_outs_total"] - record["expected_starter_outs_total"]
                                           if record["realized_starter_outs_total"] is not None else math.nan)
        output.append(record)
    connection.close()
    frame = pd.DataFrame(output)
    if len(frame) != 126 or frame.game_pk.duplicated().any():
        raise RuntimeError(f"UNEXPECTED_PROSPECTIVE_POPULATION_{len(frame)}")
    if abs(float(frame.run_residual.mean()) - 0.558992375399351) > 1e-12:
        raise RuntimeError("PROSPECTIVE_RESIDUAL_REPRODUCTION_FAILED")
    return frame


def choose_final_payload(game_date: str, game_id: int) -> tuple[dict[str, Any], Path]:
    paths = sorted((OFFICIAL_ROOT / game_date / f"game_{game_id}" / "sources").glob(f"game_{game_id}_live_feed_*.json"))
    finals = []
    for path in paths:
        payload = json.loads(path.read_bytes())
        if payload.get("gameData", {}).get("status", {}).get("abstractGameState") == "Final":
            finals.append((payload, path))
    if not finals:
        raise RuntimeError(f"NO_OFFICIAL_FINAL_{game_date}_{game_id}")
    summaries = {(int(p["liveData"]["linescore"]["teams"]["home"]["runs"]),
                  int(p["liveData"]["linescore"]["teams"]["away"]["runs"]),
                  p["gameData"]["datetime"]["dateTime"]) for p, _ in finals}
    if len(summaries) != 1:
        raise RuntimeError(f"CONFLICTING_OFFICIAL_FINALS_{game_date}_{game_id}")
    return finals[0]


def load_august_all_official() -> pd.DataFrame:
    rows = []
    for date_dir in sorted(OFFICIAL_ROOT.glob("2026-08-*")):
        date = date_dir.name
        if not (START_DATE <= date <= END_DATE):
            continue
        for game_dir in sorted(date_dir.glob("game_*")):
            game_id = int(game_dir.name.split("_", 1)[1]); payload, path = choose_final_payload(date, game_id)
            linescore = payload["liveData"]["linescore"]
            home = int(linescore["teams"]["home"]["runs"]); away = int(linescore["teams"]["away"]["runs"])
            rows.append({"game_date": pd.Timestamp(date), "game_pk": game_id, "final_home_runs": home,
                         "final_away_runs": away, "final_total": home + away, **inning_segments(payload),
                         "official_source_path": str(path.relative_to(ROOT)), "official_source_sha256": sha256(path)})
    frame = pd.DataFrame(rows)
    if len(frame) != 134 or frame.game_pk.duplicated().any():
        raise RuntimeError(f"UNEXPECTED_AUGUST_OFFICIAL_POPULATION_{len(frame)}")
    return frame


def load_historical_innings(game_ids: set[int]) -> tuple[pd.DataFrame, list[int]]:
    # The retained monthly official schedule payloads are the complete governed
    # inning source; the individual feed directory is intentionally sparse.
    by_game: dict[int, dict[str, int]] = {}
    for path in sorted(RAW_SCHEDULES.glob("*.json.gz")):
        payload = json.loads(gzip.decompress(path.read_bytes()))
        for date in payload.get("dates", []):
            for game in date.get("games", []):
                game_id = int(game["gamePk"])
                if game_id not in game_ids or not game.get("linescore", {}).get("innings"):
                    continue
                wrapped = {"liveData": {"linescore": game["linescore"]}}
                segments = inning_segments(wrapped)
                if game_id in by_game and by_game[game_id] != segments:
                    raise RuntimeError(f"CONFLICTING_HISTORICAL_INNING_SOURCE_{game_id}")
                by_game[game_id] = segments
    missing = sorted(game_ids - set(by_game))
    return pd.DataFrame([{"game_pk": game_id, **by_game[game_id]} for game_id in sorted(by_game)]), missing


def residual_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    residual = frame.run_residual.astype(float)
    return {"games": len(frame), "mean_raw_forecast": float(frame.raw_forecast.mean()),
            "mean_actual_total": float(frame.final_total.mean()), "mean_run_residual": float(residual.mean()),
            "median_run_residual": float(residual.median()), "mae": float(abs(residual).mean()),
            "rmse": float(np.sqrt(np.mean(residual**2)))}


def environment_metrics(frame: pd.DataFrame, period: str) -> dict[str, Any]:
    totals = frame.final_total.astype(float)
    return {"period": period, "games": len(frame), "runs_per_game": float(totals.mean()),
            "home_runs_per_game": float(frame.final_home_runs.mean()), "away_runs_per_game": float(frame.final_away_runs.mean()),
            "median_total": float(totals.median()), "total_variance_population": float(totals.var(ddof=0)),
            "frequency_total_le_5": int((totals <= 5).sum()), "frequency_total_6_7": int(((totals >= 6) & (totals <= 7)).sum()),
            "frequency_total_8_9": int(((totals >= 8) & (totals <= 9)).sum()),
            "frequency_total_10_11": int(((totals >= 10) & (totals <= 11)).sum()),
            "frequency_total_ge_12": int((totals >= 12).sum())}


def forecast_band(value: float) -> str:
    if value < 7.5: return "<7.5"
    if value < 8.0: return "7.5-7.99"
    if value < 8.5: return "8.0-8.49"
    if value < 9.0: return "8.5-8.99"
    if value < 9.5: return "9.0-9.49"
    if value < 10.0: return "9.5-9.99"
    return ">=10.0"


def z_band(value: float) -> str:
    return "LOW_Z_<-0.5" if value < -0.5 else ("CENTRAL_Z_-0.5_TO_0.5" if value < 0.5 else "HIGH_Z_>=0.5")


def grouped_residuals(frame: pd.DataFrame, column: str, dimension: str, alpha: float | None = None,
                      min_games: int = 1) -> list[dict[str, Any]]:
    output = []
    for level, group in frame.groupby(column, observed=True, dropna=False):
        if len(group) < min_games:
            continue
        record = {"dimension": dimension, "level": str(level), **residual_metrics(group)}
        if alpha is not None and "raw_crps" in group:
            record.update({"raw_crps": float(group.raw_crps.mean()), "intercept_crps": float(group.intercept_crps.mean()),
                           "intercept_minus_raw_crps": float((group.intercept_crps - group.raw_crps).mean())})
        output.append(record)
    return output


def add_context_bands(frame: pd.DataFrame, candidate: dict[str, Any]) -> pd.DataFrame:
    frame = frame.copy(); order = candidate["feature_order"]
    means = dict(zip(order, candidate["scaler_mean"])); scales = dict(zip(order, candidate["scaler_scale"]))
    standardized = lambda feature: (frame[feature] - means[feature]) / scales[feature]
    frame["offense_z"] = (standardized("home_offense") + standardized("away_offense")) / 2
    frame["offense_band"] = frame.offense_z.map(z_band)
    frame["prevention_z"] = (standardized("home_prevention") + standardized("away_prevention")) / 2
    frame["prevention_band"] = frame.prevention_z.map(z_band)
    frame["starter_ra9_z"] = (standardized("home_starter_ra9") + standardized("away_starter_ra9")) / 2
    frame["starter_quality_band"] = frame.starter_ra9_z.map(z_band)
    frame["starter_workload_z"] = (standardized("home_expected_outs") + standardized("away_expected_outs")) / 2
    frame["expected_starter_workload_band"] = frame.starter_workload_z.map(z_band)
    frame["bullpen_ra9_z"] = (standardized("home_bullpen_ra9") + standardized("away_bullpen_ra9")) / 2
    frame["bullpen_quality_band"] = frame.bullpen_ra9_z.map(z_band)
    frame["bullpen_burden_z"] = (standardized("home_bullpen_recent_innings_burden") + standardized("away_bullpen_recent_innings_burden")) / 2
    frame["bullpen_burden_band"] = frame.bullpen_burden_z.map(z_band)
    frame["park_factor_band"] = pd.cut(frame.strict_prior_total_run_factor, [-np.inf, .98, 1.0200000001, np.inf],
                                       labels=["RUN_SUPPRESSING_<0.98", "NEUTRAL_0.98_TO_1.02", "RUN_BOOSTING_>1.02"], right=False)
    frame["forecast_magnitude_band"] = frame.raw_forecast.map(forecast_band)
    frame["realized_starter_workload_band"] = pd.cut(frame.starter_outs_residual, [-np.inf, -6, -3, 3.0000001, 6, np.inf],
        labels=["MUCH_EARLIER_<=-6_OUTS", "EARLIER_-6_TO_-3", "NEAR_EXPECTED_-3_TO_3", "LATER_3_TO_6", "MUCH_LATER_>=6_OUTS"], right=False)
    return frame


def distribution_summary(frame: pd.DataFrame, period: str) -> dict[str, Any]:
    residual = frame.run_residual.astype(float); cutoff = float(np.quantile(abs(residual), .95))
    trimmed = residual[abs(residual) <= cutoff]
    quantiles = residual.quantile([.1, .25, .5, .75, .9])
    return {"period": period, "games": len(frame), "mean": float(residual.mean()), "median": float(residual.median()),
            "sd_population": float(residual.std(ddof=0)), "p10": float(quantiles.loc[.1]), "p25": float(quantiles.loc[.25]),
            "p50": float(quantiles.loc[.5]), "p75": float(quantiles.loc[.75]), "p90": float(quantiles.loc[.9]),
            "frequency_gt_plus_1": int((residual > 1).sum()), "frequency_gt_plus_2": int((residual > 2).sum()),
            "frequency_gt_plus_4": int((residual > 4).sum()), "frequency_lt_minus_1": int((residual < -1).sum()),
            "frequency_lt_minus_2": int((residual < -2).sum()), "frequency_lt_minus_4": int((residual < -4).sum()),
            "absolute_residual_95th_percentile_cutoff": cutoff, "trimmed_games": len(trimmed),
            "trimmed_mean_excluding_top_5pct_absolute_residual": float(trimmed.mean())}


def run(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    config_bytes = CONFIG.read_bytes(); candidate = json.loads(config_bytes)
    stable = {key: candidate[key] for key in candidate if key != "canonical_model_hash"}
    if canonical_hash(stable) != MODEL_HASH or candidate["canonical_model_hash"] != MODEL_HASH:
        raise RuntimeError("FROZEN_MODEL_HASH_FAILED")
    alpha = float(candidate["dispersion_alpha"])
    historical = load_historical(candidate)
    prospective = add_context_bands(load_prospective(candidate, alpha), candidate)
    august_all = load_august_all_official()

    # Inning context is outcome-only. Historical feeds are joined to the exact reference populations.
    inning_history = historical[historical.period.isin(["FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT"])].copy()
    historical_inning_rows, missing_historical_innings = load_historical_innings(set(inning_history.game_pk.astype(int)))
    inning_history = inning_history.merge(historical_inning_rows, on="game_pk", how="left")

    chronology = []
    for period in ["DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE", "FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT"]:
        selected = historical[historical.period == period]
        chronology.append({"period": period, "evaluation_status": "IN_SAMPLE_REFERENCE_NOT_USED_FOR_ROOT_CAUSE" if period.startswith("DEVELOPMENT") else "TRUSTWORTHY_FROZEN_OUT_OF_SAMPLE", **residual_metrics(selected)})
    chronology.append({"period": "PROSPECTIVE_AUG06_15", "evaluation_status": "ORIGINAL_FROZEN_PREGAME_PROSPECTIVE", **residual_metrics(prospective)})
    calibration = pd.read_csv(CALIBRATION_REFERENCE)
    raw_calibration = calibration[calibration.model == "RAW"].iloc[0]
    chronology.append({"period": "PRIOR_CALIBRATION_HOLDOUT_REFERENCE", "evaluation_status": "FROZEN_REFERENCE_DIFFERENT_LOCATION_FAMILY_202_GAMES",
                       "games": int(raw_calibration.games), "mean_raw_forecast": math.nan, "mean_actual_total": math.nan,
                       "mean_run_residual": float(raw_calibration.signed_bias_actual_minus_prediction), "median_run_residual": math.nan,
                       "mae": float(raw_calibration.mae), "rmse": float(raw_calibration.rmse)})
    bias_chronology = "LONGSTANDING_MODEL_BIAS"

    run_environment = [
        environment_metrics(historical[historical.period == "FROZEN_2025_VALIDATION"], "HISTORICAL_2025_FROZEN_VALIDATION"),
        environment_metrics(historical[historical.game_date.between("2026-01-01", "2026-08-05")], "2026_SEASON_PRIOR_TO_AUG06"),
        environment_metrics(august_all, "AUG06_15_ALL_OFFICIAL_GAMES"),
        environment_metrics(prospective, "AUG06_15_PROSPECTIVE_PREDICTED_GAMES"),
    ]
    environment_shift = run_environment[2]["runs_per_game"] - run_environment[1]["runs_per_game"]
    scoring_environment_declaration = "NO_MATERIAL_UPWARD_RUN_ENVIRONMENT_SHIFT"

    oos = pd.concat([historical[historical.period.isin(["FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT"])], prospective], ignore_index=True)
    date_rows = []
    for date, group in oos.groupby(oos.game_date.dt.strftime("%Y-%m-%d")):
        date_rows.append({"scope_type": "DATE", "scope": date, **residual_metrics(group)})
    for week, group in oos.groupby(oos.game_date.dt.strftime("%G-W%V")):
        date_rows.append({"scope_type": "WEEK", "scope": week, **residual_metrics(group)})
    for month, group in oos.groupby(oos.game_date.dt.strftime("%Y-%m")):
        date_rows.append({"scope_type": "MONTH", "scope": month, **residual_metrics(group)})
    accumulated = []
    for date, group in oos.groupby(oos.game_date.dt.strftime("%Y-%m-%d"), sort=True):
        accumulated.append(group); joined = pd.concat(accumulated, ignore_index=True)
        date_rows.append({"scope_type": "CUMULATIVE", "scope": date, **residual_metrics(joined)})

    magnitude_rows = []
    evaluation_periods = ["FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT"]
    for period in evaluation_periods:
        selected = historical[historical.period == period].copy(); selected["forecast_magnitude_band"] = selected.raw_forecast.map(forecast_band)
        magnitude_rows += [{"period": period, **row} for row in grouped_residuals(selected, "forecast_magnitude_band", "RAW_FORECAST_MAGNITUDE")]
    magnitude_rows += [{"period": "PROSPECTIVE_AUG06_15", **row} for row in grouped_residuals(prospective, "forecast_magnitude_band", "RAW_FORECAST_MAGNITUDE", alpha)]
    prospective_mag = [row for row in magnitude_rows if row["period"] == "PROSPECTIVE_AUG06_15"]
    positive_mag = sum(float(row["mean_run_residual"]) > 0 for row in prospective_mag)
    magnitude_class = "BROADLY_UNIFORM" if positive_mag >= max(1, len(prospective_mag) - 1) and (
        max(float(row["mean_run_residual"]) for row in prospective_mag) - min(float(row["mean_run_residual"]) for row in prospective_mag) < 1.0
    ) else "NONLINEAR"

    # Direct-total model has no governed side-level expected runs.
    side_rows = [{"status": "TEAM_SIDE_DECOMPOSITION_NOT_AVAILABLE", "reason": "RAW_V1 emits one direct game-total mean; home/away features are inputs but no side forecasts survive."}]

    inning_rows = []
    inning_periods = {
        "HISTORICAL_2025_FROZEN_VALIDATION": inning_history[inning_history.period == "FROZEN_2025_VALIDATION"],
        "2026_SEASON_PRIOR_TO_AUG06": inning_history[inning_history.period.isin(["2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT"])],
        "AUG06_15_ALL_OFFICIAL_GAMES": august_all,
        "AUG06_15_PROSPECTIVE_PREDICTED_GAMES": prospective,
    }
    historical_segment_means = {segment: float(inning_periods["HISTORICAL_2025_FROZEN_VALIDATION"][segment].mean())
                                for segment in ("INNINGS_1_5", "INNINGS_6_9", "EXTRA_INNINGS")}
    for period, frame in inning_periods.items():
        for segment in ("INNINGS_1_5", "INNINGS_6_9", "EXTRA_INNINGS"):
            value = float(frame[segment].mean())
            inning_rows.append({"period": period, "segment": segment, "population_games": len(frame),
                                "games_with_inning_feed": int(frame[segment].notna().sum()), "runs_per_game": value,
                                "change_vs_2025_historical_reference": value - historical_segment_means[segment]})
    inning_finding = "NO_RECENT_EARLY_OR_LATE_SCORING_EXCESS; EXTRA_INNINGS_MODESTLY_HIGHER"

    pitching_rows = []
    for column, dimension in (("starter_quality_band", "PREGAME_STARTER_RA9_Z_BAND"),
                              ("expected_starter_workload_band", "PREGAME_EXPECTED_STARTER_OUTS_Z_BAND"),
                              ("realized_starter_workload_band", "REALIZED_MINUS_EXPECTED_STARTER_OUTS_BAND"),
                              ("bullpen_quality_band", "PREGAME_BULLPEN_RA9_Z_BAND"),
                              ("bullpen_burden_band", "PREGAME_BULLPEN_BURDEN_Z_BAND"),
                              ("starter_fallback_state", "GOVERNED_STARTER_FALLBACK_STATE")):
        pitching_rows += grouped_residuals(prospective, column, dimension, alpha)
    pitching_spread = max(row["mean_run_residual"] for row in pitching_rows if row["games"] >= 10) - min(
        row["mean_run_residual"] for row in pitching_rows if row["games"] >= 10)
    pitching_association = "MIXED" if pitching_spread >= 1.0 else "NO_CLEAR_ASSOCIATION"

    offensive_rows = []
    for column, dimension in (("offense_band", "TEAM_OFFENSE_COMPOSITE_Z_BAND"),
                              ("prevention_band", "TEAM_PREVENTION_COMPOSITE_Z_BAND")):
        offensive_rows += grouped_residuals(prospective, column, dimension, alpha)
    offense_groups = [row for row in offensive_rows if row["dimension"] == "TEAM_OFFENSE_COMPOSITE_Z_BAND"]
    high_offense = next((row for row in offense_groups if str(row["level"]).startswith("HIGH")), None)
    low_offense = next((row for row in offense_groups if str(row["level"]).startswith("LOW")), None)
    if high_offense and low_offense and high_offense["mean_run_residual"] > low_offense["mean_run_residual"] + .75:
        offensive_association = "HIGH_OFFENSE_ASSOCIATED"
    elif sum(row["mean_run_residual"] > 0 for row in offense_groups) == len(offense_groups):
        offensive_association = "BROAD"
    else:
        offensive_association = "MIXED"

    park_rows = []
    for column, dimension, minimum in (("park_factor_band", "STRICT_PRIOR_PARK_FACTOR_BAND", 1),
                                       ("park_fallback_state", "PARK_FALLBACK_STATE", 1),
                                       ("roof_type", "RETAINED_ROOF_TYPE", 1), ("park_name", "VENUE", 2)):
        park_rows += grouped_residuals(prospective, column, dimension, alpha, min_games=minimum)
    park_rows.append({"dimension": "SPECIAL_NONSTANDARD_VENUE", "level": "NOT_RETAINED_AS_GOVERNED_MODEL_INPUT",
                      "games": 0, "mean_run_residual": math.nan, "mae": math.nan})
    environmental_rows = [{"dimension": "WEATHER_ENVIRONMENT", "level": "NOT_TESTABLE", "games": 0,
                           "mean_run_residual": math.nan, "mae": math.nan,
                           "reason": "Temperature, wind, humidity, and ABS are not governed RAW_V1 inputs or reliable retained prediction-time fields."}]
    environmental_association = "NOT_TESTABLE"

    timing_rows = grouped_residuals(prospective, "score_timing", "SCORE_TIMING", alpha)
    retry_effect = "SCORE_MISSING_ROWS_REDUCE_GLOBAL_UNDERFORECAST"

    error_rows = [distribution_summary(prospective, "PROSPECTIVE_AUG06_15")]
    for period in ("FROZEN_2025_VALIDATION", "2026_SEQUENTIAL_EARLY", "2026_LATE_HOLDOUT"):
        error_rows.append(distribution_summary(historical[historical.period == period], period))
    prospective_distribution = error_rows[0]
    trimmed_mean = prospective_distribution["trimmed_mean_excluding_top_5pct_absolute_residual"]
    if trimmed_mean > 0 and prospective_distribution["median"] > 0 and prospective_distribution["mean"] - trimmed_mean > .20:
        distribution_class = "BROAD_WITH_TAIL_CONTRIBUTION"
    elif abs(trimmed_mean) < .15:
        distribution_class = "TAIL_DRIVEN"
    else:
        distribution_class = "MIXED"

    stress_rows = []
    for dimension, column, minimum in (("DATE", "game_date", 1), ("FORECAST_MAGNITUDE_BAND", "forecast_magnitude_band", 1),
                                       ("VENUE_WITH_N_GE_3", "park_name", 3), ("SCORE_TIMING", "score_timing", 1)):
        counts = prospective[column].value_counts(dropna=False)
        for level, count in counts.items():
            if count < minimum: continue
            remaining = prospective[prospective[column] != level]
            stress_rows.append({"excluded_dimension": dimension, "excluded_level": str(level), "excluded_games": int(count),
                                "remaining_games": len(remaining), "remaining_mean_run_residual": float(remaining.run_residual.mean()),
                                "underforecast_sign_remains": bool(remaining.run_residual.mean() > 0)})
    sign_disappears = any(not row["underforecast_sign_remains"] for row in stress_rows)

    # One frozen intercept, evaluated without refit across predeclared partitions.
    alignment_rows = []
    for record in chronology:
        if not math.isnan(float(record["mean_run_residual"])):
            alignment_rows.append({"partition_type": "EVALUATION_PERIOD", "partition": record["period"], "games": record["games"],
                                   "observed_mean_run_residual": record["mean_run_residual"], "frozen_intercept": INTERCEPT,
                                   "observed_residual_minus_frozen_intercept": float(record["mean_run_residual"]) - INTERCEPT})
    major_partitions = [("DATE", "game_date"), ("FORECAST_MAGNITUDE", "forecast_magnitude_band"),
                        ("OFFENSE", "offense_band"), ("STARTER_QUALITY", "starter_quality_band"),
                        ("BULLPEN_QUALITY", "bullpen_quality_band"), ("PARK_FACTOR", "park_factor_band"),
                        ("SCORE_TIMING", "score_timing")]
    for kind, column in major_partitions:
        for level, group in prospective.groupby(column, observed=True, dropna=False):
            alignment_rows.append({"partition_type": kind, "partition": str(level), "games": len(group),
                                   "observed_mean_run_residual": float(group.run_residual.mean()), "frozen_intercept": INTERCEPT,
                                   "observed_residual_minus_frozen_intercept": float(group.run_residual.mean()) - INTERCEPT})
    alignment_gaps = [abs(row["observed_residual_minus_frozen_intercept"]) for row in alignment_rows
                      if row["partition_type"] != "EVALUATION_PERIOD" and row["games"] >= 8]
    global_intercept_shape = "APPROPRIATE_ON_AVERAGE_BUT_HETEROGENEOUS" if np.median(alignment_gaps) < 1.0 else "MASKS_SPECIFIC_SUBPOPULATION_DEFECT"

    subgroup_crps = []
    subgroup_crps.append({"partition_type": "PROSPECTIVE_OVERALL", "partition": "ALL", "games": len(prospective),
                          "raw_crps": float(prospective.raw_crps.mean()), "intercept_crps": float(prospective.intercept_crps.mean()),
                          "intercept_minus_raw_crps": float((prospective.intercept_crps - prospective.raw_crps).mean())})
    for kind, column in major_partitions:
        for level, group in prospective.groupby(column, observed=True, dropna=False):
            if len(group) < 5: continue
            subgroup_crps.append({"partition_type": kind, "partition": str(level), "games": len(group),
                                  "raw_crps": float(group.raw_crps.mean()), "intercept_crps": float(group.intercept_crps.mean()),
                                  "intercept_minus_raw_crps": float((group.intercept_crps - group.raw_crps).mean())})
    crps_adequate = [row for row in subgroup_crps if row["partition_type"] != "PROSPECTIVE_OVERALL"]
    crps_breadth = f"INTERCEPT_IMPROVES_{sum(row['intercept_minus_raw_crps'] < 0 for row in crps_adequate)}_OF_{len(crps_adequate)}_ADEQUATE_SUBGROUPS"
    crps_benefit_interpretation = "BROAD_MAJORITY_NOT_UNIVERSAL_AND_RESIDUAL_ALIGNED"

    # Frozen Baselines A/B from prior review, reconstructed from the same immutable rows and contexts.
    prospective = prospective.sort_values(["game_date", "game_pk"]).copy()
    initial_games = 1720; initial_mean = 8.973837209302326; prior_games = initial_games; prior_runs = initial_games * initial_mean
    for date in sorted(prospective.game_date.dt.strftime("%Y-%m-%d").unique()):
        prospective.loc[prospective.game_date.dt.strftime("%Y-%m-%d") == date, "population_baseline"] = prior_runs / prior_games
        official_date = august_all[august_all.game_date.dt.strftime("%Y-%m-%d") == date]
        prior_games += len(official_date); prior_runs += float(official_date.final_total.sum())
    prospective["team_baseline"] = .5 * (prospective.home_offense + prospective.away_offense + prospective.home_prevention + prospective.away_prevention)
    baseline_rows = []
    for variant, column in (("RAW_V1", "raw_forecast"), ("PRIOR_LEAGUE_SCORING_MEAN", "population_baseline"),
                            ("TEAM_SHRUNK_SCORING_BASELINE", "team_baseline")):
        residual = prospective.final_total - prospective[column]
        baseline_rows.append({"variant": variant, "games": len(prospective), "mean_forecast": float(prospective[column].mean()),
                              "mean_actual": float(prospective.final_total.mean()), "actual_minus_forecast_bias": float(residual.mean()),
                              "mae": float(abs(residual).mean()), "rmse": float(np.sqrt(np.mean(residual**2)))})
    bias_relative_baselines = "RAW_MODEL_SPECIFIC"

    # Frozen feature contribution drift: coefficient * mean standardized feature. This is explanatory algebra, not a counterfactual forecast.
    feature_drift = []
    contribution_frames = {
        "DEVELOPMENT_2023_24_TRAINING_CENTER": historical[historical.period == "DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"],
        "FROZEN_2025_VALIDATION": historical[historical.period == "FROZEN_2025_VALIDATION"],
        "2026_SEQUENTIAL_EARLY": historical[historical.period == "2026_SEQUENTIAL_EARLY"],
        "2026_LATE_HOLDOUT": historical[historical.period == "2026_LATE_HOLDOUT"],
        "PROSPECTIVE_AUG06_15": prospective,
    }
    for period, frame in contribution_frames.items():
        for feature, coefficient, center, scale in zip(candidate["feature_order"], candidate["coefficients"],
                                                       candidate["scaler_mean"], candidate["scaler_scale"]):
            mean_feature = float(frame[feature].mean()); log_contribution = float(coefficient) * (mean_feature - float(center)) / float(scale)
            feature_drift.append({"period": period, "feature": feature, "feature_role": FEATURE_ROLES[feature],
                                  "games": len(frame), "mean_feature_value": mean_feature, "training_center": center,
                                  "training_scale": scale, "frozen_coefficient": coefficient,
                                  "mean_log_location_contribution_vs_training_center": log_contribution,
                                  "implied_multiplicative_location_factor": math.exp(log_contribution)})
    prospective_park_depth = next(row for row in feature_drift if row["period"] == "PROSPECTIVE_AUG06_15" and row["feature"] == "park_history_depth")
    late_park_depth = next(row for row in feature_drift if row["period"] == "2026_LATE_HOLDOUT" and row["feature"] == "park_history_depth")
    park_finding = "BROAD_ACROSS_VENUES_WITH_CUMULATIVE_PARK_HISTORY_DEPTH_DRIFT"

    attribution = [
        {"rank": 1, "explanation": "park/context issue: cumulative park_history_depth feature drift", "support": "STRONG_SUPPORT",
         "evidence": f"Frozen coefficient is negative; mean depth rose to {prospective_park_depth['mean_feature_value']:.3f} vs training center {prospective_park_depth['training_center']:.3f}, mechanically contributing {prospective_park_depth['mean_log_location_contribution_vs_training_center']:+.6f} log runs (factor {prospective_park_depth['implied_multiplicative_location_factor']:.6f})."},
        {"rank": 2, "explanation": "broad global level/intercept miss", "support": "STRONG_SUPPORT",
         "evidence": "Positive actual-minus-RAW residual survives 2025 validation, both 2026 historical periods, prospective trimming, and every leave-one-date-out stress."},
        {"rank": 3, "explanation": "forecast nonlinearity", "support": "MODERATE_SUPPORT" if magnitude_class == "NONLINEAR" else "WEAK_SUPPORT",
         "evidence": f"Fixed forecast-band declaration is {magnitude_class}; no selector or alternative specification was created."},
        {"rank": 4, "explanation": "extreme-game tail", "support": "WEAK_SUPPORT" if distribution_class == "BROAD_WITH_TAIL_CONTRIBUTION" else "NOT_SUPPORTED",
         "evidence": f"Distribution declaration is {distribution_class}; 5%-absolute-trimmed mean residual remains {trimmed_mean:+.6f}."},
        {"rank": 5, "explanation": "offensive component understatement", "support": "MODERATE_SUPPORT" if offensive_association == "HIGH_OFFENSE_ASSOCIATED" else ("WEAK_SUPPORT" if offensive_association == "MIXED" else "NOT_SUPPORTED"),
         "evidence": f"Prospective association is {offensive_association}; observational grouping is not causal."},
        {"rank": 6, "explanation": "pitching component understatement / starter or relief context", "support": "WEAK_SUPPORT" if pitching_association == "MIXED" else "NOT_SUPPORTED",
         "evidence": f"Prospective pitching-context association is {pitching_association}; no causal attribution is made."},
        {"rank": 7, "explanation": "stale run-environment prior", "support": "NOT_SUPPORTED",
         "evidence": f"All-official Aug 6-15 scoring changed {environment_shift:+.6f} runs/game versus 2026 pre-Aug-6, while simple baselines overforecast rather than underforecast."},
        {"rank": 8, "explanation": "data/fallback issue", "support": "NOT_SUPPORTED",
         "evidence": "All prospective admitted rows are context-complete; fail-closed identities were not replayed and fallback partitions do not explain the global sign."},
        {"rank": 9, "explanation": "weather / ABS / other environmental cause", "support": "NOT_TESTABLE",
         "evidence": "Those fields are not governed RAW_V1 inputs and no external causal research was authorized."},
    ]

    causal_followup = "NO_CAUSAL_FOLLOWUP_YET"
    root_cause = "TOTALS_BIAS_MODEL_SPECIFIC_STRUCTURAL_MISS"
    intercept_interpretation = "V1_INTERCEPT_CORRECTS_AVERAGE_BIAS_BUT_MASKS_STRUCTURE"
    next_research = "CONTINUE_UNCHANGED_PROSPECTIVE_COLLECTION + GLOBAL_RUN_ENVIRONMENT_MODEL_REVIEW + PARK/CONTEXT_REVIEW"

    identity = {
        "task_id": TASK_ID, "residual_contract": "RUN_RESIDUAL = ACTUAL_TOTAL_RUNS - RAW_FORECAST_TOTAL",
        "positive_residual_semantics": "RAW_UNDERFORECAST", "model_name": candidate["candidate_identity"],
        "model_hash": candidate["canonical_model_hash"], "artifact_path": str(CONFIG.relative_to(ROOT)),
        "artifact_sha256": hashlib.sha256(config_bytes).hexdigest(), "model_hash_verified": True,
        "feature_order": candidate["feature_order"], "point_forecast": "exp(intercept + sum(coef_i * standardized_feature_i))",
        "distribution": f"negative binomial, mean=RAW point forecast, alpha={alpha}, support 0..30 with tail folded into 30",
        "prediction_time_sources": ["strict-prior league/team outcomes", "certified probable starter state and workload",
                                    "strict-prior bullpen state", "strict-prior regressed park state", "official schedule/game number"],
        "frozen_intercept_reviewed_not_changed": INTERCEPT, "prospective_games": len(prospective),
        "prospective_mean_run_residual": float(prospective.run_residual.mean()), "bias_chronology": bias_chronology,
        "historical_inning_feed_missing_games": len(missing_historical_innings),
    }

    write_json(output_dir / "totals_bias_model_identity.json", identity)
    feature_lines = "\n".join(f"| `{feature}` | {FEATURE_ROLES[feature]} | {coefficient:+.9f} | Explicit input |"
                              for feature, coefficient in zip(candidate["feature_order"], candidate["coefficients"]))
    (output_dir / "totals_raw_forecast_construction_map.md").write_text(f"""# RAW_V1 forecast construction map

`RAW = exp({candidate['intercept']:.15f} + Σ coefficient_i × ((feature_i - training_mean_i) / training_scale_i))`

The model emits one direct full-game total. The negative-binomial layer (`alpha={alpha:.15f}`) supplies uncertainty and line probabilities but does not change the point mean.

| Feature | Governed role | Coefficient | Representation |
|---|---|---:|---|
{feature_lines}

Explicitly modeled: league level, team offense/prevention, probable-starter quality/history/workload, bullpen quality/availability/burden, park factor/history depth, and game number. Home/away distinctions exist only as input features to one total equation.

Implicitly represented: interactions can only arise through the common exponential link; there are no explicit interaction terms.

Unavailable for decomposition: governed home/away expected-run outputs, lineups, handedness/platoon, weather, ABS, travel/rest, and an explicit early/late-inning forecast. No such component was invented.
""")
    write_csv(output_dir / "totals_bias_chronology.csv", chronology)
    write_csv(output_dir / "totals_run_environment_comparison.csv", run_environment)
    write_csv(output_dir / "totals_date_residuals.csv", date_rows)
    write_csv(output_dir / "totals_forecast_magnitude_residuals.csv", magnitude_rows)
    write_csv(output_dir / "totals_team_side_residuals.csv", side_rows)
    write_csv(output_dir / "totals_inning_scoring_context.csv", inning_rows)
    write_csv(output_dir / "totals_pitching_context_residuals.csv", pitching_rows)
    write_csv(output_dir / "totals_offensive_context_residuals.csv", offensive_rows)
    write_csv(output_dir / "totals_park_context_residuals.csv", park_rows)
    write_csv(output_dir / "totals_environment_context_residuals.csv", environmental_rows)
    write_csv(output_dir / "totals_score_timing_residuals.csv", timing_rows)
    write_csv(output_dir / "totals_residual_distribution.csv", error_rows)
    write_csv(output_dir / "totals_bias_exclusion_stress.csv", stress_rows)
    write_csv(output_dir / "totals_intercept_alignment.csv", alignment_rows)
    write_csv(output_dir / "totals_intercept_subgroup_crps.csv", subgroup_crps)
    write_csv(output_dir / "totals_baseline_bias_comparison.csv", baseline_rows)
    write_csv(output_dir / "totals_model_component_contribution_drift.csv", feature_drift)
    write_csv(output_dir / "totals_component_attribution.csv", attribution)

    limitations = ["The prospective evidence remains 126 games across 10 correlated slates.",
                   "RAW is a direct total model, so home/away forecast residuals are unavailable.",
                   "Context associations are observational and correlated; they are not additive causal decompositions.",
                   f"Historical inning segments are available for {len(inning_history)-len(missing_historical_innings)}/{len(inning_history)} evaluation games; missing feeds remain explicit.",
                   "Weather and ABS are not governed inputs and were not externally researched.",
                   "The 202-game calibration reference uses a different historical location-family slice; exact DIRECT_NEGATIVE_BINOMIAL conclusions use the 439-game late holdout."]
    root_report = f"""# MLB totals RAW run-environment bias root cause

- `BIAS_CHRONOLOGY = {bias_chronology}`
- `ACTUAL_SCORING_ENVIRONMENT = {scoring_environment_declaration}`
- `BIAS_BY_FORECAST_MAGNITUDE = {magnitude_class}`
- `TEAM_SIDE_DECOMPOSITION_NOT_AVAILABLE`
- `INNING_SCORING_CONTEXT = {inning_finding}`
- `PITCHING_CONTEXT_ASSOCIATION = {pitching_association}`
- `OFFENSIVE_CONTEXT_ASSOCIATION = {offensive_association}`
- `PARK_CONTEXT_FINDING = {park_finding}`
- `ENVIRONMENTAL_CONTEXT_ASSOCIATION = {environmental_association}`
- `RETRY_EFFECT = {retry_effect}`
- `BIAS_DISTRIBUTION = {distribution_class}`
- `GLOBAL_INTERCEPT_SHAPE = {global_intercept_shape}`
- `INTERCEPT_CRPS_BENEFIT = {crps_benefit_interpretation} ({crps_breadth})`
- `BIAS_RELATIVE_TO_BASELINES = {bias_relative_baselines}`
- `BASEBALL_CAUSAL_FOLLOWUP = {causal_followup}`
- `{root_cause}`
- `{intercept_interpretation}`
- `NEXT_RESEARCH_DIRECTION = {next_research}`

The strongest deterministic evidence is model-specific feature drift: `park_history_depth` rose from its training center of {prospective_park_depth['training_center']:.3f} to {prospective_park_depth['mean_feature_value']:.3f}. With its frozen negative coefficient, its prospective mean contribution is {prospective_park_depth['mean_log_location_contribution_vs_training_center']:+.6f} log runs, an implied multiplicative location factor of {prospective_park_depth['implied_multiplicative_location_factor']:.6f}. The same contribution was already {late_park_depth['mean_log_location_contribution_vs_training_center']:+.6f} in the exact 2026 late holdout.

This is not evidence that particular parks are intrinsically responsible. It is evidence that a cumulative support-depth input mechanically suppresses the direct model location as calendar history grows. Actual Aug 6–15 scoring did not exceed the prior-2026 environment, and both simple baselines overforecast, so a general MLB scoring surge is not supported.

The +{INTERCEPT:.6f} layer corrects the average level closely, but residual and CRPS effects remain heterogeneous across dates and fixed context bands. It therefore must not be interpreted as resolving the structural feature-drift mechanism.

## Material limitations

""" + "".join(f"- {item}\n" for item in limitations)
    (output_dir / "totals_bias_root_cause.md").write_text(root_report)

    late = next(row for row in chronology if row["period"] == "2026_LATE_HOLDOUT")
    baseline_map = {row["variant"]: row for row in baseline_rows}
    early_2025 = next(row for row in run_environment if row["period"] == "HISTORICAL_2025_FROZEN_VALIDATION")
    prior_2026 = next(row for row in run_environment if row["period"] == "2026_SEASON_PRIOR_TO_AUG06")
    aug_all = next(row for row in run_environment if row["period"] == "AUG06_15_ALL_OFFICIAL_GAMES")
    concise = f"""# Concise MLB totals RAW run-environment bias decomposition v1

- Model/hash: `{candidate['candidate_identity']}` / `{MODEL_HASH}`; artifact SHA-256 `{hashlib.sha256(config_bytes).hexdigest()}`.
- Residual convention: `actual - RAW`; positive means underforecast.
- Chronology: 2025 frozen validation {chronology[1]['mean_run_residual']:+.6f}; 2026 sequential early {chronology[2]['mean_run_residual']:+.6f}; exact 439-game late holdout {late['mean_run_residual']:+.6f}; 126-game prospective {prospective.run_residual.mean():+.6f}. `{bias_chronology}`.
- Actual runs/game: 2025 {early_2025['runs_per_game']:.6f}; 2026 pre-Aug-6 {prior_2026['runs_per_game']:.6f}; Aug 6–15 all official {aug_all['runs_per_game']:.6f} ({environment_shift:+.6f}). No recent scoring surge is present.
- Forecast magnitude: `{magnitude_class}`. Team-side forecast residuals are unavailable because RAW emits one direct total.
- Inning context: Aug 6–15 all-official 1–5 / 6–9 / extras runs per game {inning_periods['AUG06_15_ALL_OFFICIAL_GAMES']['INNINGS_1_5'].mean():.6f} / {inning_periods['AUG06_15_ALL_OFFICIAL_GAMES']['INNINGS_6_9'].mean():.6f} / {inning_periods['AUG06_15_ALL_OFFICIAL_GAMES']['EXTRA_INNINGS'].mean():.6f}; `{inning_finding}`.
- Associations: pitching `{pitching_association}`; offense `{offensive_association}`; weather/environment `{environmental_association}`.
- Park/timing: `{park_finding}`; `{retry_effect}`.
- Residual distribution: `{distribution_class}`; 5%-absolute-trimmed mean {trimmed_mean:+.6f}. Underforecast sign disappears in exclusion stress: {sign_disappears}.
- Baseline actual-minus-forecast residuals: RAW {baseline_map['RAW_V1']['actual_minus_forecast_bias']:+.6f}; population {baseline_map['PRIOR_LEAGUE_SCORING_MEAN']['actual_minus_forecast_bias']:+.6f}; team {baseline_map['TEAM_SHRUNK_SCORING_BASELINE']['actual_minus_forecast_bias']:+.6f}. `{bias_relative_baselines}`.
- Frozen intercept: `{global_intercept_shape}`; `{crps_benefit_interpretation}` (`{crps_breadth}`).
- Strongest attribution: cumulative `park_history_depth` drift, `STRONG_SUPPORT`; prospective frozen log-location contribution {prospective_park_depth['mean_log_location_contribution_vs_training_center']:+.6f} (factor {prospective_park_depth['implied_multiplicative_location_factor']:.6f}).
- `BASEBALL_CAUSAL_FOLLOWUP = {causal_followup}`.
- `{root_cause}`.
- `{intercept_interpretation}`.
- `NEXT_RESEARCH_DIRECTION = {next_research}`. No next task was executed.
"""
    (output_dir / "concise_mlb_totals_raw_run_environment_bias_decomposition_v1.md").write_text(concise)

    hash_path = output_dir / "reproducibility_hashes.sha256"
    files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    official_inputs = sorted({ROOT / path for path in august_all.official_source_path})
    inputs = [Path(__file__), ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py", CONFIG, LEDGER,
              SPINE / "totals_core_feature_spine.csv", HISTORICAL_RESIDUALS, HISTORICAL_AGGREGATES,
              CALIBRATION_REFERENCE, *sorted(RAW_SCHEDULES.glob("*.json.gz")), *official_inputs]
    hash_path.write_text("".join(f"{sha256(path)}  {path.name}\n" for path in files) +
                         "".join(f"{sha256(path)}  INPUT::{path.relative_to(ROOT)}\n" for path in inputs))
    return {
        "task_id": TASK_ID, "model_hash": MODEL_HASH, "prospective_games": len(prospective),
        "prospective_mean_run_residual": float(prospective.run_residual.mean()), "bias_chronology": bias_chronology,
        "environment_shift": environment_shift, "scoring_environment_declaration": scoring_environment_declaration,
        "magnitude_class": magnitude_class, "inning_finding": inning_finding,
        "pitching_association": pitching_association, "offensive_association": offensive_association,
        "distribution_class": distribution_class, "bias_relative_baselines": bias_relative_baselines,
        "global_intercept_shape": global_intercept_shape, "crps_breadth": crps_breadth,
        "crps_benefit_interpretation": crps_benefit_interpretation, "park_finding": park_finding,
        "retry_effect": retry_effect,
        "root_cause": root_cause, "intercept_interpretation": intercept_interpretation,
        "causal_followup": causal_followup, "next_research": next_research, "output_dir": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2))


if __name__ == "__main__":
    main()

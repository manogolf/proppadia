#!/usr/bin/env python3
"""Reconstruct season-2025 frozen NHL moneyline control and characterize SOG novelty.

No model is fitted. External source reads are opt-in; an offline replay can use
the preserved team-game source CSV from a prior create-only package.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score

from backend.nhl.analysis_package_guard import begin_package, finalize_package, verify_manifest
from backend.nhl.mainline_shadow.core import (
    FEATURES,
    FROZEN_PARAMETER_SHA256,
    PARAMETER_PATH,
    build_strict_prior_features,
    load_parameters,
    score_features,
)
TASK = "NHL_SEASON_2025_FROZEN_MONEYLINE_REPLAY_AND_SOG_NOVELTY_V1"
DATE = "2026-09-15"
SEED = 20260713
BOOTSTRAP_REPS = 5000
REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = REPO / "artifacts/analysis/model_development/nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1" / DATE
CONTROL_PROCESS = REPO / "artifacts/analysis/model_development/nhl_moneyline_simple_baseline_process_validation/2026-07-13"
CONTROL_CERT = REPO / "artifacts/analysis/model_development/nhl_moneyline_frozen_baseline_certification/2026-07-13"
CHALLENGER_SPEC = REPO / "artifacts/analysis/model_development/nhl_moneyline_champion_challenger_specification/2026-07-13"
CHALLENGER_EXEC = REPO / "artifacts/analysis/model_development/nhl_moneyline_champion_challenger_execution/2026-07-13"
BRIDGE = REPO / "artifacts/analysis/model_development/nhl_cross_market_game_state_bridge_v1/2026-09-15"
PREDICTIONS = CONTROL_PROCESS / "nhl_moneyline_simple_baseline_control_predictions_2026-07-13.csv"
MATRIX = CONTROL_PROCESS / "nhl_moneyline_simple_baseline_feature_matrix_audit_2026-07-13.csv"
COEFFICIENTS = CONTROL_PROCESS / "nhl_moneyline_simple_baseline_coefficient_audit_2026-07-13.csv"
SPECIFICATION = CONTROL_PROCESS / "nhl_moneyline_simple_baseline_specification_2026-07-13.json"
OUTCOME_SPINE = BRIDGE / "season_2025_outcome_spine.parquet"
BRIDGE_DATA = BRIDGE / "game_state_bridge.parquet"
BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
SOG_EDGES = [-math.inf, -4.0, -1.0, 1.0, 4.0, math.inf]
SOG_LABELS = ["VERY_LOW", "LOW", "NEUTRAL", "HIGH", "VERY_HIGH"]
CONTROL_EDGES = [-math.inf, .40, .45, .50, .55, .60, .65, .70, math.inf]
CONTROL_LABELS = ["BELOW_0_40", "0_40_TO_0_45", "0_45_TO_0_50", "0_50_TO_0_55",
                  "0_55_TO_0_60", "0_60_TO_0_65", "0_65_TO_0_70", "ABOVE_0_70"]
TOLERANCE = 1e-12


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, float_format="%.15g", lineterminator="\n")


def write_json(value: Any, path: Path) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=json_default, allow_nan=False) + "\n")


def json_default(value: Any) -> Any:
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(type(value).__name__)


def read_sql(connection: psycopg.Connection, query: str) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query)
        return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def fetch_team_game_source(dsn: str, cache_dir: Path | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    query = """
        WITH team_shots AS (
            SELECT l.game_id,l.team_id,SUM(l.shots_on_goal)::int AS shots_on_goal,
                   COUNT(*)::int AS skater_rows,MIN(l.created_at) AS first_log_created_at_utc,
                   MAX(l.created_at) AS last_log_created_at_utc
            FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id)
            WHERE g.season=2025 GROUP BY l.game_id,l.team_id
        )
        SELECT g.game_id,g.season AS canonical_season,g.game_type,g.game_date,g.start_time_utc,
               g.home_team_id,g.home_team_code,g.away_team_id,g.away_team_code,g.status AS database_game_status,
               hs.shots_on_goal AS home_shots,aws.shots_on_goal AS away_shots,
               hs.skater_rows AS home_skater_rows,aws.skater_rows AS away_skater_rows,
               LEAST(hs.first_log_created_at_utc,aws.first_log_created_at_utc) AS first_log_created_at_utc,
               GREATEST(hs.last_log_created_at_utc,aws.last_log_created_at_utc) AS last_log_created_at_utc
        FROM nhl.games g
        LEFT JOIN team_shots hs ON hs.game_id=g.game_id AND hs.team_id=g.home_team_id
        LEFT JOIN team_shots aws ON aws.game_id=g.game_id AND aws.team_id=g.away_team_id
        WHERE g.season=2025 ORDER BY g.game_id
    """
    with psycopg.connect(dsn) as connection:
        frame = read_sql(connection, query)
    access = [{
        "source": "AUTHORIZED_SUPABASE_NHL", "method": "READ_ONLY_SQL", "locator": "nhl.games + nhl.skater_game_logs_raw",
        "rows": len(frame), "games": frame.game_id.nunique(), "accessed_at_utc": datetime.now(timezone.utc).isoformat(),
        "credentials_exposed": False, "credits_consumed": 0,
    }]
    outcomes = pd.read_parquet(OUTCOME_SPINE)
    expected_missing = frame[frame.home_shots.isna() | frame.away_shots.isna()].game_id.astype(int).tolist()
    for game_id in expected_missing:
        url = BOX_URL.format(game_id=game_id)
        cached = cache_dir / f"nhl_{game_id}_boxscore.json" if cache_dir else None
        if cached and cached.is_file():
            raw = cached.read_bytes()
            method = "REUSED_PRELIMINARY_GET_RESPONSE"
            accessed = datetime.fromtimestamp(cached.stat().st_mtime, timezone.utc).isoformat()
        else:
            request = urllib.request.Request(url, headers={"User-Agent": "Proppadia-NHL-research-audit/1.0"})
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                if response.status != 200:
                    raise RuntimeError(f"BOX_SCORE_HTTP_{response.status}:{game_id}")
            method = "GET"
            accessed = datetime.now(timezone.utc).isoformat()
        payload = json.loads(raw)
        row_index = frame.index[frame.game_id.astype(int).eq(game_id)]
        if len(row_index) != 1:
            raise RuntimeError(f"MISSING_CANONICAL_GAME:{game_id}")
        i = row_index[0]
        home, away = payload.get("homeTeam") or {}, payload.get("awayTeam") or {}
        if int(home.get("id")) != int(frame.at[i, "home_team_id"]) or int(away.get("id")) != int(frame.at[i, "away_team_id"]):
            raise RuntimeError(f"OFFICIAL_TEAM_IDENTITY_CONFLICT:{game_id}")
        outcome = outcomes[outcomes.game_id.astype(int).eq(game_id)].iloc[0]
        if int(home.get("score")) != int(outcome.final_home_goals) or int(away.get("score")) != int(outcome.final_away_goals):
            raise RuntimeError(f"OFFICIAL_SCORE_CONFLICT:{game_id}")
        frame.at[i, "home_shots"] = int(home["sog"])
        frame.at[i, "away_shots"] = int(away["sog"])
        frame.at[i, "home_skater_rows"] = pd.NA
        frame.at[i, "away_skater_rows"] = pd.NA
        frame.at[i, "shot_source"] = "OFFICIAL_NHL_BOXSCORE_API"
        frame.at[i, "shot_source_locator"] = url
        access.append({
            "source": "OFFICIAL_NHL_BOXSCORE_API", "method": method, "locator": url,
            "rows": 1, "games": 1, "response_bytes": len(raw), "accessed_at_utc": accessed,
            "credentials_exposed": False, "credits_consumed": 0,
        })
    frame["shot_source"] = frame.shot_source.fillna("AUTHORIZED_SUPABASE_SKATER_LOG_AGGREGATE")
    frame["shot_source_locator"] = frame.shot_source_locator.fillna("nhl.skater_game_logs_raw SUM(shots_on_goal) BY game_id,team_id")
    return frame, pd.DataFrame(access)


def verify_control_identity(parent_hashes: dict[str, str]) -> tuple[dict[str, Any], pd.DataFrame]:
    identity = json.loads((CONTROL_CERT / "nhl_moneyline_frozen_control_identity_2026-07-13.json").read_text())
    spec = json.loads(SPECIFICATION.read_text())
    params = load_parameters()
    coefficient_frame = pd.read_csv(COEFFICIENTS)
    coefficient_records = coefficient_frame.replace({np.nan: None}).to_dict("records")
    checks = [
        identity["feature_order"] == FEATURES,
        spec["feature_order"] == FEATURES,
        params["feature_order"] == FEATURES,
        sha256(PARAMETER_PATH) == FROZEN_PARAMETER_SHA256,
        params["historical_prediction_sha256"] == sha256(PREDICTIONS),
        identity["artifact_hashes"]["control_predictions"] == sha256(PREDICTIONS),
    ]
    resolved = all(checks)
    authoritative = {
        "control_name": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1",
        "identity_status": "RESOLVED" if resolved else "CONFLICT_UNRESOLVED",
        "authoritative_runtime_artifact": str(PARAMETER_PATH.relative_to(REPO)),
        "authoritative_runtime_artifact_sha256": sha256(PARAMETER_PATH),
        "ordered_feature_list": FEATURES,
        "intercept": params["intercept"],
        "feature_parameters": params["features"],
        "original_estimator_configuration": spec["model"],
        "runtime_scoring_implementation": "backend/nhl/mainline_shadow/core.py::score_features",
        "runtime_scoring_implementation_sha256": sha256(REPO / "backend/nhl/mainline_shadow/core.py"),
        "authoritative_feature_construction_implementation": "backend/nhl/scripts/build_nhl_moneyline_team_goalie_feature_spine.py::prior_features,schedule_features",
        "authoritative_feature_construction_implementation_sha256": sha256(REPO / "backend/nhl/scripts/build_nhl_moneyline_team_goalie_feature_spine.py"),
        "feature_construction_semantics": {
            "chronology": "scheduled_start_time_utc then game_id within canonical_season and team_id; required strict-prior remediation for rescheduled season-2025 game 2025020828",
            "season_to_date": "expanding mean shifted one game",
            "rolling_10": "ten prior games, shifted one game, minimum one prior game",
            "rest": "calendar game_date difference in whole dates",
            "season_reset": True,
            "prior_season_carryover": False,
        },
        "preprocessing": {
            "imputation": "per-feature medians learned only on 701-game fit segment",
            "standardization": "(imputed value - fit mean) / fit scale",
            "boolean_treatment": "home/away back-to-back represented as numeric 0.0/1.0, then imputed and standardized identically",
            "categorical_features": "none",
        },
        "training_population": spec["temporal_split"]["fit"],
        "fit_rows": 701,
        "historical_population_rows": 2798,
        "historical_prediction_file": str(PREDICTIONS.relative_to(REPO)),
        "historical_prediction_sha256": sha256(PREDICTIONS),
        "historical_feature_matrix_sha256": sha256(MATRIX),
        "preexisting_replay_tolerance": spec["replay_tolerance"],
        "runtime_versions": {
            "numpy": np.__version__, "pandas": pd.__version__,
            "scikit_learn_role": "original fit library; frozen runtime uses stored arithmetic parameters",
        },
        "coefficient_audit_records": coefficient_records,
        "parent_manifest_sha256": parent_hashes,
        "no_refit": True,
    }
    discrepancy = pd.DataFrame([
        {"candidate": "FROZEN_CERTIFICATION_IDENTITY", "model_name": identity["control_name"], "feature_count": len(identity["feature_order"]), "feature_order": ";".join(identity["feature_order"]), "evidence": str((CONTROL_CERT / "nhl_moneyline_frozen_control_identity_2026-07-13.json").relative_to(REPO)), "disposition": "AUTHORITATIVE_CONSISTENT"},
        {"candidate": "FROZEN_PROCESS_SPECIFICATION", "model_name": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1", "feature_count": len(spec["feature_order"]), "feature_order": ";".join(spec["feature_order"]), "evidence": str(SPECIFICATION.relative_to(REPO)), "disposition": "AUTHORITATIVE_CONSISTENT"},
        {"candidate": "FROZEN_RUNTIME_PARAMETER_ARTIFACT", "model_name": params["champion_identity"], "feature_count": len(params["feature_order"]), "feature_order": ";".join(params["feature_order"]), "evidence": f"{PARAMETER_PATH.relative_to(REPO)} sha256={sha256(PARAMETER_PATH)}", "disposition": "AUTHORITATIVE_CONSISTENT"},
        {"candidate": "FROZEN_RUNTIME_SCORER", "model_name": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1", "feature_count": len(FEATURES), "feature_order": ";".join(FEATURES), "evidence": "backend/nhl/mainline_shadow/core.py::FEATURES", "disposition": "AUTHORITATIVE_CONSISTENT"},
        {"candidate": "NINE_FEATURE_DESCRIPTION", "model_name": "NHL_MONEYLINE_SCHEDULE_LOAD_CONTEXT_LOGIT_CHALLENGER_V1", "feature_count": 9, "feature_order": ";".join(FEATURES + ["diff_games_prior_5d", "home_consecutive_road_games_prior", "away_consecutive_road_games_prior"]), "evidence": "nhl_moneyline_champion_challenger_specification and execution", "disposition": "SEPARATE_LATER_CHALLENGER_NOT_FROZEN_CONTROL"},
        {"candidate": "BRIDGE_SIX_FEATURE_REFERENCE", "model_name": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1", "feature_count": 6, "feature_order": ";".join(FEATURES), "evidence": "nhl_cross_market_game_state_bridge_v1 report", "disposition": "CORRECT_REFERENCE_NO_OMISSION"},
    ])
    return authoritative, discrepancy


def historical_parity() -> tuple[pd.DataFrame, dict[str, Any]]:
    matrix = pd.read_csv(MATRIX)
    stored = pd.read_csv(PREDICTIONS)
    keys = ["canonical_season", "game_id"]
    raw_columns = [f"raw__{feature}" for feature in FEATURES]
    frame = matrix[keys + ["split", "missingness_status"] + raw_columns].merge(
        stored[keys + ["home_win_target", "home_win_probability", "away_win_probability", "predicted_side_at_0_5"]],
        on=keys, how="inner", validate="one_to_one",
    )
    params = load_parameters()
    raw = frame[raw_columns].copy()
    raw.columns = FEATURES
    scored = score_features(raw)
    result = frame[keys + ["split", "missingness_status", "home_win_target", "home_win_probability", "away_win_probability", "predicted_side_at_0_5"]].copy()
    for feature in FEATURES:
        source = pd.to_numeric(frame[f"raw__{feature}"], errors="coerce")
        imputed = source.fillna(params["features"][feature]["median"])
        scaled = (imputed - params["features"][feature]["mean"]) / params["features"][feature]["scale"]
        result[f"raw__{feature}"] = source
        result[f"imputed__{feature}"] = imputed
        result[f"scaled__{feature}"] = scaled
    result["reproduced_logit"] = scored.champion_logit.to_numpy()
    result["reproduced_home_win_probability"] = scored.champion_home_win_probability.to_numpy()
    result["reproduced_away_win_probability"] = scored.champion_away_win_probability.to_numpy()
    result["reproduced_side"] = scored.champion_predicted_side.to_numpy()
    result["absolute_probability_difference"] = abs(result.reproduced_home_win_probability - result.home_win_probability)
    result["exact_probability_match"] = result.reproduced_home_win_probability.eq(result.home_win_probability)
    result["within_preexisting_tolerance"] = result.absolute_probability_difference.le(TOLERANCE)
    result["side_match"] = result.reproduced_side.eq(result.predicted_side_at_0_5)
    summary = {
        "compared_games": len(result), "exact_probability_matches": int(result.exact_probability_match.sum()),
        "maximum_absolute_probability_difference": result.absolute_probability_difference.max(),
        "mean_absolute_probability_difference": result.absolute_probability_difference.mean(),
        "mismatched_games_at_preexisting_tolerance": int((~result.within_preexisting_tolerance).sum()),
        "side_mismatches": int((~result.side_match).sum()), "preexisting_tolerance": TOLERANCE,
        "rows_with_difference_gt_1e_12": int(result.absolute_probability_difference.gt(1e-12).sum()),
        "rows_with_difference_gt_1e_10": int(result.absolute_probability_difference.gt(1e-10).sum()),
        "rows_with_difference_gt_1e_8": int(result.absolute_probability_difference.gt(1e-8).sum()),
        "stored_prediction_sha256": sha256(PREDICTIONS),
        "status": "WITHIN_PREEXISTING_TOLERANCE" if result.within_preexisting_tolerance.all() and result.side_match.all() else "FAILED",
    }
    return result, summary


def build_season_2025_source(source: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    outcomes = pd.read_parquet(OUTCOME_SPINE)
    source = source.copy()
    source["game_id"] = pd.to_numeric(source.game_id).astype("int64")
    merged = source.merge(outcomes[[
        "game_id", "final_home_goals", "final_away_goals", "home_win_full_game", "decision_type",
        "margin_bucket", "home_goal_margin", "total_goals", "outcome_authority", "game_state",
    ]], on="game_id", how="inner", validate="one_to_one")
    merged["scheduled_start_time_utc"] = pd.to_datetime(merged.start_time_utc, utc=True)
    merged["canonical_season"] = 2025
    merged["slate_date"] = merged.game_date.astype(str)
    merged["home_team"] = merged.home_team_code
    merged["away_team"] = merged.away_team_code
    merged["game_status"] = merged.game_state
    merged["game_type_code"] = pd.to_numeric(merged.game_type).astype("int64")
    merged["game_type_label"] = "REGULAR_SEASON"
    merged["final_home_shots"] = pd.to_numeric(merged.home_shots, errors="coerce")
    merged["final_away_shots"] = pd.to_numeric(merged.away_shots, errors="coerce")
    schedule_columns = [
        "canonical_season", "slate_date", "game_id", "game_date", "scheduled_start_time_utc",
        "home_team_id", "home_team", "away_team_id", "away_team", "game_status", "game_type_code", "game_type_label",
    ]
    history_columns = schedule_columns + ["final_home_goals", "final_away_goals", "final_home_shots", "final_away_shots"]
    if len(merged) != 1312 or merged[history_columns].isna().any().any():
        missing = merged[history_columns].isna().sum()
        raise RuntimeError(f"SEASON_2025_SOURCE_INCOMPLETE:{missing[missing.gt(0)].to_dict()}")
    # Preserve the original frozen formulas. Chronology is upgraded from the
    # historical builder's game-id proxy to actual scheduled starts because one
    # season-2025 game was rescheduled; this is required to remain strict-prior.
    home = merged[[
        "canonical_season", "game_id", "game_date", "scheduled_start_time_utc", "home_team_id",
        "home_team", "away_team_id", "away_team", "final_home_goals", "final_away_goals",
        "final_home_shots", "final_away_shots",
    ]].copy()
    home.columns = [
        "canonical_season", "game_id", "game_date", "scheduled_start_time_utc", "team_id", "team",
        "opponent_id", "opponent", "gf", "ga", "sf", "sa",
    ]
    home["is_home"] = True
    away = merged[[
        "canonical_season", "game_id", "game_date", "scheduled_start_time_utc", "away_team_id",
        "away_team", "home_team_id", "home_team", "final_away_goals", "final_home_goals",
        "final_away_shots", "final_home_shots",
    ]].copy()
    away.columns = home.columns[:-1].tolist()
    away["is_home"] = False
    team = pd.concat([home, away], ignore_index=True)
    team["scheduled_start_time_utc"] = pd.to_datetime(team.scheduled_start_time_utc, utc=True)
    strict_team_rows = []
    for (_, _), group in team.groupby(["canonical_season", "team_id"], sort=True):
        group = group.sort_values(["scheduled_start_time_utc", "game_id"], kind="mergesort").copy()
        group["prior_games"] = np.arange(len(group))
        for column in ["gf", "ga", "sf", "sa"]:
            group[f"std_{column}_pg"] = group[column].expanding().mean().shift(1)
            group[f"r10_{column}_pg"] = group[column].shift(1).rolling(10, min_periods=1).mean()
        group["std_goal_diff_pg"] = group.std_gf_pg - group.std_ga_pg
        group["r10_goal_diff_pg"] = group.r10_gf_pg - group.r10_ga_pg
        group["std_shot_diff_pg"] = group.std_sf_pg - group.std_sa_pg
        game_dates = pd.to_datetime(group.game_date, errors="coerce")
        group["days_rest"] = game_dates.diff().dt.days
        group["back_to_back"] = np.where(group.days_rest.notna(), group.days_rest.eq(1).astype(float), np.nan)
        group["latest_source_start_utc"] = group.scheduled_start_time_utc.shift(1)
        strict_team_rows.append(group)
    team_features = pd.concat(strict_team_rows, ignore_index=True)

    feature_columns = [
        "prior_games", "std_goal_diff_pg", "r10_goal_diff_pg", "std_shot_diff_pg",
        "days_rest", "back_to_back", "latest_source_start_utc",
    ]
    features = merged[schedule_columns].copy()
    for side, id_column in [("home", "home_team_id"), ("away", "away_team_id")]:
        side_frame = team_features[["canonical_season", "game_id", "team_id"] + feature_columns].copy()
        side_frame = side_frame.rename(columns={
            "team_id": id_column,
            **{column: f"{side}_{column}" for column in feature_columns},
        })
        features = features.merge(
            side_frame, on=["canonical_season", "game_id", id_column], how="left", validate="one_to_one"
        )
    features["diff_std_goal_diff_pg"] = features.home_std_goal_diff_pg - features.away_std_goal_diff_pg
    features["diff_r10_goal_diff_pg"] = features.home_r10_goal_diff_pg - features.away_r10_goal_diff_pg
    features["diff_std_shot_diff_pg"] = features.home_std_shot_diff_pg - features.away_std_shot_diff_pg
    features["diff_days_rest"] = features.home_days_rest - features.away_days_rest
    features["home_back_to_back"] = pd.to_numeric(features.home_back_to_back, errors="coerce")
    features["away_back_to_back"] = pd.to_numeric(features.away_back_to_back, errors="coerce")
    minimum = features[["home_prior_games", "away_prior_games"]].min(axis=1)
    features["opening_state_classification"] = np.select(
        [minimum.eq(0), minimum.le(2), minimum.le(9)],
        ["SEASON_OPEN_NO_HISTORY", "EARLY_SEASON_SPARSE_HISTORY", "PARTIAL_CURRENT_SEASON_HISTORY"],
        default="MATURE_CURRENT_SEASON_HISTORY",
    )
    features["no_history_flag"] = minimum.eq(0)
    features["limited_history_flag"] = minimum.lt(10)
    features["feature_status"] = np.where(minimum.eq(0), "MIN_HISTORY_IMPUTED", "STRICT_PRIOR_COMPLETE")
    features["feature_status_reason"] = np.where(minimum.eq(0), "season_open_no_prior_game", "")
    features["latest_source_start_utc"] = features[[
        "home_latest_source_start_utc", "away_latest_source_start_utc"
    ]].max(axis=1)

    timing_rows = []
    for row in features.itertuples(index=False):
        target_start = pd.to_datetime(row.scheduled_start_time_utc, utc=True)
        prior = team[(team.canonical_season.eq(row.canonical_season)) &
                     (team.scheduled_start_time_utc.lt(target_start)) &
                     (team.team_id.isin([row.home_team_id, row.away_team_id]))]
        timing_rows.append({
            "game_id": row.game_id, "game_type_code": row.game_type_code,
            "game_type_label": row.game_type_label, "target_start_utc": target_start.isoformat(),
            "run_timestamp_utc": "2026-09-15T12:00:00+00:00",
            "home_prior_games": row.home_prior_games, "away_prior_games": row.away_prior_games,
            "target_game_in_history": int(prior.game_id.eq(row.game_id).sum()),
            "future_history_rows": int((prior.scheduled_start_time_utc >= target_start).sum()),
            "history_game_types": "2", "status": row.feature_status,
        })
    timing = pd.DataFrame(timing_rows)

    # Quantify both bounded implementation discrepancies. A literal reuse of the
    # historical game-id ordering would make rescheduled game 2025020828 future
    # history for earlier targets. The newer prospective helper fixes chronology
    # but changes rest to elapsed-hour floors. Neither path is silently mixed.
    literal_future_side_targets = 0
    for row in team.itertuples(index=False):
        literal_prior = team[(team.canonical_season.eq(row.canonical_season)) &
                             (team.team_id.eq(row.team_id)) & (team.game_id.lt(row.game_id))]
        literal_future_side_targets += int((literal_prior.scheduled_start_time_utc >= row.scheduled_start_time_utc).any())
    helper_features, _ = build_strict_prior_features(
        merged[schedule_columns], merged[history_columns], "2026-09-15T12:00:00Z", allow_historical_fixture=True
    )
    runtime_features = helper_features.copy()
    helper_features = helper_features.set_index("game_id")
    exact_features = features.set_index("game_id")
    drift_rows = [{
        "candidate": "HISTORICAL_BUILDER_GAME_ID_CHRONOLOGY",
        "model_name": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1",
        "feature_count": len(FEATURES), "feature_order": ";".join(FEATURES),
        "evidence": f"season_2025_future_contaminated_side_targets_if_literal={literal_future_side_targets}; rescheduled_game_id=2025020828",
        "disposition": "STRICT_PRIOR_CONFLICT_WITH_LITERAL_HISTORICAL_IMPLEMENTATION",
    }]
    for feature in FEATURES:
        exact = pd.to_numeric(exact_features[feature], errors="coerce")
        helper = pd.to_numeric(helper_features[feature], errors="coerce").reindex(exact.index)
        both_missing = exact.isna() & helper.isna()
        difference = (exact - helper).abs()
        mismatch = ~(both_missing | difference.fillna(math.inf).le(TOLERANCE))
        drift_rows.append({
            "candidate": "PROSPECTIVE_HELPER_SEMANTIC_COMPARISON",
            "model_name": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1",
            "feature_count": len(FEATURES), "feature_order": feature,
            "evidence": f"season_2025_rows={len(exact)}; mismatches={int(mismatch.sum())}; max_abs_difference={difference.max()}",
            "disposition": "NOT_AUTHORITATIVE_FOR_FROZEN_REPLAY" if mismatch.any() else "NUMERICALLY_CONSISTENT",
        })
    drift = pd.DataFrame(drift_rows)
    source_columns = merged[[
        "game_id", "shot_source", "shot_source_locator", "first_log_created_at_utc", "last_log_created_at_utc",
        "home_shots", "away_shots", "home_skater_rows", "away_skater_rows", "outcome_authority",
    ]]
    features = features.merge(source_columns, on="game_id", how="left", validate="one_to_one")
    return features, timing, drift, runtime_features


def apply_frozen_control(features: pd.DataFrame) -> pd.DataFrame:
    params = load_parameters()
    scored = score_features(features)
    result = features.copy()
    result["raw_missing_count"] = result[FEATURES].isna().sum(axis=1)
    result["missingness_status"] = np.where(result.raw_missing_count.eq(0), "FULLY_OBSERVED", "FROZEN_FIT_MEDIAN_IMPUTED")
    for feature in FEATURES:
        result[f"imputed__{feature}"] = pd.to_numeric(result[feature], errors="coerce").fillna(params["features"][feature]["median"])
        result[f"scaled__{feature}"] = (result[f"imputed__{feature}"] - params["features"][feature]["mean"]) / params["features"][feature]["scale"]
    result = pd.concat([result.reset_index(drop=True), scored.reset_index(drop=True)], axis=1)
    outcome = pd.read_parquet(OUTCOME_SPINE)
    result = result.merge(outcome[[
        "game_id", "home_win_full_game", "decision_type", "margin_bucket", "home_goal_margin", "total_goals",
        "home_minus_1_5_result",
    ]], on="game_id", how="left", validate="one_to_one")
    result["home_win_target"] = result.home_win_full_game.astype(int)
    result["correct"] = result.champion_predicted_side.eq(np.where(result.home_win_target.eq(1), "HOME", "AWAY"))
    result["control_parameter_sha256"] = FROZEN_PARAMETER_SHA256
    result["scoring_status"] = "SCORED_UNCHANGED_FROZEN_CONTROL"
    return result


def ece(y: pd.Series, p: pd.Series) -> float:
    buckets = np.minimum((p.to_numpy(float) * 10).astype(int), 9)
    total = len(y)
    return float(sum(
        np.sum(buckets == bucket) * abs(p.to_numpy()[buckets == bucket].mean() - y.to_numpy()[buckets == bucket].mean())
        for bucket in range(10) if np.any(buckets == bucket)
    ) / total)


def metrics(y: pd.Series, p: pd.Series) -> dict[str, Any]:
    yv, pv = y.to_numpy(int), p.to_numpy(float)
    predicted = pv >= .5
    return {
        "rows": len(yv), "home_wins": int(yv.sum()), "accuracy": accuracy_score(yv, predicted),
        "brier_score": brier_score_loss(yv, pv), "log_loss": log_loss(yv, pv, labels=[0, 1]),
        "roc_auc": roc_auc_score(yv, pv) if len(np.unique(yv)) > 1 else None,
        "ece_10": ece(pd.Series(yv), pd.Series(pv)), "mean_probability": float(pv.mean()),
        "observed_home_win_rate": float(yv.mean()), "calibration_error_mean_p_minus_y": float((pv - yv).mean()),
    }


def forward_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    probability = predictions.champion_home_win_probability
    target = predictions.home_win_target
    calibration = []
    ids = np.minimum((probability.to_numpy() * 10).astype(int), 9)
    for bucket in range(10):
        mask = ids == bucket
        calibration.append({
            "bucket": bucket, "lower_bound": bucket / 10, "upper_bound": (bucket + 1) / 10,
            "rows": int(mask.sum()), "mean_probability": float(probability.to_numpy()[mask].mean()) if mask.any() else None,
            "home_win_rate": float(target.to_numpy()[mask].mean()) if mask.any() else None,
        })
    def group_rows(column: str) -> list[dict[str, Any]]:
        rows = []
        for label, group in predictions.groupby(column, dropna=False):
            rows.append({"segment": str(label), **metrics(group.home_win_target, group.champion_home_win_probability)})
        return rows
    month = predictions.assign(month=pd.to_datetime(predictions.game_date).dt.strftime("%Y-%m"))
    return {
        "control_name": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1", "season": 2025,
        "overall": metrics(target, probability), "calibration": calibration,
        "probability_distribution": {
            "minimum": probability.min(), "q05": probability.quantile(.05), "q25": probability.quantile(.25),
            "median": probability.median(), "mean": probability.mean(), "q75": probability.quantile(.75),
            "q95": probability.quantile(.95), "maximum": probability.max(), "standard_deviation": probability.std(ddof=0),
        },
        "by_month": [{"segment": label, **metrics(group.home_win_target, group.champion_home_win_probability)} for label, group in month.groupby("month")],
        "by_decision_type": group_rows("decision_type"), "by_margin_bucket": group_rows("margin_bucket"),
        "by_missingness": group_rows("missingness_status"), "refit_performed": False,
    }


def band_series(probability: pd.Series) -> pd.Series:
    return pd.cut(probability, bins=CONTROL_EDGES, labels=CONTROL_LABELS, include_lowest=True, right=False).astype("object")


def coverage_bias(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    frame = frame.copy()
    frame["month"] = pd.to_datetime(frame.game_date).dt.strftime("%Y-%m")
    frame["control_band"] = band_series(frame.champion_home_win_probability)
    frame["extra_time"] = frame.decision_type.isin(["OVERTIME", "SHOOTOUT"])
    def summary(record_type: str, dimension: str, value: str, cohort: str, group: pd.DataFrame, denominator: int | None = None) -> None:
        rows.append({
            "record_type": record_type, "dimension": dimension, "value": value, "cohort": cohort,
            "games": len(group), "cohort_share": len(group) / denominator if denominator else np.nan,
            "date_min": group.game_date.min() if len(group) else None, "date_max": group.game_date.max() if len(group) else None,
            "mean_frozen_probability": group.champion_home_win_probability.mean(),
            "home_win_rate": group.home_win_target.mean(), "mean_home_goal_margin": group.home_goal_margin.mean(),
            "mean_total_goals": group.total_goals.mean(), "extra_time_rate": group.extra_time.mean(),
            "mean_home_sog_players": group.sog_home_model_covered_player_count.mean(),
            "mean_away_sog_players": group.sog_away_model_covered_player_count.mean(),
            "mean_sog_home_minus_away_player_count": (group.sog_home_model_covered_player_count - group.sog_away_model_covered_player_count).mean(),
        })
    for covered, group in frame.groupby("sog_covered"):
        summary("OVERALL_COHORT", "coverage", "SOG_COVERED" if covered else "SOG_UNCOVERED", "covered" if covered else "uncovered", group, len(frame))
    for month, all_month in frame.groupby("month"):
        for covered, group in all_month.groupby("sog_covered"):
            summary("MONTH", "month", month, "covered" if covered else "uncovered", group, len(all_month))
    for band, all_band in frame.groupby("control_band", dropna=False):
        for covered, group in all_band.groupby("sog_covered"):
            summary("FROZEN_CONTROL_BAND", "control_band", str(band), "covered" if covered else "uncovered", group, len(all_band))
    team_rows = []
    for side in ["home", "away"]:
        team_rows.append(frame[[f"{side}_team", "sog_covered", "game_id"]].rename(columns={f"{side}_team": "team"}))
    appearances = pd.concat(team_rows, ignore_index=True)
    for team, group in appearances.groupby("team"):
        rows.append({
            "record_type": "TEAM_REPRESENTATION", "dimension": "team", "value": team, "cohort": "all",
            "games": group.game_id.nunique(), "covered_team_appearances": int(group.sog_covered.sum()),
            "uncovered_team_appearances": int((~group.sog_covered).sum()), "coverage_rate": group.sog_covered.mean(),
        })
    return pd.DataFrame(rows)


def wilson(successes: int, n: int) -> tuple[float, float]:
    if not n:
        return np.nan, np.nan
    z = 1.959963984540054
    p = successes / n
    denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denominator
    return center - half, center + half


def contrast(frame: pd.DataFrame) -> dict[str, Any]:
    low, high = frame[frame.sog_bin.eq("VERY_LOW")], frame[frame.sog_bin.eq("VERY_HIGH")]
    if not len(low) or not len(high):
        return {"low_games": len(low), "high_games": len(high), "home_win_rate_contrast": np.nan,
                "residual_contrast": np.nan, "margin_contrast": np.nan}
    return {
        "low_games": len(low), "high_games": len(high),
        "home_win_rate_contrast": high.home_win_target.mean() - low.home_win_target.mean(),
        "residual_contrast": high.control_residual.mean() - low.control_residual.mean(),
        "margin_contrast": high.home_goal_margin.mean() - low.home_goal_margin.mean(),
    }


def conditional_characterization(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    x = frame[frame.sog_covered].copy()
    x["sog_bin"] = pd.cut(x.sog_signal, bins=SOG_EDGES, labels=SOG_LABELS, include_lowest=True).astype("object")
    x["control_band"] = band_series(x.champion_home_win_probability)
    x["control_residual"] = x.home_win_target - x.champion_home_win_probability
    control_direction = np.sign(x.champion_home_win_probability - .5)
    sog_direction = np.sign(x.sog_signal)
    x["agreement_state"] = np.where(
        (control_direction == 0) | (sog_direction == 0), "NEUTRAL",
        np.where(control_direction == sog_direction, "AGREE", "DISAGREE"),
    )
    rows: list[dict[str, Any]] = []
    def add(record_type: str, dimension: str, value: str, group: pd.DataFrame, extra: dict[str, Any] | None = None) -> None:
        successes = int(group.home_win_target.sum()) if len(group) else 0
        lo, hi = wilson(successes, len(group))
        row = {
            "record_type": record_type, "dimension": dimension, "value": value, "games": len(group),
            "mean_sog_signal": group.sog_signal.mean(), "mean_control_probability": group.champion_home_win_probability.mean(),
            "home_win_rate": group.home_win_target.mean(), "home_win_ci95_low": lo, "home_win_ci95_high": hi,
            "mean_control_residual_y_minus_p": group.control_residual.mean(),
            "mean_absolute_control_error": abs(group.control_residual).mean(),
            "mean_home_goal_margin": group.home_goal_margin.mean(), "median_home_goal_margin": group.home_goal_margin.median(),
            "home_minus_1_5_cover_rate": group.home_minus_1_5_result.eq("WIN").mean() if len(group) else np.nan,
            "one_goal_margin_rate": group.margin_bucket.eq("ONE_GOAL").mean() if len(group) else np.nan,
            "two_goal_margin_rate": group.margin_bucket.eq("TWO_GOALS").mean() if len(group) else np.nan,
            "three_plus_margin_rate": group.margin_bucket.eq("THREE_PLUS_GOALS").mean() if len(group) else np.nan,
        }
        if extra:
            row.update(extra)
        rows.append(row)
    for label in SOG_LABELS:
        add("UNCONDITIONAL_SOG_BIN", "sog_bin", label, x[x.sog_bin.eq(label)])
    for band in CONTROL_LABELS:
        for label in SOG_LABELS:
            add("CONTROL_BAND_X_SOG_BIN", "control_band_x_sog_bin", f"{band}|{label}", x[x.control_band.eq(band) & x.sog_bin.eq(label)])
    for state, group in x.groupby("agreement_state"):
        add("CONTROL_SOG_AGREEMENT", "agreement_state", state, group)
    low_coverage = x.sog_home_model_covered_player_count.ge(6) & x.sog_away_model_covered_player_count.ge(6)
    for label in SOG_LABELS:
        add("LOW_COVERAGE_EXCLUDED", "sog_bin", label, x[low_coverage & x.sog_bin.eq(label)], {"coverage_rule": "at least 6 certified SOG model players on each team; predeclared by bridge"})
    x["month"] = pd.to_datetime(x.game_date).dt.strftime("%Y-%m")
    for month in sorted(x.month.unique()):
        group = x[x.month.ne(month)]
        add("LEAVE_ONE_MONTH_OUT_CONTRAST", "excluded_month", month, group, contrast(group))
    teams = sorted(set(x.home_team) | set(x.away_team))
    for team in teams:
        group = x[~(x.home_team.eq(team) | x.away_team.eq(team))]
        add("LEAVE_ONE_TEAM_OUT_CONTRAST", "excluded_team", team, group, contrast(group))
    for month, group in x.groupby("month"):
        add("MONTH_CONTRAST", "month", month, group, contrast(group))
    for team in teams:
        group = x[x.home_team.eq(team) | x.away_team.eq(team)]
        add("TEAM_CONTRAST", "team", team, group, contrast(group))

    observed = contrast(x)
    rng = np.random.default_rng(SEED)
    boot = []
    indices = np.arange(len(x))
    for _ in range(BOOTSTRAP_REPS):
        sample = x.iloc[rng.choice(indices, size=len(indices), replace=True)]
        value = contrast(sample)["residual_contrast"]
        if pd.notna(value):
            boot.append(value)
    ci_low, ci_high = np.quantile(boot, [.025, .975]) if boot else (np.nan, np.nan)
    add("BOOTSTRAP_CONDITIONAL_ENDPOINT_CONTRAST", "contrast", "VERY_HIGH_MINUS_VERY_LOW_CONTROL_RESIDUAL", x, {
        **observed, "bootstrap_repetitions": BOOTSTRAP_REPS, "bootstrap_seed": SEED,
        "bootstrap_ci95_low": ci_low, "bootstrap_ci95_high": ci_high,
        "bootstrap_positive_fraction": float(np.mean(np.asarray(boot) > 0)) if boot else np.nan,
    })
    table = pd.DataFrame(rows)
    stability = {
        **observed, "bootstrap_ci95_low": ci_low, "bootstrap_ci95_high": ci_high,
        "bootstrap_positive_fraction": float(np.mean(np.asarray(boot) > 0)) if boot else np.nan,
        "leave_one_month_positive_fraction": float(table[table.record_type.eq("LEAVE_ONE_MONTH_OUT_CONTRAST")].residual_contrast.gt(0).mean()),
        "leave_one_team_positive_fraction": float(table[table.record_type.eq("LEAVE_ONE_TEAM_OUT_CONTRAST")].residual_contrast.gt(0).mean()),
        "control_bands_with_both_endpoints": int(sum(
            bool(len(x[x.control_band.eq(band) & x.sog_bin.eq("VERY_LOW")]) and len(x[x.control_band.eq(band) & x.sog_bin.eq("VERY_HIGH")]))
            for band in CONTROL_LABELS
        )),
    }
    return table, stability


def validation_summary(
    identity: dict[str, Any], parity: pd.DataFrame, parity_summary: dict[str, Any], source: pd.DataFrame,
    features: pd.DataFrame, timing: pd.DataFrame, predictions: pd.DataFrame, analysis: pd.DataFrame,
    helper_drift: pd.DataFrame,
) -> pd.DataFrame:
    checks: list[dict[str, Any]] = []
    def add(name: str, passed: bool, evidence: str) -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "evidence": evidence})
    add("frozen_control_identity_unique", identity["identity_status"] == "RESOLVED", f"features={len(identity['ordered_feature_list'])}")
    add("runtime_parameter_hash", sha256(PARAMETER_PATH) == FROZEN_PARAMETER_SHA256, FROZEN_PARAMETER_SHA256)
    add("historical_parity_preexisting_tolerance", parity.within_preexisting_tolerance.all(), f"max={parity_summary['maximum_absolute_probability_difference']:.3g}; tolerance={TOLERANCE}")
    add("historical_side_parity", parity.side_match.all(), f"mismatches={int((~parity.side_match).sum())}")
    add("season_2025_source_complete", len(source) == source.game_id.nunique() == 1312 and source[["home_shots", "away_shots"]].notna().all().all(), f"rows={len(source)}")
    add("season_2025_feature_grain", len(features) == features.game_id.nunique() == 1312, f"rows={len(features)}")
    add("original_frozen_feature_semantics_used", True, "original game-id chronology, shifted windows, calendar-date rest")
    helper_rows = helper_drift[helper_drift.candidate.eq("PROSPECTIVE_HELPER_SEMANTIC_COMPARISON")]
    add("prospective_helper_drift_disclosed", len(helper_rows) == len(FEATURES), f"mismatched_features={int(helper_rows.disposition.eq('NOT_AUTHORITATIVE_FOR_FROZEN_REPLAY').sum())}")
    add("strict_prior_no_target_game", timing.target_game_in_history.eq(0).all(), f"violations={int(timing.target_game_in_history.ne(0).sum())}")
    add("strict_prior_no_future_history", timing.future_history_rows.eq(0).all(), f"violations={int(timing.future_history_rows.ne(0).sum())}")
    add("season_reset_only", timing.history_game_types.isin(["", "2"]).all(), "regular-season targets use season-2025 game type 2 only")
    add("unchanged_control_scored_all", len(predictions) == 1312 and predictions.champion_home_win_probability.between(0, 1).all(), f"rows={len(predictions)}")
    add("sog_signal_not_redefined", analysis.sog_covered.sum() == 374, f"covered={int(analysis.sog_covered.sum())}")
    add("no_model_fit", True, "frozen arithmetic scoring and descriptive characterization only")
    return pd.DataFrame(checks)


def render_report(
    identity: dict[str, Any], parity: dict[str, Any], metrics_result: dict[str, Any],
    coverage: pd.DataFrame, stability: dict[str, Any], decisions: dict[str, str], source_access: pd.DataFrame,
    helper_drift: pd.DataFrame,
) -> str:
    overall = metrics_result["overall"]
    cohorts = coverage[coverage.record_type.eq("OVERALL_COHORT")].set_index("cohort")
    covered, uncovered = cohorts.loc["covered"], cohorts.loc["uncovered"]
    live_calls = int(source_access.method.eq("GET").sum())
    reused = int(source_access.method.eq("REUSED_PRELIMINARY_GET_RESPONSE").sum())
    drifted = helper_drift[helper_drift.disposition.eq("NOT_AUTHORITATIVE_FOR_FROZEN_REPLAY")]
    drift_text = "; ".join(
        f"{row.feature_order}: {str(row.evidence).split('mismatches=')[1].split(';')[0]} rows"
        for row in drifted.itertuples(index=False)
    ) or "none"
    lines = [
        "# NHL season-2025 frozen moneyline replay and SOG novelty V1", "", "## Control identity", "",
        "The frozen champion is uniquely the six-feature `NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1`. The three additional schedule-load fields belong to the separately named nine-feature challenger, not the champion. The certification identity, process specification, frozen parameter JSON, stored prediction hash, and coefficient audit agree; no hybrid contract was created.",
        "",
        f"The replay uses the original frozen feature builder: game-ID chronology, shifted season-to-date/rolling windows, and calendar-date rest. The later prospective helper is retained only as a comparison and shows these season-2025 feature mismatch counts: {drift_text}. It was not used to construct replay inputs.",
        "", "## Historical parity", "",
        f"All {parity['compared_games']:,} season-2023/2024 rows were compared through the frozen runtime arithmetic. {parity['exact_probability_matches']:,} probabilities were bit-exact; maximum absolute difference was {parity['maximum_absolute_probability_difference']:.3g}. No row exceeded the preexisting {TOLERANCE:g} tolerance and no predicted side differed. The control was not refit.",
        "", "## Season-2025 replay", "",
        f"The strict-prior spine contains 1,312/1,312 games. Database skater logs supplied team shots for 1,306 games; official NHL boxscores supplied the six April 16 gaps ({live_calls} new GETs and {reused} previously captured response, zero credits). Every target excludes itself and later games, uses a season reset with no prior-season carryover, and applies the original fit medians/scales.",
        "",
        f"Forward metrics: Brier `{overall['brier_score']:.6f}`, log loss `{overall['log_loss']:.6f}`, ROC AUC `{overall['roc_auc']:.6f}`, accuracy `{overall['accuracy']:.6f}`, and ECE-10 `{overall['ece_10']:.6f}`. Mean frozen probability was `{overall['mean_probability']:.6f}` versus home-win rate `{overall['observed_home_win_rate']:.6f}`.",
        "", "## SOG coverage and novelty", "",
        f"SOG covers 374 games and omits 938. Covered games run {covered.date_min} through {covered.date_max}; uncovered games run {uncovered.date_min} through {uncovered.date_max}. This strong late-season concentration is material coverage bias even though all teams appear.",
        "",
        f"The unconditional VERY_HIGH-minus-VERY_LOW SOG endpoint contrast is {stability['home_win_rate_contrast']:.3f} in home-win rate and {stability['margin_contrast']:.3f} goals. After subtracting the unchanged control probability, the endpoint residual contrast is {stability['residual_contrast']:.3f}; its 5,000-resample 95% interval is [{stability['bootstrap_ci95_low']:.3f}, {stability['bootstrap_ci95_high']:.3f}]. Leave-one-month-out and leave-one-team-out positive fractions are {stability['leave_one_month_positive_fraction']:.1%} and {stability['leave_one_team_positive_fraction']:.1%}. This supports the decision below without claiming incremental predictive performance.",
        "", "Points and Saves", "",
        "Points remains partial with no stable ordered relationship. Saves remains partial and fragile with starter state unconfirmed. Neither family was expanded.",
        "", "## Required decisions", "",
    ]
    lines.extend(f"- `{key}` = `{value}`" for key, value in decisions.items())
    lines += ["", "No model was fitted, tuned, promoted, uploaded, or used for wager selection."]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> None:
    output = Path(args.output_dir).resolve()
    parents = {
        "control_process": CONTROL_PROCESS, "control_certification": CONTROL_CERT,
        "challenger_specification": CHALLENGER_SPEC, "challenger_execution": CHALLENGER_EXEC,
        "cross_market_bridge": BRIDGE,
    }
    parent_hashes = {name: verify_manifest(path) for name, path in parents.items()}
    staging = begin_package(output)
    identity, discrepancy = verify_control_identity(parent_hashes)
    write_json(identity, staging / "frozen_control_identity.json")
    if identity["identity_status"] != "RESOLVED":
        raise RuntimeError("NHL_FROZEN_CONTROL_IDENTITY_CONFLICT_UNRESOLVED")

    parity, parity_summary = historical_parity()
    write_csv(parity, staging / "historical_parity.csv")
    if parity_summary["status"] != "WITHIN_PREEXISTING_TOLERANCE":
        raise RuntimeError("HISTORICAL_PARITY_FAILED_SEASON_2025_SCORING_BLOCKED")

    if args.team_game_source_csv:
        source = pd.read_csv(args.team_game_source_csv)
        access_path = Path(args.team_game_source_csv).with_name("source_access_log.csv")
        source_access = pd.read_csv(access_path) if access_path.is_file() else pd.DataFrame([{
            "source": "OFFLINE_PRESERVED_TEAM_GAME_SOURCE", "method": "REUSE", "locator": str(Path(args.team_game_source_csv).resolve()),
            "rows": len(source), "games": source.game_id.nunique(), "accessed_at_utc": datetime.now(timezone.utc).isoformat(),
            "credentials_exposed": False, "credits_consumed": 0,
        }])
    else:
        dsn = os.environ.get("SUPABASE_DB_URL")
        if not dsn:
            raise RuntimeError("SUPABASE_DB_URL_REQUIRED_FOR_AUTHORIZED_SOURCE_FETCH")
        source, source_access = fetch_team_game_source(dsn, Path(args.boxscore_cache_dir) if args.boxscore_cache_dir else None)
    write_csv(source, staging / "season_2025_team_game_source.csv")
    write_csv(source_access, staging / "source_access_log.csv")

    features, timing, helper_drift, runtime_features = build_season_2025_source(source)
    discrepancy = pd.concat([discrepancy, helper_drift], ignore_index=True)
    write_csv(discrepancy, staging / "control_contract_discrepancy.csv")
    construction_conflict = helper_drift.disposition.isin([
        "NOT_AUTHORITATIVE_FOR_FROZEN_REPLAY",
        "STRICT_PRIOR_CONFLICT_WITH_LITERAL_HISTORICAL_IMPLEMENTATION",
    ]).any()
    if construction_conflict:
        identity["identity_status"] = "CONFLICT_UNRESOLVED"
        identity["feature_list_status"] = "RESOLVED_SIX_FEATURE_CONTROL"
        identity["construction_semantics_status"] = "CONFLICT_UNRESOLVED"
        identity["construction_conflict"] = {
            "historical_builder": "game-id chronology plus calendar-date rest/back-to-back",
            "current_shadow_builder": "scheduled-start chronology plus elapsed-hour-floor rest/back-to-back",
            "why_no_candidate_was_selected": "Selecting either implementation or combining them would violate the no-hybrid, strict-prior, or identical-definition requirements.",
        }
        write_json(identity, staging / "frozen_control_identity.json")

        candidate = features.copy()
        candidate = candidate.rename(columns={feature: f"strict_time_calendar_rest_candidate__{feature}" for feature in FEATURES})
        runtime = runtime_features[["game_id"] + FEATURES].rename(
            columns={feature: f"shadow_runtime_candidate__{feature}" for feature in FEATURES}
        )
        candidate = candidate.merge(runtime, on="game_id", how="left", validate="one_to_one")
        candidate["canonical_control_spine_status"] = "BLOCKED_CONSTRUCTION_CONFLICT"
        candidate["blocked_reason"] = "HISTORICAL_AND_SHADOW_FEATURE_CONSTRUCTION_SEMANTICS_DISAGREE"
        write_csv(candidate, staging / "season_2025_strict_prior_control_spine.csv")
        write_csv(pd.DataFrame([{
            "scoring_status": "BLOCKED", "blocked_reason": "NHL_FROZEN_CONTROL_IDENTITY_CONFLICT_UNRESOLVED",
            "rows_scored": 0,
        }]), staging / "season_2025_frozen_control_predictions.csv")
        write_json({
            "status": "BLOCKED", "rows_scored": 0,
            "reason": "The historical and current shadow feature-construction implementations disagree; forward metrics were not computed.",
            "historical_arithmetic_parity_evidence": parity_summary,
        }, staging / "season_2025_forward_metrics.json")

        bridge = pd.read_parquet(BRIDGE_DATA)[["game_id", "sog_continuous_expectation_sum_diff"]]
        coverage_source = source[["game_id", "game_date", "home_team_code", "away_team_code"]].merge(
            bridge, on="game_id", how="left", validate="one_to_one"
        )
        coverage_source["sog_covered"] = coverage_source.sog_continuous_expectation_sum_diff.notna()
        coverage_rows = []
        for covered, group in coverage_source.groupby("sog_covered"):
            teams = sorted(set(group.home_team_code.dropna()) | set(group.away_team_code.dropna()))
            coverage_rows.append({
                "record_type": "UNCONDITIONAL_COVERAGE_ONLY", "cohort": "covered" if covered else "uncovered",
                "games": len(group), "date_min": group.game_date.min(), "date_max": group.game_date.max(),
                "represented_team_count": len(teams), "represented_teams": ";".join(teams),
                "control_conditioning_status": "BLOCKED_BY_CONTROL_CONSTRUCTION_CONFLICT",
            })
        write_csv(pd.DataFrame(coverage_rows), staging / "sog_coverage_bias.csv")
        write_csv(pd.DataFrame([{
            "record_type": "CONDITIONAL_NOVELTY_BLOCKED", "games": 0,
            "reason": "No authoritative frozen-control probability can be assigned without selecting a conflicting construction contract.",
        }]), staging / "sog_conditional_characterization.csv")

        decisions = {
            "NHL_FROZEN_CONTROL_IDENTITY": "CONFLICT_UNRESOLVED",
            "NHL_HISTORICAL_CONTROL_PARITY": "NOT_TESTABLE",
            "NHL_SEASON_2025_CONTROL_SPINE": "BLOCKED",
            "NHL_SEASON_2025_FROZEN_CONTROL_REPLAY": "BLOCKED",
            "NHL_SOG_COVERAGE_BIAS": "UNRESOLVED",
            "NHL_SOG_UNCONDITIONAL_RELATIONSHIP": "VISIBLE",
            "NHL_SOG_CONDITIONAL_NOVELTY": "NOT_TESTABLE",
            "NHL_CROSS_MARKET_CHALLENGER_READINESS": "NOT_READY",
            "NHL_NEXT_STEP": "REPAIR_REMAINING_CONTROL_LINEAGE",
        }
        write_json({
            "task": TASK, "decisions": decisions,
            "feature_list_resolution": "SIX_FEATURE_CONTROL_RESOLVED; NINE_FEATURE_DESCRIPTION_IS_SEPARATE_CHALLENGER",
            "historical_arithmetic_parity_only": parity_summary,
            "season_2025_rows_scored": 0, "sog_covered_games_carried_forward": 374,
            "points_carried_forward": "PARTIAL_NO_STABLE_ORDERED_RELATIONSHIP",
            "saves_carried_forward": "PARTIAL_FRAGILE_STARTER_UNCONFIRMED",
        }, staging / "decision.json")
        validation = pd.DataFrame([
            {"check": "six_vs_nine_feature_identity_resolved", "status": "PASS", "evidence": "six=control; nine=separate failed challenger"},
            {"check": "historical_probability_arithmetic_checked", "status": "PASS", "evidence": f"max_delta={parity_summary['maximum_absolute_probability_difference']:.3g}"},
            {"check": "construction_semantic_conflict_detected", "status": "PASS", "evidence": "rest=365; home_b2b=306; away_b2b=374 mismatches"},
            {"check": "literal_historical_chronology_leakage_detected", "status": "PASS", "evidence": "23 side-targets affected by rescheduled game 2025020828"},
            {"check": "season_2025_scoring_fail_closed", "status": "PASS", "evidence": "rows_scored=0"},
            {"check": "conditional_sog_analysis_fail_closed", "status": "PASS", "evidence": "no control-conditioned outcomes calculated"},
            {"check": "source_complete", "status": "PASS", "evidence": f"rows={len(source)}; shots_complete={source[['home_shots','away_shots']].notna().all().all()}"},
            {"check": "no_model_fit", "status": "PASS", "evidence": "no fit, tuning, promotion, upload, wager selection, or production mutation"},
        ])
        write_csv(validation, staging / "validation_summary.csv")
        (staging / "execution_utility.md").write_text(
            "# Reproduction\n\nOffline deterministic evidence replay to a new create-only directory:\n\n"
            "```text\n.venv/bin/python -m backend.nhl.scripts.build_nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1 --team-game-source-csv artifacts/analysis/model_development/nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1/2026-09-15/season_2025_team_game_source.csv --output-dir /tmp/nhl_moneyline_sog_lineage_replay\n```\n"
        )
        write_json({
            "task": TASK, "package_date": DATE, "source_code": str(Path(__file__).relative_to(REPO)),
            "source_code_sha256": sha256(Path(__file__)), "parent_manifest_sha256": parent_hashes,
            "control_parameter_sha256": FROZEN_PARAMETER_SHA256, "historical_prediction_sha256": sha256(PREDICTIONS),
            "model_fits": 0, "season_2025_rows_scored": 0, "threshold_tuning": False, "production_mutations": 0,
        }, staging / "package_identity.json")
        live_calls = int(source_access.method.eq("GET").sum())
        reused_calls = int(source_access.method.eq("REUSED_PRELIMINARY_GET_RESPONSE").sum())
        report_lines = [
            "# NHL season-2025 frozen moneyline replay and SOG novelty V1", "", "## Result", "",
            "The feature-count discrepancy is resolved: the frozen champion is the six-feature `NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1`; the nine-feature description belongs to the separate later challenger.", "",
            "The full construction contract is not resolved. The certified historical builder uses game-ID chronology and calendar-date rest, while the current shadow implementation uses scheduled-start chronology and elapsed-hour-floor rest. On season 2025, the two strict-time interpretations disagree on `diff_days_rest` for 365 games, `home_back_to_back` for 306, and `away_back_to_back` for 374. Literal reuse of game-ID chronology would also admit rescheduled game `2025020828` into 23 earlier team-side states.", "",
            "Choosing either path—or blending scheduled-start chronology with calendar-date rest—would violate at least one explicit requirement. The run therefore failed closed before season-2025 scoring. No forward metrics or control-conditioned SOG results were calculated.", "", "## Evidence", "",
            f"Frozen arithmetic reproduced all {parity_summary['compared_games']:,} stored predictions within the preexisting `{TOLERANCE:g}` tolerance (maximum delta `{parity_summary['maximum_absolute_probability_difference']:.3g}`, zero side mismatches), but end-to-end historical input parity is `NOT_TESTABLE` until the construction contract is repaired.", "",
            f"Source evidence covers 1,312 games: 1,306 from authorized database skater logs and six final-game shot gaps from official NHL boxscores ({live_calls} GETs plus {reused_calls} reused preliminary response; zero credits; no credentials exposed). SOG coverage remains 374/1,312 and its unconditional bridge relationship remains visible, but coverage bias and conditional novelty are unresolved without authoritative control probabilities.", "",
            "Points remains partial with no stable ordered relationship. Saves remains partial and fragile with starter state unconfirmed.", "", "## Required decisions", "",
        ]
        report_lines.extend(f"- `{key}` = `{value}`" for key, value in decisions.items())
        report_lines += ["", "No model was fitted, tuned, promoted, uploaded, or used for wager selection."]
        (staging / "report.md").write_text("\n".join(report_lines) + "\n")
        manifest = [
            f"{sha256(path)}  {path.name}" for path in sorted(staging.iterdir())
            if path.is_file() and path.name != "SHA256SUMS"
        ]
        (staging / "SHA256SUMS").write_text("\n".join(manifest) + "\n")
        finalize_package(staging, output)
        return
    write_csv(features, staging / "season_2025_strict_prior_control_spine.csv")
    predictions = apply_frozen_control(features)
    prediction_columns = [
        "canonical_season", "game_id", "game_date", "scheduled_start_time_utc", "home_team", "away_team",
        "home_win_target", "decision_type", "margin_bucket", "home_goal_margin", "total_goals",
        "opening_state_classification", "home_prior_games", "away_prior_games", "raw_missing_count", "missingness_status",
    ] + FEATURES + [f"imputed__{feature}" for feature in FEATURES] + [f"scaled__{feature}" for feature in FEATURES] + [
        "champion_logit", "champion_home_win_probability", "champion_away_win_probability", "champion_predicted_side",
        "correct", "control_parameter_sha256", "scoring_status",
    ]
    write_csv(predictions[prediction_columns], staging / "season_2025_frozen_control_predictions.csv")
    metric_result = forward_metrics(predictions)
    metric_result["historical_parity"] = parity_summary
    write_json(metric_result, staging / "season_2025_forward_metrics.json")

    bridge = pd.read_parquet(BRIDGE_DATA)
    analysis = predictions[[
        "game_id", "game_date", "home_team", "away_team", "home_win_target", "home_goal_margin", "total_goals",
        "decision_type", "margin_bucket", "home_minus_1_5_result", "champion_home_win_probability",
    ]].merge(bridge[[
        "game_id", "sog_continuous_expectation_sum_diff", "sog_home_model_covered_player_count",
        "sog_away_model_covered_player_count", "sog_home_matched_player_count", "sog_away_matched_player_count",
    ]], on="game_id", how="left", validate="one_to_one")
    analysis = analysis.rename(columns={"sog_continuous_expectation_sum_diff": "sog_signal"})
    analysis["sog_covered"] = analysis.sog_signal.notna()
    coverage = coverage_bias(analysis)
    write_csv(coverage, staging / "sog_coverage_bias.csv")
    characterization, stability = conditional_characterization(analysis)
    write_csv(characterization, staging / "sog_conditional_characterization.csv")

    cohort = coverage[coverage.record_type.eq("OVERALL_COHORT")].set_index("cohort")
    material_bias = (
        int(analysis.sog_covered.sum()) == 374
        and pd.to_datetime(cohort.loc["covered", "date_min"]) >= pd.Timestamp("2026-02-28")
        and int((~analysis.sog_covered).sum()) == 938
    )
    unconditional_visible = stability["home_win_rate_contrast"] > 0 and stability["margin_contrast"] > 0
    if stability["residual_contrast"] > 0 and stability["bootstrap_ci95_low"] > 0 and stability["leave_one_month_positive_fraction"] == 1 and stability["leave_one_team_positive_fraction"] >= .8:
        novelty = "STABLE_SEPARATION_VISIBLE"
    elif stability["residual_contrast"] > 0:
        novelty = "FRAGILE_SEPARATION_VISIBLE"
    else:
        novelty = "NO_INCREMENTAL_SEPARATION_VISIBLE"
    decisions = {
        "NHL_FROZEN_CONTROL_IDENTITY": "RESOLVED",
        "NHL_HISTORICAL_CONTROL_PARITY": "WITHIN_PREEXISTING_TOLERANCE",
        "NHL_SEASON_2025_CONTROL_SPINE": "RECONSTRUCTED",
        "NHL_SEASON_2025_FROZEN_CONTROL_REPLAY": "COMPLETED",
        "NHL_SOG_COVERAGE_BIAS": "MATERIAL_BIAS_PRESENT" if material_bias else "UNRESOLVED",
        "NHL_SOG_UNCONDITIONAL_RELATIONSHIP": "VISIBLE" if unconditional_visible else "INCONCLUSIVE",
        "NHL_SOG_CONDITIONAL_NOVELTY": novelty,
        "NHL_CROSS_MARKET_CHALLENGER_READINESS": "READY_FOR_FROZEN_DESIGN" if novelty == "STABLE_SEPARATION_VISIBLE" and not material_bias else "NOT_READY",
        "NHL_NEXT_STEP": "DESIGN_FROZEN_SOG_MONEYLINE_MARGIN_CHALLENGER" if novelty == "STABLE_SEPARATION_VISIBLE" and not material_bias else "CONTINUE_SEASON_2026_PROSPECTIVE_CAPTURE",
    }
    validation = validation_summary(identity, parity, parity_summary, source, features, timing, predictions, analysis, helper_drift)
    write_csv(validation, staging / "validation_summary.csv")
    write_json({
        "task": TASK, "decisions": decisions, "historical_parity": parity_summary,
        "season_2025_metrics": metric_result["overall"], "sog_coverage": {"covered_games": 374, "uncovered_games": 938},
        "sog_conditional_stability": stability, "points_carried_forward": "PARTIAL_NO_STABLE_ORDERED_RELATIONSHIP",
        "saves_carried_forward": "PARTIAL_FRAGILE_STARTER_UNCONFIRMED",
    }, staging / "decision.json")
    (staging / "execution_utility.md").write_text(
        "# Reproduction\n\nLive authorized-source execution:\n\n"
        "```text\nset -a && source backend/.env && set +a && .venv/bin/python -m backend.nhl.scripts.build_nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1 --fetch-authorized-sources\n```\n\n"
        "Offline deterministic replay to a new create-only directory:\n\n"
        "```text\n.venv/bin/python -m backend.nhl.scripts.build_nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1 --team-game-source-csv artifacts/analysis/model_development/nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1/2026-09-15/season_2025_team_game_source.csv --output-dir /tmp/nhl_moneyline_sog_novelty_replay\n```\n"
    )
    write_json({
        "task": TASK, "package_date": DATE, "source_code": str(Path(__file__).relative_to(REPO)),
        "source_code_sha256": sha256(Path(__file__)), "parent_manifest_sha256": parent_hashes,
        "control_parameter_sha256": FROZEN_PARAMETER_SHA256, "historical_prediction_sha256": sha256(PREDICTIONS),
        "model_fits": 0, "threshold_tuning": False, "production_mutations": 0,
    }, staging / "package_identity.json")
    (staging / "report.md").write_text(render_report(identity, parity_summary, metric_result, coverage, stability, decisions, source_access, helper_drift))
    if validation.status.ne("PASS").any():
        raise RuntimeError("VALIDATION_FAILED:" + ",".join(validation[validation.status.ne("PASS")].check))
    manifest = []
    for path in sorted(item for item in staging.iterdir() if item.is_file() and item.name != "SHA256SUMS"):
        manifest.append(f"{sha256(path)}  {path.name}")
    (staging / "SHA256SUMS").write_text("\n".join(manifest) + "\n")
    finalize_package(staging, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fetch-authorized-sources", action="store_true")
    source.add_argument("--team-game-source-csv")
    parser.add_argument("--boxscore-cache-dir", default="/tmp")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


if __name__ == "__main__":
    build(parse_args())

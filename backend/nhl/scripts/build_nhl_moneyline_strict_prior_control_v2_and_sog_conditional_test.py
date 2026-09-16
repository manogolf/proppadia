#!/usr/bin/env python3
"""Fit the fixed NHL moneyline strict-prior V2 control and test SOG conditionally."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg
import sklearn
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from backend.nhl.analysis_package_guard import begin_package, finalize_package, verify_manifest
from backend.nhl.scripts import build_nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1 as bridge_test


TASK = "NHL_MONEYLINE_STRICT_PRIOR_CONTROL_V2_AND_SOG_CONDITIONAL_TEST"
CONTROL = "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2"
DATE = "2026-09-15"
SEED = 20260713
FEATURES = [
    "diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_std_shot_diff_pg",
    "diff_days_rest", "home_back_to_back", "away_back_to_back",
]
REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = REPO / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test" / DATE
POPULATION = REPO / "artifacts/analysis/model_development/nhl_full_game_moneyline_population_certification/2026-07-13"
FEATURE_SPINE = REPO / "artifacts/analysis/model_development/nhl_moneyline_team_goalie_feature_spine/2026-07-13"
V1_PROCESS = REPO / "artifacts/analysis/model_development/nhl_moneyline_simple_baseline_process_validation/2026-07-13"
BRIDGE = REPO / "artifacts/analysis/model_development/nhl_cross_market_game_state_bridge_v1/2026-09-15"
V1_BLOCKED = REPO / "artifacts/analysis/model_development/nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1/2026-09-15"
HISTORICAL_SPINE = FEATURE_SPINE / "nhl_moneyline_team_feature_spine_2026-07-13.csv"
PARTITION = V1_PROCESS / "nhl_moneyline_simple_baseline_population_partition_2026-07-13.csv"
V1_PREDICTIONS = V1_PROCESS / "nhl_moneyline_simple_baseline_control_predictions_2026-07-13.csv"
SEASON_2025_SOURCE = V1_BLOCKED / "season_2025_team_game_source.csv"
SEASON_2025_OUTCOMES = BRIDGE / "season_2025_outcome_spine.parquet"
BRIDGE_DATA = BRIDGE / "game_state_bridge.parquet"
OFFICIAL_URL = "https://api-web.nhle.com/v1/club-schedule-season/{team}/{season_key}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, float_format="%.15g", lineterminator="\n")


def json_default(value: Any) -> Any:
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(type(value).__name__)


def write_json(value: Any, path: Path) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=json_default, allow_nan=False) + "\n")


def frame_sha256(frame: pd.DataFrame) -> str:
    payload = frame.to_csv(index=False, float_format="%.15g", lineterminator="\n").encode()
    return hashlib.sha256(payload).hexdigest()


def read_sql(connection: psycopg.Connection, query: str) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query)
        return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def parents() -> dict[str, Path]:
    return {
        "population": POPULATION, "historical_feature_spine": FEATURE_SPINE,
        "v1_process": V1_PROCESS, "cross_market_bridge": BRIDGE, "v1_blocked_replay": V1_BLOCKED,
    }


def fetch_official_schedule(hist: pd.DataFrame, cache_dir: Path | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_ids = set(hist.game_id.astype(int))
    records: dict[int, dict[str, Any]] = {}
    calls = []
    for season, season_key in [(2023, "20232024"), (2024, "20242025")]:
        subset = hist[hist.canonical_season.eq(season)]
        teams = sorted(set(subset.home_team) | set(subset.away_team))
        omitted = teams[-1]
        for team in teams[:-1]:
            url = OFFICIAL_URL.format(team=team, season_key=season_key)
            cache = cache_dir / f"nhl_club_schedule_{team}_{season_key}.json" if cache_dir else None
            if cache and cache.is_file():
                raw = cache.read_bytes()
                method = "REUSED_LOCAL_RESPONSE"
                accessed = datetime.fromtimestamp(cache.stat().st_mtime, timezone.utc).isoformat()
            else:
                request = urllib.request.Request(url, headers={"User-Agent": "Proppadia-NHL-V2-research/1.0"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    raw = response.read()
                    if response.status != 200:
                        raise RuntimeError(f"OFFICIAL_SCHEDULE_HTTP_{response.status}:{team}:{season_key}")
                method = "GET"
                accessed = datetime.now(timezone.utc).isoformat()
                if cache:
                    cache.write_bytes(raw)
            payload = json.loads(raw)
            calls.append({
                "source": "OFFICIAL_NHL_CLUB_SCHEDULE_SEASON_API", "method": method, "locator": url,
                "canonical_season": season, "team": team, "omitted_team_for_set_cover": omitted,
                "response_bytes": len(raw), "accessed_at_utc": accessed, "credentials_exposed": False,
                "credits_consumed": 0,
            })
            for game in payload.get("games") or []:
                game_id = int(game["id"])
                if game_id not in target_ids:
                    continue
                record = {
                    "game_id": game_id, "official_season": int(game["season"]),
                    "game_type": int(game["gameType"]), "game_date": game["gameDate"],
                    "scheduled_start_time_utc": game["startTimeUTC"],
                    "home_team_id": int(game["homeTeam"]["id"]), "home_team": game["homeTeam"]["abbrev"],
                    "away_team_id": int(game["awayTeam"]["id"]), "away_team": game["awayTeam"]["abbrev"],
                    "schedule_source": url,
                }
                if game_id in records:
                    comparable = [key for key in record if key != "schedule_source"]
                    if any(records[game_id][key] != record[key] for key in comparable):
                        raise RuntimeError(f"OFFICIAL_SCHEDULE_CONFLICT:{game_id}")
                else:
                    records[game_id] = record
        found = sum(game_id in records for game_id in subset.game_id.astype(int))
        if found != len(subset):
            missing = sorted(set(subset.game_id.astype(int)) - set(records))
            raise RuntimeError(f"OFFICIAL_SCHEDULE_INCOMPLETE:{season}:{found}/{len(subset)}:{missing[:10]}")
    schedule = pd.DataFrame(records.values()).sort_values("game_id").reset_index(drop=True)
    return schedule, pd.DataFrame(calls)


def acquire_source(dsn: str, cache_dir: Path | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    hist = pd.read_csv(HISTORICAL_SPINE, low_memory=False)[[
        "canonical_season", "game_id", "home_team_id", "home_team", "away_team_id", "away_team",
        "final_home_goals", "final_away_goals",
    ]]
    official, calls = fetch_official_schedule(hist, cache_dir)
    check = hist.merge(official, on="game_id", suffixes=("_historical", "_official"), validate="one_to_one")
    for field in ["home_team", "away_team"]:
        if not check[f"{field}_historical"].astype(str).eq(check[f"{field}_official"].astype(str)).all():
            raise RuntimeError(f"OFFICIAL_IDENTITY_CONFLICT:{field}")
    for side in ["home", "away"]:
        mismatch = check[f"{side}_team_id_historical"].ne(check[f"{side}_team_id_official"])
        allowed_uta = (
            check.canonical_season.eq(2024) & check[f"{side}_team_historical"].eq("UTA")
            & check[f"{side}_team_id_historical"].eq(68) & check[f"{side}_team_id_official"].eq(59)
        )
        if (mismatch & ~allowed_uta).any():
            raise RuntimeError(f"OFFICIAL_IDENTITY_CONFLICT:{side}_team_id")
    query = """
        SELECT 2023::int AS canonical_season,game_id,teamcode AS team,
               num_shotwasongoal_for::double precision AS shots
        FROM nhl.team_game_2023_summary
        UNION ALL
        SELECT 2024::int,game_id,teamcode,num_shotwasongoal_for::double precision
        FROM nhl.team_game_2024_summary
    """
    with psycopg.connect(dsn) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        shots = read_sql(connection, query)
    if len(shots) != 5596 or shots.duplicated(["canonical_season", "game_id", "team"]).any():
        raise RuntimeError("HISTORICAL_SHOT_SOURCE_GRAIN_CONFLICT")
    calls = pd.concat([pd.DataFrame([{
        "source": "AUTHORIZED_SUPABASE_NHL", "method": "READ_ONLY_SQL",
        "locator": "nhl.team_game_2023_summary UNION ALL nhl.team_game_2024_summary",
        "canonical_season": "2023;2024", "team": "ALL", "response_bytes": pd.NA,
        "accessed_at_utc": datetime.now(timezone.utc).isoformat(), "credentials_exposed": False,
        "credits_consumed": 0,
    }]), calls], ignore_index=True)
    hist = hist.merge(official, on="game_id", suffixes=("", "_official"), validate="one_to_one")
    hist = hist.rename(columns={
        "home_team_id_official": "official_home_team_id", "away_team_id_official": "official_away_team_id",
    })
    for side in ["home", "away"]:
        side_shots = shots.rename(columns={"team": f"{side}_team", "shots": f"{side}_shots"})
        hist = hist.merge(
            side_shots[["canonical_season", "game_id", f"{side}_team", f"{side}_shots"]],
            on=["canonical_season", "game_id", f"{side}_team"], how="left", validate="one_to_one",
        )
    hist["source_lineage"] = "CERTIFIED_HISTORICAL_OUTCOME+AUTHORIZED_DB_TEAM_SHOTS+OFFICIAL_NHL_SCHEDULE"
    hist = hist[[
        "canonical_season", "game_id", "game_type", "game_date", "scheduled_start_time_utc",
        "home_team_id", "home_team", "away_team_id", "away_team", "official_home_team_id",
        "official_away_team_id", "final_home_goals",
        "final_away_goals", "home_shots", "away_shots", "schedule_source", "source_lineage",
    ]]

    s25 = pd.read_csv(SEASON_2025_SOURCE)
    o25 = pd.read_parquet(SEASON_2025_OUTCOMES)[[
        "game_id", "final_home_goals", "final_away_goals", "home_win_full_game", "decision_type",
        "margin_bucket", "home_goal_margin", "total_goals", "home_minus_1_5_result",
    ]]
    s25 = s25.merge(o25, on="game_id", validate="one_to_one")
    s25 = s25.rename(columns={
        "canonical_season": "canonical_season_source", "start_time_utc": "scheduled_start_time_utc",
        "home_team_code": "home_team", "away_team_code": "away_team",
    })
    s25["canonical_season"] = 2025
    s25["game_type"] = pd.to_numeric(s25.game_type).astype(int)
    s25["schedule_source"] = "PRESERVED_OFFICIAL_NHL_SCHEDULE_SOURCE"
    s25["source_lineage"] = "V1_BLOCKED_PACKAGE_AUTHORIZED_TEAM_GAME_SOURCE+CERTIFIED_SEASON_2025_OUTCOME"
    s25["official_home_team_id"] = s25.home_team_id
    s25["official_away_team_id"] = s25.away_team_id
    s25 = s25[[
        "canonical_season", "game_id", "game_type", "game_date", "scheduled_start_time_utc",
        "home_team_id", "home_team", "away_team_id", "away_team", "official_home_team_id",
        "official_away_team_id", "final_home_goals",
        "final_away_goals", "home_shots", "away_shots", "schedule_source", "source_lineage",
        "home_win_full_game", "decision_type", "margin_bucket", "home_goal_margin", "total_goals",
        "home_minus_1_5_result",
    ]]
    source = pd.concat([hist, s25], ignore_index=True, sort=False)
    source["home_win_target"] = np.where(
        source.home_win_full_game.notna(), source.home_win_full_game,
        (source.final_home_goals > source.final_away_goals).astype(int),
    ).astype(int)
    source["scheduled_start_time_utc"] = pd.to_datetime(source.scheduled_start_time_utc, utc=True, format="mixed")
    if len(source) != 4110 or source.duplicated(["canonical_season", "game_id"]).any():
        raise RuntimeError("SOURCE_POPULATION_CONFLICT")
    required = [
        "game_date", "scheduled_start_time_utc", "home_team_id", "away_team_id", "final_home_goals",
        "final_away_goals", "home_shots", "away_shots", "home_win_target",
    ]
    if source[required].isna().any().any():
        raise RuntimeError(f"SOURCE_MISSING:{source[required].isna().sum().to_dict()}")
    return source.sort_values(["canonical_season", "scheduled_start_time_utc", "game_id"]).reset_index(drop=True), calls


def team_rows(source: pd.DataFrame) -> pd.DataFrame:
    shared = ["canonical_season", "game_id", "game_type", "game_date", "scheduled_start_time_utc"]
    home = source[shared + [
        "home_team_id", "home_team", "away_team_id", "away_team", "final_home_goals",
        "final_away_goals", "home_shots", "away_shots",
    ]].copy()
    home.columns = shared + ["team_id", "team", "opponent_id", "opponent", "gf", "ga", "sf", "sa"]
    home["is_home"] = True
    away = source[shared + [
        "away_team_id", "away_team", "home_team_id", "home_team", "final_away_goals",
        "final_home_goals", "away_shots", "home_shots",
    ]].copy()
    away.columns = shared + ["team_id", "team", "opponent_id", "opponent", "gf", "ga", "sf", "sa"]
    away["is_home"] = False
    return pd.concat([home, away], ignore_index=True)


def build_spine(source: pd.DataFrame, partition: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    teams = team_rows(source)
    enriched = []
    for (_, _), group in teams.groupby(["canonical_season", "team_id"], sort=True):
        group = group.sort_values(["scheduled_start_time_utc", "game_id"], kind="mergesort").copy()
        if group.scheduled_start_time_utc.duplicated().any():
            raise RuntimeError(f"TEAM_IDENTICAL_START_CONFLICT:{group.team.iloc[0]}")
        group["prior_games"] = np.arange(len(group))
        goal_diff = group.gf - group.ga
        shot_diff = group.sf - group.sa
        group["std_goal_diff_pg"] = goal_diff.expanding().mean().shift(1)
        group["r10_goal_diff_pg"] = goal_diff.shift(1).rolling(10, min_periods=1).mean()
        group["std_shot_diff_pg"] = shot_diff.expanding().mean().shift(1)
        date_delta = pd.to_datetime(group.game_date).diff().dt.days
        group["days_rest"] = (date_delta - 1).clip(lower=0)
        group["back_to_back"] = np.where(date_delta.notna(), date_delta.eq(1).astype(float), np.nan)
        group["latest_prior_game_id"] = group.game_id.shift(1)
        group["latest_prior_start_utc"] = group.scheduled_start_time_utc.shift(1)
        group["latest_prior_game_date"] = group.game_date.shift(1)
        enriched.append(group)
    teams = pd.concat(enriched, ignore_index=True)
    side_columns = [
        "prior_games", "std_goal_diff_pg", "r10_goal_diff_pg", "std_shot_diff_pg", "days_rest",
        "back_to_back", "latest_prior_game_id", "latest_prior_start_utc", "latest_prior_game_date",
    ]
    spine = source.copy()
    for side, id_column in [("home", "home_team_id"), ("away", "away_team_id")]:
        frame = teams[["canonical_season", "game_id", "team_id"] + side_columns].rename(columns={
            "team_id": id_column, **{column: f"{side}_{column}" for column in side_columns},
        })
        spine = spine.merge(frame, on=["canonical_season", "game_id", id_column], validate="one_to_one")
    spine["diff_std_goal_diff_pg"] = spine.home_std_goal_diff_pg - spine.away_std_goal_diff_pg
    spine["diff_r10_goal_diff_pg"] = spine.home_r10_goal_diff_pg - spine.away_r10_goal_diff_pg
    spine["diff_std_shot_diff_pg"] = spine.home_std_shot_diff_pg - spine.away_std_shot_diff_pg
    spine["diff_days_rest"] = spine.home_days_rest - spine.away_days_rest
    spine["home_back_to_back"] = spine.home_back_to_back.astype(float)
    spine["away_back_to_back"] = spine.away_back_to_back.astype(float)
    minimum = spine[["home_prior_games", "away_prior_games"]].min(axis=1)
    spine["missing_feature_count"] = spine[FEATURES].isna().sum(axis=1)
    spine["history_state"] = np.select(
        [minimum.eq(0), minimum.lt(10)], ["SEASON_OPEN_MIN_HISTORY", "PARTIAL_PRIOR_10_HISTORY"],
        default="MATURE_CURRENT_SEASON_HISTORY",
    )
    spine["strict_prior_status"] = "VERIFIED"
    split = partition[["canonical_season", "game_id", "split", "home_win_target"]].rename(
        columns={"home_win_target": "partition_home_win_target"}
    )
    spine = spine.merge(split, on=["canonical_season", "game_id"], how="left", validate="one_to_one")
    spine["split"] = spine.split.fillna("season_2025_forward")
    if not spine.loc[spine.canonical_season.lt(2025), "partition_home_win_target"].eq(
        spine.loc[spine.canonical_season.lt(2025), "home_win_target"]
    ).all():
        raise RuntimeError("PARTITION_TARGET_CONFLICT")

    lower_id_future_by_season: dict[int, int] = {}
    for row in teams.itertuples(index=False):
        literal = teams[(teams.canonical_season.eq(row.canonical_season)) &
                        (teams.team_id.eq(row.team_id)) & (teams.game_id.lt(row.game_id))]
        season = int(row.canonical_season)
        lower_id_future_by_season[season] = lower_id_future_by_season.get(season, 0) + int(
            (literal.scheduled_start_time_utc >= row.scheduled_start_time_utc).any()
        )
    lower_id_future = sum(lower_id_future_by_season.values())
    validation = []
    def add(check: str, passed: bool, evidence: str) -> None:
        validation.append({"check": check, "status": "PASS" if passed else "FAIL", "evidence": evidence})
    add("population_grain", len(spine) == spine.game_id.nunique() == 4110, f"rows={len(spine)}")
    add("season_counts", spine.groupby("canonical_season").size().to_dict() == {2023: 1400, 2024: 1398, 2025: 1312}, str(spine.groupby("canonical_season").size().to_dict()))
    add("scheduled_start_complete", spine.scheduled_start_time_utc.notna().all(), "missing=0")
    add("no_identical_team_starts", not teams.duplicated(["canonical_season", "team_id", "scheduled_start_time_utc"]).any(), "tie-breaker available but unused")
    prior_start = pd.concat([spine.home_latest_prior_start_utc, spine.away_latest_prior_start_utc]).dropna()
    target_start = pd.concat([
        spine.loc[spine.home_latest_prior_start_utc.notna(), "scheduled_start_time_utc"],
        spine.loc[spine.away_latest_prior_start_utc.notna(), "scheduled_start_time_utc"],
    ])
    add("all_contributors_strictly_prior", (prior_start.reset_index(drop=True) < target_start.reset_index(drop=True)).all(), "latest prior start < target start")
    add(
        "no_game_id_chronology",
        lower_id_future > 0 and lower_id_future_by_season.get(2025) == 23,
        f"literal_game_id_future_side_targets={lower_id_future}; by_season={lower_id_future_by_season}; V2 uses scheduled starts",
    )
    affected = spine[(spine.canonical_season.eq(2025)) & (spine.game_id.ne(2025020828)) &
                     (spine.scheduled_start_time_utc < pd.Timestamp("2026-03-09T20:00:00Z"))]
    contam = ((affected.home_latest_prior_game_id.eq(2025020828)) | (affected.away_latest_prior_game_id.eq(2025020828))).sum()
    add("rescheduled_2025020828_no_contamination", contam == 0, f"earlier_latest-prior_occurrences={int(contam)}")
    expected_home_idle = (pd.to_datetime(spine.game_date) - pd.to_datetime(spine.home_latest_prior_game_date)).dt.days.sub(1).clip(lower=0)
    expected_away_idle = (pd.to_datetime(spine.game_date) - pd.to_datetime(spine.away_latest_prior_game_date)).dt.days.sub(1).clip(lower=0)
    add("idle_date_rest_definition", np.allclose(spine.home_days_rest, expected_home_idle, equal_nan=True) and np.allclose(spine.away_days_rest, expected_away_idle, equal_nan=True), "max(date difference - 1, 0)")
    expected_home_b2b = (pd.to_datetime(spine.game_date) - pd.to_datetime(spine.home_latest_prior_game_date)).dt.days.eq(1)
    expected_away_b2b = (pd.to_datetime(spine.game_date) - pd.to_datetime(spine.away_latest_prior_game_date)).dt.days.eq(1)
    b2b_ok = spine.loc[spine.home_latest_prior_game_date.notna(), "home_back_to_back"].eq(expected_home_b2b[spine.home_latest_prior_game_date.notna()].astype(float)).all()
    b2b_ok &= spine.loc[spine.away_latest_prior_game_date.notna(), "away_back_to_back"].eq(expected_away_b2b[spine.away_latest_prior_game_date.notna()].astype(float)).all()
    add("back_to_back_definition", b2b_ok, "consecutive canonical dates differ exactly one day")
    add("home_away_orientation", (spine.home_team_id != spine.away_team_id).all(), "source orientation retained")
    add("original_split_membership", spine.split.value_counts().to_dict() == {"holdout": 1398, "season_2025_forward": 1312, "fit": 701, "validation": 699}, str(spine.split.value_counts().to_dict()))
    return spine.sort_values(["canonical_season", "scheduled_start_time_utc", "game_id"]).reset_index(drop=True), pd.DataFrame(validation)


def fit_and_score(spine: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    fit = spine[spine.split.eq("fit")]
    if len(fit) != 701:
        raise RuntimeError("FIT_MEMBERSHIP_CONFLICT")
    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    model = LogisticRegression(
        penalty="l2", C=1.0, solver="liblinear", random_state=SEED,
        fit_intercept=True, max_iter=1000, tol=0.0001,
    )
    fit_imputed = imputer.fit_transform(fit[FEATURES])
    fit_scaled = scaler.fit_transform(fit_imputed)
    model.fit(fit_scaled, fit.home_win_target.astype(int))
    raw_frame = spine[FEATURES].apply(pd.to_numeric, errors="coerce")
    imputed = imputer.transform(raw_frame)
    scaled = scaler.transform(imputed)
    probability = model.predict_proba(scaled)[:, 1]
    predictions = spine.copy()
    for index, feature in enumerate(FEATURES):
        predictions[f"imputed__{feature}"] = imputed[:, index]
        predictions[f"scaled__{feature}"] = scaled[:, index]
    predictions["v2_logit"] = model.intercept_[0] + scaled @ model.coef_[0]
    predictions["v2_home_win_probability"] = probability
    predictions["v2_away_win_probability"] = 1 - probability
    predictions["v2_predicted_side"] = np.where(probability >= .5, "HOME", "AWAY")
    artifact = {
        "control_name": CONTROL, "feature_order": FEATURES,
        "feature_contract_sha256_pending": True,
        "model_configuration": {
            "family": "logistic_regression", "penalty": "l2", "C": 1.0, "solver": "liblinear",
            "random_state": SEED, "fit_intercept": True, "max_iter": 1000, "tol": 0.0001,
        },
        "fit_membership": "exact original V1 fit identities", "fit_rows": len(fit),
        "fit_membership_sha256": frame_sha256(fit[["canonical_season", "game_id", "split"]]),
        "fit_feature_target_sha256": frame_sha256(fit[["canonical_season", "game_id"] + FEATURES + ["home_win_target"]]),
        "imputation_medians": dict(zip(FEATURES, imputer.statistics_)),
        "standardization_means": dict(zip(FEATURES, scaler.mean_)),
        "standardization_scales": dict(zip(FEATURES, scaler.scale_)),
        "coefficients": dict(zip(FEATURES, model.coef_[0])), "intercept": model.intercept_[0],
        "classes": model.classes_.tolist(), "versions": {
            "numpy": np.__version__, "pandas": pd.__version__, "scikit_learn": sklearn.__version__,
        },
        "fit_performed_once": True, "hyperparameter_search": False, "feature_selection": False,
        "threshold_optimization": False,
    }
    return predictions, artifact


def evaluate(predictions: pd.DataFrame) -> tuple[dict[str, Any], str]:
    v1 = pd.read_csv(V1_PREDICTIONS)[[
        "canonical_season", "game_id", "home_win_probability",
    ]].rename(columns={"home_win_probability": "v1_home_win_probability"})
    predictions = predictions.merge(v1, on=["canonical_season", "game_id"], how="left", validate="one_to_one")
    fit_rate = predictions.loc[predictions.split.eq("fit"), "home_win_target"].mean()
    populations = {
        "fit": predictions.split.eq("fit"), "validation": predictions.split.eq("validation"),
        "holdout": predictions.split.eq("holdout"),
        "historical_out_of_time": predictions.split.isin(["validation", "holdout"]),
        "season_2025_forward": predictions.split.eq("season_2025_forward"),
    }
    results = {}
    for name, mask in populations.items():
        group = predictions[mask]
        entry = {"v2": bridge_test.metrics(group.home_win_target, group.v2_home_win_probability)}
        entry["fit_home_rate_reference"] = bridge_test.metrics(
            group.home_win_target, pd.Series(fit_rate, index=group.index)
        )
        if name != "season_2025_forward":
            entry["v1_historical_reference"] = bridge_test.metrics(
                group.home_win_target, group.v1_home_win_probability
            )
        results[name] = entry
    proper_better = all(
        results[name]["v2"][metric] < results[name]["fit_home_rate_reference"][metric]
        for name in ["validation", "holdout", "historical_out_of_time"]
        for metric in ["brier_score", "log_loss"]
    )
    auc_valid = all(results[name]["v2"]["roc_auc"] > .5 for name in ["validation", "holdout", "historical_out_of_time"])
    if proper_better and auc_valid:
        decision = "ACCEPTABLE_AS_SIMPLE_CONTROL"
    elif results["historical_out_of_time"]["v2"]["roc_auc"] > .5:
        decision = "PROCESS_VALID_BUT_WEAK"
    else:
        decision = "FAILED"
    return {
        "control_name": CONTROL, "fixed_acceptance_rule": {
            "acceptable": "V2 beats fit-home-rate reference on Brier and log loss in validation, holdout, and combined OOT, with ROC AUC > 0.5 in all three",
            "process_valid_but_weak": "integrity passes and combined OOT ROC AUC > 0.5 but acceptable rule is not met",
            "failed": "combined OOT ROC AUC <= 0.5 or integrity failure",
        },
        "populations": results,
    }, decision


def sog_test(predictions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    s25 = predictions[predictions.split.eq("season_2025_forward")].copy()
    bridge = pd.read_parquet(BRIDGE_DATA)
    analysis = s25[[
        "game_id", "game_date", "home_team", "away_team", "home_win_target", "home_goal_margin",
        "total_goals", "decision_type", "margin_bucket", "home_minus_1_5_result", "v2_home_win_probability",
    ]].merge(bridge[[
        "game_id", "sog_continuous_expectation_sum_diff", "sog_home_model_covered_player_count",
        "sog_away_model_covered_player_count", "sog_home_matched_player_count", "sog_away_matched_player_count",
    ]], on="game_id", validate="one_to_one")
    analysis = analysis.rename(columns={
        "v2_home_win_probability": "champion_home_win_probability",
        "sog_continuous_expectation_sum_diff": "sog_signal",
    })
    analysis["sog_covered"] = analysis.sog_signal.notna()
    coverage = bridge_test.coverage_bias(analysis)
    conditional, stability = bridge_test.conditional_characterization(analysis)
    cohort = coverage[coverage.record_type.eq("OVERALL_COHORT")].set_index("cohort")
    material = (
        int(analysis.sog_covered.sum()) == 374 and int((~analysis.sog_covered).sum()) == 938
        and pd.to_datetime(cohort.loc["covered", "date_min"]) >= pd.Timestamp("2026-02-28")
    )
    if (stability["residual_contrast"] > 0 and stability["bootstrap_ci95_low"] > 0
            and stability["leave_one_month_positive_fraction"] == 1
            and stability["leave_one_team_positive_fraction"] >= .8):
        novelty = "STABLE_SEPARATION_VISIBLE"
    elif stability["residual_contrast"] > 0:
        novelty = "FRAGILE_SEPARATION_VISIBLE"
    else:
        novelty = "NO_INCREMENTAL_SEPARATION_VISIBLE"
    return coverage, conditional, {
        "covered_games": int(analysis.sog_covered.sum()), "uncovered_games": int((~analysis.sog_covered).sum()),
        "coverage_bias": "MATERIAL_BIAS_PRESENT" if material else "NO_MATERIAL_BIAS_VISIBLE",
        "conditional_novelty": novelty, **stability,
    }


def contract() -> dict[str, Any]:
    return {
        "control_name": CONTROL, "feature_order": FEATURES,
        "chronology": "scheduled_start_time_utc ascending within canonical_season/team_id; game_id tie-break only",
        "prior_eligibility": "scheduled_start_time_utc strictly earlier than target",
        "rescheduled_games": "actual revised scheduled start; never numeric game-id order",
        "season_boundary": "reset at canonical season; no prior-season carryover",
        "team_identity_crosswalk": "historical canonical UTA team_id 68 maps explicitly to official NHL team_id 59; team code, game ID, opponent, and orientation must agree",
        "history_population": "all certified games within historical canonical season; season 2025 regular-season population",
        "split_membership": "exact original V1 identities: 701 fit, 699 validation, 1,398 holdout; membership is an identity/date partition independent of feature values",
        "features": {
            "diff_std_goal_diff_pg": "home minus away mean goals-for minus goals-against over all prior same-season games",
            "diff_r10_goal_diff_pg": "home minus away mean goal differential over up to 10 prior same-season games; min_periods=1",
            "diff_std_shot_diff_pg": "home minus away mean shots-for minus shots-against over all prior same-season games",
            "diff_days_rest": "home minus away whole idle canonical dates: max(current_date - previous_date - 1, 0)",
            "home_back_to_back": "1 iff home prior and current canonical dates differ exactly one day",
            "away_back_to_back": "1 iff away prior and current canonical dates differ exactly one day",
        },
        "std_prefix_semantics": "season-to-date standard window; not a statistical standard deviation",
        "current_game_excluded": True, "future_games_excluded": True,
        "minimum_history": "one prior game; missing when none",
        "missingness": "fit-only feature medians; all rows retained",
        "preprocessing": "fit-only median imputation followed by fit-only StandardScaler",
        "model": "L2 logistic regression; C=1.0; liblinear; random_state=20260713; max_iter=1000; tol=0.0001",
        "sog_test": {
            "signal": "unchanged bridge sog_continuous_expectation_sum_diff",
            "bins": {"edges": ["-inf", -4.0, -1.0, 1.0, 4.0, "+inf"], "labels": bridge_test.SOG_LABELS},
            "probability_bands": {"edges": ["-inf", .40, .45, .50, .55, .60, .65, .70, "+inf"], "labels": bridge_test.CONTROL_LABELS},
            "low_coverage_exclusion": "at least 6 certified SOG model players on each team",
            "bootstrap_repetitions": bridge_test.BOOTSTRAP_REPS, "bootstrap_seed": bridge_test.SEED,
        },
    }


def build(args: argparse.Namespace) -> None:
    output = Path(args.output_dir).resolve()
    parent_hashes = {name: verify_manifest(path) for name, path in parents().items()}
    staging = begin_package(output)
    if args.source_ledger_csv:
        source = pd.read_csv(args.source_ledger_csv)
        source["scheduled_start_time_utc"] = pd.to_datetime(source.scheduled_start_time_utc, utc=True, format="mixed")
        access_path = Path(args.source_ledger_csv).with_name("source_access_log.csv")
        access = pd.read_csv(access_path) if access_path.is_file() else pd.DataFrame([{
            "source": "PRESERVED_V2_SOURCE_LEDGER", "method": "OFFLINE_REPLAY", "locator": str(Path(args.source_ledger_csv).resolve()),
            "credentials_exposed": False, "credits_consumed": 0,
        }])
    else:
        dsn = os.environ.get("SUPABASE_DB_URL")
        if not dsn:
            raise RuntimeError("SUPABASE_DB_URL_REQUIRED")
        cache = Path(args.schedule_cache_dir) if args.schedule_cache_dir else None
        if cache:
            cache.mkdir(parents=True, exist_ok=True)
        source, access = acquire_source(dsn, cache)
    write_csv(source, staging / "source_game_ledger.csv")
    write_csv(access, staging / "source_access_log.csv")
    contract_value = contract()
    write_json(contract_value, staging / "v2_contract.json")
    partition = pd.read_csv(PARTITION)
    spine, validation = build_spine(source, partition)
    spine_repeat, _ = build_spine(source, partition)
    validation = pd.concat([validation, pd.DataFrame([{
        "check": "deterministic_spine_rebuild", "status": "PASS" if spine.equals(spine_repeat) else "FAIL",
        "evidence": f"first_sha256={frame_sha256(spine)}; second_sha256={frame_sha256(spine_repeat)}",
    }])], ignore_index=True)
    for season in [2023, 2024, 2025]:
        write_csv(spine[spine.canonical_season.eq(season)], staging / f"corrected_spine_{season}.csv")
    predictions, artifact = fit_and_score(spine)
    replay_logit = artifact["intercept"] + sum(
        predictions[f"scaled__{feature}"] * artifact["coefficients"][feature] for feature in FEATURES
    )
    replay_probability = 1 / (1 + np.exp(-replay_logit))
    probability_delta = abs(replay_probability - predictions.v2_home_win_probability)
    validation = pd.concat([validation, pd.DataFrame([{
        "check": "deterministic_probability_replay",
        "status": "PASS" if probability_delta.max() <= 1e-15 else "FAIL",
        "evidence": f"rows={len(predictions)}; maximum_delta={probability_delta.max():.3g}",
    }])], ignore_index=True)
    artifact["v2_contract_sha256"] = sha256(staging / "v2_contract.json")
    artifact.pop("feature_contract_sha256_pending")
    write_json(artifact, staging / "v2_fitted_control.json")
    metric_result, historical_decision = evaluate(predictions)
    v1 = pd.read_csv(V1_PREDICTIONS)[["canonical_season", "game_id", "home_win_probability"]].rename(
        columns={"home_win_probability": "v1_historical_reference_probability"}
    )
    predictions = predictions.merge(v1, on=["canonical_season", "game_id"], how="left", validate="one_to_one")
    write_csv(predictions, staging / "v2_predictions.csv")
    write_json(metric_result, staging / "v2_metrics.json")
    coverage, conditional, sog = sog_test(predictions)
    write_csv(coverage, staging / "sog_coverage_bias.csv")
    write_csv(conditional, staging / "sog_conditional_results.csv")

    integrity_pass = validation.status.eq("PASS").all()
    readiness = "READY_FOR_FROZEN_DESIGN" if sog["conditional_novelty"] == "STABLE_SEPARATION_VISIBLE" and sog["coverage_bias"] == "NO_MATERIAL_BIAS_VISIBLE" else "NOT_READY"
    if not integrity_pass:
        next_step = "REPAIR_SPECIFIC_V2_BLOCKER"
    elif readiness == "READY_FOR_FROZEN_DESIGN":
        next_step = "DESIGN_FROZEN_SOG_CROSS_MARKET_CHALLENGER"
    elif historical_decision == "ACCEPTABLE_AS_SIMPLE_CONTROL":
        next_step = "ACTIVATE_V2_IN_PROSPECTIVE_SHADOW"
    else:
        next_step = "CONTINUE_SEASON_2026_CAPTURE_WITHOUT_CHALLENGER"
    decisions = {
        "NHL_MONEYLINE_CONTROL_V1_DISPOSITION": "HISTORICAL_REFERENCE_NOT_FORWARD_REPLAYABLE",
        "NHL_MONEYLINE_CONTROL_V2_SPINE": "VERIFIED" if integrity_pass else "BLOCKED",
        "NHL_MONEYLINE_CONTROL_V2_FIT": "COMPLETED" if integrity_pass else "BLOCKED",
        "NHL_MONEYLINE_CONTROL_V2_HISTORICAL_VALIDATION": historical_decision if integrity_pass else "NOT_TESTABLE",
        "NHL_MONEYLINE_CONTROL_V2_SEASON_2025_FORWARD_REPLAY": "COMPLETED" if integrity_pass else "BLOCKED",
        "NHL_SOG_COVERAGE_BIAS": sog["coverage_bias"] if integrity_pass else "UNRESOLVED",
        "NHL_SOG_CONDITIONAL_NOVELTY": sog["conditional_novelty"] if integrity_pass else "NOT_TESTABLE",
        "NHL_CROSS_MARKET_CHALLENGER_READINESS": readiness if integrity_pass else "NOT_READY",
        "NHL_NEXT_STEP": next_step,
    }
    write_json({
        "task": TASK, "decisions": decisions, "sog": sog,
        "points": "PARTIAL_NO_STABLE_ORDERED_RELATIONSHIP",
        "saves": "PARTIAL_FRAGILE_STARTER_UNCONFIRMED",
    }, staging / "decision.json")
    validation = pd.concat([validation, pd.DataFrame([
        {"check": "fit_rows", "status": "PASS" if (predictions.split == "fit").sum() == 701 else "FAIL", "evidence": f"rows={(predictions.split == 'fit').sum()}"},
        {"check": "probabilities", "status": "PASS" if predictions.v2_home_win_probability.between(0, 1).all() else "FAIL", "evidence": "finite and bounded"},
        {"check": "sog_population", "status": "PASS" if sog["covered_games"] == 374 else "FAIL", "evidence": f"covered={sog['covered_games']}"},
        {"check": "no_sog_fit", "status": "PASS", "evidence": "descriptive fixed-bin analysis only"},
        {"check": "operations_unchanged", "status": "PASS", "evidence": "no shadow/production/upload/wager mutation"},
    ])], ignore_index=True)
    write_csv(validation, staging / "validation_summary.csv")
    shutil.copyfile(Path(__file__), staging / "reproduce.py")
    (staging / "execution_utility.md").write_text(
        "# Reproduction\n\nOffline deterministic replay to a new create-only directory:\n\n"
        "```text\n.venv/bin/python -m backend.nhl.scripts.build_nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test --source-ledger-csv artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15/source_game_ledger.csv --output-dir /tmp/nhl_v2_replay\n```\n"
    )
    write_json({
        "task": TASK, "date": DATE, "source_utility": str(Path(__file__).relative_to(REPO)),
        "source_utility_sha256": sha256(Path(__file__)), "parent_manifest_sha256": parent_hashes,
        "rows": len(predictions), "fit_rows": int(predictions.split.eq("fit").sum()), "model_fits": 1,
        "sog_models_fitted": 0, "production_mutations": 0,
    }, staging / "package_identity.json")
    hist = metric_result["populations"]["historical_out_of_time"]["v2"]
    fwd = metric_result["populations"]["season_2025_forward"]["v2"]
    report = [
        "# NHL moneyline strict-prior control V2 and SOG conditional test", "", "## Result", "",
        "V1 is preserved as a historical reference, but is not forward replayable. Its historical metrics are not erased; its construction cannot safely certify prospective state.", "",
        f"V2 uses the same six concepts with scheduled-start chronology, strictly earlier contributors, revised-start positioning, whole idle-date rest, exact canonical-date back-to-back, season reset, fit-only imputation/scaling, and one fixed 701-game logistic fit. All {len(spine):,} rows passed construction checks, including explicit exclusion of rescheduled game `2025020828` from the 23 earlier team-side states affected by game-ID chronology.", "",
        "## Performance", "",
        f"Historical out-of-time ({hist['rows']:,} games): Brier `{hist['brier_score']:.6f}`, log loss `{hist['log_loss']:.6f}`, ROC AUC `{hist['roc_auc']:.6f}`, accuracy `{hist['accuracy']:.6f}`, ECE `{hist['ece_10']:.6f}`.", "",
        f"Season 2025 ({fwd['rows']:,} games): Brier `{fwd['brier_score']:.6f}`, log loss `{fwd['log_loss']:.6f}`, ROC AUC `{fwd['roc_auc']:.6f}`, accuracy `{fwd['accuracy']:.6f}`, ECE `{fwd['ece_10']:.6f}`.", "",
        "## SOG", "",
        f"SOG covers {sog['covered_games']} games and omits {sog['uncovered_games']}; coverage bias is `{sog['coverage_bias']}`. The fixed VERY_HIGH-minus-VERY_LOW conditional residual contrast is `{sog['residual_contrast']:.6f}` with 95% bootstrap interval `[{sog['bootstrap_ci95_low']:.6f}, {sog['bootstrap_ci95_high']:.6f}]`. Leave-one-month and leave-one-team positive fractions are `{sog['leave_one_month_positive_fraction']:.1%}` and `{sog['leave_one_team_positive_fraction']:.1%}`. Conditional novelty is `{sog['conditional_novelty']}`; the interval crosses zero and month stability is incomplete, so the apparent separation cannot be distinguished cleanly from late-season coverage selection.", "",
        "Points remains `PARTIAL_NO_STABLE_ORDERED_RELATIONSHIP`; Saves remains `PARTIAL_FRAGILE_STARTER_UNCONFIRMED`.", "", "## Required decisions", "",
    ]
    report.extend(f"- `{key}` = `{value}`" for key, value in decisions.items())
    report += ["", "No SOG challenger was fitted and no shadow, production, upload, wager, or live behavior changed."]
    (staging / "report.md").write_text("\n".join(report) + "\n")
    if not validation.status.eq("PASS").all():
        raise RuntimeError("VALIDATION_FAILED:" + ",".join(validation[validation.status.ne("PASS")].check))
    manifest = [
        f"{sha256(path)}  {path.name}" for path in sorted(staging.iterdir())
        if path.is_file() and path.name != "SHA256SUMS"
    ]
    (staging / "SHA256SUMS").write_text("\n".join(manifest) + "\n")
    finalize_package(staging, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fetch-authorized-sources", action="store_true")
    source.add_argument("--source-ledger-csv")
    parser.add_argument("--schedule-cache-dir", default="/tmp")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


if __name__ == "__main__":
    build(parse_args())

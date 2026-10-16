#!/usr/bin/env python3
"""Build exact strict-prior NHL_POINTS_COUNT_HGB_V1 feature rows.

Inputs are read-only canonical official outcomes, official skater logs, and a
canonical slate spine. This exporter never reads Phoenix feature values.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from backend.nhl.daily_capture import canonical_game_set_hash

FEATURES = ["is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
            "current_season_points_prior", "current_season_games_prior", "mean_toi_last10",
            "mean_pp_toi_last10", "team_d10_sf_per_game", "last10_team_sog_share"]
HISTORY = "120_DAY_LEGACY_BOUND"
TEAM_SHOT_FEATURE_CONTRACT = "NHL_POINTS_TEAM_D10_OFFICIAL_CANONICAL_V1"
ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/shadow_contract.json"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def normalize_history(logs: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    logs = logs.copy()
    outcomes = outcomes.copy()
    aliases = {"official_goals":"goals", "official_assists":"assists", "official_points":"realized_points"}
    if "game_date" not in outcomes and "slate_date" in outcomes:
        outcomes = outcomes.rename(columns={"slate_date":"game_date"})
    outcomes = outcomes.rename(columns=aliases)
    needed_out = {"game_id", "player_id", "game_date", "canonical_season", "goals", "assists", "official_sog", "outcome_source_timestamp_utc"}
    needed_log = {"game_id", "player_id", "team_id", "toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts"}
    if not needed_out.issubset(outcomes) or not needed_log.issubset(logs):
        raise ValueError("HISTORY_SOURCE_SCHEMA_MISSING")
    for df in (logs, outcomes):
        for col in ("game_id", "player_id"):
            df[col] = pd.to_numeric(df[col], errors="raise").astype("int64")
    if outcomes.duplicated(["game_id", "player_id"]).any() or logs.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_HISTORY_IDENTITY")
    cols = [c for c in ("game_id", "player_id", "team_id", "is_home", "home_team_id", "away_team_id", "toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts", "start_time_utc") if c in logs]
    hist = outcomes.merge(logs[cols], on=["game_id", "player_id"], how="left", suffixes=("_outcome", "_log"), validate="one_to_one")
    if "team_id_outcome" in hist:
        hist["team_id"] = hist.team_id_outcome.fillna(hist.team_id_log)
        hist = hist.drop(columns=["team_id_outcome", "team_id_log"])
    if "is_home_outcome" in hist:
        hist["is_home"] = hist.is_home_outcome.fillna(hist.is_home_log)
        hist = hist.drop(columns=["is_home_outcome", "is_home_log"])
    hist["game_date"] = pd.to_datetime(hist.game_date).dt.normalize()
    hist["start_time_utc"] = pd.to_datetime(hist.get("start_time_utc"), utc=True, errors="coerce")
    hist["outcome_source_timestamp_utc"] = pd.to_datetime(hist.outcome_source_timestamp_utc, utc=True, errors="coerce")
    for c in ("canonical_season", "goals", "assists", "team_id", "is_home", "home_team_id", "away_team_id", "toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts", "official_sog"):
        if c not in hist:
            hist[c] = np.nan
    hist["canonical_season"] = pd.to_numeric(hist.canonical_season, errors="raise").astype(int)
    # The team context feature is bound to the physical home/away team in the
    # canonical game spine. Raw team IDs are retained only for the unchanged
    # legacy player/team share calculation below.
    for col in ("team_id", "home_team_id", "away_team_id"):
        hist[col] = pd.to_numeric(hist[col], errors="coerce")
    home_mask = pd.to_numeric(hist.is_home, errors="coerce").eq(1)
    away_mask = pd.to_numeric(hist.is_home, errors="coerce").eq(0)
    hist["canonical_team_id"] = np.where(home_mask, hist.home_team_id,
        np.where(away_mask, hist.away_team_id, np.nan))
    if hist.canonical_team_id.isna().any() or not hist.canonical_team_id.eq(hist.team_id).all():
        raise ValueError("HGB_HISTORY_TEAM_BINDING_INVALID")
    hist["canonical_team_id"] = hist.canonical_team_id.astype("int64")
    hist["realized_points"] = pd.to_numeric(hist.goals, errors="raise") + pd.to_numeric(hist.assists, errors="raise")
    hist["official_sog"] = pd.to_numeric(hist.official_sog, errors="coerce")
    if (hist.official_sog.dropna() < 0).any():
        raise ValueError("NEGATIVE_OFFICIAL_SOG")
    for c in ("toi_minutes", "pp_toi_minutes"):
        hist[c] = pd.to_numeric(hist[c], errors="coerce")
    # This is the bakeoff export rule: missing SOG/attempt logs become zero;
    # TOI remains missing and zero TOI is excluded from rate means.
    for c in ("shots_on_goal", "shot_attempts"):
        hist[c] = pd.to_numeric(hist[c], errors="coerce").fillna(0.0)
    return hist.sort_values(["game_date", "start_time_utc", "game_id", "player_id"], kind="mergesort").reset_index(drop=True)


def normalize_slate(slate: pd.DataFrame, season: int) -> pd.DataFrame:
    slate = slate.copy()
    required = {"game_id", "player_id", "game_date", "game_start_utc", "is_home", "home_team_id", "away_team_id"}
    if not required.issubset(slate):
        raise ValueError("SLATE_SPINE_SCHEMA_MISSING")
    if slate.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_SLATE_IDENTITY")
    slate["game_id"] = pd.to_numeric(slate.game_id, errors="raise").astype("int64")
    slate["player_id"] = pd.to_numeric(slate.player_id, errors="raise").astype("int64")
    slate["game_date"] = pd.to_datetime(slate.game_date).dt.normalize()
    slate["start_time_utc"] = pd.to_datetime(slate.game_start_utc, utc=True, errors="raise")
    slate["canonical_season"] = int(season)
    home = slate.is_home.map({True:1, False:0, "t":1, "f":0, "true":1, "false":0, "1":1, "0":0})
    if home.isna().any():
        raise ValueError("INVALID_HOME_AWAY_IDENTITY")
    slate["is_home"] = home.astype(int)
    slate["team_id"] = np.where(slate.is_home.eq(1), slate.home_team_id, slate.away_team_id).astype("int64")
    if not slate.game_id.astype(str).str[4:6].eq("02").all():
        raise ValueError("NON_REGULAR_SEASON_GAME_IN_SLATE")
    return slate


def build_team_game_aggregation(history: pd.DataFrame) -> pd.DataFrame:
    """Aggregate official player SOG observations by canonical physical team-game.

    Official SOG is summed with min_count semantics: one missing official source
    row makes the team-game unavailable and raises instead of silently becoming
    a zero. Duplicate player-game inputs and invalid team bindings are rejected.
    """
    required = {"canonical_team_id", "game_id", "game_date", "player_id", "official_sog"}
    if not required.issubset(history.columns):
        raise ValueError("HGB_OFFICIAL_TEAM_SHOT_SOURCE_SCHEMA_MISSING")
    source = history.loc[history.canonical_team_id.notna()].copy()
    if source.duplicated(["canonical_team_id", "game_id", "player_id"]).any():
        raise ValueError("HGB_DUPLICATE_CANONICAL_TEAM_GAME_PLAYER")
    if source.outcome_source_timestamp_utc.isna().any():
        raise ValueError("HGB_OFFICIAL_TEAM_GAME_SOURCE_TIMESTAMP_MISSING")
    grouped = source.groupby(["canonical_team_id", "game_id", "game_date"], as_index=False, sort=True)
    result = grouped.agg(team_sog=("official_sog", lambda values: values.sum(min_count=len(values))),
                         source_player_rows=("player_id", "size"), unique_players=("player_id", "nunique"),
                         missing_official_sog_rows=("official_sog", lambda values: int(values.isna().sum())),
                         source_timestamps=("outcome_source_timestamp_utc", lambda values: ";".join(sorted(set(values.dropna().astype(str))))),
                         game_start_timestamps=("start_time_utc", lambda values: ";".join(sorted(set(values.dropna().astype(str))))))
    if result.missing_official_sog_rows.gt(0).any() or result.team_sog.isna().any():
        raise ValueError("HGB_OFFICIAL_TEAM_GAME_SOURCE_INCOMPLETE")
    if not result.source_player_rows.eq(result.unique_players).all():
        raise ValueError("HGB_DUPLICATE_CANONICAL_TEAM_GAME_PLAYER")
    result = result.rename(columns={"canonical_team_id": "team_id"})
    return result


def build_team_shot_history_evidence(history: pd.DataFrame, slate: pd.DataFrame) -> pd.DataFrame:
    """Retain strict-prior contributing game IDs and D10 for every HGB target."""
    team_games = build_team_game_aggregation(history)
    groups = {int(team): group.sort_values(["game_date", "game_id"], kind="mergesort")
              for team, group in team_games.groupby("team_id", sort=False)}
    rows = []
    for target in slate.itertuples(index=False):
        day = pd.Timestamp(target.game_date)
        team_history = groups.get(int(target.team_id), team_games.iloc[:0])
        eligible = team_history.loc[
            team_history.game_date.lt(day)
            & team_history.game_date.ge(day - pd.Timedelta(days=120))
        ].tail(10)
        rows.append({
            "game_id": int(target.game_id), "player_id": int(target.player_id),
            "game_date": day.date().isoformat(), "team_id": int(target.team_id),
            "canonical_team_id": int(target.team_id),
            "contributing_game_ids": ";".join(map(str, eligible.game_id.astype(int))),
            "contributing_team_sog": ";".join(format(float(v), ".17g") for v in eligible.team_sog),
            "source_timestamps": ";".join(sorted(set(";".join(eligible.source_timestamps.astype(str)).split(";")))),
            "game_start_timestamps": ";".join(sorted(set(";".join(eligible.game_start_timestamps.astype(str)).split(";")))),
            "valid_game_count": int(len(eligible)),
            "rolling_denominator": int(len(eligible)),
            "team_d10_sf_per_game": float(eligible.team_sog.mean()) if len(eligible) else 0.0,
        })
    return pd.DataFrame(rows)


def build_features(history: pd.DataFrame, slate: pd.DataFrame) -> pd.DataFrame:
    team_games = (history.loc[history.team_id.notna()]
                  .groupby(["team_id", "game_id", "game_date"], as_index=False)
                  .agg(team_sog=("shots_on_goal", "sum")))
    player_groups = {int(pid): group for pid, group in history.groupby("player_id", sort=False)}
    team_groups = {int(tid): group.sort_values(["game_date", "game_id"]) for tid, group in team_games.groupby("team_id")}
    official_team_games = build_team_game_aggregation(history)
    official_team_groups = {int(tid): group.sort_values(["game_date", "game_id"], kind="mergesort")
                            for tid, group in official_team_games.groupby("team_id", sort=False)}
    rows = []
    for target in slate.itertuples(index=False):
        d = target.game_date
        player_hist = player_groups.get(int(target.player_id), history.iloc[:0])
        prior = player_hist.loc[player_hist.game_date < d]
        h = prior.loc[prior.game_date >= d - pd.Timedelta(days=120)].tail(10)
        season_prior = prior.loc[prior.canonical_season.eq(int(target.canonical_season))]
        team_hist = team_groups.get(int(target.team_id), team_games.iloc[:0])
        th = team_hist.loc[(team_hist.game_date < d) & (team_hist.game_date >= d - pd.Timedelta(days=120))].tail(10)
        team_sog = float(th.team_sog.sum()) if len(th) else 0.0
        official_team_hist = official_team_groups.get(int(target.team_id), official_team_games.iloc[:0])
        official_th = official_team_hist.loc[(official_team_hist.game_date < d)
            & (official_team_hist.game_date >= d - pd.Timedelta(days=120))].tail(10)
        sog10 = float(h.shots_on_goal.fillna(0).sum()) if len(h) else 0.0
        rates = (h.shots_on_goal * 60 / h.toi_minutes.replace(0, np.nan))
        att_rates = (h.shot_attempts * 60 / h.toi_minutes.replace(0, np.nan))
        rows.append({"slate_date": d.date().isoformat(), "game_id": int(target.game_id), "player_id": int(target.player_id),
            "game_date": d.date().isoformat(), "game_start_utc": target.start_time_utc.isoformat(),
            "is_home": int(target.is_home), "d10_sog_per60": rates.mean(), "attempts_d10_per60": att_rates.mean(),
            "player_points_last10": h.realized_points.mean() if len(h) else np.nan,
            "current_season_points_prior": int(season_prior.realized_points.sum()),
            "current_season_games_prior": int(len(season_prior)),
            "mean_toi_last10": h.toi_minutes.mean() if len(h) else np.nan,
            "mean_pp_toi_last10": h.pp_toi_minutes.mean() if len(h) else np.nan,
            "team_d10_sf_per_game": float(official_th.team_sog.mean()) if len(official_th) else 0.0,
            "last10_team_sog_share": sog10 / team_sog if team_sog > 0 else 0.0,
            "history_contract": HISTORY, "history_cutoff_exclusive": d.date().isoformat()})
    result = pd.DataFrame(rows)
    return result[["slate_date", "game_id", "player_id", "game_date", "game_start_utc", *FEATURES, "history_contract", "history_cutoff_exclusive"]]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--logs", type=Path, required=True)
    p.add_argument("--outcomes", type=Path, nargs="+", required=True)
    p.add_argument("--slate", type=Path, required=True)
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--cutoff-utc", required=True)
    p.add_argument("--parent-run-id", required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    slate_raw = pd.read_csv(a.slate)
    slate = normalize_slate(slate_raw, a.season)
    cutoff = pd.Timestamp(a.cutoff_utc)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    if not (slate.start_time_utc > cutoff).all():
        raise ValueError("NOT_STRICTLY_PREGAME")
    outcomes = pd.concat([pd.read_csv(path) for path in a.outcomes], ignore_index=True)
    history = normalize_history(pd.read_csv(a.logs), outcomes)
    if history.game_date.max() >= slate.game_date.min():
        raise ValueError("SAME_DAY_OR_FUTURE_HISTORY_PRESENT")
    result = build_features(history, slate)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.mkdir(parents=True, exist_ok=False)
    feature_path = a.out / "features.csv"
    result.to_csv(feature_path, index=False, float_format="%.17g")
    contract_hash = sha(CONTRACT)
    manifest = {"schema_version":"NHL_POINTS_HGB_OPERATIONAL_FEATURES_V1", "research_model_id":"NHL_POINTS_COUNT_HGB_V1",
        "history_contract":"NHL_POINTS_COUNT_HGB_V1_HISTORY_120_DAY_VALIDATED", "slate_date":str(result.slate_date.iloc[0]),
        "team_shot_feature_contract": TEAM_SHOT_FEATURE_CONTRACT,
        "team_shot_source": "official per-player SOG observations, aggregated by canonical physical team-game identity",
        "canonical_game_ids":sorted(map(int, result.game_id.unique())),
        "canonical_game_set_hash":canonical_game_set_hash(result.game_id.unique()),
        "parent_operational_run_id":a.parent_run_id, "capture_time_utc":pd.Timestamp.now(tz="UTC").isoformat(),
        "pregame_cutoff_utc":cutoff.isoformat(), "feature_sha256":sha(feature_path), "contract_sha256":contract_hash,
        "log_source_sha256":sha(a.logs), "outcome_source_sha256s":{str(path):sha(path) for path in a.outcomes},
        "slate_source_sha256":sha(a.slate), "rows":len(result), "ordered_model_features":FEATURES,
        "official_outcome_dates":sorted(pd.to_datetime(outcomes.game_date if "game_date" in outcomes else outcomes.slate_date).dt.date.astype(str).unique().tolist()),
        "max_history_game_date":history.game_date.max().date().isoformat(), "same_day_history_rows":0,
        "history_rows_after_cutoff":int((history.game_date >= slate.game_date.min()).sum()),
        "feature_construction":"backend.nhl.scripts.export_nhl_points_hgb_features.build_features",
        "feature_exporter_sha256":sha(Path(__file__).resolve()),
        "production_authority":False}
    (a.out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True)+"\n")
    print(json.dumps({"status":"FEATURES_EXPORTED", "path":str(a.out), **manifest}, indent=2))


if __name__ == "__main__":
    main()

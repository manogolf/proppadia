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
    needed_out = {"game_id", "player_id", "game_date", "canonical_season", "goals", "assists"}
    needed_log = {"game_id", "player_id", "team_id", "toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts"}
    if not needed_out.issubset(outcomes) or not needed_log.issubset(logs):
        raise ValueError("HISTORY_SOURCE_SCHEMA_MISSING")
    for df in (logs, outcomes):
        for col in ("game_id", "player_id"):
            df[col] = pd.to_numeric(df[col], errors="raise").astype("int64")
    if outcomes.duplicated(["game_id", "player_id"]).any() or logs.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_HISTORY_IDENTITY")
    cols = [c for c in ("game_id", "player_id", "team_id", "is_home", "toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts", "start_time_utc") if c in logs]
    hist = outcomes.merge(logs[cols], on=["game_id", "player_id"], how="left", suffixes=("_outcome", "_log"), validate="one_to_one")
    if "team_id_outcome" in hist:
        hist["team_id"] = hist.team_id_outcome.fillna(hist.team_id_log)
        hist = hist.drop(columns=["team_id_outcome", "team_id_log"])
    if "is_home_outcome" in hist:
        hist["is_home"] = hist.is_home_outcome.fillna(hist.is_home_log)
        hist = hist.drop(columns=["is_home_outcome", "is_home_log"])
    hist["game_date"] = pd.to_datetime(hist.game_date).dt.normalize()
    hist["start_time_utc"] = pd.to_datetime(hist.get("start_time_utc"), utc=True, errors="coerce")
    for c in ("canonical_season", "goals", "assists", "team_id", "is_home", "toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts"):
        if c not in hist:
            hist[c] = np.nan
    hist["canonical_season"] = pd.to_numeric(hist.canonical_season, errors="raise").astype(int)
    hist["realized_points"] = pd.to_numeric(hist.goals, errors="raise") + pd.to_numeric(hist.assists, errors="raise")
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


def build_features(history: pd.DataFrame, slate: pd.DataFrame) -> pd.DataFrame:
    team_games = (history.loc[history.team_id.notna()]
                  .groupby(["team_id", "game_id", "game_date"], as_index=False)
                  .agg(team_sog=("shots_on_goal", "sum")))
    player_groups = {int(pid): group for pid, group in history.groupby("player_id", sort=False)}
    team_groups = {int(tid): group.sort_values(["game_date", "game_id"]) for tid, group in team_games.groupby("team_id")}
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
            "team_d10_sf_per_game": float(th.team_sog.mean()) if len(th) else 0.0,
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

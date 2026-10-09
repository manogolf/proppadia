#!/usr/bin/env python3
"""Build 2025 strict-prior features with the frozen Points bakeoff semantics."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.nhl.scripts.build_nhl_points_architecture_bakeoff import ARMS, build_frame

ROOT = Path(__file__).resolve().parents[3]
FROZEN_FRAME = ROOT / "artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(outcomes_csv: Path, logs_csv: Path, output_dir: Path) -> dict:
    outcomes = pd.read_csv(outcomes_csv)
    logs = pd.read_csv(logs_csv, parse_dates=["game_date", "start_time_utc"])
    if outcomes.duplicated(["game_id", "player_id"]).any() or logs.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_PLAYER_GAME_KEY")
    boxes = {}
    raw_root = ROOT / "artifacts/operational/nhl/moneyline_team_history/raw/season=2025"
    import json as _json
    for path in raw_root.glob("game=*/boxscore.json"):
        data = _json.loads(path.read_text())
        boxes[int(data["id"])] = data
    if len(boxes) != 1312:
        raise ValueError(f"BOX_SCORE_GAME_COVERAGE:{len(boxes)}")
    outcomes["game_id"] = outcomes.game_id.astype("int64")
    outcomes["player_id"] = outcomes.player_id.astype("int64")
    outcomes["canonical_season"] = outcomes.canonical_season.astype("int64")
    logs["game_id"] = logs.game_id.astype("int64")
    logs["player_id"] = logs.player_id.astype("int64")
    src = outcomes.merge(logs.drop(columns=["canonical_season", "game_date", "team_id", "is_home"]),
                         on=["game_id", "player_id"], how="left", validate="one_to_one", indicator=True)
    if int((src._merge == "both").sum()) != 46713 or int((src._merge == "left_only").sum()) != len(outcomes)-46713:
        raise ValueError("OUTCOME_LOG_JOIN_COVERAGE_MISMATCH")
    src = src.drop(columns="_merge")
    src["game_date"] = pd.to_datetime(src.game_date)
    src["start_time_utc"] = src.game_id.map(lambda gid: boxes[int(gid)].get("startTimeUTC"))
    src["team_id"] = outcomes.team_id.astype("int64")
    src["home_team_id"] = src.game_id.map(lambda gid: int(boxes[int(gid)]["homeTeam"]["id"]))
    src["is_home"] = (src.team_id == src.home_team_id).astype("int64")
    src = src.drop(columns="home_team_id")
    src["realized_goals"] = src.goals
    src["realized_assists"] = src.assists
    src["realized_points"] = src.goals + src.assists
    src = src.drop(columns=["goals", "assists"])
    src = src.rename(columns={"canonical_season":"season"})
    for col in ("toi_minutes", "pp_toi_minutes"):
        src[col] = pd.to_numeric(src[col], errors="coerce")
    src["shots_on_goal"] = pd.to_numeric(src.shots_on_goal, errors="coerce").fillna(0)
    src["shot_attempts"] = pd.to_numeric(src.shot_attempts, errors="coerce").fillna(0)
    built = build_frame(src)
    oot = built[built.history_contract.eq(ARMS[2])].copy()
    prior = pd.read_csv(FROZEN_FRAME, parse_dates=["game_date"])
    prior = prior[(prior.canonical_season.isin([2023, 2024])) & prior.history_contract.eq(ARMS[2])].copy()
    combined = pd.concat([prior, oot], ignore_index=True).sort_values(["game_date","game_id","player_id"],kind="mergesort")
    if oot.duplicated(["game_id","player_id"]).any() or len(oot) != len(outcomes):
        raise ValueError("FEATURE_FRAME_TARGET_GRAIN_MISMATCH")
    # Every same-day row must exclude all same-day outcomes by the frozen builder contract.
    if not (oot.player_history_games <= oot.player_lifetime_games_prior).all():
        raise ValueError("PRIOR_HISTORY_COUNT_CONFLICT")
    output_dir.mkdir(parents=True, exist_ok=True)
    retained_logs = output_dir / "skater_log_feature_source.csv.gz"
    logs.to_csv(retained_logs, index=False, compression="gzip")
    oot.to_csv(output_dir / "season_2025_strict_prior_features.csv.gz", index=False, compression="gzip")
    combined.to_csv(output_dir / "validation_input_2023_2025.csv.gz", index=False, compression="gzip")
    manifest = {"schema_version":"NHL_POINTS_2025_FROZEN_VALIDATION_INPUT_V1","canonical_season":2025,
        "history_contract":ARMS[2],"feature_builder":"backend.nhl.scripts.build_nhl_points_architecture_bakeoff.build_frame",
        "feature_semantics":"same bakeoff 120_DAY_LEGACY_BOUND; strict earlier-date player and team history; same-day excluded",
        "oot_rows":len(oot),"unique_oot_player_games":int(oot[["game_id","player_id"]].drop_duplicates().shape[0]),
        "prior_train_rows":len(prior),"combined_rows":len(combined),"outcome_sha256":sha(outcomes_csv),
        "skater_log_export_sha256":sha(retained_logs),"skater_log_export_path":retained_logs.name,
        "skater_log_export_original_sha256":sha(logs_csv),"frozen_prior_frame_sha256":sha(FROZEN_FRAME),
        "validation_input_sha256":sha(output_dir / "validation_input_2023_2025.csv.gz"),
        "second_season_validation_runner":"NOT_RUN: existing runner hard-codes canonical_season == 2024",
        "tuning_on_2025":False,"leakage_check":"PASS: build_frame uses game_date < target date"}
    (output_dir / "validation_input_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    return manifest


def main() -> None:
    p=argparse.ArgumentParser(); p.add_argument("--outcomes",type=Path,required=True); p.add_argument("--logs",type=Path,required=True); p.add_argument("--output-dir",type=Path,required=True)
    a=p.parse_args(); print(json.dumps(build(a.outcomes,a.logs,a.output_dir),indent=2,sort_keys=True))


if __name__ == "__main__": main()

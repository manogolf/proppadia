#!/usr/bin/env python3
"""Bind the legacy Points feature export into a create-only canonical snapshot."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg

from backend.nhl.points_quote_capture.core import sha256_file, write_manifest
from backend.nhl.points_shadow.core import frozen_identity

ROOT = Path(__file__).resolve().parents[3]


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def read_manifest(path: Path) -> dict[str, str]:
    return {name: digest for digest, name in (line.split("  ", 1) for line in path.read_text().splitlines())}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--game-spine-csv", type=Path, required=True)
    parser.add_argument("--game-spine-manifest", type=Path, required=True)
    parser.add_argument("--features-csv", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("backend/nhl/exports/points_input_snapshots"))
    args = parser.parse_args()
    identity = frozen_identity()
    source_sql = ROOT / identity["feature_construction"]["path"]
    if sha256_file(source_sql) != identity["feature_construction"]["sha256"]:
        raise SystemExit("POINTS_FEATURE_CONSTRUCTION_HASH_DRIFT")
    parent = read_manifest(args.game_spine_manifest)
    direct = parent.get(args.game_spine_csv.name)
    if direct is None:
        health_path = args.game_spine_manifest.parent / "morning_health.json"
        if parent.get("morning_health.json") != (sha256_file(health_path) if health_path.exists() else None):
            raise SystemExit("PARENT_HEALTH_HASH_MISMATCH")
        health = json.loads(health_path.read_text())
        direct = health.get("canonical_game_spine_sha256")
    if sha256_file(args.game_spine_csv) != direct:
        raise SystemExit("PARENT_HASH_MISMATCH_OR_MUTABLE_GAME_SPINE")
    games, features = pd.read_csv(args.game_spine_csv), pd.read_csv(args.features_csv)
    if games.empty or games.game_id.duplicated().any() or not games.canonical_season.eq(2026).all() or not games.slate_date.astype(str).eq(args.slate_date).all():
        raise SystemExit("GAME_SPINE_IDENTITY_FAILURE")
    if features.empty or features.duplicated(["game_id", "player_id"]).any():
        raise SystemExit("POINTS_FEATURE_IDENTITY_FAILURE")
    database = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not database:
        raise SystemExit("MISSING_DATABASE_CREDENTIAL")
    game_ids = [int(value) for value in games.game_id]
    with psycopg.connect(database) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT rs.game_id, rs.player_id, rs.team_id,
                   COALESCE(NULLIF(BTRIM(p.full_name), ''), rs.player_id::text) AS player_name,
                   COALESCE(rs.active_flag, FALSE) AS active_flag,
                   rs.asof_ts
            FROM nhl.roster_status rs
            LEFT JOIN nhl.players p USING (player_id)
            WHERE rs.game_id = ANY(%s)
            ORDER BY rs.game_id, rs.player_id
            """,
            (game_ids,),
        )
        rows = cursor.fetchall()
        columns = [item.name for item in cursor.description]
    roster = pd.DataFrame(rows, columns=columns)
    if roster.empty or roster.duplicated(["game_id", "player_id"]).any():
        raise SystemExit("ROSTER_IDENTITY_MISSING_OR_AMBIGUOUS")
    frame = features.merge(roster, on=["game_id", "player_id"], how="left", validate="one_to_one")
    frame = frame.merge(games[["canonical_season", "slate_date", "game_id", "scheduled_start_time_utc", "game_type_code", "home_team_id", "home_team", "away_team_id", "away_team"]], on="game_id", how="left", validate="many_to_one")
    if frame.player_name.isna().any() or frame.team_id.isna().any() or frame.canonical_season.isna().any():
        raise SystemExit("PLAYER_GAME_CANONICAL_BINDING_FAILURE")
    home = pd.to_numeric(frame.team_id).eq(pd.to_numeric(frame.home_team_id))
    away = pd.to_numeric(frame.team_id).eq(pd.to_numeric(frame.away_team_id))
    if not (home | away).all():
        raise SystemExit("PLAYER_TEAM_ORIENTATION_MISMATCH")
    frame["team"] = frame.home_team.where(home, frame.away_team)
    frame["opponent"] = frame.away_team.where(home, frame.home_team)
    frame["pregame_participation_state"] = frame.active_flag.map(lambda value: "ACTIVE" if bool(value) else "UNRESOLVED")
    frame["roster_source_timestamp_utc"] = pd.to_datetime(frame.asof_ts, utc=True, errors="coerce")
    captured = utc()
    frame["feature_cutoff_timestamp_utc"] = captured
    slate_midnight = datetime.combine(date.fromisoformat(args.slate_date), time.min, tzinfo=timezone.utc)
    frame["feature_history_max_timestamp_utc"] = (slate_midnight - timedelta(microseconds=1)).isoformat().replace("+00:00", "Z")
    ordered = [
        "canonical_season", "slate_date", "game_id", "player_id", "player_name", "team", "opponent",
        "scheduled_start_time_utc", "game_type_code", "feature_cutoff_timestamp_utc",
        "feature_history_max_timestamp_utc", "pregame_participation_state", "roster_source_timestamp_utc",
    ] + [column for column in features.columns if column not in {"game_id", "player_id"}]
    frame = frame[ordered].sort_values(["game_id", "player_id"])
    starts = pd.to_datetime(frame.scheduled_start_time_utc, utc=True, errors="coerce")
    if starts.isna().any() or frame.roster_source_timestamp_utc.isna().any() or (pd.to_datetime(captured, utc=True) >= starts).any() or (frame.roster_source_timestamp_utc >= starts).any():
        raise SystemExit("PREGAME_INPUT_SNAPSHOT_TIMING_FAILURE")
    snapshot_id = "nhlpointsinputs_s2026_d" + args.slate_date.replace("-", "") + "_t" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_v1"
    destination = args.output_root / "2026" / args.slate_date / snapshot_id
    staging = destination.with_name(destination.name + ".incomplete")
    if destination.exists() or staging.exists():
        raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
    staging.mkdir(parents=True, exist_ok=False)
    shutil.copy2(args.game_spine_csv, staging / "canonical_game_spine.csv")
    frame.to_csv(staging / "points_player_inputs.csv", index=False)
    (staging / "snapshot_metadata.json").write_text(json.dumps({
        "schema_version": "nhl_points_input_snapshot_v1", "snapshot_id": snapshot_id,
        "canonical_season": 2026, "slate_date": args.slate_date, "capture_timestamp_utc": captured,
        "source_feature_csv_path": str(args.features_csv), "source_feature_csv_sha256": sha256_file(args.features_csv),
        "source_feature_sql_sha256": sha256_file(source_sql), "game_spine_sha256": sha256_file(args.game_spine_csv),
        "game_spine_manifest_sha256": sha256_file(args.game_spine_manifest), "rows": len(frame),
        "mutable_source_snapshotted_before_critical_decision": True,
        "strict_prior_contract": identity["feature_construction"]["strict_prior_contract"],
    }, indent=2, sort_keys=True) + "\n")
    (staging / "RUN_COMPLETE.json").write_text(json.dumps({"snapshot_id": snapshot_id, "status": "COMPLETE"}, sort_keys=True) + "\n")
    write_manifest(staging, complete_only=True)
    staging.rename(destination)
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

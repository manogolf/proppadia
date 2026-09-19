#!/usr/bin/env python3
"""Targeted no-API prediction recovery for independent NHL prop lanes."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.nhl.points_shadow.core import evaluate_ladder_coherence, score_frozen as score_points, verify_frozen_identity as verify_points
from backend.nhl.saves_shadow.core import score_frozen as score_saves, verify_frozen_identity as verify_saves


ROOT = Path(__file__).resolve().parents[3]
SLATE_DATE = "2026-09-19"
RUN_TYPE = "SEPTEMBER_19_PRESEASON_CATCHUP"


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def export(sql: Path, database: str) -> bytes:
    result = subprocess.run(
        ["psql", database, "--no-psqlrc", "-v", "ON_ERROR_STOP=1", "-v", f"slate_date={SLATE_DATE}", "-f", str(sql)],
        cwd=ROOT, capture_output=True, check=True,
    )
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--game-spine", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--observation-timestamp-utc", required=True)
    args = parser.parse_args()
    database = (os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not database:
        raise SystemExit("DATABASE_CREDENTIAL_MISSING_FAIL_CLOSED")
    root = args.output_root.resolve()
    staging = root.with_name(root.name + ".incomplete")
    if root.exists() or staging.exists():
        raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
    staging.mkdir(parents=True, exist_ok=False)
    try:
        spine = pd.read_csv(args.game_spine)
        if len(spine) != 7 or spine.game_id.duplicated().any() or not spine.slate_date.astype(str).eq(SLATE_DATE).all():
            raise RuntimeError("CANONICAL_GAME_SPINE_FAILURE")
        shutil.copy2(args.game_spine, staging / "canonical_game_spine.csv")

        points_path = staging / "points_features.csv"
        points_path.write_bytes(export(ROOT / "backend/nhl/sql/export_points.sql", database))
        points = pd.read_csv(points_path)
        point_identity = verify_points()
        point_predictions = score_points(points, point_identity)
        point_ladders = evaluate_ladder_coherence(point_predictions, point_identity)
        point_predictions.to_csv(staging / "points_immutable_predictions.csv", index=False)
        point_ladders.to_csv(staging / "points_ladder_health.csv", index=False)

        saves_path = staging / "saves_features.csv"
        saves_path.write_bytes(export(ROOT / "backend/nhl/sql/export_saves_from_denali.sql", database))
        saves = pd.read_csv(saves_path)
        save_identity = verify_saves()
        save_predictions, _ = score_saves(saves, 1.0, save_identity)
        save_predictions.to_csv(staging / "saves_conditional_start_predictions.csv", index=False)

        summary = {
            "schema_version": "nhl_september19_independent_lane_predictions_v1",
            "run_type": RUN_TYPE,
            "slate_date": SLATE_DATE,
            "observation_timestamp_utc": args.observation_timestamp_utc,
            "completion_timestamp_utc": utc(),
            "game_count": len(spine),
            "points": {
                "status": "PREDICTION_ONLY_MARKET_UNAVAILABLE",
                "feature_rows": len(points),
                "players": int(points.player_id.nunique()) if len(points) else 0,
                "games": int(points.game_id.nunique()) if len(points) else 0,
                "prediction_rows": len(point_predictions),
                "ladder_pass": int(point_ladders.ladder_coherence_decision.eq("PASS_LADDER_COHERENCE").sum()),
                "model": point_identity["model_name"],
                "model_version": point_identity["model_version"],
            },
            "saves": {
                "status": "CONDITIONAL_PREDICTION_ONLY_MARKET_STARTER_UNAVAILABLE",
                "feature_rows": len(saves),
                "goalies": int(saves.player_id.nunique()) if len(saves) else 0,
                "games": int(saves.game_id.nunique()) if len(saves) else 0,
                "prediction_rows": len(save_predictions),
                "starter_selections": 0,
                "prediction_semantics": save_identity["operational_amendment"]["semantic_contract"],
                "model": save_identity["model_name"],
                "model_version": save_identity["model_version"],
            },
            "sog": {"status": "BLOCKED_SEASON_2026_5V5_TOI_UNAVAILABLE", "prediction_rows": 0},
            "external_api_requests": 0,
        }
        (staging / "lane_prediction_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        files = sorted(path for path in staging.iterdir() if path.is_file())
        (staging / "SHA256SUMS").write_text("".join(f"{digest(path)}  {path.name}\n" for path in files))
        staging.rename(root)
        print(root)
        return 0
    except BaseException:
        raise


if __name__ == "__main__":
    raise SystemExit(main())

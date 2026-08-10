#!/usr/bin/env python3
"""Create a governed mainline/SOG game-spine CSV from nhl.games."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

import psycopg

LABELS = {1: "PRESEASON", 2: "REGULAR_SEASON", 3: "POSTSEASON"}
FIELDS = ["canonical_season", "slate_date", "game_id", "game_date",
          "scheduled_start_time_utc", "home_team_id", "home_team",
          "away_team_id", "away_team", "game_status", "game_type_code",
          "game_type_label"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--health-root", type=Path, default=Path("artifacts/operational/nhl/slates"))
    args = ap.parse_args()
    if args.output.exists():
        raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
    health = json.loads((args.health_root / args.slate_date / "slate_health.json").read_text())
    if health.get("slate_date") != args.slate_date or not health.get("downstream_ready"):
        raise SystemExit("SLATE_NOT_DOWNSTREAM_READY")
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("Missing SUPABASE_DB_URL / DATABASE_URL")
    with psycopg.connect(db) as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT season, game_date, game_id, game_date, start_time_utc,
                 home_team_id, home_team_code, away_team_id, away_team_code,
                 status, game_type
          FROM nhl.games WHERE game_date=%s ORDER BY game_id
        """, (args.slate_date,))
        rows = cur.fetchall()
    if health["completion_status"] == "VALID_EMPTY_SLATE" and rows:
        raise SystemExit("EMPTY_SLATE_DATABASE_MISMATCH")
    if health["completion_status"] == "READY" and len(rows) != health["normalized_game_count"]:
        raise SystemExit("SLATE_COMPLETENESS_MISMATCH")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            record = dict(zip(FIELDS[:-1], row))
            code = record["game_type_code"]
            record["game_type_label"] = LABELS.get(code, "UNKNOWN_GAME_TYPE")
            writer.writerow(record)
    print(args.output)


if __name__ == "__main__":
    main()

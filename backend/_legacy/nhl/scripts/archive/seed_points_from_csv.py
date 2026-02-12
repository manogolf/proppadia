#!/usr/bin/env python3
"""
Seed goals/assists 'points' from a CSV into Postgres (Supabase) without psql.
- Creates a temp table _points_stage
- COPY FROM the CSV
- Upserts into nhl.import_skater_logs_stage
- Promotes to nhl.skater_game_logs_raw for the given slate date

Usage:
  SUPABASE_DB_URL=postgres://... python backend/nhl/scripts/seed_points_from_csv.py \
      --csv backend/exports/points_stage_2025-10-28.csv \
      --date 2025-10-28
"""
from __future__ import annotations
import os, sys, argparse, pathlib
import psycopg2
from psycopg2.extras import execute_values

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to points_stage_<date>.csv")
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD (ET)")
    ap.add_argument("--dburl", default=os.environ.get("SUPABASE_DB_URL", ""), help="Postgres URL")
    args = ap.parse_args()

    if not args.dburl:
        print("FATAL: Provide --dburl or set SUPABASE_DB_URL", file=sys.stderr)
        sys.exit(2)

    csv_path = pathlib.Path(args.csv).resolve()
    if not csv_path.exists():
        print(f"FATAL: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    conn = psycopg2.connect(args.dburl)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1) temp stage table
        cur.execute("""
        DROP TABLE IF EXISTS _points_stage;
        CREATE TEMP TABLE _points_stage(
          player_id bigint,
          game_id   bigint,
          game_date date,
          goals     int,
          assists   int
        );
        """)

        # 2) COPY CSV into temp table
        with csv_path.open("r", encoding="utf-8") as f:
            cur.copy_expert(
                "COPY _points_stage (player_id, game_id, game_date, goals, assists) FROM STDIN WITH (FORMAT csv, HEADER true)",
                f
            )
        # Count loaded rows
        cur.execute("SELECT COUNT(*) FROM _points_stage;")
        (loaded_rows,) = cur.fetchone()
        print(f"[seed_points_from_csv.py] loaded_rows={loaded_rows}")

        # 3) Upsert into nhl.import_skater_logs_stage
        cur.execute("""
        INSERT INTO nhl.import_skater_logs_stage (player_id, game_id, game_date, goals, assists)
        SELECT player_id, game_id, game_date, goals, assists
        FROM _points_stage
        ON CONFLICT (player_id, game_id) DO UPDATE
          SET goals = EXCLUDED.goals,
              assists = EXCLUDED.assists;
        """)
        print(f"[seed_points_from_csv.py] stage upsert: {cur.rowcount} rows considered")

        # 4) Promote/keep raw in sync for the slate date
        cur.execute("""
        WITH src AS (
          SELECT DISTINCT s.player_id, s.game_id, s.game_date, s.goals, s.assists
          FROM nhl.import_skater_logs_stage s
          WHERE s.game_date = %s::date
        )
        INSERT INTO nhl.skater_game_logs_raw (player_id, game_id, game_date, goals, assists, points)
        SELECT player_id, game_id, game_date, goals, assists, COALESCE(goals,0)+COALESCE(assists,0)
        FROM src
        ON CONFLICT (player_id, game_id) DO UPDATE
          SET goals   = EXCLUDED.goals,
              assists = EXCLUDED.assists,
              points  = EXCLUDED.points;
        """, (args.date,))
        print(f"[seed_points_from_csv.py] raw sync upserted: {cur.rowcount} rows (by conflict rules)")

        conn.commit()
        print("✅ seed_points_from_csv.py finished.")
    except Exception as e:
        conn.rollback()
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()

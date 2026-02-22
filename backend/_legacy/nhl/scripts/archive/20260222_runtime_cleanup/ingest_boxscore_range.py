#!/usr/bin/env python3
import argparse, os
import datetime as dt
import psycopg

from ingest_boxscore import ingest_game  # same module you just fixed

DB = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    raise SystemExit("Missing SUPABASE_DB_URL / DATABASE_URL")

def main():
    ap = argparse.ArgumentParser(description="Backfill NHL boxscore+PBP for a date range")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)

    # Option A: drive from existing nhl.games (schedule already loaded)
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT game_id
            FROM nhl.games
            WHERE game_date BETWEEN %s AND %s
            ORDER BY game_id
        """, (start, end))
        rows = cur.fetchall()

    if not rows:
        print(f"No games found in nhl.games between {start} and {end}")
        return

    for (game_id,) in rows:
        print(f"--> Ingesting game {game_id}")
        ingest_game(int(game_id))

if __name__ == "__main__":
    main()

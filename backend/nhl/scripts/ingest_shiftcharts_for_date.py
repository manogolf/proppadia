#!/usr/bin/env python3
"""
ingest_shiftcharts_for_date.py

Fetch NHL shiftcharts from api.nhle.com for all games on a given game_date (ET),
and upsert raw shift rows into Postgres.

Usage:
  SLATE_DATE=2026-01-02 python backend/nhl/scripts/ingest_shiftcharts_for_date.py
  python backend/nhl/scripts/ingest_shiftcharts_for_date.py --date 2026-01-02

Env:
  DATABASE_URL or SUPABASE_DB_URL must be set.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests


SHIFTCHARTS_URL = "https://api.nhle.com/stats/rest/en/shiftcharts"
DEFAULT_TIMEOUT_SEC = 30
SLEEP_BETWEEN_GAMES_SEC = 0.25  # be nice to the endpoint


def require_db_url() -> str:
    return os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL") or ""


def die(msg: str) -> None:
    raise SystemExit(msg)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (ET). If omitted, uses SLATE_DATE env.")
    ap.add_argument("--dry-run", action="store_true", help="Fetch + parse, but do not write to DB.")
    return ap.parse_args()


def fetch_game_ids_for_date(conn, game_date: str) -> List[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT game_id::bigint
            FROM nhl.games
            WHERE game_date = DATE %s
            ORDER BY game_id;
            """,
            (game_date,),
        )
        return [int(r[0]) for r in cur.fetchall()]


def get_latest_team_map_for_games(conn, game_ids: List[int]) -> Dict[Tuple[int, int], int]:
    if not game_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH rs AS (
              SELECT DISTINCT ON (game_id, player_id)
                game_id::bigint,
                player_id::bigint,
                team_id::int
              FROM nhl.roster_status
              WHERE game_id = ANY(%s)
              ORDER BY game_id, player_id, asof_ts DESC
            )
            SELECT game_id, player_id, team_id
            FROM rs;
            """,
            (game_ids,),
        )
        out: Dict[Tuple[int, int], int] = {}
        for game_id, player_id, team_id in cur.fetchall():
            out[(int(game_id), int(player_id))] = int(team_id)
        return out


def fetch_shiftcharts(game_id: int) -> List[Dict[str, Any]]:
    params = {"cayenneExp": f"gameId={game_id}"}
    r = requests.get(SHIFTCHARTS_URL, params=params, timeout=DEFAULT_TIMEOUT_SEC)
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data", [])
    return data if isinstance(data, list) else []


# --- ensure_table(): change raw -> raw_json (and PK order if you want, but not required) ---
def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nhl.shiftcharts_raw (
              game_id     bigint      NOT NULL,
              shift_id    bigint      NOT NULL,
              player_id   bigint      NULL,
              team_id     int         NULL,

              period      int         NULL,
              start_time  text        NULL,
              end_time    text        NULL,
              duration    text        NULL,

              raw_json    jsonb       NOT NULL,
              ingested_at timestamptz NOT NULL DEFAULT now(),

              PRIMARY KEY (game_id, shift_id)
            );

            CREATE INDEX IF NOT EXISTS idx_shiftcharts_raw_game ON nhl.shiftcharts_raw (game_id);
            CREATE INDEX IF NOT EXISTS idx_shiftcharts_raw_player ON nhl.shiftcharts_raw (player_id);
            """
        )
    conn.commit()


def to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return int(x)
        return int(str(x))
    except Exception:
        return None


# --- upsert_rows(): change raw -> raw_json in the column list + update set ---
def upsert_rows(
    conn,
    rows: List[Tuple[int, int, Optional[int], Optional[int], Optional[int], Optional[str], Optional[str], Optional[str], str]],
) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO nhl.shiftcharts_raw
              (game_id, shift_id, player_id, team_id, period, start_time, end_time, duration, raw_json)
            VALUES %s
            ON CONFLICT (game_id, shift_id) DO UPDATE SET
              player_id   = EXCLUDED.player_id,
              team_id     = COALESCE(EXCLUDED.team_id, nhl.shiftcharts_raw.team_id),
              period      = EXCLUDED.period,
              start_time  = EXCLUDED.start_time,
              end_time    = EXCLUDED.end_time,
              duration    = EXCLUDED.duration,
              raw_json    = EXCLUDED.raw_json,
              ingested_at = now();
            """,
            rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
            page_size=5000,
        )
    conn.commit()
    return len(rows)

def main() -> None:
    args = parse_args()

    game_date = args.date or os.environ.get("SLATE_DATE")
    if not game_date:
        die("Provide --date YYYY-MM-DD or set SLATE_DATE.")
    try:
        _ = datetime.fromisoformat(game_date).date()
    except Exception:
        die(f"Invalid date: {game_date} (expected YYYY-MM-DD)")

    db_url = require_db_url()
    if not db_url:
        die("Missing DATABASE_URL or SUPABASE_DB_URL in environment.")

    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    ensure_table(conn)

    game_ids = fetch_game_ids_for_date(conn, game_date)
    if not game_ids:
        print(f"[shiftcharts] No games found in nhl.games for {game_date}; nothing to ingest.")
        return

    team_map = get_latest_team_map_for_games(conn, game_ids)

    total_rows = 0
    total_games = 0
    total_errors = 0

    print(f"[shiftcharts] date={game_date} games={len(game_ids)} dry_run={args.dry_run}")

    for gid in game_ids:
        total_games += 1
        try:
            data = fetch_shiftcharts(gid)

            out_rows = []
            for item in data:
                shift_id = to_int(item.get("id"))
                if shift_id is None:
                    continue

                player_id = to_int(item.get("playerId"))
                period = to_int(item.get("period"))
                start_time = item.get("startTime")
                end_time = item.get("endTime")
                duration = item.get("duration")

                team_id = team_map.get((gid, player_id)) if (player_id is not None) else None

                # ✅ critical fix: serialize dict -> json text for psycopg2
                raw_json = json.dumps(item, separators=(",", ":"), ensure_ascii=False)

                out_rows.append(
                    (
                        int(gid),
                        int(shift_id),
                        int(player_id) if player_id is not None else None,
                        int(team_id) if team_id is not None else None,
                        int(period) if period is not None else None,
                        str(start_time) if start_time is not None else None,
                        str(end_time) if end_time is not None else None,
                        str(duration) if duration is not None else None,
                        raw_json,
                    )
                )

            if args.dry_run:
                print(f"[shiftcharts] game_id={gid} fetched={len(data)} parsed={len(out_rows)} (dry-run)")
                total_rows += len(out_rows)
            else:
                n = upsert_rows(conn, out_rows)
                print(f"[shiftcharts] game_id={gid} fetched={len(data)} upserted={n}")
                total_rows += len(out_rows)

        except Exception as e:
            total_errors += 1
            conn.rollback()
            print(f"[shiftcharts] ⚠️ game_id={gid} failed: {e}")
            continue


        time.sleep(SLEEP_BETWEEN_GAMES_SEC)

    print(f"[shiftcharts] done date={game_date} games={total_games} rows_parsed={total_rows} errors={total_errors}")


if __name__ == "__main__":
    main()

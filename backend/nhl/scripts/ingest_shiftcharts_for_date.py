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

import psycopg
import requests


SHIFTCHARTS_URL = "https://api.nhle.com/stats/rest/en/shiftcharts"
DEFAULT_TIMEOUT_SEC = 30
SLEEP_BETWEEN_GAMES_SEC = 0.25


def _executemany(cur, sql: str, rows: List[tuple], *, page_size: int = 5000) -> None:
    """Batch executemany for psycopg v3 without psycopg2.extras helpers."""
    if not rows:
        return
    for i in range(0, len(rows), page_size):
        cur.executemany(sql, rows[i : i + page_size])

def require_db_url() -> str:
    return os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL") or ""

def die(msg: str) -> None:
    raise SystemExit(msg)

def parse_mmss(s: str | None) -> int | None:
    if not s:
        return None
    s = s.strip()
    # common cases: "00:00", "1:23" (some feeds omit leading 0), etc.
    parts = s.split(":")
    if len(parts) != 2:
        return None
    try:
        mm = int(parts[0])
        ss = int(parts[1])
    except ValueError:
        return None
    if mm < 0 or ss < 0 or ss >= 60:
        return None
    return mm * 60 + ss

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

              -- NEW: numeric seconds (derived from the mm:ss strings)
              start_sec   int         NULL,
              end_sec     int         NULL,
              duration_sec int        NULL,

              raw_json    jsonb       NOT NULL,
              ingested_at timestamptz NOT NULL DEFAULT now(),

              PRIMARY KEY (game_id, shift_id)
            )
            """
        )
        # psycopg v3 executes one statement per prepared query.
        cur.execute(
            "ALTER TABLE nhl.shiftcharts_raw ADD COLUMN IF NOT EXISTS start_sec int"
        )
        cur.execute(
            "ALTER TABLE nhl.shiftcharts_raw ADD COLUMN IF NOT EXISTS end_sec int"
        )
        cur.execute(
            "ALTER TABLE nhl.shiftcharts_raw ADD COLUMN IF NOT EXISTS duration_sec int"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_shiftcharts_raw_game ON nhl.shiftcharts_raw (game_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_shiftcharts_raw_player ON nhl.shiftcharts_raw (player_id)"
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
    
def upsert_base_shifts(
    conn,
    rows: List[
        Tuple[
            int,  # game_id
            int,  # shift_id
            int,  # player_id
            int,  # team_id
            int,  # period
            int,  # start_sec
            int,  # end_sec
            int,  # dur_sec
            Optional[str],  # start_time
            Optional[str],  # end_time
            Optional[str],  # duration
        ]
    ],
) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        _executemany(
            cur,
            """
            INSERT INTO nhl.shiftcharts_shifts
              (game_id, shift_id, player_id, team_id, period,
               start_sec, end_sec, dur_sec,
               start_time, end_time, duration)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (game_id, shift_id) DO UPDATE SET
              player_id   = EXCLUDED.player_id,
              team_id     = EXCLUDED.team_id,
              period      = EXCLUDED.period,
              start_sec   = EXCLUDED.start_sec,
              end_sec     = EXCLUDED.end_sec,
              dur_sec     = EXCLUDED.dur_sec,
              start_time  = EXCLUDED.start_time,
              end_time    = EXCLUDED.end_time,
              duration    = EXCLUDED.duration,
              ingested_at = now();
            """,
            rows,
            page_size=5000,
        )
    conn.commit()
    return len(rows)


def to_base_shift_row(raw_row: tuple) -> Optional[tuple]:
    """
    raw_row matches out_rows tuple shape:
      (game_id, shift_id, player_id, team_id, period,
       start_time, end_time, duration,
       start_sec, end_sec, duration_sec,
       raw_json)
    Returns row for nhl.shiftcharts_shifts, or None if invalid.
    """
    (
        game_id,
        shift_id,
        player_id,
        team_id,
        period,
        start_time,
        end_time,
        duration,
        start_sec,
        end_sec,
        duration_sec,
        _raw_json,
    ) = raw_row

    # Base table requires these NOT NULL
    if game_id is None or shift_id is None or player_id is None or team_id is None or period is None:
        return None
    if start_sec is None or end_sec is None:
        return None

    # Fix cross-period / bogus cases: if end < start, clamp to period end
    if end_sec < start_sec:
        end_sec = 1200

    dur_sec = end_sec - start_sec
    if dur_sec <= 0:
        return None

    return (
        int(game_id),
        int(shift_id),
        int(player_id),
        int(team_id),
        int(period),
        int(start_sec),
        int(end_sec),
        int(dur_sec),
        str(start_time) if start_time is not None else None,
        str(end_time) if end_time is not None else None,
        str(duration) if duration is not None else None,
    )

# --- upsert_rows(): change raw -> raw_json in the column list + update set ---
from typing import List, Tuple, Optional

def upsert_rows(
    conn,
    rows: List[
        Tuple[
            int,              # game_id
            int,              # shift_id
            Optional[int],    # player_id
            Optional[int],    # team_id
            Optional[int],    # period
            Optional[str],    # start_time
            Optional[str],    # end_time
            Optional[str],    # duration
            Optional[int],    # start_sec
            Optional[int],    # end_sec
            Optional[int],    # duration_sec
            str,              # raw_json (json text)
        ]
    ],
) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        _executemany(
            cur,
            """
            INSERT INTO nhl.shiftcharts_raw
              (game_id, shift_id, player_id, team_id, period,
               start_time, end_time, duration,
               start_sec, end_sec, duration_sec,
               raw_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CAST(%s AS jsonb))
            ON CONFLICT (game_id, shift_id) DO UPDATE SET
              player_id    = EXCLUDED.player_id,
              team_id      = COALESCE(EXCLUDED.team_id, nhl.shiftcharts_raw.team_id),
              period       = EXCLUDED.period,
              start_time   = EXCLUDED.start_time,
              end_time     = EXCLUDED.end_time,
              duration     = EXCLUDED.duration,
              start_sec    = EXCLUDED.start_sec,
              end_sec      = EXCLUDED.end_sec,
              duration_sec = EXCLUDED.duration_sec,
              raw_json     = EXCLUDED.raw_json,
              ingested_at  = now();
            """,
            rows,
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

    conn = psycopg.connect(db_url, prepare_threshold=0)
    try:
        conn.prepare_threshold = 0  # type: ignore[attr-defined]
    except Exception:
        pass
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

                # NEW: compute seconds *here*
                start_sec = parse_mmss(start_time)
                end_sec = parse_mmss(end_time)

                duration_sec = parse_mmss(duration)
                if duration_sec is None and start_sec is not None and end_sec is not None:
                    duration_sec = (end_sec - start_sec) if end_sec >= start_sec else None

                team_id = team_map.get((gid, player_id)) if (player_id is not None) else None

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
                        int(start_sec) if start_sec is not None else None,
                        int(end_sec) if end_sec is not None else None,
                        int(duration_sec) if duration_sec is not None else None,
                        raw_json,
                    )
                )


            if args.dry_run:
                print(f"[shiftcharts] game_id={gid} fetched={len(data)} parsed={len(out_rows)} (dry-run)")
                total_rows += len(out_rows)
            else:
                n_raw = upsert_rows(conn, out_rows)
                print(f"[shiftcharts] game_id={gid} fetched={len(data)} upserted={n_raw}")

                total_rows += len(out_rows)

                # NEW: also upsert base shifts table
                base_rows = []
                for r in out_rows:
                    br = to_base_shift_row(r)
                    if br is not None:
                        base_rows.append(br)

                n_base = upsert_base_shifts(conn, base_rows)

                print(f"[shiftcharts] game_id={gid} fetched={len(data)} upserted_raw={n_raw} upserted_base={n_base}")
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

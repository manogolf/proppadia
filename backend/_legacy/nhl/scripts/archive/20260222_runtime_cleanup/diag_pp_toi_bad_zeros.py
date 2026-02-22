#!/usr/bin/env python3
"""
backend/nhl/scripts/diag_pp_toi_bad_zeros.py

Per game (in a date or date range), prints:
- team PP seconds (goalies-present; i.e., excludes empty-net advantage if your source supports it)
- whether shiftcharts has rows
- "bad zero" counts: skaters with pp_toi_minutes=0/NULL even though their TEAM had PP time

This is intentionally "missingness-aware": it distinguishes legit zeros from "needs fill".

USAGE
  python backend/nhl/scripts/diag_pp_toi_bad_zeros.py --date 2025-12-23
  python backend/nhl/scripts/diag_pp_toi_bad_zeros.py --start 2025-12-20 --end 2025-12-27
  python backend/nhl/scripts/diag_pp_toi_bad_zeros.py --date 2025-12-23 --db-url "$SUPABASE_DB_URL"

ENV
  SUPABASE_DB_URL or DATABASE_URL should be set if --db-url not provided.
  Requires `psql` installed and on PATH.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple


# ------------------------- helpers -------------------------


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def run_psql_csv(db_url: str, sql: str) -> List[Dict[str, str]]:
    """
    Runs a single SELECT query via psql --csv and returns a list of dict rows.
    """
    cmd = [
        "psql",
        db_url,
        "-v",
        "ON_ERROR_STOP=1",
        "--csv",
        "-q",
        "-c",
        sql,
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except FileNotFoundError:
        die("psql not found on PATH. Install Postgres client tools (psql) and retry.")
    except subprocess.CalledProcessError as e:
        die(f"psql failed:\n{e.output}")

    out = out.strip()
    if not out:
        return []
    reader = csv.DictReader(out.splitlines())
    return [row for row in reader]


def daterange(start: date, end: date) -> List[date]:
    if end < start:
        return []
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


@dataclass
class PpSource:
    rel: str
    col_game_id: str
    col_team_id: str
    col_pp_sec: str


# ------------------------- discovery -------------------------


def discover_pp_source(db_url: str) -> Optional[PpSource]:
    """
    Prefer an existing table/view that already computes team PP seconds with your rules.
    We look for a relation in schema nhl that has: game_id, team_id, and a pp-seconds column.
    Acceptable pp-seconds column names (in order of preference):
      - team_pp_seconds_goalies_present
      - pp_seconds_goalies_present
      - team_pp_seconds
      - pp_seconds
      - pp_sec
      - pp_seconds_for
    """
    sql = r"""
    WITH cols AS (
      SELECT
        c.table_schema,
        c.table_name,
        c.column_name
      FROM information_schema.columns c
      WHERE c.table_schema = 'nhl'
    ),
    rels AS (
      SELECT
        table_schema,
        table_name,
        bool_or(column_name = 'game_id') AS has_game_id,
        bool_or(column_name = 'team_id') AS has_team_id,
        bool_or(column_name = 'team_pp_seconds_goalies_present') AS has_a,
        bool_or(column_name = 'pp_seconds_goalies_present')       AS has_b,
        bool_or(column_name = 'team_pp_seconds')                  AS has_c,
        bool_or(column_name = 'pp_seconds')                       AS has_d,
        bool_or(column_name = 'pp_sec')                           AS has_e,
        bool_or(column_name = 'pp_seconds_for')                   AS has_f
      FROM cols
      GROUP BY 1,2
    )
    SELECT
      table_schema,
      table_name,
      CASE
        WHEN has_a THEN 'team_pp_seconds_goalies_present'
        WHEN has_b THEN 'pp_seconds_goalies_present'
        WHEN has_c THEN 'team_pp_seconds'
        WHEN has_d THEN 'pp_seconds'
        WHEN has_e THEN 'pp_sec'
        WHEN has_f THEN 'pp_seconds_for'
        ELSE NULL
      END AS pp_col
    FROM rels
    WHERE has_game_id AND has_team_id
      AND (has_a OR has_b OR has_c OR has_d OR has_e OR has_f)
    ORDER BY
      -- pick the "best" semantic first
      (CASE WHEN has_a THEN 0
            WHEN has_b THEN 1
            WHEN has_c THEN 2
            WHEN has_d THEN 3
            WHEN has_e THEN 4
            WHEN has_f THEN 5
            ELSE 99 END),
      table_name
    LIMIT 1;
    """
    rows = run_psql_csv(db_url, sql)
    if not rows:
        return None

    r = rows[0]
    schema = r["table_schema"]
    name = r["table_name"]
    pp_col = r["pp_col"]
    if not pp_col:
        return None
    return PpSource(rel=f"{schema}.{name}", col_game_id="game_id", col_team_id="team_id", col_pp_sec=pp_col)


# ------------------------- core diagnostics -------------------------


def fetch_games_for_date(db_url: str, game_day: date) -> List[Dict[str, str]]:
    sql = f"""
    SELECT
      game_id::bigint      AS game_id,
      home_team_id::bigint AS home_team_id,
      away_team_id::bigint AS away_team_id
    FROM nhl.games
    WHERE game_date = DATE '{game_day.isoformat()}'
    ORDER BY game_id;
    """
    return run_psql_csv(db_url, sql)


def fetch_shiftcharts_rows_by_game(db_url: str, game_day: date) -> Dict[int, int]:
    # shiftcharts may be absent for some games; we just count rows in shiftcharts_raw
    sql = f"""
    SELECT
      game_id::bigint AS game_id,
      COUNT(*)::bigint AS rows
    FROM nhl.shiftcharts_raw
    WHERE game_date = DATE '{game_day.isoformat()}'
    GROUP BY 1;
    """
    rows = run_psql_csv(db_url, sql)
    out: Dict[int, int] = {}
    for r in rows:
        out[int(r["game_id"])] = int(r["rows"])
    return out


def fetch_team_pp_seconds_by_game(db_url: str, game_day: date, src: PpSource) -> Dict[Tuple[int, int], int]:
    """
    Returns map: (game_id, team_id) -> pp_seconds (int)
    """
    sql = f"""
    SELECT
      {src.col_game_id}::bigint AS game_id,
      {src.col_team_id}::bigint AS team_id,
      COALESCE({src.col_pp_sec}, 0)::bigint AS pp_seconds
    FROM {src.rel}
    WHERE game_id IN (
      SELECT game_id FROM nhl.games WHERE game_date = DATE '{game_day.isoformat()}'
    );
    """
    rows = run_psql_csv(db_url, sql)
    out: Dict[Tuple[int, int], int] = {}
    for r in rows:
        out[(int(r["game_id"]), int(r["team_id"]))] = int(r["pp_seconds"])
    return out


def fetch_bad_zero_counts(
    db_url: str,
    game_day: date,
    src: PpSource,
) -> Dict[Tuple[int, int], int]:
    """
    bad_zero := (pp_toi_minutes is NULL or 0) AND (toi_minutes > 0) AND (team_pp_seconds > 0)

    Returns map: (game_id, team_id) -> bad_zero_count
    """
    sql = f"""
    WITH day_games AS (
      SELECT game_id::bigint AS game_id
      FROM nhl.games
      WHERE game_date = DATE '{game_day.isoformat()}'
    ),
    team_pp AS (
      SELECT
        {src.col_game_id}::bigint AS game_id,
        {src.col_team_id}::bigint AS team_id,
        COALESCE({src.col_pp_sec}, 0)::bigint AS pp_seconds
      FROM {src.rel}
      WHERE {src.col_game_id} IN (SELECT game_id FROM day_games)
    )
    SELECT
      l.game_id::bigint AS game_id,
      l.team_id::bigint AS team_id,
      COUNT(*)::bigint AS bad_zero
    FROM nhl.skater_game_logs_raw l
    JOIN day_games g ON g.game_id = l.game_id
    JOIN team_pp  tp ON tp.game_id = l.game_id AND tp.team_id = l.team_id
    WHERE COALESCE(l.pp_toi_minutes, 0) = 0
      AND COALESCE(l.toi_minutes, 0) > 0
      AND tp.pp_seconds > 0
    GROUP BY 1,2;
    """
    rows = run_psql_csv(db_url, sql)
    out: Dict[Tuple[int, int], int] = {}
    for r in rows:
        out[(int(r["game_id"]), int(r["team_id"]))] = int(r["bad_zero"])
    return out


# ------------------------- main -------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=False)
    g.add_argument("--date", help="single date YYYY-MM-DD")
    ap.add_argument("--start", help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--db-url", default=None, help="override DB url (else SUPABASE_DB_URL or DATABASE_URL)")
    ap.add_argument("--print-games", action="store_true", help="also print the list of game_ids per day")
    args = ap.parse_args()

    db_url = args.db_url or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        die("No DB url. Provide --db-url or set SUPABASE_DB_URL / DATABASE_URL.")

    # resolve date(s)
    if args.date:
        d0 = datetime.strptime(args.date, "%Y-%m-%d").date()
        start_d = end_d = d0
    else:
        if not args.start or not args.end:
            die("Provide either --date or both --start and --end.")
        start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_d = datetime.strptime(args.end, "%Y-%m-%d").date()

    # discover PP seconds source (table/view)
    src = discover_pp_source(db_url)
    if not src:
        die(
            "Could not discover a team-PP-seconds source relation in schema nhl.\n"
            "Expected a table/view with columns (game_id, team_id, <pp_seconds col>).\n"
            "If you tell me the actual relation name, I’ll hard-wire it in the script."
        )

    print(f"PP seconds source: {src.rel} (pp_col={src.col_pp_sec})")

    all_days = daterange(start_d, end_d)
    if not all_days:
        die("Empty date range.")

    grand_rows = 0
    grand_bad_zero = 0
    grand_games = 0
    grand_games_with_bad_zero = 0
    grand_games_shiftcharts_empty = 0
    grand_games_bad_zero_and_shiftcharts_empty = 0

    for day in all_days:
        games = fetch_games_for_date(db_url, day)
        if args.print_games:
            game_ids = [r["game_id"] for r in games]
            print(f"\n=== {day.isoformat()} games={len(games)}: {', '.join(game_ids)}")
        else:
            print(f"\n=== {day.isoformat()} games={len(games)}")

        shift_rows = fetch_shiftcharts_rows_by_game(db_url, day)
        pp_map = fetch_team_pp_seconds_by_game(db_url, day, src)
        bad_map = fetch_bad_zero_counts(db_url, day, src)

        # print header
        print(
            "game_id,shiftcharts_rows,pp_home_sec,pp_away_sec,bad_zero_home,bad_zero_away"
        )

        day_bad_total = 0
        day_games_with_bad = 0
        day_shift_empty = 0
        day_bad_and_shift_empty = 0

        for g_row in games:
            game_id = int(g_row["game_id"])
            home = int(g_row["home_team_id"])
            away = int(g_row["away_team_id"])

            srows = shift_rows.get(game_id, 0)
            pp_home = pp_map.get((game_id, home), 0)
            pp_away = pp_map.get((game_id, away), 0)
            bad_home = bad_map.get((game_id, home), 0)
            bad_away = bad_map.get((game_id, away), 0)

            # track totals
            grand_rows += 1
            grand_games += 1

            bad_sum = bad_home + bad_away
            day_bad_total += bad_sum
            grand_bad_zero += bad_sum

            has_bad = bad_sum > 0
            if has_bad:
                day_games_with_bad += 1
                grand_games_with_bad_zero += 1

            is_shift_empty = srows == 0
            if is_shift_empty:
                day_shift_empty += 1
                grand_games_shiftcharts_empty += 1

            if has_bad and is_shift_empty:
                day_bad_and_shift_empty += 1
                grand_games_bad_zero_and_shiftcharts_empty += 1

            print(
                f"{game_id},{srows},{pp_home},{pp_away},{bad_home},{bad_away}"
            )

        print(
            f"-- day summary: rows={len(games)} bad_zero_total={day_bad_total} "
            f"games_with_bad_zero={day_games_with_bad} shiftcharts_empty_games={day_shift_empty} "
            f"bad_zero_and_shiftcharts_empty_games={day_bad_and_shift_empty}"
        )

    print("\n=== grand summary ===")
    print(
        f"days={len(all_days)} games={grand_games} "
        f"bad_zero_total={grand_bad_zero} "
        f"games_with_bad_zero={grand_games_with_bad_zero} "
        f"shiftcharts_empty_games={grand_games_shiftcharts_empty} "
        f"bad_zero_and_shiftcharts_empty_games={grand_games_bad_zero_and_shiftcharts_empty}"
    )
    print("\nInterpretation:")
    print(
        "- bad_zero_total counts only players who have pp_toi=0/NULL even though their team had PP seconds (>0) and they played (toi>0).\n"
        "- games_with_bad_zero are the only games where a shiftcharts-derived overlap fill can actually improve coverage.\n"
        "- bad_zero_and_shiftcharts_empty_games indicates games where your fill is blocked by missing shiftcharts payload.\n"
    )


if __name__ == "__main__":
    main()

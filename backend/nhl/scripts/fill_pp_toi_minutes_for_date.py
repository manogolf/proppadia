#!/usr/bin/env python3
"""
fill_pp_toi_minutes_for_date.py

Canonical daily fixer for nhl.skater_game_logs_raw.pp_toi_minutes.

- Reads SLATE_DATE (ET) or --date
- Finds regular-season games (game_type=2) on that date
- For each game, fetches api-web.nhle.com gamecenter boxscore JSON
- Extracts per-skater PP TOI
- Updates skater_game_logs_raw.pp_toi_minutes ONLY where current value is NULL or 0
- Then enforces an invariant:
    For each (game_id, team_id) on that date, if SUM(toi_minutes)>0 and SUM(pp_toi_minutes)=0 -> FAIL

This prevents silent regressions and makes PP role/share auditable.

Usage:
  SLATE_DATE=2026-01-08 python backend/nhl/scripts/fill_pp_toi_minutes_for_date.py --commit
  python backend/nhl/scripts/fill_pp_toi_minutes_for_date.py --date 2026-01-08 --commit
  ... omit --commit to dry-run (no DB writes)

Env:
  SUPABASE_DB_URL or DATABASE_URL
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from typing import Any, Dict, Iterable, Optional, Tuple, List

import psycopg2
import psycopg2.extras
import requests


BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
UA = "proppadia-pp-toi-canonical/1.0"


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def parse_mmss_to_minutes(val: Any) -> Optional[float]:
    if val is None:
        return None

    if isinstance(val, (int, float)):
        # If a large number, treat as seconds; otherwise minutes
        if val > 60:
            return float(val) / 60.0
        return float(val)

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        m = re.match(r"^(\d+):(\d{2})$", s)
        if m:
            mm = int(m.group(1))
            ss = int(m.group(2))
            return mm + ss / 60.0

    return None


def walk_json(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_json(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from walk_json(it)


def _pid_from_item(it: Dict[str, Any]) -> Optional[int]:
    # common direct forms
    for k in ("playerId", "player_id"):
        if k in it:
            try:
                return int(it[k])
            except Exception:
                pass

    # nested player object
    p = it.get("player")
    if isinstance(p, dict):
        for k in ("playerId", "id", "player_id"):
            if k in p:
                try:
                    return int(p[k])
                except Exception:
                    pass

    # sometimes "id" is player id in skater dicts, but often it's not — keep last.
    if "id" in it:
        try:
            return int(it["id"])
        except Exception:
            pass

    return None


def _pp_minutes_from_item(it: Dict[str, Any]) -> Optional[float]:
    # try all plausible keys (minutes or seconds)
    pp_keys = (
        "powerPlayTimeOnIce",
        "ppTimeOnIce",
        "ppToi",
        "powerPlayToi",
        "powerPlayTimeOnIceSeconds",
        "ppTimeOnIceSeconds",
        "ppToiSeconds",
    )
    for k in pp_keys:
        if k in it:
            mins = parse_mmss_to_minutes(it.get(k))
            if mins is not None:
                return float(mins)

    # nested stat dict variants
    for nest_key in ("stats", "skaterStats", "playerStats"):
        v = it.get(nest_key)
        if isinstance(v, dict):
            for k in pp_keys:
                if k in v:
                    mins = parse_mmss_to_minutes(v.get(k))
                    if mins is not None:
                        return float(mins)

    return None


def extract_pp_toi_minutes_from_db(cur, game_id: int) -> Dict[int, float]:
    """
    Build {player_id: pp_minutes} from overlap between:
      - nhl.game_manpower_segments (PP windows)
      - nhl.shiftcharts_raw (player shifts)
    """
    cur.execute(
        """
        WITH pp AS (
          SELECT game_id, period, start_sec, end_sec, pp_team_id AS team_id
          FROM nhl.game_manpower_segments
          WHERE game_id = %s
        ),
        ov AS (
          SELECT
            s.player_id::bigint AS player_id,
            SUM(
              GREATEST(
                0,
                LEAST(s.end_sec, pp.end_sec) - GREATEST(s.start_sec, pp.start_sec)
              )
            )::bigint AS pp_seconds
          FROM nhl.shiftcharts_raw s
          JOIN pp
            ON pp.game_id = s.game_id
           AND pp.period  = s.period
           AND pp.team_id = s.team_id
          WHERE s.game_id = %s
          GROUP BY 1
        )
        SELECT player_id, (pp_seconds / 60.0)::float AS pp_minutes
        FROM ov
        WHERE pp_seconds > 0;
        """,
        (game_id, game_id),
    )
    out: Dict[int, float] = {}
    for pid, mins in cur.fetchall():
        out[int(pid)] = float(mins)
    return out

def fetch_boxscore(game_id: int, timeout: int = 30) -> Dict[str, Any]:
    url = BOX_URL.format(game_id=game_id)
    r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_games_for_date(cur, slate_date: str) -> List[int]:
    cur.execute(
        """
        SELECT game_id::bigint
        FROM nhl.games
        WHERE game_date = %s::date
          AND game_type = 2;
        """,
        (slate_date,),
    )
    return [int(r[0]) for r in cur.fetchall()]


def update_game(cur, game_id: int, pp_map: Dict[int, float]) -> Tuple[int, int, int]:
    """
    Returns:
      (db_rows, overlap_players, updated_statements)
    Only updates where pp_toi_minutes IS NULL OR 0.
    """
    cur.execute(
        "SELECT player_id::bigint FROM nhl.skater_game_logs_raw WHERE game_id = %s;",
        (game_id,),
    )
    db_players = {int(r[0]) for r in cur.fetchall()}
    overlap = sorted(db_players.intersection(pp_map.keys()))
    if not overlap:
        return (len(db_players), 0, 0)

    rows = [(pp_map[pid], game_id, pid) for pid in overlap]

    psycopg2.extras.execute_batch(
        cur,
        """
        UPDATE nhl.skater_game_logs_raw
           SET pp_toi_minutes = %s
         WHERE game_id = %s
           AND player_id = %s
           AND (pp_toi_minutes IS NULL OR pp_toi_minutes = 0);
        """,
        rows,
        page_size=500,
    )
    return (len(db_players), len(overlap), len(rows))


def assert_no_bad_team_games(cur, slate_date: str) -> int:
    """
    Returns count of (game_id, team_id) where:
      - team TOI > 0
      - team PP TOI sum = 0
      - BUT the team actually had PP windows (>0 seconds) in game_manpower_segments
        (i.e., appears as pp_team_id with any segment duration > 0)
    Regular season only (game_type = 2).
    """
    cur.execute(
        """
        WITH team_games AS (
          SELECT
            g.game_id::bigint AS game_id,
            l.team_id::bigint AS team_id,
            SUM(COALESCE(l.toi_minutes,0))    AS team_toi_sum,
            SUM(COALESCE(l.pp_toi_minutes,0)) AS team_pp_toi_sum
          FROM nhl.skater_game_logs_raw l
          JOIN nhl.games g ON g.game_id = l.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND l.team_id IS NOT NULL
          GROUP BY 1,2
        ),
        pp_windows AS (
          SELECT
            s.game_id::bigint      AS game_id,
            s.pp_team_id::bigint   AS team_id,
            SUM(GREATEST(0, s.end_sec - s.start_sec))::bigint AS pp_window_sec
          FROM nhl.game_manpower_segments s
          JOIN nhl.games g ON g.game_id = s.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND s.pp_team_id IS NOT NULL
          GROUP BY 1,2
        ),
        bad AS (
          SELECT
            tg.game_id,
            tg.team_id,
            tg.team_toi_sum,
            tg.team_pp_toi_sum,
            COALESCE(pw.pp_window_sec,0) AS pp_window_sec
          FROM team_games tg
          LEFT JOIN pp_windows pw
            ON pw.game_id = tg.game_id
           AND pw.team_id = tg.team_id
          WHERE tg.team_toi_sum > 0
            AND tg.team_pp_toi_sum = 0
            AND COALESCE(pw.pp_window_sec,0) > 0
        )
        SELECT COUNT(*) FROM bad;
        """,
        (slate_date, slate_date),
    )
    return int(cur.fetchone()[0])

def print_bad_team_games(cur, slate_date: str, limit: int = 20) -> None:
    cur.execute(
        """
        WITH team_games AS (
          SELECT
            g.game_id::bigint AS game_id,
            l.team_id::bigint AS team_id,
            SUM(COALESCE(l.toi_minutes,0))    AS team_toi_sum,
            SUM(COALESCE(l.pp_toi_minutes,0)) AS team_pp_toi_sum
          FROM nhl.skater_game_logs_raw l
          JOIN nhl.games g ON g.game_id = l.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND l.team_id IS NOT NULL
          GROUP BY 1,2
        ),
        pp_windows AS (
          SELECT
            s.game_id::bigint      AS game_id,
            s.pp_team_id::bigint   AS team_id,
            SUM(GREATEST(0, s.end_sec - s.start_sec))::bigint AS pp_window_sec
          FROM nhl.game_manpower_segments s
          JOIN nhl.games g ON g.game_id = s.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND s.pp_team_id IS NOT NULL
          GROUP BY 1,2
        )
        SELECT
          tg.game_id,
          tg.team_id,
          tg.team_toi_sum,
          tg.team_pp_toi_sum,
          COALESCE(pw.pp_window_sec,0) AS pp_window_sec,
          g.home_team_code,
          g.away_team_code
        FROM team_games tg
        JOIN nhl.games g ON g.game_id = tg.game_id
        LEFT JOIN pp_windows pw
          ON pw.game_id = tg.game_id AND pw.team_id = tg.team_id
        WHERE tg.team_toi_sum > 0
          AND tg.team_pp_toi_sum = 0
          AND COALESCE(pw.pp_window_sec,0) > 0
        ORDER BY pp_window_sec DESC, tg.team_toi_sum DESC
        LIMIT %s;
        """,
        (slate_date, slate_date, limit),
    )
    for r in cur.fetchall():
        print(f"BAD team-game: {r}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (ET). If omitted, uses SLATE_DATE env.")
    ap.add_argument("--commit", action="store_true", help="Write updates to DB (default: dry-run rollback).")
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep between API calls.")
    ap.add_argument("--db-url", default=None, help="Override DB url (else SUPABASE_DB_URL or DATABASE_URL).")
    args = ap.parse_args()

    slate_date = args.date or os.environ.get("SLATE_DATE")
    if not slate_date:
        die("Provide --date YYYY-MM-DD or set SLATE_DATE.")

    db_url = args.db_url or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        die("Set SUPABASE_DB_URL (or DATABASE_URL) or pass --db-url")

    commit = bool(args.commit)

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    game_ids = get_games_for_date(cur, slate_date)
    if not game_ids:
        print(f"No regular-season games found for {slate_date} (game_type=2).")
        conn.rollback()
        conn.close()
        return

    print(f"PP TOI canonical fill for {slate_date}: games={len(game_ids)} mode={'COMMIT' if commit else 'DRY-RUN'}")

    total_updates = 0
    for game_id in game_ids:
        # --- PP TOI source is DB overlap (segments + shiftcharts), not boxscore JSON ---
        try:
            pp_map = extract_pp_toi_minutes_from_db(cur, game_id)
        except Exception as e:
            print(f"game_id={game_id}: pp overlap query failed: {e}", file=sys.stderr)
            conn.rollback()
            continue

        # If we can’t compute PP overlap for this game, do not write anything.
        # Let the end-of-run invariant catch the systemic failure (if any).
        if not pp_map:
            print(
                f"game_id={game_id}: extracted pp_map empty (no PP overlap found); skipping updates for this game",
                file=sys.stderr,
            )
            conn.rollback()
            time.sleep(args.sleep)
            continue

        # Apply updates (only where pp_toi_minutes is NULL or 0)
        try:
            db_rows, overlap, upd = update_game(cur, game_id, pp_map)
        except Exception as e:
            print(f"game_id={game_id}: update_game failed: {e}", file=sys.stderr)
            conn.rollback()
            continue

        total_updates += upd
        print(f"game_id={game_id}: db_rows={db_rows} overlap={overlap} update_statements={upd}")

        if commit:
            conn.commit()
        else:
            conn.rollback()

        time.sleep(args.sleep)

    # invariant check (run in a transaction that matches mode)
    bad = assert_no_bad_team_games(cur, slate_date)
    if bad > 0:
        print_bad_team_games(cur, slate_date)
        if commit:
            conn.rollback()
        conn.close()
        die(f"Invariant failed: {bad} team-games on {slate_date} have TOI>0 but PP TOI sum=0. PP role is unsafe.", 2)

    if commit:
        conn.commit()
    else:
        conn.rollback()

    conn.close()
    print(f"DONE. total_update_statements={total_updates} bad_team_games={bad} mode={'COMMIT' if commit else 'DRY-RUN'}")


if __name__ == "__main__":
    main()

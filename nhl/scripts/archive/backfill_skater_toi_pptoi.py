#!/usr/bin/env python3
"""
Backfill TOI and PP TOI for skaters in nhl.import_skater_logs_stage
by fetching api-web boxscores per game and updating rows where those
fields are NULL. Shot attempts are NOT backfilled here (requires PBP).

Usage:
  python scripts/backfill_skater_toi_pptoi.py --db-url "<POOLER_DSN>"
"""

import argparse
import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras
import requests

API_BASE = "https://api-web.nhle.com/v1"

def mmss_to_minutes(s: Optional[str]) -> Optional[float]:
    if not s or ":" not in s:
        return None
    try:
        m, sec = s.split(":")
        return round(int(m) + int(sec) / 60.0, 2)
    except Exception:
        return None

def fetch_boxscore(game_id: int) -> Optional[Dict[str, Any]]:
    url = f"{API_BASE}/gamecenter/{game_id}/boxscore"
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        # some responses are quoted/str; try to parse
        return json.loads(r.text)

def iter_skater_arrays(bx: Dict[str, Any]):
    """Yield (is_home, side_dict, player_list) for common skater shapes."""
    bs = bx.get("boxscore")
    if isinstance(bs, dict):
        # boxscore.homeTeam / boxscore.awayTeam
        for key, is_home in (("homeTeam", True), ("awayTeam", False)):
            side = bs.get(key)
            if isinstance(side, dict):
                for pk in ("players", "skaters", "forwards", "defense"):
                    arr = side.get(pk)
                    if isinstance(arr, list) and arr:
                        yield (is_home, side, arr)
        # boxscore.teams.home / .away
        teams = bs.get("teams")
        if isinstance(teams, dict):
            for key, is_home in (("home", True), ("away", False)):
                side = teams.get(key)
                if isinstance(side, dict):
                    for pk in ("players", "skaters", "forwards", "defense"):
                        arr = side.get(pk)
                        if isinstance(arr, list) and arr:
                            yield (is_home, side, arr)

    # playerByGameStats.homeTeam / awayTeam
    pg = bx.get("playerByGameStats") or (bx.get("boxscore") or {}).get("playerByGameStats")
    if isinstance(pg, dict):
        for key, is_home in (("homeTeam", True), ("awayTeam", False)):
            side = pg.get(key)
            if isinstance(side, dict):
                for pk in ("players", "skaters", "forwards", "defense"):
                    arr = side.get(pk)
                    if isinstance(arr, list) and arr:
                        yield (is_home, side, arr)

def pick_abbr(side_dict: Dict[str, Any]) -> Optional[str]:
    if not isinstance(side_dict, dict):
        return None
    t = side_dict.get("team")
    if isinstance(t, dict):
        ab = t.get("abbrev") or t.get("teamAbbrev")
        if ab:
            return str(ab).upper()
    ab = side_dict.get("teamAbbrev") or side_dict.get("abbrev")
    return str(ab).upper() if ab else None

def find_player_record(bx: Dict[str, Any], player_id: int) -> Tuple[Optional[bool], Optional[Dict[str, Any]]]:
    """
    Return (is_home, player_dict) for the given player_id from any skater array.
    Excludes explicit goalies (position 'G').
    """
    for is_home, side_dict, arr in iter_skater_arrays(bx):
        for p in arr:
            if not isinstance(p, dict):
                continue
            pid = p.get("playerId") or p.get("id")
            if not pid or int(pid) != int(player_id):
                continue
            # exclude goalies
            pos_raw = p.get("position") or p.get("positionCode") or ""
            if isinstance(pos_raw, dict):
                pos_code = (pos_raw.get("code") or pos_raw.get("abbrev") or pos_raw.get("name") or "").upper()
            else:
                pos_code = str(pos_raw).upper()
            if pos_code == "G":
                continue
            return (bool(is_home), p)
    return (None, None)

def backfill_toi_pp(conn, limit_per_game: int = 500) -> Tuple[int, int, int]:
    """
    For games that have skaters with NULL TOI/PPTOI, fetch boxscore once
    and update all matching rows for that game.
    Returns: (games_seen, rows_considered, rows_updated)
    """
    q_games = """
        SELECT DISTINCT game_id
        FROM nhl.import_skater_logs_stage
        WHERE (toi_minutes IS NULL OR pp_toi_minutes IS NULL)
        ORDER BY game_id
    """
    with conn.cursor() as cur:
        cur.execute(q_games)
        games = [r[0] for r in cur.fetchall()]

    games_seen = 0
    rows_considered = 0
    rows_updated = 0

    for gid in games:
        bx = fetch_boxscore(gid)
        games_seen += 1
        if not isinstance(bx, dict):
            continue

        # pull player_ids needing update for this game
        q_rows = """
            SELECT player_id
            FROM nhl.import_skater_logs_stage
            WHERE game_id = %s
              AND (toi_minutes IS NULL OR pp_toi_minutes IS NULL)
            LIMIT %s
        """
        with conn.cursor() as cur:
            cur.execute(q_rows, (gid, limit_per_game))
            pids = [r[0] for r in cur.fetchall()]

        if not pids:
            continue

        updates = []  # list of (toi_minutes, pp_toi_minutes, player_id, game_id)
        for pid in pids:
            rows_considered += 1
            is_home, prec = find_player_record(bx, pid)
            if prec is None:
                continue
            toi = mmss_to_minutes(prec.get("timeOnIce") or prec.get("toi"))
            pp  = mmss_to_minutes(prec.get("powerPlayTimeOnIce") or prec.get("ppTimeOnIce"))
            if toi is None and pp is None:
                continue
            updates.append((toi if toi is not None else None,
                            pp if pp is not None else None,
                            int(pid), int(gid)))

        if not updates:
            continue

        q_upd = """
            UPDATE nhl.import_skater_logs_stage AS s
            SET
              toi_minutes    = COALESCE(%s, s.toi_minutes),
              pp_toi_minutes = COALESCE(%s, s.pp_toi_minutes)
            WHERE s.player_id = %s AND s.game_id = %s
        """
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, q_upd, updates, page_size=200)
        conn.commit()
        rows_updated += len(updates)
        # be polite to API
        time.sleep(0.06)

    return games_seen, rows_considered, rows_updated

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", default=os.environ.get("SUPABASE_DB_URL", ""),
                    help="Postgres DSN (use Supabase Session Pooler with sslmode=require)")
    args = ap.parse_args()
    if not args.db_url:
        print("❌ Provide --db-url or set SUPABASE_DB_URL", file=sys.stderr)
        sys.exit(2)

    print("🔌 Connecting to DB…")
    conn = psycopg2.connect(args.db_url)
    try:
        games_seen, rows_considered, rows_updated = backfill_toi_pp(conn)
    finally:
        conn.close()

    print(f"✅ Done. Games scanned: {games_seen}, rows considered: {rows_considered}, rows updated: {rows_updated}")
    print("ℹ️ Shot attempts were not filled (requires play-by-play).")

if __name__ == "__main__":
    main()

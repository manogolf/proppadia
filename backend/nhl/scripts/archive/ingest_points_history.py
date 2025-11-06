#!/usr/bin/env python3
"""
Backfill NHL skater points (goals + assists) from NHL boxscores.

- Reads game_ids from nhl.games within [--start, --end]
- Fetches api-web.nhle.com/v1/gamecenter/{game_id}/boxscore
- Extracts per-skater goals/assists from forwards/defense arrays
- Loads into nhl.import_skater_points_stage (for observability)
- Directly MERGEs into nhl.skater_points_raw using known home/away team_ids
  (does NOT depend on nhl.roster_status; avoids multi-match + coverage gaps)

Env:
  SUPABASE_DB_URL (psql connection string)

Deps:
  requests
"""

from __future__ import annotations
import argparse, os, sys, subprocess
from typing import List, Dict, Any, Tuple
import requests

PSQL = ["psql", "-v", "ON_ERROR_STOP=1"]

def die(msg: str, code: int = 2):
    print(f"[ingest_points] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)

def run_psql(sql: str, db_url: str) -> str:
    """Run a single SQL string via psql -c; return stdout (raises on error)."""
    proc = subprocess.run([*PSQL, db_url, "-c", sql], capture_output=True, text=True)
    if proc.returncode != 0:
        if proc.stdout:
            print(proc.stdout)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
        die(f"psql failed (exit {proc.returncode})")
    return proc.stdout

def fetch_games(db_url: str, start: str, end: str) -> List[Tuple[int,int,int,str]]:
    """Return list of (game_id, home_team_id, away_team_id, game_date 'YYYY-MM-DD')."""
    sql = f"""
    COPY (
      SELECT game_id, home_team_id, away_team_id, to_char(game_date,'YYYY-MM-DD') AS game_date
      FROM nhl.games
      WHERE game_date >= DATE '{start}' AND game_date <= DATE '{end}'
      ORDER BY game_date, game_id
    ) TO STDOUT WITH CSV HEADER;
    """
    proc = subprocess.run([*PSQL, db_url, "-A", "-F,", "-c", sql], capture_output=True, text=True)
    if proc.returncode != 0:
        if proc.stdout:
            print(proc.stdout)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
        die("failed to fetch games")
    lines = [l.strip() for l in proc.stdout.splitlines() if l.strip()]
    lines = [l for l in lines if not l.startswith("COPY ")]
    if not lines:
        return []
    header = lines[0].split(",")
    idx = {h:i for i,h in enumerate(header)}
    out: List[Tuple[int,int,int,str]] = []
    for row in lines[1:]:
        parts = row.split(",")
        try:
            out.append((
                int(parts[idx["game_id"]]),
                int(parts[idx["home_team_id"]]),
                int(parts[idx["away_team_id"]]),
                parts[idx["game_date"]],
            ))
        except Exception:
            continue
    return out

def get_boxscore(game_id: int) -> Dict[str, Any] | None:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def insert_stage_rows(db_url: str, rows: List[Tuple[int,int,int,str,int,int,int]]):
    """
    rows: (player_id, game_id, team_id, game_date, goals, assists, points)
    Stage table schema (no team_id column): nhl.import_skater_points_stage
      (player_id, game_id, game_date, goals, assists, points)
    """
    if not rows:
        return
    BATCH = 1000
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i+BATCH]
        values_sql = ",\n".join(
            f"({pid},{gid},DATE '{gdate}',{g},{a},{('NULL' if pts is None else pts)})"
            for (pid, gid, _tid, gdate, g, a, pts) in chunk
        )
        sql = f"""
        INSERT INTO nhl.import_skater_points_stage
          (player_id, game_id, game_date, goals, assists, points)
        VALUES
          {values_sql};
        """
        run_psql(sql, db_url)

def merge_rows_direct(db_url: str, rows: List[Tuple[int,int,int,str,int,int,int]]):
    """
    Directly MERGE rows into nhl.skater_points_raw using team_id from parsing,
    deriving opponent_id and is_home from nhl.games (no roster_status needed).

    rows: (player_id, game_id, team_id, game_date, goals, assists, points)
    """
    if not rows:
        return
    BATCH = 1000
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i+BATCH]
        # Build VALUES list
        values_sql = ",\n".join(
            f"({pid},{gid},{tid},DATE '{gdate}',{g},{a},{('NULL' if pts is None else pts)})"
            for (pid, gid, tid, gdate, g, a, pts) in chunk
        )
        sql = f"""
        WITH v(player_id, game_id, team_id, game_date, goals, assists, points) AS (
          VALUES
          {values_sql}
        ),
        -- Deduplicate any accidental duplicates from parsing within this batch
        v_dedup AS (
          SELECT
            player_id, game_id,
            MIN(game_date) AS game_date,
            MAX(team_id)   AS team_id,
            MAX(goals)     AS goals,
            MAX(assists)   AS assists,
            MAX(COALESCE(points, goals + assists)) AS points
          FROM v
          GROUP BY player_id, game_id
        ),
        g AS (
          SELECT game_id, home_team_id, away_team_id
          FROM nhl.games
          WHERE game_id IN (SELECT DISTINCT game_id FROM v_dedup)
        ),
        src AS (
          SELECT
            d.player_id,
            d.game_id,
            d.team_id,
            CASE
              WHEN d.team_id = g.home_team_id THEN g.away_team_id
              WHEN d.team_id = g.away_team_id THEN g.home_team_id
              ELSE NULL
            END AS opponent_id,
            (d.team_id = g.home_team_id) AS is_home,
            d.game_date,
            d.goals,
            d.assists,
            d.points
          FROM v_dedup d
          JOIN g ON g.game_id = d.game_id
          WHERE d.team_id IS NOT NULL
        )
        MERGE INTO nhl.skater_points_raw AS t
        USING src
        ON (t.player_id = src.player_id AND t.game_id = src.game_id)
        WHEN MATCHED THEN UPDATE SET
          team_id     = src.team_id,
          opponent_id = src.opponent_id,
          is_home     = src.is_home,
          game_date   = src.game_date,
          goals       = src.goals,
          assists     = src.assists,
          points      = src.points
        WHEN NOT MATCHED THEN
          INSERT (player_id, game_id, team_id, opponent_id, is_home, game_date, goals, assists, points)
          VALUES (src.player_id, src.game_id, src.team_id, src.opponent_id, src.is_home, src.game_date, src.goals, src.assists, src.points);
        """
        run_psql(sql, db_url)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD
    args = ap.parse_args()

    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        die("SUPABASE_DB_URL not set")

    games = fetch_games(db_url, args.start, args.end)
    print(f"[ingest_points] games in range {args.start}..{args.end}: {len(games)}")

    staged_count = 0
    rows_for_stage: List[Tuple[int,int,int,str,int,int,int]] = []
    rows_for_merge: List[Tuple[int,int,int,str,int,int,int]] = []

    for (game_id, home_tid, away_tid, gdate) in games:
        box = get_boxscore(game_id)
        if not box:
            continue

        pstats = (box.get("playerByGameStats") or {})
        for side_key, team_id in (("homeTeam", home_tid), ("awayTeam", away_tid)):
            team = pstats.get(side_key) or {}
            for bucket in ("forwards", "defense"):  # exclude goalies
                arr = team.get(bucket) or []
                if not isinstance(arr, list):
                    continue
                for p in arr:
                    nhl_id = p.get("playerId") or p.get("id")
                    if not nhl_id:
                        continue
                    try:
                        pid = int(nhl_id)
                    except Exception:
                        continue
                    try:
                        g = int(p.get("goals") or 0)
                        a = int(p.get("assists") or 0)
                    except Exception:
                        g, a = 0, 0
                    pts = g + a
                    tup = (pid, game_id, team_id, gdate, g, a, pts)
                    rows_for_stage.append(tup)
                    rows_for_merge.append(tup)
                    staged_count += 1

        # Periodic flush to DB to keep memory modest
        if len(rows_for_stage) >= 5000:
            insert_stage_rows(db_url, rows_for_stage)
            rows_for_stage.clear()
        if len(rows_for_merge) >= 3000:
            merge_rows_direct(db_url, rows_for_merge)
            rows_for_merge.clear()

    # Final flushes
    if rows_for_stage:
        insert_stage_rows(db_url, rows_for_stage)
        rows_for_stage.clear()
    if rows_for_merge:
        merge_rows_direct(db_url, rows_for_merge)
        rows_for_merge.clear()

    # Summaries
    out1 = run_psql(
        f"SELECT COUNT(*) AS stage_rows FROM nhl.import_skater_points_stage "
        f"WHERE game_date BETWEEN DATE '{args.start}' AND DATE '{args.end}';",
        db_url
    )
    out2 = run_psql(
        f"SELECT COUNT(*) AS raw_rows FROM nhl.skater_points_raw "
        f"WHERE game_date BETWEEN DATE '{args.start}' AND DATE '{args.end}';",
        db_url
    )
    print(out1.strip())
    print(out2.strip())
    print(f"[ingest_points] Done. Staged ~{staged_count} events from boxscores.")

if __name__ == "__main__":
    main()

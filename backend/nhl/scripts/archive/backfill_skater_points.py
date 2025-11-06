#  backend/nhl/scripts/backfill_skater_points.py

#!/usr/bin/env python3
"""
Backfill NHL skater points (goals + assists) into nhl.skater_points_raw using public PBP.

- Source: https://api-web.nhle.com/v1/gamecenter/{gamePk}/play-by-play
- Schedule: https://api-web.nhle.com/v1/schedule/YYYY-MM-DD

Writes one row per (player_id, game_id) with:
    goals, assists, points, updated_at

Mapping NHL numeric IDs -> internal player_id via nhl.player_external_ids(provider='nhl').

Usage examples:
  python backend/nhl/scripts/backfill_skater_points.py --start 2023-10-10 --end 2025-10-31
  python backend/nhl/scripts/backfill_skater_points.py --since-days 30

Environment:
  SUPABASE_DB_URL or DATABASE_URL   (Postgres DSN)
  (Optional) DEBUG_PBP=1            (prints debug)
"""

from __future__ import annotations
import argparse, os, sys, time, datetime as dt
from typing import Dict, Any, List, Optional, Tuple
import requests
import psycopg
from psycopg.rows import dict_row

API_SCHEDULE = "https://api-web.nhle.com/v1/schedule"
API_PBP      = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"

DB_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB_URL:
    print("ERROR: set SUPABASE_DB_URL or DATABASE_URL", file=sys.stderr)
    sys.exit(2)

# Force SSL/gss settings if not present (Supabase-friendly)
if "?sslmode=" not in DB_URL and "&sslmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
if "?gssencmode=" not in DB_URL and "&gssencmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "gssencmode=disable"

DEBUG = os.getenv("DEBUG_PBP") not in (None, "", "0", "false", "False")

def dprint(*a, **k):
    if DEBUG: print(*a, **k)

# ───────────────────────────────── HTTP helpers ─────────────────────────────────

class Http:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "proppadia-nhl-points/1.0"})

    def get_json(self, url: str, tries: int = 4, timeout: int = 20) -> Optional[dict]:
        for i in range(tries):
            try:
                r = self.s.get(url, timeout=timeout)
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if i + 1 == tries:
                    dprint(f"[http] fail: {url} -> {e}")
                    return None
                time.sleep(0.4 * (i + 1))
        return None

HTTP = Http()

# ───────────────────────────── PBP parsing utils ───────────────────────────────

def plays_list(pbp_root) -> List[dict]:
    """Return a flat list of play dicts across common shapes."""
    if pbp_root is None:
        return []
    if isinstance(pbp_root, list):
        return [p for p in pbp_root if isinstance(p, dict)]
    if isinstance(pbp_root, dict):
        # api-web current shape
        p = pbp_root.get("plays")
        if isinstance(p, list): return [x for x in p if isinstance(x, dict)]
        # older 'liveData.plays.allPlays'
        p2 = ((pbp_root.get("liveData") or {}).get("plays") or {}).get("allPlays")
        if isinstance(p2, list): return [x for x in p2 if isinstance(x, dict)]
        # sometimes nested under 'playByPlay'
        p3 = (pbp_root.get("playByPlay") or {}).get("plays")
        if isinstance(p3, list): return [x for x in p3 if isinstance(x, dict)]
    return []

def is_goal_event(play: dict) -> bool:
    """Detect GOAL events across shapes."""
    # Common fields
    if (play.get("typeDescKey") or "").upper() == "GOAL":
        return True
    res = play.get("result") or {}
    if (res.get("eventTypeId") or "").upper() == "GOAL":
        return True
    # Some payloads have numeric codes; we skip guessing here.
    return False

def extract_goal_and_assists(play: dict) -> Tuple[Optional[int], List[int]]:
    """
    Return (scorer_nhl_id, [assist_nhl_ids...]) robustly.
    We scan several shapes: details.*, participants[], players[].
    """
    scorer: Optional[int] = None
    assists: List[int] = []

    det = play.get("details") or {}

    # Direct keys often present
    for k in ("scoringPlayerId", "scorerId", "playerId"):
        v = det.get(k)
        if isinstance(v, int):
            scorer = v
            break
        if isinstance(v, str) and v.isdigit():
            scorer = int(v); break

    # Participants list with roles
    parts = det.get("participants") or play.get("participants") or []
    if isinstance(parts, list):
        for p in parts:
            if not isinstance(p, dict):
                continue
            role = (p.get("type") or p.get("role") or "").lower()
            pid  = p.get("playerId") or p.get("id")
            if isinstance(pid, str) and pid.isdigit():
                pid = int(pid)
            if not isinstance(pid, int):
                continue
            if role in ("scorer", "goal_scorer", "scoringplayer"):
                scorer = scorer or pid
            elif role.startswith("assist"):
                assists.append(pid)

    # Legacy 'players' array with playerType
    if scorer is None or not assists:
        for p in play.get("players") or []:
            if not isinstance(p, dict): continue
            role = (p.get("playerType") or p.get("type") or p.get("role") or "").lower()
            pid  = (p.get("player") or {}).get("id") or p.get("playerId") or p.get("id")
            if isinstance(pid, str) and pid.isdigit():
                pid = int(pid)
            if not isinstance(pid, int):
                continue
            if role in ("scorer", "scoringplayer"):
                scorer = scorer or pid
            elif role.startswith("assist"):
                assists.append(pid)

    # De-dup & cap to at most 2 assists (NHL standard)
    uniq_assists: List[int] = []
    for a in assists:
        if a not in uniq_assists:
            uniq_assists.append(a)
    if len(uniq_assists) > 2:
        uniq_assists = uniq_assists[:2]

    return scorer, uniq_assists

# ───────────────────────────── DB helpers ──────────────────────────────────────

def nhl_ext_map(conn, nhl_ids: List[int]) -> Dict[int, int]:
    """Return {nhl_numeric_id -> player_id} for the given NHL IDs using player_external_ids."""
    ids = sorted({int(x) for x in nhl_ids if isinstance(x, int)})
    if not ids:
        return {}
    sql = """
      SELECT provider_player_id::bigint AS nhl_id, player_id
      FROM nhl.player_external_ids
      WHERE provider='nhl'
        AND provider_player_id ~ '^[0-9]+$'
        AND provider_player_id::bigint = ANY(%s)
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (ids,))
        return {int(r["nhl_id"]): int(r["player_id"]) for r in cur.fetchall()}

def upsert_points(conn, tuples: List[Tuple[int,int,int,int,int]]) -> int:
    """
    Upsert rows into nhl.skater_points_raw:
      (player_id, game_id, goals, assists, points)
    ON CONFLICT (player_id, game_id) DO UPDATE SET goals/assists/points.
    """
    if not tuples:
        return 0
    sql = """
      INSERT INTO nhl.skater_points_raw (player_id, game_id, goals, assists, points, updated_at)
      VALUES (%s,%s,%s,%s,%s, NOW())
      ON CONFLICT (player_id, game_id) DO UPDATE SET
        goals    = EXCLUDED.goals,
        assists  = EXCLUDED.assists,
        points   = EXCLUDED.points,
        updated_at = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, tuples)
    return len(tuples)

def game_ids_for_date(date_str: str) -> List[int]:
    """
    Fetch schedule and return gameIds that fall on date_str in ET (API already date-keyed).
    """
    url = f"{API_SCHEDULE}/{date_str}"
    js = HTTP.get_json(url)
    if not isinstance(js, dict):
        return []
    games = []
    if "games" in js and isinstance(js["games"], list):
        for g in js["games"]:
            gid = g.get("id") or g.get("gamePk") or g.get("gameId")
            if gid: games.append(int(gid))
    elif "gameWeek" in js and isinstance(js["gameWeek"], list):
        for day in js["gameWeek"]:
            for g in day.get("games", []):
                gid = g.get("id") or g.get("gamePk") or g.get("gameId")
                if gid: games.append(int(gid))
    # De-dup and keep
    return sorted({int(x) for x in games})

# ───────────────────────────── Core backfill ───────────────────────────────────

def process_game(conn, gid: int) -> Tuple[int,int,int]:
    """
    Parse PBP for a single game, assemble points, and upsert.
    Returns (rows_upserted, unknown_nhl_ids, total_goals_seen)
    """
    url = API_PBP.format(gid=gid)
    pbp = HTTP.get_json(url)
    plays = plays_list(pbp)
    if not plays:
        dprint(f"[{gid}] empty/no PBP")
        return (0, 0, 0)

    # Collect goal events → (scorer, [assists...])
    goal_rows: List[Tuple[Optional[int], List[int]]] = []
    for p in plays:
        if not is_goal_event(p):
            continue
        scorer, assists = extract_goal_and_assists(p)
        goal_rows.append((scorer, assists))

    if not goal_rows:
        dprint(f"[{gid}] no GOAL events")
        return (0, 0, 0)

    # Aggregate NHL ID → (goals, assists)
    agg: Dict[int, Dict[str, int]] = {}
    unknown_ids: set[int] = set()

    for scorer, assists in goal_rows:
        if isinstance(scorer, int):
            agg.setdefault(scorer, {"g": 0, "a": 0})
            agg[scorer]["g"] += 1
        # Assist credit (up to two)
        for aid in assists:
            if isinstance(aid, int):
                agg.setdefault(aid, {"g": 0, "a": 0})
                agg[aid]["a"] += 1

    nhl_ids = list(agg.keys())
    xmap = nhl_ext_map(conn, nhl_ids)  # NHL id -> internal player_id

    tuples: List[Tuple[int,int,int,int,int]] = []
    for nhl_id, ga in agg.items():
        pid = xmap.get(nhl_id)
        if not pid:
            unknown_ids.add(nhl_id)
            continue
        g = int(ga.get("g", 0)); a = int(ga.get("a", 0)); pts = g + a
        tuples.append((pid, gid, g, a, pts))

    n_up = upsert_points(conn, tuples)
    return (n_up, len(unknown_ids), sum(ga["g"] for ga in agg.values()))

def daterange(start: dt.date, end: dt.date) -> List[str]:
    """Inclusive start, inclusive end: yields ISO dates."""
    out = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out

def main():
    ap = argparse.ArgumentParser(description="Backfill nhl.skater_points_raw from PBP (GOAL events).")
    ap.add_argument("--start", type=str, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end",   type=str, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--since-days", type=int, default=0, help="If set, backfills from today-n days to today.")
    ap.add_argument("--commit-every", type=int, default=250, help="Commit frequency (games).")
    args = ap.parse_args()

    today = dt.date.today()
    if args.since_days and (args.start or args.end):
        print("Use either --since-days OR --start/--end.", file=sys.stderr)
        sys.exit(2)

    if args.since_days:
        start = today - dt.timedelta(days=args.since_days)
        end   = today
    else:
        if not args.start:
            print("Provide --start YYYY-MM-DD (and optional --end).", file=sys.stderr)
            sys.exit(2)
        start = dt.date.fromisoformat(args.start)
        end   = dt.date.fromisoformat(args.end) if args.end else today

    dates = daterange(start, end)
    total_rows = total_unknown = total_goals = total_games = 0

    with psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row) as conn:
        processed = 0
        for ds in dates:
            gids = game_ids_for_date(ds)
            if not gids:
                continue
            for gid in gids:
                processed += 1
                try:
                    n_up, n_unknown, n_goals = process_game(conn, gid)
                    total_rows    += n_up
                    total_unknown += n_unknown
                    total_goals   += n_goals
                    total_games   += 1
                except Exception as e:
                    # keep moving
                    dprint(f"[{gid}] ERROR: {e}")
                if processed % args.commit_every == 0:
                    conn.commit()
        conn.commit()

    print(f"✅ Done. games={total_games} upserts={total_rows} goals_seen={total_goals} unknown_nhl_ids={total_unknown}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# backend/nhl/scripts/approx_pp_toi_from_pbp.py
#
# Purpose:
#   Compute per-skater PP TOI minutes using SHIFTCHARTS (actual shifts),
#   and PBP situationCode only to define team PP windows.
#
# DSN behavior:
#   - Uses --db-url if provided
#   - Else SUPABASE_DB_URL / DATABASE_URL from environment
#   - Else falls back to {project}/db_url.txt (back-compat)

import argparse, os, sys, time
from typing import Dict, Any, List, Tuple, Optional
import requests
import psycopg2, psycopg2.extras

API_WEB_BASE = "https://api-web.nhle.com/v1/gamecenter"
API_STATS_BASE = "https://api.nhle.com/stats/rest/en"
UA = {"User-Agent": "proppadia-nhl/1.0"}


# ----------------------------- HTTP helpers -----------------------------

def gj(url: str) -> Optional[Dict[str, Any]]:
    for _ in range(3):
        try:
            r = requests.get(url, timeout=20, headers=UA)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.5)
    return None


def fetch_pbp(gid: int) -> Optional[List[Dict[str, Any]]]:
    pbp = gj(f"{API_WEB_BASE}/{gid}/play-by-play")
    if not isinstance(pbp, dict):
        return None
    plays = pbp.get("plays") or []
    return list(plays) if isinstance(plays, list) else None


def fetch_shiftcharts(gid: int) -> Optional[List[Dict[str, Any]]]:
    # NOTE: cayenneExp syntax is required by this endpoint
    url = f"{API_STATS_BASE}/shiftcharts?cayenneExp=gameId={gid}"
    js = gj(url)
    if not isinstance(js, dict):
        return None
    data = js.get("data")
    return list(data) if isinstance(data, list) else None


# ----------------------------- Time parsing -----------------------------

def parse_mmss(v: Any) -> Optional[int]:
    if not isinstance(v, str) or ":" not in v:
        return None
    try:
        mm, ss = v.split(":", 1)
        return int(mm) * 60 + int(ss)
    except Exception:
        return None


def play_abs_seconds(ev: Dict[str, Any]) -> Optional[int]:
    """
    Absolute seconds since game start, using api-web play-by-play.
    Uses periodDescriptor.number + timeInPeriod "MM:SS" (elapsed within period).
    """
    pd = ev.get("periodDescriptor") or {}
    per = pd.get("number")
    tip = ev.get("timeInPeriod")
    if per is None:
        return None
    t = parse_mmss(tip)
    if t is None:
        return None
    return (int(per) - 1) * 20 * 60 + t


def shift_abs_interval(row: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    """
    Build absolute [start,end) seconds for a shiftchart row.
    Common fields in shiftcharts rows include:
      period (int), startTime ("MM:SS"), endTime ("MM:SS")
    """
    per = row.get("period")
    st = parse_mmss(row.get("startTime"))
    en = parse_mmss(row.get("endTime"))
    if per is None or st is None or en is None:
        return None
    try:
        per_i = int(per)
    except Exception:
        return None

    start = (per_i - 1) * 20 * 60 + st
    end = (per_i - 1) * 20 * 60 + en
    # Defensive: if end <= start, ignore
    if end <= start:
        return None
    return (start, end)


# ----------------------------- situationCode -> PP windows -----------------------------

def parse_situation(code: str) -> Optional[Tuple[int, int, int, int]]:
    """
    situationCode is typically 4 digits: A B C D
      A = away goalie present (0/1)
      B = away skaters
      C = home skaters
      D = home goalie present (0/1)
    Example: 1551 => 5v5, 1541 => away 5 vs home 4 (away advantage), 1451 => home advantage.
    """
    if not code or len(code) != 4 or not code.isdigit():
        return None
    return (int(code[0]), int(code[1]), int(code[2]), int(code[3]))


def team_has_advantage(code: str) -> Optional[Tuple[bool, bool]]:
    parsed = parse_situation(code)
    if not parsed:
        return None
    _ag, away_skaters, home_skaters, _hg = parsed
    return (home_skaters > away_skaters, away_skaters > home_skaters)


def build_pp_intervals_from_situation(plays: List[Dict[str, Any]]) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
    """
    Returns (home_pp_intervals, away_pp_intervals) as absolute seconds [start,end).
    We treat any segment where one side has MORE skaters than the other as PP time for that side.
    """
    pts: List[Tuple[int, bool, bool]] = []
    for ev in plays:
        t = play_abs_seconds(ev)
        adv = team_has_advantage(ev.get("situationCode") or "")
        if t is None or adv is None:
            continue
        pts.append((t, adv[0], adv[1]))

    if not pts:
        return ([], [])

    pts.sort(key=lambda x: x[0])

    home_int: List[Tuple[int, int]] = []
    away_int: List[Tuple[int, int]] = []

    cur_home: Optional[int] = None
    cur_away: Optional[int] = None

    for i in range(len(pts) - 1):
        t0, h_adv, a_adv = pts[i]
        t1, _, _ = pts[i + 1]
        if t1 <= t0:
            continue

        if h_adv:
            if cur_home is None:
                cur_home = t0
        else:
            if cur_home is not None:
                home_int.append((cur_home, t0))
                cur_home = None

        if a_adv:
            if cur_away is None:
                cur_away = t0
        else:
            if cur_away is not None:
                away_int.append((cur_away, t0))
                cur_away = None

    # close at last timestamp we saw
    last_t = pts[-1][0]
    if cur_home is not None and last_t > cur_home:
        home_int.append((cur_home, last_t))
    if cur_away is not None and last_t > cur_away:
        away_int.append((cur_away, last_t))

    # drop any degenerate
    home_int = [(s, e) for (s, e) in home_int if e > s]
    away_int = [(s, e) for (s, e) in away_int if e > s]
    return (home_int, away_int)


def intervals_total_seconds(ints: List[Tuple[int, int]]) -> int:
    return sum(max(0, e - s) for (s, e) in ints)


def overlap_seconds(a: Tuple[int, int], b: Tuple[int, int]) -> int:
    s = max(a[0], b[0])
    e = min(a[1], b[1])
    return max(0, e - s)


def interval_list_overlap_seconds(seg: Tuple[int, int], intervals: List[Tuple[int, int]]) -> int:
    # intervals are few; simple scan is fine
    return sum(overlap_seconds(seg, iv) for iv in intervals)


# ----------------------------- Main -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", default=None, help="Postgres DSN/URL. If omitted, uses env then ./db_url.txt")
    ap.add_argument("--project", default=".", help="Project root for db_url.txt fallback")
    ap.add_argument("--limit-games", type=int, default=0, help="Optional limit of games to process")
    ap.add_argument("--commit-every", type=int, default=50, help="Commit frequency")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    dsn = (args.db_url or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL"))
    if not dsn:
        try:
            with open(os.path.join(args.project, "db_url.txt"), "r") as f:
                dsn = f.read().strip()
        except Exception:
            print("ERROR: provide --db-url or set SUPABASE_DB_URL / DATABASE_URL", file=sys.stderr)
            sys.exit(2)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # Games with missing PP TOI (NULL or 0)
    cur.execute("""
      WITH g AS (
        SELECT game_id
        FROM nhl.skater_game_logs_raw
        WHERE pp_toi_minutes IS NULL OR pp_toi_minutes = 0
        GROUP BY game_id
      )
      SELECT g.game_id,
             MAX(CASE WHEN gm.home_team_id = t.team_id THEN t.team ELSE opp.team END) AS home_abbr,
             MAX(CASE WHEN gm.away_team_id = t.team_id THEN t.team ELSE opp.team END) AS away_abbr
      FROM g
      JOIN nhl.games gm ON gm.game_id = g.game_id
      JOIN nhl.teams t  ON t.team_id  = gm.home_team_id
      JOIN nhl.teams opp ON opp.team_id = gm.away_team_id
      GROUP BY g.game_id
      ORDER BY g.game_id
    """)
    games = cur.fetchall()
    if args.limit_games and len(games) > args.limit_games:
        games = games[:args.limit_games]

    processed = 0
    updated_rows = 0

    for (gid, home_abbr, away_abbr) in games:
        processed += 1

        plays = fetch_pbp(int(gid))
        if not plays:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: no/empty PBP → skip", flush=True)
            continue

        shifts = fetch_shiftcharts(int(gid))
        if not shifts:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: no shiftcharts → skip", flush=True)
            continue

        home_pp_int, away_pp_int = build_pp_intervals_from_situation(plays)
        home_pp_sec = intervals_total_seconds(home_pp_int)
        away_pp_sec = intervals_total_seconds(away_pp_int)

        # Fetch skater rows needing update (NULL or 0)
        cur.execute("""
          SELECT player_id, is_home, toi_minutes
          FROM nhl.skater_game_logs_raw
          WHERE game_id = %s AND (pp_toi_minutes IS NULL OR pp_toi_minutes = 0)
        """, (gid,))
        sk_rows = cur.fetchall()
        if not sk_rows:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: nothing to update", flush=True)
            continue

        want_pids = set()
        toi_by_pid: Dict[int, Optional[float]] = {}
        home_flag_by_pid: Dict[int, bool] = {}
        for (pid, is_home, toi_min) in sk_rows:
            try:
                pid_i = int(pid)
            except Exception:
                continue
            want_pids.add(pid_i)
            home_flag_by_pid[pid_i] = bool(is_home)
            try:
                toi_by_pid[pid_i] = float(toi_min) if toi_min is not None else None
            except Exception:
                toi_by_pid[pid_i] = None

        # Sum PP overlap seconds per player from shiftcharts
        pp_sec_by_pid: Dict[int, int] = {pid: 0 for pid in want_pids}

        for row in shifts:
            pid = row.get("playerId")
            tab = row.get("teamAbbrev")
            if not isinstance(pid, int):
                continue
            if pid not in want_pids:
                continue
            if tab not in (home_abbr, away_abbr):
                continue

            seg = shift_abs_interval(row)
            if seg is None:
                continue

            if tab == home_abbr:
                pp_sec_by_pid[pid] += interval_list_overlap_seconds(seg, home_pp_int)
            else:
                pp_sec_by_pid[pid] += interval_list_overlap_seconds(seg, away_pp_int)

        updates: List[Tuple[float, int, int]] = []
        for pid in want_pids:
            pp_min = round(pp_sec_by_pid.get(pid, 0) / 60.0, 2)
            toi = toi_by_pid.get(pid)
            if toi is not None:
                pp_min = min(pp_min, float(toi))
            updates.append((pp_min, pid, int(gid)))

        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE nhl.skater_game_logs_raw AS s SET
              pp_toi_minutes = data.pp_min
            FROM (VALUES %s) AS data(pp_min, player_id, game_id)
            WHERE s.player_id = data.player_id
              AND s.game_id   = data.game_id
              AND (s.pp_toi_minutes IS NULL OR s.pp_toi_minutes = 0)
            """,
            updates,
            page_size=500,
        )
        updated_rows += cur.rowcount

        if args.verbose:
            print(
                f"[{processed}/{len(games)}] {gid}: "
                f"pp_home_sec={home_pp_sec} pp_away_sec={away_pp_sec} "
                f"updates={len(updates)} rowcount={cur.rowcount}",
                flush=True,
            )

        if processed % args.commit_every == 0:
            conn.commit()
            if args.verbose:
                print(f"… committed @ {processed}/{len(games)} (rows updated so far: {updated_rows})", flush=True)

    conn.commit()
    print(f"✅ Done. Games scanned: {processed}, rows updated: {updated_rows}")


if __name__ == "__main__":
    main()

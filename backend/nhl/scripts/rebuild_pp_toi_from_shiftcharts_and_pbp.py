#!/usr/bin/env python3
# backend/nhl/scripts/rebuild_pp_toi_from_shiftcharts_and_pbp.py
#
# Replacement (not supplementation):
#   Rebuild pp_toi_minutes from scratch for games in a date range by:
#     1) deriving team PP windows from api-web PBP situationCode
#        - include manpower advantages (5v4, 5v3, 4v3, etc.)
#        - EXCLUDE empty-net / goalie-pulled segments (require both goalies present)
#     2) summing overlap between each player's shift intervals (shiftcharts) and PP windows
#   Overwrites pp_toi_minutes for ALL skater rows in each processed game.
#
# Usage:
#   python backend/nhl/scripts/rebuild_pp_toi_from_shiftcharts_and_pbp.py \
#     --start-date 2025-10-07 \
#     --end-date   2025-12-27 \
#     --verbose
#
# DSN resolution order:
#   1) --db-url
#   2) env SUPABASE_DB_URL / DATABASE_URL
#   3) {project}/db_url.txt fallback

import argparse
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict

import requests
import psycopg2
import psycopg2.extras

PBP_BASE = "https://api-web.nhle.com/v1/gamecenter"
SHIFTCHARTS_BASE = "https://api.nhle.com/stats/rest/en/shiftcharts"
UA = {"User-Agent": "proppadia-nhl/1.0"}

# ----------------------------- HTTP helpers -----------------------------

def gj(url: str, headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    h = dict(UA)
    if headers:
        h.update(headers)
    for _ in range(3):
        try:
            r = requests.get(url, timeout=20, headers=h)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.5)
    return None

def fetch_pbp_plays(game_id: int) -> Optional[List[Dict[str, Any]]]:
    j = gj(f"{PBP_BASE}/{game_id}/play-by-play")
    if not isinstance(j, dict):
        return None
    plays = j.get("plays") or []
    return plays if isinstance(plays, list) else None

def fetch_shiftcharts_rows(game_id: int) -> Optional[List[Dict[str, Any]]]:
    # NHL stats API shape is typically: {"data":[...]}.
    # cayenneExp must be URL-encoded.
    url = f"{SHIFTCHARTS_BASE}?cayenneExp=gameId%3D{game_id}"
    j = gj(url)
    if not isinstance(j, dict):
        return None
    rows = j.get("data")
    if not isinstance(rows, list):
        return []
    # keep only dict rows
    return [r for r in rows if isinstance(r, dict)]

# ----------------------------- time parsing -----------------------------

def mmss_to_seconds(mmss: str) -> Optional[int]:
    if not isinstance(mmss, str) or ":" not in mmss:
        return None
    try:
        m, s = mmss.split(":", 1)
        return int(m) * 60 + int(s)
    except Exception:
        return None

def play_abs_seconds(ev: Dict[str, Any]) -> Optional[int]:
    # api-web pbp uses periodDescriptor.number + timeInPeriod "MM:SS"
    pd = ev.get("periodDescriptor") or {}
    per = pd.get("number")
    tip = ev.get("timeInPeriod")
    if per is None:
        # some payloads also include "period" directly; accept as fallback
        per = ev.get("period")
    if per is None:
        return None
    sec_in_per = mmss_to_seconds(tip) if isinstance(tip, str) else None
    if sec_in_per is None:
        return None
    try:
        per_i = int(per)
    except Exception:
        return None
    # Use 20-min periods as base. (Good enough for interval edges; OT intervals will still be consistent.)
    return (per_i - 1) * 20 * 60 + sec_in_per

def shift_abs_interval(row: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    # shiftcharts rows typically have: period (int), startTime "MM:SS", endTime "MM:SS"
    per = row.get("period")
    st = row.get("startTime")
    et = row.get("endTime")
    if per is None or not isinstance(st, str) or not isinstance(et, str):
        return None
    ss = mmss_to_seconds(st)
    es = mmss_to_seconds(et)
    if ss is None or es is None:
        return None
    try:
        per_i = int(per)
    except Exception:
        return None
    start_abs = (per_i - 1) * 20 * 60 + ss
    end_abs = (per_i - 1) * 20 * 60 + es
    # Sometimes endTime can be equal/earlier due to edge formatting; guard it.
    if end_abs <= start_abs:
        return None
    return (start_abs, end_abs)

# ----------------------------- situationCode parsing -----------------------------

def parse_situation(code: str) -> Optional[Tuple[int, int, int, int]]:
    """
    situationCode is typically 4 digits "ABCD":
      A = away goalie present (0/1)
      B = away skaters
      C = home skaters
      D = home goalie present (0/1)
    """
    if not code or not isinstance(code, str) or len(code) != 4 or not code.isdigit():
        return None
    ag = int(code[0])
    a  = int(code[1])
    h  = int(code[2])
    hg = int(code[3])
    return (ag, a, h, hg)

def build_pp_windows_from_situation(
    plays: List[Dict[str, Any]],
    require_goalies_present: bool = True
) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
    """
    Return (home_pp_windows, away_pp_windows) where each is a list of [start,end) abs-seconds.

    Rules (default per your request):
      - PP = manpower advantage (home skaters > away skaters) or vice versa
      - Exclude empty net / goalie-pulled: require both goalies present (ag==1 and hg==1)
    """
    pts: List[Tuple[int, bool, bool]] = []  # (t, home_adv, away_adv)
    for ev in plays:
        t = play_abs_seconds(ev)
        if t is None:
            continue
        parsed = parse_situation(ev.get("situationCode") or "")
        if not parsed:
            continue
        ag, away_skaters, home_skaters, hg = parsed
        if require_goalies_present and not (ag == 1 and hg == 1):
            # exclude any segment where either goalie is absent
            home_adv = False
            away_adv = False
        else:
            home_adv = (home_skaters > away_skaters)
            away_adv = (away_skaters > home_skaters)
        pts.append((t, home_adv, away_adv))

    if len(pts) < 2:
        return ([], [])

    pts.sort(key=lambda x: x[0])

    home_w: List[Tuple[int, int]] = []
    away_w: List[Tuple[int, int]] = []

    cur_home: Optional[int] = None
    cur_away: Optional[int] = None

    for i in range(len(pts) - 1):
        t0, home_adv, away_adv = pts[i]
        t1, _, _ = pts[i + 1]
        if t1 <= t0:
            continue

        # home PP window tracking
        if home_adv:
            if cur_home is None:
                cur_home = t0
        else:
            if cur_home is not None:
                home_w.append((cur_home, t0))
                cur_home = None

        # away PP window tracking
        if away_adv:
            if cur_away is None:
                cur_away = t0
        else:
            if cur_away is not None:
                away_w.append((cur_away, t0))
                cur_away = None

    # close any open interval at last observed timestamp
    last_t = pts[-1][0]
    if cur_home is not None and last_t > cur_home:
        home_w.append((cur_home, last_t))
    if cur_away is not None and last_t > cur_away:
        away_w.append((cur_away, last_t))

    # merge adjacent/overlapping windows (defensive)
    def merge(ws: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        if not ws:
            return []
        ws = sorted(ws)
        out = [ws[0]]
        for s, e in ws[1:]:
            ps, pe = out[-1]
            if s <= pe:
                out[-1] = (ps, max(pe, e))
            else:
                out.append((s, e))
        return out

    return (merge(home_w), merge(away_w))

# ----------------------------- overlap math -----------------------------

def overlap_seconds(a: Tuple[int, int], b: Tuple[int, int]) -> int:
    s = max(a[0], b[0])
    e = min(a[1], b[1])
    return max(0, e - s)

def sum_overlap_seconds(shifts: List[Tuple[int, int]], windows: List[Tuple[int, int]]) -> int:
    if not shifts or not windows:
        return 0
    # both lists are typically small; O(n*m) is fine
    tot = 0
    for sh in shifts:
        for w in windows:
            tot += overlap_seconds(sh, w)
    return tot

# ----------------------------- main -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--db-url", default=None, help="Postgres DSN/URL (pooler OK)")
    ap.add_argument("--project", default=".", help="Project root for db_url.txt fallback")
    ap.add_argument("--limit-games", type=int, default=0, help="Optional limit of games to process")
    ap.add_argument("--commit-every", type=int, default=50, help="Commit frequency (games)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    # DSN resolution order:
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

    # Team code map (team_id -> team code like NYI)
    cur.execute("SELECT team_id, team FROM nhl.teams;")
    team_code_by_id = {int(tid): code for (tid, code) in cur.fetchall()}

    # Candidate games: any skater rows with pp_toi NULL/0 within date range
    cur.execute(
        """
        WITH cand AS (
          SELECT DISTINCT l.game_id
          FROM nhl.skater_game_logs_raw l
          WHERE l.game_date BETWEEN DATE %s AND DATE %s
            AND (l.pp_toi_minutes IS NULL OR l.pp_toi_minutes = 0)
        )
        SELECT g.game_id, g.game_date::date, g.home_team_id, g.away_team_id
        FROM nhl.games g
        JOIN cand c ON c.game_id = g.game_id
        WHERE g.game_date BETWEEN DATE %s AND DATE %s
        ORDER BY g.game_date, g.game_id
        """,
        (args.start_date, args.end_date, args.start_date, args.end_date),
    )
    games = cur.fetchall()
    if args.limit_games and len(games) > args.limit_games:
        games = games[: args.limit_games]

    processed = 0
    updated_rows = 0
    skipped_shiftcharts = 0
    skipped_pbp = 0

    for (gid, gdate, home_tid, away_tid) in games:
        processed += 1
        gid_i = int(gid)

        home_code = team_code_by_id.get(int(home_tid))
        away_code = team_code_by_id.get(int(away_tid))
        if not home_code or not away_code:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid_i}: missing team codes → skip", flush=True)
            continue

        # Pull shiftcharts
        sh_rows = fetch_shiftcharts_rows(gid_i)
        if sh_rows is None or len(sh_rows) == 0:
            skipped_shiftcharts += 1
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid_i}: shiftcharts empty → skip", flush=True)
            continue

        # Pull PBP
        plays = fetch_pbp_plays(gid_i)
        if plays is None or len(plays) == 0:
            skipped_pbp += 1
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid_i}: PBP empty → skip", flush=True)
            continue

        home_pp_w, away_pp_w = build_pp_windows_from_situation(plays, require_goalies_present=True)

        # Build player shifts from shiftcharts
        shifts_by_player: DefaultDict[int, List[Tuple[int, int]]] = defaultdict(list)
        team_by_player: Dict[int, str] = {}

        for r in sh_rows:
            pid = r.get("playerId")
            tab = r.get("teamAbbrev")
            if not isinstance(pid, int) or not isinstance(tab, str):
                continue
            if tab not in (home_code, away_code):
                continue
            itv = shift_abs_interval(r)
            if not itv:
                continue
            shifts_by_player[pid].append(itv)
            # if a player appears for a team, keep it (last wins; fine)
            team_by_player[pid] = tab

        if not shifts_by_player:
            skipped_shiftcharts += 1
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid_i}: shiftcharts had no usable rows → skip", flush=True)
            continue

        # Fetch ALL skater rows for this game (we overwrite pp_toi for the game)
        cur.execute(
            """
            SELECT player_id::bigint, is_home, toi_minutes
            FROM nhl.skater_game_logs_raw
            WHERE game_id = %s
            """,
            (gid_i,),
        )
        db_rows = cur.fetchall()
        if not db_rows:
            continue

        toi_by_player: Dict[int, Optional[float]] = {}
        is_home_by_player: Dict[int, bool] = {}
        for (pid, is_home, toi_min) in db_rows:
            try:
                pid_i2 = int(pid)
            except Exception:
                continue
            is_home_by_player[pid_i2] = bool(is_home)
            try:
                toi_by_player[pid_i2] = float(toi_min) if toi_min is not None else None
            except Exception:
                toi_by_player[pid_i2] = None

        # Compute pp seconds per player via overlaps
        updates: List[Tuple[float, int, int]] = []  # (pp_minutes, player_id, game_id)

        for pid_i2, is_home in is_home_by_player.items():
            # Prefer DB is_home to pick team side; do not trust shiftcharts teamAbbrev for side
            windows = home_pp_w if is_home else away_pp_w
            shifts = shifts_by_player.get(pid_i2, [])
            pp_sec = sum_overlap_seconds(shifts, windows)
            pp_min = round(pp_sec / 60.0, 2)

            # Clamp to toi_minutes (can't have more PP than total TOI)
            toi = toi_by_player.get(pid_i2)
            if toi is not None:
                pp_min = min(pp_min, float(toi))

            updates.append((pp_min, pid_i2, gid_i))

        if not updates:
            continue

        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE nhl.skater_game_logs_raw AS s SET
              pp_toi_minutes = data.pp_min
            FROM (VALUES %s) AS data(pp_min, player_id, game_id)
            WHERE s.player_id = data.player_id
              AND s.game_id   = data.game_id
            """,
            updates,
            page_size=200,
        )

        updated_rows += cur.rowcount

        if args.verbose:
            home_pp_sec = sum(e - s for (s, e) in home_pp_w)
            away_pp_sec = sum(e - s for (s, e) in away_pp_w)
            print(
                f"[{processed}/{len(games)}] {gid_i} {gdate}: "
                f"home_pp_sec={home_pp_sec} away_pp_sec={away_pp_sec} "
                f"players={len(updates)} rowcount={cur.rowcount}",
                flush=True,
            )

        if processed % args.commit_every == 0:
            conn.commit()
            if args.verbose:
                print(f"… committed @ {processed}/{len(games)} (rows updated so far: {updated_rows})", flush=True)

    conn.commit()
    print(
        f"✅ Done. Games scanned: {processed}, rows updated: {updated_rows}, "
        f"skipped_shiftcharts={skipped_shiftcharts}, skipped_pbp={skipped_pbp}"
    )

if __name__ == "__main__":
    main()

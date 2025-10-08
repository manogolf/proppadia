#  scripts/approx_pp_toi_from_pbp.py

#!/usr/bin/env python3
import argparse, os, sys, time, json
from typing import Dict, Any, List, Tuple, Optional
import requests
import psycopg2, psycopg2.extras

API_BASE = "https://api-web.nhle.com/v1/gamecenter"

def gj(url: str) -> Optional[Dict[str, Any]]:
    for _ in range(3):
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.4)
    return None

def strength_tuple(ev: Dict[str, Any]) -> Tuple[int,int]:
    """
    Return (home_on_ice, away_on_ice). If missing, return (0,0).
    """
    st = ev.get("homeTeamDefendingStrength") or ev.get("homeTeamStrength")
    # api-web PBP typically includes 'homeTeamDefendingStrength' like "5x5", "4x5" etc.
    # If absent, try details.strength
    s = (ev.get("details") or {}).get("strength")
    def parse_pair(sv):
        if not isinstance(sv, str) or "x" not in sv: return (0,0)
        a,b = sv.split("x",1)
        try: return (int(a), int(b))
        except: return (0,0)
    if s and isinstance(s, str) and "v" in s:
        # sometimes "5v4" style
        a,b = s.split("v",1)
        try: return (int(a), int(b))
        except: pass
    if st and isinstance(st, str) and "x" in st:
        return parse_pair(st)
    # last resort: event has on-ice counts?
    h = (ev.get("homeTeamOnIceCount") or 0) or 0
    a = (ev.get("awayTeamOnIceCount") or 0) or 0
    return (int(h or 0), int(a or 0))

def event_team_abbr(ev: Dict[str, Any]) -> Optional[str]:
    # team is often under 'team' or in 'details.eventOwnerTeamAbbrev'
    t = ev.get("team")
    if isinstance(t, dict):
        ab = t.get("abbrev")
        if ab: return ab
    d = ev.get("details")
    if isinstance(d, dict):
        ab = d.get("eventOwnerTeamAbbrev")
        if ab: return ab
    ab = ev.get("eventOwnerTeamAbbrev")
    return ab if isinstance(ab, str) else None

def players_by_role(ev: Dict[str, Any]) -> Dict[str,int]:
    out={}
    for p in (ev.get("players") or []):
        if not isinstance(p, dict): continue
        pid = p.get("playerId") or p.get("id")
        role= p.get("playerType") or p.get("role") or "Player"
        if isinstance(pid, int): out[role]=pid
    return out

def clock_seconds(ev: Dict[str, Any]) -> int:
    """
    Convert game clock to absolute seconds since start (approx).
    We’ll combine period + periodTimeRemaining or timeInPeriod if available.
    """
    # api-web tends to have 'period', 'timeInPeriod' like "12:34"
    per = int(ev.get("period", 0) or 0)
    t = ev.get("timeInPeriod")
    sec = 0
    if isinstance(t, str) and ":" in t:
        m,s = t.split(":",1)
        try:
            # timeInPeriod is elapsed, not remaining (api-web), but some endpoints use remaining.
            # If this ends up backwards on a few events, it only affects ordering within a small window.
            sec = int(m)*60 + int(s)
        except: pass
    # 20-min periods baseline; OT not special-cased (OK for relative windows)
    return (per-1)*20*60 + sec

def build_pp_intervals(plays: List[Dict[str,Any]], home_abbr: str, away_abbr: str) -> Dict[str,List[Tuple[int,int]]]:
    """
    Return { team_abbr: [(start_sec, end_sec), ...] } for offensive PP (they have more skaters).
    We detect any interval where one side has skater advantage (e.g., 5v4, 5v3).
    """
    pp = {home_abbr: [], away_abbr: []}
    # Iterate chronologically
    sp = sorted(plays, key=clock_seconds)
    cur = {home_abbr: None, away_abbr: None}  # start time if in PP
    for ev in sp:
        h,a = strength_tuple(ev)
        if not h or not a:
            continue
        if h > a:
            # home on PP
            if cur[home_abbr] is None: cur[home_abbr] = clock_seconds(ev)
            if cur[away_abbr] is not None:
                # away no longer PP
                s = cur[away_abbr]; e = clock_seconds(ev)
                if e>s: pp[away_abbr].append((s,e))
                cur[away_abbr]=None
        elif a > h:
            # away on PP
            if cur[away_abbr] is None: cur[away_abbr] = clock_seconds(ev)
            if cur[home_abbr] is not None:
                s = cur[home_abbr]; e = clock_seconds(ev)
                if e>s: pp[home_abbr].append((s,e))
                cur[home_abbr]=None
        else:
            # even strength—close any open PP
            for ab in (home_abbr, away_abbr):
                if cur[ab] is not None:
                    s = cur[ab]; e = clock_seconds(ev)
                    if e>s: pp[ab].append((s,e))
                    cur[ab]=None
    # close any trailing intervals at last event time
    if sp:
        last_t = clock_seconds(sp[-1])
        for ab in (home_abbr, away_abbr):
            if cur[ab] is not None:
                s = cur[ab]; e = last_t
                if e>s: pp[ab].append((s,e))
                cur[ab]=None
    return pp

def in_any_interval(t: int, intervals: List[Tuple[int,int]]) -> bool:
    for s,e in intervals:
        if s <= t <= e: return True
    return False

def is_pp_attempt(ev: Dict[str,Any]) -> bool:
    typ = (ev.get("typeDescKey") or ev.get("eventType") or "").upper()
    return typ in ("SHOT", "GOAL", "MISSED_SHOT", "BLOCKED_SHOT")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", help="Postgres DSN/URL (pooler OK). If omitted, reads ./db_url.txt", default=None)
    ap.add_argument("--project", default=".", help="Project root for db_url.txt fallback")
    ap.add_argument("--limit-games", type=int, default=0, help="Optional limit of games to process")
    ap.add_argument("--commit-every", type=int, default=200, help="Commit frequency")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    dsn = args.db_url
    if not dsn:
        with open(os.path.join(args.project, "db_url.txt"), "r") as f:
            dsn = f.read().strip()

    conn = psycopg2.connect(dsn); conn.autocommit = False
    cur = conn.cursor()

    # 1) games where skater rows have NULL pp_toi_minutes
    cur.execute("""
      WITH g AS (
        SELECT game_id
        FROM nhl.skater_game_logs_raw
        WHERE pp_toi_minutes IS NULL
        GROUP BY game_id
      )
      SELECT g.game_id,
             MAX(CASE WHEN s.is_home THEN t.abbr ELSE opp.abbr END) AS home_abbr,
             MAX(CASE WHEN NOT s.is_home THEN t.abbr ELSE opp.abbr END) AS away_abbr
      FROM g
      JOIN nhl.skater_game_logs_raw s ON s.game_id = g.game_id
      JOIN nhl.teams t  ON t.team_id  = s.team_id
      JOIN nhl.teams opp ON opp.team_id = s.opponent_id
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
        pbp = gj(f"{API_BASE}/{gid}/play-by-play")
        if not isinstance(pbp, dict):
            if args.verbose: print(f"[{processed}/{len(games)}] {gid}: no PBP → skip", flush=True)
            continue
        plays = list(pbp.get("plays") or [])
        if not plays:
            if args.verbose: print(f"[{processed}/{len(games)}] {gid}: empty PBP → skip", flush=True)
            continue

        pp_windows = build_pp_intervals(plays, home_abbr, away_abbr)
        # Count PP attempts per player per team
        team_attempts: Dict[str, Dict[int,int]] = {home_abbr:{}, away_abbr:{}}

        for ev in plays:
            if not is_pp_attempt(ev): continue
            t = clock_seconds(ev)
            ab = event_team_abbr(ev)
            if ab not in (home_abbr, away_abbr): continue
            if not in_any_interval(t, pp_windows[ab]): continue
            roles = players_by_role(ev)
            shooter = roles.get("Shooter")
            if isinstance(shooter, int):
                team_attempts[ab][shooter] = team_attempts[ab].get(shooter, 0) + 1

        # total PP seconds per team
        pp_sec = {ab: sum(e-s for (s,e) in pp_windows[ab]) for ab in (home_abbr, away_abbr)}

        # fetch all skater rows for gid (need team_id/opponent_id/is_home/player_id)
        cur.execute("""
          SELECT player_id, team_id, opponent_id, is_home
          FROM nhl.skater_game_logs_raw
          WHERE game_id = %s AND (pp_toi_minutes IS NULL)
        """, (gid,))
        sk_rows = cur.fetchall()
        if not sk_rows:
            if args.verbose: print(f"[{processed}/{len(games)}] {gid}: nothing to update", flush=True)
            continue

        # Map team_id → abbr for convenience (we already have home/away, but be safe)
        # Find which abbr matches each row
        # Simple: if is_home = true → team_abbr = home_abbr else away_abbr
        updates: List[Tuple[float,int,int]] = []  # (pp_minutes, pid, gid)

        # Precompute even-split fallback candidates: skaters who had ANY PP event as Player/Shooter etc.
        pp_participants: Dict[str, set] = {home_abbr:set(), away_abbr:set()}
        for ev in plays:
            t = clock_seconds(ev)
            ab = event_team_abbr(ev)
            if ab not in (home_abbr, away_abbr): continue
            if not in_any_interval(t, pp_windows[ab]): continue
            for pid in players_by_role(ev).values():
                if isinstance(pid, int):
                    pp_participants[ab].add(pid)

        for (pid, team_id, opp_id, is_home) in sk_rows:
            team_ab = home_abbr if is_home else away_abbr
            team_pp = pp_sec.get(team_ab, 0)
            if team_pp <= 0:
                # no PP time for this team → zero
                updates.append((0.0, pid, gid))
                continue

            attempts = team_attempts[team_ab]
            total_w = sum(attempts.values())
            if total_w > 0:
                w = attempts.get(pid, 0)
                pp_secs_for_player = (team_pp * (w / total_w))
            else:
                # even split among participants; if none, skip
                parts = pp_participants[team_ab]
                if parts:
                    share = 1.0/len(parts)
                    pp_secs_for_player = team_pp * (share if pid in parts else 0.0)
                else:
                    # cannot infer any participant—skip this game
                    pp_secs_for_player = None

            if pp_secs_for_player is None:
                continue
            updates.append((round(pp_secs_for_player/60.0, 2), pid, gid))

        if updates:
            psycopg2.extras.execute_values(
                cur,
                """
                UPDATE nhl.skater_game_logs_raw AS s SET
                  pp_toi_minutes = data.pp_min
                FROM (VALUES %s) AS data(pp_min, player_id, game_id)
                WHERE s.player_id = data.player_id
                  AND s.game_id   = data.game_id
                  AND s.pp_toi_minutes IS NULL
                """,
                updates, page_size=100
            )
            updated_rows += cur.rowcount

        if processed % args.commit_every == 0:
            conn.commit()
            if args.verbose:
                print(f"… committed @ {processed}/{len(games)} (rows updated so far: {updated_rows})", flush=True)

    conn.commit()
    print(f"✅ Done. Games scanned: {processed}, rows updated: {updated_rows}")

if __name__ == "__main__":
    main()

#  scripts/backfill_pp_toi_from_shifts.py

#!/usr/bin/env python3
import os, time, math
from typing import List, Tuple, Dict, Any
import requests
import psycopg2, psycopg2.extras

API = "https://api-web.nhle.com/v1/gamecenter"

def gj(url, tries=3, timeout=20):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.25)
    raise RuntimeError(f"GET failed {url}: {last}")

def pbp_powerplay_windows(gid: int) -> Dict[str, List[Tuple[int,int]]]:
    """
    Build man-advantage windows (in game seconds) per team from PBP.
    Returns { 'HOME'/'AWAY' team abbrev -> [(start,end), ...] } using team abbrevs.
    """
    j = gj(f"{API}/{gid}/play-by-play")
    plays = (j.get("plays") if isinstance(j, dict) else None) or []

    # We’ll track strength by team as it changes.
    # For robustness, detect from 'details' or top-level strength fields.
    def clk(pl) -> int:
        # many payloads store period/periodTime. Also often have "timeInPeriod" as "MM:SS"
        # api-web sends "timeInPeriod" and "periodDescriptor". We'll convert to game seconds.
        pd = (pl.get("periodDescriptor") or {}).get("number") or pl.get("period") or 1
        tip = pl.get("timeInPeriod") or "00:00"
        try:
            m, s = tip.split(":")
            sec = int(m)*60 + int(s)
        except Exception:
            sec = 0
        # period lengths are 20 minutes (1200s) in reg/OT we’ll keep simple.
        return (int(pd)-1)*1200 + sec

    # strength for the *attacking* team (owner) vs opponent:
    # api-web includes manpower info under 'details' in some events (e.g., 'homeSkaters','awaySkaters').
    # We'll generate windows where |homeSkaters - awaySkaters| > 0.
    last_sec = 0
    last_h = 5
    last_a = 5
    windows = {"home": [], "away": []}
    open_h: Tuple[int,int] = None
    open_a: Tuple[int,int] = None

    # build a sorted timeline of (t, homeSkaters, awaySkaters)
    timeline = []
    for pl in plays:
        d = pl.get("details") or {}
        hs = d.get("homeSkaters")
        as_ = d.get("awaySkaters")
        if hs is None and as_ is None:
            continue
        t = clk(pl)
        # sometimes only one side present—keep the previous other
        if hs is None: hs = last_h
        if as_ is None: as_ = last_a
        timeline.append((t, int(hs), int(as_)))

    # ensure a sentinel at end (approx 65 minutes)
    if timeline and timeline[-1][0] < 3900:
        timeline.append((3900, timeline[-1][1], timeline[-1][2]))

    # walk timeline, open/close PP windows when manpower advantage toggles
    for t, hs, as_ in timeline:
        # close previous segment at time t
        if open_h and last_h <= last_a:
            open_h = None
        if open_a and last_a <= last_h:
            open_a = None

        # open if advantage starts
        if hs > as_ and not open_h:
            open_h = (t, t)  # start now, end later
        if as_ > hs and not open_a:
            open_a = (t, t)

        # extend running windows
        if open_h:
            open_h = (open_h[0], t)
        if open_a:
            open_a = (open_a[0], t)

        last_h, last_a, last_sec = hs, as_, t

    # save any open windows
    if open_h and open_h[1] > open_h[0]:
        windows["home"].append(open_h)
    if open_a and open_a[1] > open_a[0]:
        windows["away"].append(open_a)

    # dedupe / merge adjacent windows
    def merge(ws):
        if not ws: return []
        ws.sort()
        out=[ws[0]]
        for s,e in ws[1:]:
            ps,pe = out[-1]
            if s <= pe: out[-1]=(ps, max(pe,e))
            else: out.append((s,e))
        return out

    windows["home"] = merge(windows["home"])
    windows["away"] = merge(windows["away"])

    # map actual team abbrevs
    home_abbr = (j.get("homeTeam") or {}).get("abbrev") or (j.get("gameCenter") or {}).get("homeTeamAbbrev") or "HOME"
    away_abbr = (j.get("awayTeam") or {}).get("abbrev") or (j.get("gameCenter") or {}).get("awayTeamAbbrev") or "AWAY"

    return {
        home_abbr: windows["home"],
        away_abbr: windows["away"],
    }

def shifts_for_game(gid: int) -> Dict[int, List[Tuple[int,int,str]]]:
    """
    Return {player_id: [(start,end,teamAbbrev), ...]} shift intervals in game seconds.
    """
    j = gj(f"{API}/{gid}/shiftcharts")
    out: Dict[int, List[Tuple[int,int,str]]] = {}
    # expected shape: { homeTeam: {players:[{playerId, teamAbbrev, shifts:[{startTime, endTime},...]},...]}, awayTeam: {...}}
    for side in ("homeTeam","awayTeam"):
        d = j.get(side) or {}
        for p in (d.get("players") or []):
            pid = p.get("playerId")
            ab  = p.get("teamAbbrev") or p.get("team") or ""
            if not isinstance(pid, int): continue
            arr = out.setdefault(pid, [])
            for sh in (p.get("shifts") or []):
                st = int(sh.get("startTime",0))
                en = int(sh.get("endTime",st))
                if en > st:
                    arr.append((st,en,ab))
    return out

def intersect_len(a: Tuple[int,int], b: Tuple[int,int]) -> int:
    s = max(a[0], b[0]); e = min(a[1], b[1])
    return max(0, e - s)

def compute_pp_minutes(gid: int) -> Dict[int, float]:
    pp_win = pbp_powerplay_windows(gid)     # {abbr: [(s,e),...]}
    shifts = shifts_for_game(gid)           # {pid: [(s,e,abbr),...]}

    # flatten PP windows into per-team map for quick check
    # we’ll sum intersections for each player's team’s PP windows.
    team_pp_map = pp_win

    pid_to_minutes: Dict[int, float] = {}
    for pid, segs in shifts.items():
        total = 0
        for s,e,abbr in segs:
            for w in team_pp_map.get(abbr, []):
                total += intersect_len((s,e), w)
        if total > 0:
            pid_to_minutes[pid] = round(total/60.0, 2)
    return pid_to_minutes

def main():
    # DSN from db_url.txt
    here = os.path.dirname(os.path.abspath(__file__))
    proj = os.path.dirname(here)
    with open(os.path.join(proj, "db_url.txt"), "r") as f:
        dsn = f.read().strip()

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    # games needing backfill
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT game_id
            FROM nhl.skater_game_logs_raw
            WHERE pp_toi_minutes IS NULL
            ORDER BY game_id
        """)
        game_ids = [r[0] for r in cur.fetchall()]

    total = 0
    for i,gid in enumerate(game_ids, 1):
        try:
            pid_minutes = compute_pp_minutes(gid)
        except Exception:
            # network/shape hiccup
            time.sleep(0.1)
            continue
        if not pid_minutes:
            if i % 50 == 0:
                print(f"[{i}/{len(game_ids)}] gid={gid}: no PP windows or no shifts", flush=True)
            continue

        rows = [(mins, pid, gid) for pid, mins in pid_minutes.items()]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                UPDATE nhl.skater_game_logs_raw AS s
                SET pp_toi_minutes = data.mins
                FROM (VALUES %s) AS data(mins, player_id, game_id)
                WHERE s.player_id = data.player_id
                  AND s.game_id   = data.game_id
                  AND s.pp_toi_minutes IS NULL
            """, rows, page_size=400)
        conn.commit()
        total += len(rows)
        if i % 25 == 0:
            print(f"[{i}/{len(game_ids)}] gid={gid}: updated {len(rows)} (total {total})", flush=True)
        time.sleep(0.06)

    print(f"✅ Done. Updated rows: {total}")
    conn.close()

if __name__ == "__main__":
    main()

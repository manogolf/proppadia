#!/usr/bin/env python3
"""
Backfill NHL skater PP TOI (pp_toi_minutes) using PBP + Shiftcharts.

What it does (per game):
  - PBP → build team PP windows whenever one side has more skaters (e.g., 5v4, 5v3).
  - Shiftcharts → per skater, list (period, start, end).
  - Intersect a skater's shifts with their TEAM's PP windows → sum seconds → minutes.
  - UPDATE nhl.skater_game_logs_raw.pp_toi_minutes (only when it's NULL or 0).

Why this script:
  - api-web 'boxscore' often lacks PP TOI for skaters; shiftcharts are reliable and granular.
  - Prior attempts based on PBP shooters miss non-shooting PP participants.

Run examples:
  # Opening fortnight of 2023–24
  python backend/nhl/scripts/backfill_pp_toi_from_shiftcharts.py \
    --db "$SUPABASE_DB_URL" --start 2023-10-10 --end 2023-10-24 --verbose

  # Full 2024 calendar year with larger commit batches
  python backend/nhl/scripts/backfill_pp_toi_from_shiftcharts.py \
    --db "$SUPABASE_DB_URL" --start 2024-01-01 --end 2024-12-31 \
    --commit-every 300 --verbose

  # Today only (default zone: ET), no DB writes
  python backend/nhl/scripts/backfill_pp_toi_from_shiftcharts.py \
    --db "$SUPABASE_DB_URL" --days 1 --dry-run

Notes:
  - Uses psycopg (v3). Disables PREPARE to avoid prepared-statement issues.
  - Safe to re-run; only touches rows still NULL/0.
"""

from __future__ import annotations
import argparse, os, sys, time, math, json
from typing import Dict, Any, List, Tuple, Optional, Iterable
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import psycopg
from psycopg.rows import dict_row
from psycopg import sql

ET = ZoneInfo("America/New_York")
API_PBP   = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
API_SHIFTS= "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gid}"

# ---- HTTP session with retries ----
def _session() -> requests.Session:
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-pp-toi-backfill"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

S = _session()

# ---- Helpers: time & strengths ----
def _parse_mmss(s: str) -> int:
    """'MM:SS' → seconds (int). Returns 0 on bad input."""
    try:
        m, sec = s.split(":", 1)
        return int(m) * 60 + int(sec)
    except Exception:
        return 0

def _abs_sec(period: int, time_in_period: str) -> int:
    """Absolute seconds from game start (20-minute periods assumed for PP windows)."""
    per = int(period or 0)
    return (per - 1) * 20 * 60 + _parse_mmss(time_in_period or "0:00")

def _strength_pair(ev: Dict[str, Any]) -> Tuple[int, int]:
    """
    Returns (home_on_ice, away_on_ice). Tries multiple spots used by api-web.
    """
    # preferred: "homeTeamDefendingStrength": "5x4"
    st = ev.get("homeTeamDefendingStrength") or ev.get("homeTeamStrength")
    if isinstance(st, str) and "x" in st:
        try:
            a, b = st.split("x", 1)
            return (int(a), int(b))
        except Exception:
            pass
    # fallback: details.strength = "5v4"
    d = ev.get("details") or {}
    s = d.get("strength")
    if isinstance(s, str) and "v" in s:
        try:
            a, b = s.split("v", 1)
            return (int(a), int(b))
        except Exception:
            pass
    # last resort: explicit on-ice counts
    h = ev.get("homeTeamOnIceCount") or 0
    a = ev.get("awayTeamOnIceCount") or 0
    return (int(h or 0), int(a or 0))

def _event_team_abbr(ev: Dict[str, Any]) -> Optional[str]:
    t = ev.get("team")
    if isinstance(t, dict):
        ab = t.get("abbrev"); 
        if isinstance(ab, str): return ab
    d = ev.get("details") or {}
    ab = d.get("eventOwnerTeamAbbrev") or ev.get("eventOwnerTeamAbbrev")
    return ab if isinstance(ab, str) else None

def _norm_abbr(ab: Optional[str]) -> Optional[str]:
    """Normalize team abbreviations to NHL 3-letter forms used by api-web/shiftcharts."""
    if not isinstance(ab, str):
        return None
    ab = ab.strip().upper()
    # Common aliases seen in some payloads / local tables
    aliases = {
        "LA": "LAK",
        "NJ": "NJD",
        "MON": "MTL",
        "TB": "TBL",
        "PHX": "ARI",
        # Historic / rare variants you might encounter:
        "PHO": "ARI",
        "NYR": "NYR",
        "NYI": "NYI",
        "STL": "STL",
        "SJ": "SJS",
        "SJSHARKS": "SJS",
        "VGOLDEN": "VGK",
    }
    return aliases.get(ab, ab)

def _merge_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Merge overlapping [s,e] intervals."""
    if not intervals:
        return []
    xs = sorted(intervals)
    out = [xs[0]]
    for s, e in xs[1:]:
        ls, le = out[-1]
        if s <= le:
            out[-1] = (ls, max(le, e))
        else:
            out.append((s, e))
    return out

def build_pp_windows_from_penalties(plays: List[Dict[str, Any]], home_abbr: str, away_abbr: str) -> Dict[str, List[Tuple[int,int]]]:
    """
    Broader fallback: detect penalties from multiple fields/variants, infer penalized team & duration.
    NOTE: We intentionally DO NOT cancel minors on goals here — we just want reliable PP windows first.
    """
    # Normalize the game-side codes once
    home_abbr = _norm_abbr(home_abbr) or home_abbr
    away_abbr = _norm_abbr(away_abbr) or away_abbr

    adv_windows = {home_abbr: [], away_abbr: []}

    def _pen_event_info(ev: Dict[str, Any]) -> Optional[Tuple[str, int]]:
        """
        Try hard to extract (penalized_team_abbr, minutes).
        Fields observed across seasons: committedByTeamAbbrev, againstTeamAbbrev, teamAbbrev,
        drawnByTeamAbbrev (then penalized is the OTHER team), eventOwnerTeamAbbrev, penaltyMinutes.
        """
        d = ev.get("details") or {}

        # Identify penalty-like events by type fields OR presence of penaltyMinutes
        kind = (ev.get("typeDescKey") or ev.get("typeDesc") or d.get("type") or d.get("descKey") or "")
        k = str(kind).lower()
        looks_pen = (
            "penalty" in k or "minor" in k or "major" in k or
            "bench" in k or "too many men" in k or d.get("penaltyMinutes") is not None
        )
        if not looks_pen:
            return None

        # Candidate team fields (direct: penalized team)
        penalized = (
            d.get("committedByTeamAbbrev")
            or d.get("againstTeamAbbrev")
            or d.get("teamAbbrev")
        )
        penalized = _norm_abbr(penalized)

        # If still unknown, try "drawnByTeamAbbrev": penalized = the OTHER team
        if penalized not in (home_abbr, away_abbr):
            drawn = _norm_abbr(d.get("drawnByTeamAbbrev"))
            if drawn in (home_abbr, away_abbr):
                penalized = away_abbr if drawn == home_abbr else home_abbr

        # Last resort: use the event owner as the penalized team
        if penalized not in (home_abbr, away_abbr):
            owner = _norm_abbr(_event_team_abbr(ev))
            if owner in (home_abbr, away_abbr):
                penalized = owner

        if penalized not in (home_abbr, away_abbr):
            return None  # still unresolved

        # Minutes: prefer explicit field; otherwise infer (major=5, "double"=4, else 2)
        mins = None
        try:
            pm = d.get("penaltyMinutes")
            if pm is not None:
                mins = int(pm)
        except Exception:
            mins = None
        if mins is None:
            mins = 5 if "major" in k else (4 if "double" in k else 2)

        return penalized, int(mins)

    pen_like = 0
    pen_resolved = 0

    # Sweep plays chronologically and collect windows (no goal-cancel for now)
    sorted_plays = sorted(plays, key=lambda ev: _abs_sec(int(ev.get("period",0) or 0), ev.get("timeInPeriod") or "0:00"))
    for ev in sorted_plays:
        t = _abs_sec(int(ev.get("period",0) or 0), ev.get("timeInPeriod") or "0:00")

        d = ev.get("details") or {}
        typ = (ev.get("typeDescKey") or ev.get("typeDesc") or d.get("type") or d.get("descKey") or "")
        if any(w in str(typ).lower() for w in ("penalty","minor","major","bench","too many men")) or d.get("penaltyMinutes") is not None:
            pen_like += 1

        info = _pen_event_info(ev)
        if info is None:
            continue

        pen_resolved += 1
        penalized, mins = info
        opp = away_abbr if penalized == home_abbr else home_abbr

        if mins == 4:  # double minor as two chunks
            adv_windows[opp].append((t, t+120))
            adv_windows[opp].append((t+120, t+240))
        else:
            adv_windows[opp].append((t, t + mins*60))

    # Merge overlapping intervals per advantaged side
    for ab in (home_abbr, away_abbr):
        adv_windows[ab] = _merge_intervals(adv_windows[ab])

    # Expose debug counters for caller
    build_pp_windows_from_penalties._last_pen_like = pen_like
    build_pp_windows_from_penalties._last_pen_resolved = pen_resolved
    return adv_windows

def build_pp_windows(plays: List[Dict[str, Any]], home_abbr: str, away_abbr: str) -> Dict[str, List[Tuple[int,int]]]:
    """
    Build offensive PP windows per team_abbr. Window is open whenever that side has more skaters.
    """
    windows = {home_abbr: [], away_abbr: []}
    cur = {home_abbr: None, away_abbr: None}
    # sort chronologically by our absolute seconds
    sorted_plays = sorted(plays, key=lambda ev: _abs_sec(int(ev.get("period",0) or 0), ev.get("timeInPeriod") or "0:00"))
    for ev in sorted_plays:
        h, a = _strength_pair(ev)
        t = _abs_sec(int(ev.get("period",0) or 0), ev.get("timeInPeriod") or "0:00")
        if not h or not a:
            continue
        if h > a:
            # home has advantage
            if cur[home_abbr] is None:
                cur[home_abbr] = t
            if cur[away_abbr] is not None:
                s = cur[away_abbr]
                if t > s: windows[away_abbr].append((s, t))
                cur[away_abbr] = None
        elif a > h:
            if cur[away_abbr] is None:
                cur[away_abbr] = t
            if cur[home_abbr] is not None:
                s = cur[home_abbr]
                if t > s: windows[home_abbr].append((s, t))
                cur[home_abbr] = None
        else:
            # even strength; close any open windows
            for ab in (home_abbr, away_abbr):
                if cur[ab] is not None:
                    s = cur[ab]
                    if t > s: windows[ab].append((s, t))
                    cur[ab] = None
    # close trailing windows at last timestamp
    if sorted_plays:
        # close any still-open windows at the last play time
        last_t = _abs_sec(int(sorted_plays[-1].get("period",0) or 0), sorted_plays[-1].get("timeInPeriod") or "0:00")
        for ab in (home_abbr, away_abbr):
            if cur[ab] is not None:
                s = cur[ab]
                if last_t > s: windows[ab].append((s, last_t))
                cur[ab] = None
    return windows

def build_pp_windows_from_shifts(shifts: List[Dict[str, Any]], home_abbr: str, away_abbr: str) -> Dict[str, List[Tuple[int,int]]]:
    """
    Build PP windows using *only* shiftcharts:
      - Count on-ice skaters per side over time
      - If away_count > home_count → AWAY PP
      - If home_count > away_count → HOME PP
    """
    home_abbr = _norm_abbr(home_abbr) or home_abbr
    away_abbr = _norm_abbr(away_abbr) or away_abbr

    # Collect (start,end) intervals per team
    team_iv = {home_abbr: [], away_abbr: []}
    for s in (shifts or []):
        ab = _norm_abbr(s.get("teamAbbrev"))
        if ab not in (home_abbr, away_abbr):
            continue
        per = int(s.get("period") or 0)
        a = _abs_sec(per, s.get("startTime") or "0:00")
        b = _abs_sec(per, s.get("endTime") or "0:00")
        if b <= a:
            continue
        team_iv[ab].append((a, b))

    def _timeline(intervals: List[Tuple[int,int]]) -> List[Tuple[int,int,int]]:
        # Sweep-line: convert intervals to [start,end,count] segments
        events = []
        for a,b in intervals:
            events.append((a, +1))
            events.append((b, -1))
        if not events:
            return []
        events.sort()
        out = []
        count = 0
        prev_t = events[0][0]
        for t, delta in events:
            if t > prev_t:
                out.append((prev_t, t, count))
            count += delta
            prev_t = t
        return out

    home_tl = _timeline(team_iv[home_abbr])
    away_tl = _timeline(team_iv[away_abbr])

    # Merge timelines & produce advantage windows
    i = j = 0
    adv = {home_abbr: [], away_abbr: []}
    while i < len(home_tl) and j < len(away_tl):
        hs, he, hc = home_tl[i]
        as_, ae, ac = away_tl[j]
        s = max(hs, as_)
        e = min(he, ae)
        if e > s:
            if ac > hc:
                adv[away_abbr].append((s, e))
            elif hc > ac:
                adv[home_abbr].append((s, e))
        if he <= ae:
            i += 1
        else:
            j += 1

    # Merge overlaps per side
    for ab in (home_abbr, away_abbr):
        adv[ab] = _merge_intervals(adv[ab])

    return adv

def _overlap(a: Tuple[int,int], b: Tuple[int,int]) -> int:
    """Overlap in seconds between [a0,a1] and [b0,b1]."""
    s = max(a[0], b[0]); e = min(a[1], b[1])
    return max(0, e - s)

# ---- DB helpers ----
def _connect(db_url: str):
    if "?sslmode=" not in db_url and "&sslmode=" not in db_url:
        db_url += ("&" if "?" in db_url else "?") + "sslmode=require"
    conn = psycopg.connect(db_url, row_factory=dict_row, autocommit=False)
    try:
        conn.prepare_threshold = 0  # avoid PREPARE
    except Exception:
        pass
    return conn

def _games_to_process(cur, start: date, end: date) -> List[Tuple[int,str,str]]:
    # returns [(game_id, home_abbr, away_abbr), ...]
    cur.execute("""
      SELECT g.game_id,
             ht.abbr AS home_abbr,
             at.abbr AS away_abbr
      FROM nhl.games g
      JOIN nhl.teams ht ON ht.team_id = g.home_team_id
      JOIN nhl.teams at ON at.team_id = g.away_team_id
      WHERE g.game_date >= %s::date AND g.game_date <= %s::date
        AND EXISTS (
          SELECT 1 FROM nhl.skater_game_logs_raw s
          WHERE s.game_id = g.game_id
            AND COALESCE(s.pp_toi_minutes,0) = 0
        )
      ORDER BY g.game_date, g.game_id
    """, (start.isoformat(), end.isoformat()))
    return [(int(r["game_id"]), r["home_abbr"], r["away_abbr"]) for r in cur.fetchall()]

def _ext_id_map(cur, nhl_ids: Iterable[int]) -> Dict[int,int]:
    ids = sorted({int(x) for x in nhl_ids if x is not None})
    if not ids:
        return {}
    cur.execute("""
      SELECT provider_player_id::bigint AS nhl_id, player_id::bigint AS player_id
      FROM nhl.player_external_ids
      WHERE provider='nhl'
        AND provider_player_id ~ '^[0-9]+$'
        AND provider_player_id::bigint = ANY(%s)
    """, (ids,))
    return {int(r["nhl_id"]): int(r["player_id"]) for r in cur.fetchall()}

def _ensure_tmp_table(cur) -> None:
    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS tmp_pp_to_update (
            pp_min numeric,
            player_id bigint,
            game_id bigint
        ) ON COMMIT PRESERVE ROWS;
    """)

def _updates_for_game(gid: int, home_abbr: str, away_abbr: str, cur, verbose=False) -> List[Tuple[float,int,int]]:
    """Return [(pp_minutes, player_id, game_id), ...] for this game."""
    # 1) PBP (for strengths) — may be incomplete for some seasons
    r = S.get(API_PBP.format(gid=gid), timeout=20)
    if r.status_code == 404:
        if verbose: print(f"[{gid}] PBP 404; continuing with shift-only derivation")
        pbp = {}
        plays = []
    else:
        r.raise_for_status()
        pbp = r.json() or {}
        plays = list(pbp.get("plays") or [])

    # Team context from PBP header if present
    game_info = pbp.get("gameCenter") or {}
    home_team_obj = (game_info.get("homeTeam") or {})
    away_team_obj = (game_info.get("awayTeam") or {})
    home_ab = (home_team_obj.get("abbrev") or home_abbr)
    away_ab = (away_team_obj.get("abbrev") or away_abbr)

    # 2) Shiftcharts (we will ALWAYS use them; they’re our source of per-player intervals)
    rs = S.get(API_SHIFTS.format(gid=gid), timeout=30)
    if rs.status_code == 404:
        if verbose: print(f"[{gid}] shiftcharts 404; skip")
        return []
    rs.raise_for_status()
    shifts = (rs.json() or {}).get("data") or []
    if not shifts:
        if verbose: print(f"[{gid}] shiftcharts empty; skip")
        return []

    # --- Build PP windows (three-tier fallback) ---
    pp_windows = {"dummy": []}  # init sentinel

    # A) Try strengths from PBP if present
    if plays:
        pp_windows = build_pp_windows(plays, home_ab, away_ab)

    # B) If empty, try penalties-from-PBP fallback (broad detector)
    if not (pp_windows.get(home_ab) or pp_windows.get(away_ab)):
        pp_windows = build_pp_windows_from_penalties(plays, home_ab, away_ab)
        if verbose and plays:
            pen_like = getattr(build_pp_windows_from_penalties, "_last_pen_like", None)
            pen_resolved = getattr(build_pp_windows_from_penalties, "_last_pen_resolved", None)
            print(f"[{gid}] penalty-fallback: pen_like={pen_like} resolved={pen_resolved} H:{len(pp_windows.get(home_ab,[]))} A:{len(pp_windows.get(away_ab,[]))}")

    # C) If still empty (common for feeds without clean penalty fields), derive from SHIFTS ONLY
    if not (pp_windows.get(home_ab) or pp_windows.get(away_ab)):
        pp_windows = build_pp_windows_from_shifts(shifts, home_ab, away_ab)
        if verbose:
            print(f"[{gid}] shift-only fallback: H:{len(pp_windows.get(home_ab,[]))} A:{len(pp_windows.get(away_ab,[]))}")

    # Bail if absolutely nothing
    if not (pp_windows.get(home_ab) or pp_windows.get(away_ab)):
        if verbose:
            print(f"[{gid}] no PP windows after strengths+penalties+shift-only; skip")
        return []

    # 3) Build per-player shift intervals in absolute seconds (by team abbrev)
    if verbose:
        # tiny per-game summary to prove data path
        total_plays = len(plays)
        count_events = sum(1 for ev in plays if ev.get("homeTeamOnIceCount") is not None and ev.get("awayTeamOnIceCount") is not None)
        diff_events = sum(1 for ev in plays
                          if ev.get("homeTeamOnIceCount") is not None
                          and ev.get("awayTeamOnIceCount") is not None
                          and (ev.get("homeTeamOnIceCount") or 0) > 0
                          and (ev.get("awayTeamOnIceCount") or 0) > 0
                          and (ev.get("homeTeamOnIceCount") != ev.get("awayTeamOnIceCount")))
        print(f"[{gid}] plays={total_plays} counts={count_events} diff={diff_events} pp_windows H:{len(pp_windows.get(home_ab,[]))} A:{len(pp_windows.get(away_ab,[]))}", flush=True)

    by_player: Dict[int, Dict[str, Any]] = {}
    for s in shifts:
        try:
            pid = int(s.get("playerId"))
        except Exception:
            continue
        per = int(s.get("period") or 0)
        st = _parse_mmss(s.get("startTime") or "0:00")
        et = _parse_mmss(s.get("endTime") or "0:00")
        if et < st:  # guard
            continue
        a = _abs_sec(per, s.get("startTime") or "0:00")
        b = _abs_sec(per, s.get("endTime") or "0:00")
        t_ab = s.get("teamAbbrev") or ""
        if not t_ab:
            continue
        by_player.setdefault(pid, {"team_abbr": t_ab, "intervals": []})
        by_player[pid]["intervals"].append((a, b))

    if not by_player:
        if verbose: print(f"[{gid}] no player shifts parsed; skip")
        return []

    # 4) Map NHL ids -> internal player_id (bulk)
    nhl_ids = list(by_player.keys())
    xmap = _ext_id_map(cur, nhl_ids)
    missing = [nid for nid in nhl_ids if nid not in xmap]
    if verbose and missing:
        print(f"[{gid}] missing extids: {len(missing)}", flush=True)

    # 5) Intersect per skater with team windows
    updates: List[Tuple[float,int,int]] = []
    for nhl_id, blob in by_player.items():
        team_ab = blob["team_abbr"]
        win = pp_windows.get(team_ab) or []
        if not win:
            # that team never on PP
            continue
        total_sec = 0
        for iv in blob["intervals"]:
            for w in win:
                total_sec += _overlap(iv, w)
        if total_sec <= 0:
            continue
        # Need internal player_id
        pid = xmap.get(nhl_id)
        if pid is None:
            # leave for a later learning pass (roster-based); skip for now
            continue
        updates.append((round(total_sec / 60.0, 2), int(pid), int(gid)))

    return updates

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", "--db-url", dest="db", default=os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"),
                    help="Postgres connection URL")
    ap.add_argument("--start", type=str, help="Start date YYYY-MM-DD (ET).")
    ap.add_argument("--end", type=str, help="End date YYYY-MM-DD (ET).")
    ap.add_argument("--days", type=int, default=0, help="If set, process [today - days + 1 .. today] (ET).")
    ap.add_argument("--commit-every", type=int, default=200, help="Commit frequency in games.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    if not args.db:
        print("Set --db or SUPABASE_DB_URL", file=sys.stderr); sys.exit(2)

    today_et = datetime.now(ET).date()
    if args.days and (args.start or args.end):
        print("Use either --days OR --start/--end, not both.", file=sys.stderr); sys.exit(2)

    if args.days:
        start = today_et - timedelta(days=max(1, args.days) - 1)
        end = today_et
    else:
        if not args.start or not args.end:
            print("Provide --start and --end (YYYY-MM-DD) or use --days.", file=sys.stderr); sys.exit(2)
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)

    conn = _connect(args.db)
    total_updates = 0
    processed = 0

    try:
        with conn.cursor() as cur:
            games = _games_to_process(cur, start, end)
            _ensure_tmp_table(cur)  # create once per connection/transaction

        if args.verbose:
            print(f"Games needing PP TOI in range {start}..{end}: {len(games)}", flush=True)

        for (gid, home_ab, away_ab) in games:
            processed += 1
            with conn.cursor() as cur:
                updates = _updates_for_game(gid, home_ab, away_ab, cur, verbose=args.verbose)

            if not updates:
                if args.verbose:
                    print(f"[{processed}/{len(games)}] {gid}: no PP skater minutes > 0; skip")
                continue

            if args.dry_run:
                if args.verbose:
                    print(f"[{processed}/{len(games)}] {gid}: would update {len(updates)} rows")
            else:
                with conn.cursor() as cur:
                    _ensure_tmp_table(cur)
                    cur.execute("TRUNCATE tmp_pp_to_update;")
                    cur.executemany(
                        "INSERT INTO tmp_pp_to_update (pp_min, player_id, game_id) VALUES (%s,%s,%s)",
                        updates
                    )
                    cur.execute("""
                        UPDATE nhl.skater_game_logs_raw AS s
                        SET pp_toi_minutes = t.pp_min
                        FROM tmp_pp_to_update t
                        WHERE s.player_id = t.player_id
                        AND s.game_id   = t.game_id
                        AND COALESCE(s.pp_toi_minutes,0) = 0;
                    """)

                total_updates += len(updates)

            if not args.dry_run and (processed % max(1, args.commit_every) == 0):
                conn.commit()
                if args.verbose:
                    print(f"… committed @ {processed}/{len(games)}; rows updated so far: {total_updates}", flush=True)

        if not args.dry_run:
            conn.commit()

        print(f"✅ Done. Games scanned: {processed}, rows updated: {total_updates}")

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()

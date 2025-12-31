#!/usr/bin/env python3
# backend/nhl/scripts/approx_pp_toi_from_shiftcharts.py
#
# Replacement behavior:
# - If shiftcharts are available for a game, compute PP TOI from shift overlap and
#   overwrite pp_toi_minutes for ALL skaters in that game.
# - Also ingests raw shiftcharts rows into nhl.shiftcharts_raw.
# - PP segments come from PBP situationCode with GOALIES PRESENT (no empty net).
#
# Run:
#   python backend/nhl/scripts/approx_pp_toi_from_shiftcharts.py \
#     --start-date 2025-10-07 --end-date 2025-12-27 --verbose
#
import argparse, os, sys, time
from typing import Dict, Any, List, Tuple, Optional
import requests
import psycopg2, psycopg2.extras

PBP_BASE = "https://api-web.nhle.com/v1/gamecenter"
SHIFTCHARTS_BASE = "https://api.nhle.com/stats/rest/en/shiftcharts"
UA = {"User-Agent": "proppadia-nhl/1.0"}

# ----------------------------- HTTP -----------------------------

def get_json(url: str, params: Optional[dict] = None) -> Optional[Dict[str, Any]]:
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=20, headers=UA)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.4)
    return None

# ----------------------------- Time parsing -----------------------------

def mmss_to_sec(s: Any) -> Optional[int]:
    if not isinstance(s, str) or ":" not in s:
        return None
    try:
        mm, ss = s.split(":", 1)
        return int(mm) * 60 + int(ss)
    except Exception:
        return None

def abs_sec(period: int, mmss: str) -> Optional[int]:
    t = mmss_to_sec(mmss)
    if t is None:
        return None
    # Treat periods as 20:00 regulation blocks; OT not handled (fine for your 2025-10..2025-12 range)
    return (int(period) - 1) * 20 * 60 + t

# ----------------------------- PBP → PP segments -----------------------------

def parse_situation(code: str) -> Optional[Tuple[int, int, int, int]]:
    """
    situationCode is typically 4 digits: A B C D
      A = away goalie present (0/1)
      B = away skaters
      C = home skaters
      D = home goalie present (0/1)
    Example: 1551 => 5v5 goalies present
    """
    if not code or len(code) != 4 or not code.isdigit():
        return None
    return (int(code[0]), int(code[1]), int(code[2]), int(code[3]))

def team_advantage_goalies_present(code: str) -> Optional[Tuple[bool, bool]]:
    """
    Returns (home_adv, away_adv) ONLY when BOTH goalies present.
    Empty-net situations are ignored by returning (False, False).
    """
    parsed = parse_situation(code)
    if not parsed:
        return None
    away_goalie, away_skaters, home_skaters, home_goalie = parsed
    if away_goalie != 1 or home_goalie != 1:
        return (False, False)  # ignore empty net for now
    return (home_skaters > away_skaters, away_skaters > home_skaters)

def pbp_event_abs_sec(ev: Dict[str, Any]) -> Optional[int]:
    pd = ev.get("periodDescriptor") or {}
    per = pd.get("number")
    tip = ev.get("timeInPeriod")
    if per is None or not tip:
        return None
    return abs_sec(int(per), str(tip))

def build_pp_segments_from_pbp(pbp_plays: List[Dict[str, Any]]) -> Tuple[List[Tuple[int,int]], List[Tuple[int,int]]]:
    """
    Returns (home_pp_segments, away_pp_segments) as lists of [start,end) in abs seconds.
    Based on situationCode advantage transitions (goalies-present only).
    """
    pts: List[Tuple[int, bool, bool]] = []
    for ev in pbp_plays:
        t = pbp_event_abs_sec(ev)
        adv = team_advantage_goalies_present(ev.get("situationCode") or "")
        if t is None or adv is None:
            continue
        pts.append((t, adv[0], adv[1]))

    if not pts:
        return ([], [])

    pts.sort(key=lambda x: x[0])

    home_seg: List[Tuple[int,int]] = []
    away_seg: List[Tuple[int,int]] = []

    cur_home: Optional[int] = None
    cur_away: Optional[int] = None

    for i in range(len(pts) - 1):
        t0, h0, a0 = pts[i]
        t1, _, _ = pts[i + 1]

        # open/close home
        if h0 and cur_home is None:
            cur_home = t0
        if (not h0) and cur_home is not None:
            if t0 > cur_home:
                home_seg.append((cur_home, t0))
            cur_home = None

        # open/close away
        if a0 and cur_away is None:
            cur_away = t0
        if (not a0) and cur_away is not None:
            if t0 > cur_away:
                away_seg.append((cur_away, t0))
            cur_away = None

    # close any trailing at last timestamp we saw
    last_t = pts[-1][0]
    if cur_home is not None and last_t > cur_home:
        home_seg.append((cur_home, last_t))
    if cur_away is not None and last_t > cur_away:
        away_seg.append((cur_away, last_t))

    # merge any accidental adjacent/overlap
    def merge(segs: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
        if not segs:
            return []
        segs = sorted(segs)
        out = [segs[0]]
        for s,e in segs[1:]:
            ps,pe = out[-1]
            if s <= pe:
                out[-1] = (ps, max(pe,e))
            else:
                out.append((s,e))
        return out

    return (merge(home_seg), merge(away_seg))

def sum_segments(segs: List[Tuple[int,int]]) -> int:
    return sum(max(0, e - s) for s,e in segs)

# ----------------------------- Shiftcharts fetch + ingest -----------------------------

def fetch_shiftcharts(game_id: int) -> Optional[List[Dict[str, Any]]]:
    """
    Stats REST shiftcharts endpoint. The payload is typically {"data":[...], ...}.
    """
    params = {"cayenneExp": f"gameId={int(game_id)}"}
    js = get_json(SHIFTCHARTS_BASE, params=params)
    if not isinstance(js, dict):
        return None
    data = js.get("data")
    if not isinstance(data, list):
        return []
    # keep only dict rows
    return [r for r in data if isinstance(r, dict)]

def normalize_shift_row(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Map API row → shiftcharts_raw columns (best-effort).
    """
    try:
        gid = int(r.get("gameId"))
        pid = int(r.get("playerId"))
        tid = int(r.get("teamId")) if r.get("teamId") is not None else None
        per = int(r.get("period")) if r.get("period") is not None else None
    except Exception:
        return None

    st = r.get("startTime")
    et = r.get("endTime")
    dur = r.get("duration")

    if per is None:
        return None

    start_s = abs_sec(per, str(st)) if st is not None else None
    end_s = abs_sec(per, str(et)) if et is not None else None

    dur_s = mmss_to_sec(str(dur)) if dur is not None else None
    if start_s is not None and end_s is not None:
        computed = max(0, end_s - start_s)
        if dur_s is None:
            dur_s = computed

    out = {
        "shift_id": int(r.get("id")) if r.get("id") is not None else None,
        "game_id": gid,
        "player_id": pid,
        "team_id": tid,
        "team_abbrev": r.get("teamAbbrev"),
        "team_name": r.get("teamName"),
        "first_name": r.get("firstName"),
        "last_name": r.get("lastName"),
        "period": per,
        "shift_number": int(r.get("shiftNumber")) if r.get("shiftNumber") is not None else None,
        "start_time": str(st) if st is not None else None,
        "end_time": str(et) if et is not None else None,
        "duration": str(dur) if dur is not None else None,
        "start_sec": start_s,
        "end_sec": end_s,
        "duration_sec": dur_s,
        "type_code": int(r.get("typeCode")) if r.get("typeCode") is not None else None,
        "detail_code": int(r.get("detailCode")) if r.get("detailCode") is not None else None,
        "event_number": int(r.get("eventNumber")) if r.get("eventNumber") is not None else None,
        "event_description": r.get("eventDescription"),
        "event_details": r.get("eventDetails"),
        "hex_value": r.get("hexValue"),
        "raw_json": r,
    }
    return out

def upsert_shiftcharts_raw(cur, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = [
        "shift_id","game_id","player_id","team_id","team_abbrev","team_name",
        "first_name","last_name","period","shift_number","start_time","end_time","duration",
        "start_sec","end_sec","duration_sec","type_code","detail_code","event_number",
        "event_description","event_details","hex_value","raw_json"
    ]
    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                row_vals.append(psycopg2.extras.Json(v) if v is not None else None)
            else:
                row_vals.append(v)
        values.append(row_vals)

    sql = f"""
      INSERT INTO nhl.shiftcharts_raw ({",".join(cols)})
      VALUES %s
      ON CONFLICT (game_id, player_id, period, shift_number) DO UPDATE SET
        shift_id = EXCLUDED.shift_id,
        team_id = EXCLUDED.team_id,
        team_abbrev = EXCLUDED.team_abbrev,
        team_name = EXCLUDED.team_name,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time,
        duration = EXCLUDED.duration,
        start_sec = EXCLUDED.start_sec,
        end_sec = EXCLUDED.end_sec,
        duration_sec = EXCLUDED.duration_sec,
        type_code = EXCLUDED.type_code,
        detail_code = EXCLUDED.detail_code,
        event_number = EXCLUDED.event_number,
        event_description = EXCLUDED.event_description,
        event_details = EXCLUDED.event_details,
        hex_value = EXCLUDED.hex_value,
        raw_json = EXCLUDED.raw_json
    """
    psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    return cur.rowcount

# ----------------------------- Overlap math -----------------------------

def overlap_seconds(a0: int, a1: int, b0: int, b1: int) -> int:
    s = max(a0, b0)
    e = min(a1, b1)
    return max(0, e - s)

def pp_seconds_for_player(shifts: List[Tuple[int,int]], pp_segs: List[Tuple[int,int]]) -> int:
    tot = 0
    for ss,se in shifts:
        if ss is None or se is None:
            continue
        for ps,pe in pp_segs:
            tot += overlap_seconds(ss,se,ps,pe)
    return tot

# ----------------------------- Main -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--db-url", default=None, help="Postgres DSN/URL (pooler OK). If omitted, uses env then ./db_url.txt")
    ap.add_argument("--project", default=".", help="Project root for db_url.txt fallback")
    ap.add_argument("--limit-games", type=int, default=0, help="Optional limit of games to process")
    ap.add_argument("--commit-every", type=int, default=100, help="Commit frequency")
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

    # Games to process: those in range with any skater rows (we overwrite only when shiftcharts exist)
    cur.execute(
        """
        SELECT DISTINCT g.game_id::bigint, g.game_date::date
        FROM nhl.games g
        JOIN nhl.skater_game_logs_raw s USING (game_id)
        WHERE g.game_date BETWEEN %s::date AND %s::date
        ORDER BY g.game_date, g.game_id
        """,
        (args.start_date, args.end_date),
    )
    games = cur.fetchall()
    if args.limit_games and len(games) > args.limit_games:
        games = games[: args.limit_games]

    processed = 0
    updated_rows = 0
    skipped_shiftcharts = 0
    skipped_pbp = 0

    for (gid, gdate) in games:
        processed += 1

        # 1) Shiftcharts
        raw = fetch_shiftcharts(int(gid))
        if raw is None:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: shiftcharts fetch failed → skip", flush=True)
            skipped_shiftcharts += 1
            continue
        if not raw:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: shiftcharts empty → skip", flush=True)
            skipped_shiftcharts += 1
            continue

        norm_rows = []
        for r in raw:
            nr = normalize_shift_row(r)
            if nr and nr.get("start_sec") is not None and nr.get("end_sec") is not None:
                norm_rows.append(nr)

        # ingest raw shifts
        upsert_shiftcharts_raw(cur, norm_rows)

        # 2) PBP segments (goalies-present only)
        pbp = get_json(f"{PBP_BASE}/{int(gid)}/play-by-play")
        plays = list((pbp or {}).get("plays") or [])
        if not plays:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: PBP empty → skip", flush=True)
            skipped_pbp += 1
            continue

        home_pp_segs, away_pp_segs = build_pp_segments_from_pbp(plays)
        home_pp_sec = sum_segments(home_pp_segs)
        away_pp_sec = sum_segments(away_pp_segs)

        # 3) Build per-player shift intervals and compute PP TOI by overlap
        by_player: Dict[int, List[Tuple[int,int]]] = {}
        team_abbrev_by_player: Dict[int, str] = {}

        for r in norm_rows:
            pid = int(r["player_id"])
            ss = int(r["start_sec"])
            ee = int(r["end_sec"])
            if ee <= ss:
                continue
            by_player.setdefault(pid, []).append((ss, ee))
            if r.get("team_abbrev"):
                team_abbrev_by_player[pid] = str(r["team_abbrev"])

        # Determine home/away team abbrevs from skater_game_logs_raw (authoritative for that game)
        cur.execute(
            """
            SELECT
              MAX(CASE WHEN s.is_home THEN t.team END) AS home_abbr,
              MAX(CASE WHEN NOT s.is_home THEN t.team END) AS away_abbr
            FROM nhl.skater_game_logs_raw s
            JOIN nhl.teams t ON t.team_id = s.team_id
            WHERE s.game_id = %s
            """,
            (gid,),
        )
        row = cur.fetchone() or (None, None)
        home_abbr, away_abbr = row[0], row[1]
        if not home_abbr or not away_abbr:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: missing home/away abbrev → skip", flush=True)
            skipped_shiftcharts += 1
            continue

        # Updates for ALL skaters in the game (replacement), but only if shiftcharts exist
        cur.execute(
            """
            SELECT player_id::bigint, team_id::bigint, is_home, toi_minutes
            FROM nhl.skater_game_logs_raw
            WHERE game_id = %s
            """,
            (gid,),
        )
        sk_rows = cur.fetchall()

        updates: List[Tuple[float, int, int]] = []
        for (pid, team_id, is_home, toi_min) in sk_rows:
            try:
                pid_i = int(pid)
            except Exception:
                continue

            # choose PP segs by side
            segs = home_pp_segs if bool(is_home) else away_pp_segs

            pp_sec = 0
            shifts = by_player.get(pid_i) or []
            if shifts and segs:
                pp_sec = pp_seconds_for_player(shifts, segs)

            pp_min = round(pp_sec / 60.0, 2)

            # clamp: pp can't exceed toi
            if toi_min is not None:
                try:
                    pp_min = min(pp_min, float(toi_min))
                except Exception:
                    pass

            updates.append((pp_min, pid_i, int(gid)))

        # If no PP segments at all, we still overwrite with 0s (replacement)
        if not updates:
            if args.verbose:
                print(f"[{processed}/{len(games)}] {gid}: no skater rows → skip", flush=True)
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
            page_size=1000,
        )
        updated_rows += cur.rowcount

        if args.verbose:
            print(
                f"[{processed}/{len(games)}] {gid} {gdate}: "
                f"home_pp_sec={home_pp_sec} away_pp_sec={away_pp_sec} "
                f"players={len(updates)} rowcount={cur.rowcount}",
                flush=True,
            )

        if processed % args.commit_every == 0:
            conn.commit()

    conn.commit()
    print(
        f"✅ Done. Games scanned: {processed}, rows updated: {updated_rows}, "
        f"skipped_shiftcharts={skipped_shiftcharts}, skipped_pbp={skipped_pbp}"
    )

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
backend/nhl/scripts/diag_pp_windows_game.py

Diagnose PP-window construction using api-web.nhle.com play-by-play for a single game_id,
and compare computed team-advantage seconds to stored sums in nhl.skater_game_logs_raw.

Assumption (matches observed codes like 1551, 0651):
  situationCode is 4 digits: A B C D
    A = away_goalie_present (1/0)
    B = away_skaters (0-6)
    C = home_skaters (0-6)
    D = home_goalie_present (1/0)

Advantage windows:
  - advantage exists when B != C
  - goalies-present windows require A==1 and D==1 (excludes empty net)

Usage:
  python backend/nhl/scripts/diag_pp_windows_game.py --game-id 2025020586
  python backend/nhl/scripts/diag_pp_windows_game.py --game-id 2025020586 --db-url "$SUPABASE_DB_URL"
"""

import argparse
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


@dataclass(frozen=True)
class Sit:
    away_goalie: int
    away_skaters: int
    home_skaters: int
    home_goalie: int


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def parse_situation(code: Optional[str]) -> Optional[Sit]:
    if not code:
        return None
    s = str(code).strip()
    if len(s) != 4 or not s.isdigit():
        return None
    a, b, c, d = (int(s[0]), int(s[1]), int(s[2]), int(s[3]))
    return Sit(away_goalie=a, away_skaters=b, home_skaters=c, home_goalie=d)


def mmss_to_seconds(mmss: str) -> Optional[int]:
    try:
        mm, ss = mmss.split(":")
        return int(mm) * 60 + int(ss)
    except Exception:
        return None


def abs_seconds(period: int, time_in_period_elapsed: str) -> Optional[int]:
    # api-web gives timeInPeriod as elapsed MM:SS (you pasted 00:00 at period start)
    t = mmss_to_seconds(time_in_period_elapsed)
    if t is None:
        return None
    if period < 1:
        return None
    return (period - 1) * 1200 + t  # 20-min chunks for timeline ordering


def fetch_pbp_api_web(game_id: int, timeout: int = 30) -> Dict[str, Any]:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    r = requests.get(url, timeout=timeout)
    if r.status_code != 200:
        die(f"api-web play-by-play HTTP {r.status_code} for game {game_id}")
    return r.json()


def compute_advantage_windows(pbp: Dict[str, Any]) -> Dict[str, Any]:
    home = pbp.get("homeTeam") or {}
    away = pbp.get("awayTeam") or {}

    home_id = home.get("id")
    away_id = away.get("id")
    home_name = ((home.get("commonName") or {}).get("default")) or home.get("abbrev")
    away_name = ((away.get("commonName") or {}).get("default")) or away.get("abbrev")

    plays = pbp.get("plays") or []
    if not isinstance(plays, list) or len(plays) == 0:
        return {
            "home_id": home_id,
            "away_id": away_id,
            "home_name": home_name,
            "away_name": away_name,
            "plays": 0,
            "note": "no plays payload",
        }

    # Build timeline points (t_abs, situationCode)
    points: List[Tuple[int, str]] = []
    sit_counter = Counter()

    for p in plays:
        pd = p.get("periodDescriptor") or {}
        period = pd.get("number")
        tip = p.get("timeInPeriod")
        sc = p.get("situationCode")
        if period is None or tip is None or sc is None:
            continue
        t = abs_seconds(int(period), str(tip))
        if t is None:
            continue
        scs = str(sc)
        points.append((t, scs))
        sit_counter[scs] += 1

    if not points:
        return {
            "home_id": home_id,
            "away_id": away_id,
            "home_name": home_name,
            "away_name": away_name,
            "plays": len(plays),
            "note": "no timeline points (missing period/timeInPeriod/situationCode)",
        }

    points.sort(key=lambda x: x[0])

    # Collapse: keep last situationCode per timestamp
    collapsed: List[Tuple[int, str]] = []
    last_t: Optional[int] = None
    for t, sc in points:
        if last_t is None or t != last_t:
            collapsed.append((t, sc))
            last_t = t
        else:
            collapsed[-1] = (t, sc)

    # Add period-end sentinels so intervals don't “bridge” gaps if last play isn't exactly at 20:00.
    max_t = max(t for t, _ in collapsed)
    max_period = (max_t // 1200) + 1
    period_ends = [p * 1200 for p in range(1, max_period + 1)]  # 1200, 2400, 3600...

    timeline_map: Dict[int, str] = {t: sc for t, sc in collapsed}

    # For each period end, carry forward the last known situation within/at that time.
    idx = 0
    current_sc = collapsed[0][1]
    for t_end in period_ends:
        while idx < len(collapsed) and collapsed[idx][0] <= t_end:
            current_sc = collapsed[idx][1]
            idx += 1
        timeline_map.setdefault(t_end, current_sc)

    timeline = sorted(timeline_map.items(), key=lambda x: x[0])
    # Ensure we have a final end marker
    timeline.append((timeline[-1][0] + 1, timeline[-1][1]))

    home_adv_sec_goalies = 0
    away_adv_sec_goalies = 0
    home_adv_sec_any = 0
    away_adv_sec_any = 0
    excluded_empty_net_sec = 0
    excluded_equal_strength_sec = 0
    interval_debug_counts = defaultdict(int)

    for (t0, sc0), (t1, _) in zip(timeline, timeline[1:]):
        dt = max(0, t1 - t0)
        if dt == 0:
            continue

        sit = parse_situation(sc0)
        if sit is None:
            interval_debug_counts["unparsed_code_intervals"] += 1
            continue

        if sit.away_skaters == sit.home_skaters:
            excluded_equal_strength_sec += dt
            interval_debug_counts["equal_strength_intervals"] += 1
            continue

        goalies_present = (sit.away_goalie == 1 and sit.home_goalie == 1)

        if sit.home_skaters > sit.away_skaters:
            home_adv_sec_any += dt
            interval_debug_counts["home_adv_any_intervals"] += 1
            if goalies_present:
                home_adv_sec_goalies += dt
                interval_debug_counts["home_adv_goalies_intervals"] += 1
            else:
                excluded_empty_net_sec += dt
                interval_debug_counts["excluded_empty_net_intervals"] += 1
        else:
            away_adv_sec_any += dt
            interval_debug_counts["away_adv_any_intervals"] += 1
            if goalies_present:
                away_adv_sec_goalies += dt
                interval_debug_counts["away_adv_goalies_intervals"] += 1
            else:
                excluded_empty_net_sec += dt
                interval_debug_counts["excluded_empty_net_intervals"] += 1

    return {
        "home_id": home_id,
        "away_id": away_id,
        "home_name": home_name,
        "away_name": away_name,
        "plays": len(plays),
        "timeline_points_raw": len(points),
        "timeline_points_collapsed": len(collapsed),
        "unique_situation_codes": len(sit_counter),
        "top_situation_codes": sit_counter.most_common(12),
        "home_adv_sec_any": home_adv_sec_any,
        "away_adv_sec_any": away_adv_sec_any,
        "home_adv_sec_goalies": home_adv_sec_goalies,
        "away_adv_sec_goalies": away_adv_sec_goalies,
        "excluded_empty_net_sec": excluded_empty_net_sec,
        "excluded_equal_strength_sec": excluded_equal_strength_sec,
        "interval_debug_counts": dict(interval_debug_counts),
    }


def fetch_db_pp_totals(db_url: str, game_id: int) -> List[Dict[str, Any]]:
    if psycopg2 is None:
        die("psycopg2 is not installed; cannot query DB totals.")
    q = """
    SELECT
      team_id,
      ROUND(SUM(COALESCE(pp_toi_minutes,0))::numeric, 3) AS sum_pp_toi_min,
      ROUND(SUM(COALESCE(toi_minutes,0))::numeric, 1)    AS sum_toi_min,
      COUNT(*) FILTER (WHERE COALESCE(toi_minutes,0) > 0) AS skaters_played,
      COUNT(*) FILTER (WHERE COALESCE(toi_minutes,0) > 0 AND COALESCE(pp_toi_minutes,0) > 0) AS skaters_with_pp
    FROM nhl.skater_game_logs_raw
    WHERE game_id = %s
    GROUP BY 1
    ORDER BY team_id;
    """
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(q, (game_id,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [{cols[i]: r[i] for i in range(len(cols))} for r in rows]
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game-id", type=int, required=True)
    ap.add_argument(
        "--db-url",
        default=None,
        help="Postgres URL (defaults to SUPABASE_DB_URL or DATABASE_URL). Optional but recommended.",
    )
    args = ap.parse_args()

    game_id = args.game_id
    db_url = args.db_url or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    pbp = fetch_pbp_api_web(game_id)
    res = compute_advantage_windows(pbp)

    print("=== api-web play-by-play advantage-window diagnostic ===")
    print(f"game_id: {game_id}")
    print(f"home: {res.get('home_name')} (id={res.get('home_id')})")
    print(f"away: {res.get('away_name')} (id={res.get('away_id')})")
    print(f"plays: {res.get('plays')} | timeline_raw={res.get('timeline_points_raw')} | timeline_collapsed={res.get('timeline_points_collapsed')}")
    note = res.get("note")
    if note:
        print(f"NOTE: {note}")

    print("\n--- situationCode distribution (top 12) ---")
    for code, n in (res.get("top_situation_codes") or []):
        sit = parse_situation(code)
        if sit:
            label = f"awayG={sit.away_goalie} awayS={sit.away_skaters} homeS={sit.home_skaters} homeG={sit.home_goalie}"
        else:
            label = "unparsed"
        print(f"{code}: {n}   ({label})")

    def fmt_sec(sec: int) -> str:
        return f"{sec} sec ({sec/60.0:.2f} min)"

    print("\n--- computed advantage seconds ---")
    print(f"HOME advantage (any):     {fmt_sec(int(res.get('home_adv_sec_any', 0)))}")
    print(f"AWAY advantage (any):     {fmt_sec(int(res.get('away_adv_sec_any', 0)))}")
    print(f"HOME advantage (goalies): {fmt_sec(int(res.get('home_adv_sec_goalies', 0)))}   <-- excludes empty net")
    print(f"AWAY advantage (goalies): {fmt_sec(int(res.get('away_adv_sec_goalies', 0)))}   <-- excludes empty net")
    print(f"Excluded empty-net secs:  {fmt_sec(int(res.get('excluded_empty_net_sec', 0)))}")
    print(f"Excluded equal-strength:  {fmt_sec(int(res.get('excluded_equal_strength_sec', 0)))}")

    print("\n--- interval debug counts ---")
    for k, v in sorted((res.get("interval_debug_counts") or {}).items()):
        print(f"{k}: {v}")

    if db_url:
        print("\n=== DB totals from nhl.skater_game_logs_raw ===")
        rows = fetch_db_pp_totals(db_url, game_id)
        if not rows:
            print("No rows returned for this game_id.")
        else:
            for r in rows:
                print(
                    f"team_id={r['team_id']}  sum_pp_toi_min={r['sum_pp_toi_min']}  "
                    f"skaters_with_pp={r['skaters_with_pp']}/{r['skaters_played']}  sum_toi_min={r['sum_toi_min']}"
                )

            home_id = res.get("home_id")
            away_id = res.get("away_id")
            if home_id and away_id:
                by_team = {int(x["team_id"]): x for x in rows}
                home_db = by_team.get(int(home_id))
                away_db = by_team.get(int(away_id))

                print("\n--- direct comparison (computed goalies-present advantage vs stored pp_toi) ---")
                print(f"Computed HOME adv (goalies): {fmt_sec(int(res.get('home_adv_sec_goalies', 0)))}")
                print(f"Computed AWAY adv (goalies): {fmt_sec(int(res.get('away_adv_sec_goalies', 0)))}")
                if home_db:
                    print(f"Stored HOME sum(pp_toi):     {home_db['sum_pp_toi_min']} min")
                if away_db:
                    print(f"Stored AWAY sum(pp_toi):     {away_db['sum_pp_toi_min']} min")
    else:
        print("\n(DB compare skipped: set SUPABASE_DB_URL or pass --db-url)")

    print("\nDone.")


if __name__ == "__main__":
    main()

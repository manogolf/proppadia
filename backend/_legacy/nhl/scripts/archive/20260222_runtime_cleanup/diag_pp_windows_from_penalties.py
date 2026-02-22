# backend/nhl/scripts/diag_pp_windows_from_penalties.py
# Usage:
#   python backend/nhl/scripts/diag_pp_windows_from_penalties.py --game-id 2025020586 --db-url "$SUPABASE_DB_URL"
#
# Notes:
# - Uses api-web.nhle.com play-by-play (statsapi.web.nhl.com is dead).
# - Builds PP/advantage windows from penalties (not situationCode carry-forward).
# - Terminates minors on PP goals by the advantaged team (standard NHL rule).
# - Compares to DB sum(pp_toi_minutes) from nhl.skater_game_logs_raw (skater-summed; divide by ~5 for team time).

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


API_WEB_PBP = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def mmss_to_sec(mmss: str) -> int:
    # "12:13" -> 733
    mm, ss = mmss.split(":")
    return int(mm) * 60 + int(ss)


def play_abs_sec(period: int, time_in_period: str) -> int:
    # abs seconds from game start (reg only; OT still works as period blocks)
    # NHL periods are 20 min.
    return (period - 1) * 20 * 60 + mmss_to_sec(time_in_period)


@dataclass
class Penalty:
    t0: int              # abs sec start
    t1: int              # abs sec scheduled end
    team_id: int         # penalized team
    type_code: str       # MIN/MAJ/etc
    minutes: int


@dataclass
class Goal:
    t: int               # abs sec
    team_id: int         # scoring team


def fetch_pbp(game_id: int) -> dict:
    url = API_WEB_PBP.format(game_id=game_id)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def extract_teams(pbp: dict) -> Tuple[int, int, str, str]:
    home_id = int(pbp["homeTeam"]["id"])
    away_id = int(pbp["awayTeam"]["id"])
    home_name = pbp["homeTeam"]["commonName"]["default"]
    away_name = pbp["awayTeam"]["commonName"]["default"]
    return home_id, away_id, home_name, away_name


def extract_penalties_and_goals(pbp: dict) -> Tuple[List[Penalty], List[Goal]]:
    pens: List[Penalty] = []
    goals: List[Goal] = []

    for p in pbp.get("plays", []):
        td = p.get("typeDescKey")
        period = int(p.get("period", p.get("periodDescriptor", {}).get("number", 0)) or 0)
        t = p.get("t", p.get("timeInPeriod"))
        if not period or not t:
            continue

        t_abs = play_abs_sec(period, t)
        details = p.get("details") or {}

        if td == "penalty":
            team_id = details.get("eventOwnerTeamId")
            minutes = details.get("duration")
            type_code = details.get("typeCode")
            if team_id is None or minutes is None or type_code is None:
                continue
            team_id = int(team_id)
            minutes = int(minutes)
            type_code = str(type_code)

            t1 = t_abs + minutes * 60
            pens.append(Penalty(t0=t_abs, t1=t1, team_id=team_id, type_code=type_code, minutes=minutes))

        elif td == "goal":
            team_id = details.get("eventOwnerTeamId")
            if team_id is None:
                continue
            goals.append(Goal(t=t_abs, team_id=int(team_id)))

    pens.sort(key=lambda x: x.t0)
    goals.sort(key=lambda x: x.t)
    return pens, goals


def advantaged_team(home_id: int, away_id: int, penalized_team: int) -> Optional[int]:
    if penalized_team == home_id:
        return away_id
    if penalized_team == away_id:
        return home_id
    return None


def is_minor(p: Penalty) -> bool:
    # api-web uses MIN for minors in your sample
    return p.type_code.upper() in ("MIN", "BEN") and p.minutes in (2, 4)  # 4 could be double-minor total


def is_major(p: Penalty) -> bool:
    return p.type_code.upper() in ("MAJ",) or p.minutes >= 5


def apply_pp_goal_termination(
    home_id: int,
    away_id: int,
    penalties: List[Penalty],
    goals: List[Goal],
) -> List[Penalty]:
    """
    Terminate one active minor on a PP goal by the advantaged team.
    We implement a conservative, rule-aligned version:
      - Only minors terminate on goals.
      - Only if the scoring team is advantaged at that moment (other team has more active penalties).
      - Terminates the earliest-ending active minor against the penalized team (standard “one minor comes off”).
    """
    # We'll simulate on goal times only, maintaining active penalties.
    active: List[Penalty] = []
    out: List[Penalty] = [Penalty(**p.__dict__) for p in penalties]  # copy

    # Index into out list to allow mutation of end times
    # Keep mapping by object identity via list indices.
    for g in goals:
        # refresh active at time g.t
        active = [p for p in out if p.t0 <= g.t < p.t1]

        # count active penalties by team (penalized team IDs)
        cnt_home = sum(1 for p in active if p.team_id == home_id)
        cnt_away = sum(1 for p in active if p.team_id == away_id)

        # Determine which team is advantaged at g.t
        # If home has more penalties, away is advantaged, etc.
        if cnt_home == cnt_away:
            continue

        adv_team = away_id if cnt_home > cnt_away else home_id
        if g.team_id != adv_team:
            continue  # goal not by advantaged team -> no minor comes off

        # Penalized team is the *other* team
        penalized_team = home_id if adv_team == away_id else away_id

        # Find terminable minors against penalized_team active at g.t
        candidates_idx = [
            i for i, p in enumerate(out)
            if p.team_id == penalized_team and p.t0 <= g.t < p.t1 and is_minor(p)
        ]
        if not candidates_idx:
            continue

        # Terminate the one that would end soonest (common convention)
        i_best = min(candidates_idx, key=lambda i: out[i].t1)
        if g.t < out[i_best].t1:
            out[i_best].t1 = g.t

    return out


def build_advantage_intervals(
    home_id: int,
    away_id: int,
    penalties: List[Penalty],
) -> Dict[int, int]:
    """
    Returns advantage seconds per team_id based on active penalty count difference.
    If both teams have equal active penalties -> no advantage.
    """
    # Event boundaries: all starts and ends
    bounds = set()
    for p in penalties:
        bounds.add(p.t0)
        bounds.add(p.t1)
    if not bounds:
        return {home_id: 0, away_id: 0}

    timeline = sorted(bounds)
    adv_sec = {home_id: 0, away_id: 0}

    for a, b in zip(timeline, timeline[1:]):
        if b <= a:
            continue
        # active in [a,b)
        active = [p for p in penalties if p.t0 <= a < p.t1]
        cnt_home = sum(1 for p in active if p.team_id == home_id)
        cnt_away = sum(1 for p in active if p.team_id == away_id)

        if cnt_home == cnt_away:
            continue
        adv_team = away_id if cnt_home > cnt_away else home_id
        adv_sec[adv_team] += (b - a)

    return adv_sec


def db_team_pp_sums(db_url: str, game_id: int) -> List[dict]:
    # expects nhl.skater_game_logs_raw has pp_toi_minutes numeric
    q = """
    SELECT
      team_id,
      COUNT(*) FILTER (WHERE COALESCE(pp_toi_minutes,0) > 0) AS skaters_with_pp,
      COUNT(*) FILTER (WHERE COALESCE(toi_minutes,0) > 0) AS skaters_played,
      ROUND(SUM(COALESCE(pp_toi_minutes,0))::numeric, 3) AS sum_pp_toi_min,
      ROUND(SUM(COALESCE(toi_minutes,0))::numeric, 1)   AS sum_toi_min
    FROM nhl.skater_game_logs_raw
    WHERE game_id = %s
    GROUP BY team_id
    ORDER BY team_id;
    """
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q, (game_id,))
            return list(cur.fetchall())
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game-id", type=int, required=True)
    ap.add_argument("--db-url", default=None, help="else SUPABASE_DB_URL or DATABASE_URL")
    args = ap.parse_args()

    db_url = args.db_url or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        die("Missing --db-url (or SUPABASE_DB_URL/DATABASE_URL).")

    pbp = fetch_pbp(args.game_id)
    home_id, away_id, home_name, away_name = extract_teams(pbp)

    pens, goals = extract_penalties_and_goals(pbp)
    pens_term = apply_pp_goal_termination(home_id, away_id, pens, goals)
    adv_sec = build_advantage_intervals(home_id, away_id, pens_term)

    print("=== penalty-derived advantage window diagnostic (api-web) ===")
    print(f"game_id: {args.game_id}")
    print(f"home: {home_name} (id={home_id})")
    print(f"away: {away_name} (id={away_id})")
    print(f"penalties: {len(pens)} | goals: {len(goals)}")
    print()

    print("--- advantage seconds from penalty windows ---")
    print(f"HOME advantage: {adv_sec[home_id]} sec ({adv_sec[home_id]/60:.2f} min)")
    print(f"AWAY advantage: {adv_sec[away_id]} sec ({adv_sec[away_id]/60:.2f} min)")
    print()

    rows = db_team_pp_sums(db_url, args.game_id)
    if not rows:
        print("=== DB: no skater_game_logs_raw rows for this game_id ===")
        return

    print("=== DB totals from nhl.skater_game_logs_raw ===")
    for r in rows:
        team = int(r["team_id"])
        sum_pp = float(r["sum_pp_toi_min"])
        print(
            f"team_id={team}  sum_pp_toi_min={sum_pp:.3f}  "
            f"(÷5≈{sum_pp/5:.3f})  skaters_with_pp={r['skaters_with_pp']}/{r['skaters_played']}  "
            f"sum_toi_min={r['sum_toi_min']}"
        )

    print()
    print("--- reconciliation check (window minutes vs sum(pp_toi)/5) ---")
    for r in rows:
        team = int(r["team_id"])
        window_min = adv_sec.get(team, 0) / 60.0
        sum_pp_div5 = float(r["sum_pp_toi_min"]) / 5.0
        print(f"team_id={team}: windows={window_min:.3f} min | sum_pp/5≈{sum_pp_div5:.3f} | diff={window_min - sum_pp_div5:+.3f}")

    print("\nDone.")


if __name__ == "__main__":
    main()

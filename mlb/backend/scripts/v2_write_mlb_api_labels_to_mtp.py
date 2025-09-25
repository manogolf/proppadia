"""
v2_write_mlb_api_labels_to_mtp.py
==================================
Purpose
- Generate labeled training examples in `model_training_props` (MTP) from MLB boxscores.
- Computes each player’s ACTUAL stat for a prop on a finished game and writes a
  win/loss label using a ±0.5 line (no pushes).

What it DOES
- Reads FINAL games’ boxscores and extracts per-player actuals.
- Applies a training-time line strategy (e.g., actual ± 0.5) and over/under,
  ensuring pushes are impossible.
- Upserts labeled rows into MTP with prop_source='mlb_api'.
- (Optionally) limits pitcher props to starters if your extractor enforces that.

What it DOES NOT do
- Does not build features (that’s the backfill script).
- Does not read PFP; it only uses MLB boxscore data for labels.

Reads
- MLB schedule (to find completed games) and boxscore feeds.

Writes
- `public.model_training_props` (one row per (player_id, game_id, prop_type, prop_source='mlb_api')).
  Includes: game_date, team/opponent context, line, over_under, outcome/win-loss,
  and any other training metadata your schema supports.

Run Order
- Run after games go FINAL (overnight or multiple times late evening).
- After this, run v2_write_training_from_pfp.py to attach features to these rows.

Idempotency
- Upsert via the unique key (player_id, game_id, prop_type, prop_source).
  Safe to re-run for the same dates.

Env / Flags
- QUIET / VERBOSE / DEBUG — optional logging controls.

Typical cron
- Nightly: label yesterday’s games.
- Optional: hourly sweep to catch late finals.
"""

from __future__ import annotations

import os, sys, time, random, requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from zoneinfo import ZoneInfo

# --- Supabase handle (same pattern you’ve used elsewhere) --------------------
try:
    from backend.scripts.shared.supabase_utils import supabase
except Exception:
    try:
        from scripts.shared.supabase_utils import supabase  # fallback
    except Exception:
        supabase = None

MLB = "https://statsapi.mlb.com/api/v1"

# ---- props we’ll emit -------------------------------------------------------
BATTER_PROPS = [
    "singles", "hits", "total_bases", "hits_runs_rbis",
    "rbis", "runs_scored", "home_runs", "doubles",
    "triples", "walks", "strikeouts_batting", "stolen_bases", "runs_rbis"
]

PITCHER_PROPS = [
    "earned_runs", "strikeouts_pitching", "walks_allowed",
    "hits_allowed", "outs_recorded",
]

# ---- tiny time helpers (ET, buckets) ---------------------------------------
def _to_naive_et(utc_iso: str | None) -> Optional[str]:
    if not utc_iso:
        return None
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        et = dt.astimezone(ZoneInfo("America/New_York"))
        return et.replace(tzinfo=None, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _dow_3(date_yyyy_mm_dd: str) -> str:
    try:
        d = datetime.strptime(date_yyyy_mm_dd, "%Y-%m-%d")
        return ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][d.weekday()]
    except Exception:
        return "Mon"

def _bucket(naive_et_str: str | None) -> str:
    if not naive_et_str:
        return "evening"
    try:
        hh = int(naive_et_str.split(" ")[1].split(":")[0])
        return "day" if hh < 17 else "evening"
    except Exception:
        return "evening"

# ---- stat extraction --------------------------------------------------------
def _canon_prop(p: str) -> str:
    p = (p or "").strip().lower()
    if p in {"hitsrundrbis", "h+r+rbi", "hrr"}:
        return "hits_runs_rbis"
    if p in {"runsrbis", "r+rbi", "runs_rbi"}:
        return "runs_rbis"
    return p

def _extract_batter_actual(bat: Dict[str, Any], ptype: str) -> Optional[float]:
    H   = float(bat.get("hits") or 0)
    _2  = float(bat.get("doubles") or 0)
    _3  = float(bat.get("triples") or 0)
    HR  = float(bat.get("homeRuns") or 0)
    R   = float(bat.get("runs") or 0)
    RBI = float(bat.get("rbi") or 0)
    BB  = float(bat.get("baseOnBalls") or 0)
    SO  = float(bat.get("strikeOuts") or 0)
    SB  = float(bat.get("stolenBases") or 0)

    # derived
    singles = max(0.0, H - _2 - _3 - HR)
    total_bases = singles + 2*_2 + 3*_3 + 4*HR
    hrr = H + R + RBI            # hits + runs + RBIs
    rr  = R + RBI                # runs_rbis

    match ptype:
        case "singles":             return singles
        case "hits":                return H
        case "total_bases":         return total_bases
        case "hits_runs_rbis":      return hrr
        case "runs_rbis":           return rr
        case "rbis":                return RBI
        case "runs_scored":         return R
        case "home_runs":           return HR
        case "doubles":             return _2
        case "triples":             return _3
        case "walks":               return BB
        case "strikeouts_batting":  return SO
        case "stolen_bases":        return SB
    return None

def _ip_to_outs(ip_str: str) -> int:
    """
    Convert MLB IP string to outs, e.g. "5.2" -> 17 (5 * 3 + 2).
    """
    if not ip_str:
        return 0
    try:
        parts = ip_str.split(".")
        whole = int(parts[0])
        dec = int(parts[1]) if len(parts) > 1 else 0
        return whole * 3 + dec
    except Exception:
        return 0

def _extract_pitcher_actual(pit: Dict[str, Any], ptype: str) -> Optional[float]:
    ER = float(pit.get("earnedRuns") or 0)
    K  = float(pit.get("strikeOuts") or 0)
    BB = float(pit.get("baseOnBalls") or 0)
    H  = float(pit.get("hits") or 0)
    outs = pit.get("outs")
    if outs is None:
        ip = pit.get("inningsPitched")
        outs = _ip_to_outs(ip) if ip else 0
    else:
        outs = int(outs or 0)

    match ptype:
        case "earned_runs":         return ER
        case "strikeouts_pitching": return K
        case "walks_allowed":       return BB
        case "hits_allowed":        return H
        case "outs_recorded":       return float(outs)
    return None

def _grade(ou: str, line: float, actual: float) -> str:
    ou = (ou or "over").strip().lower()
    if ou == "under":
        return "win" if actual < line else "loss"
    return "win" if actual > line else "loss"

# ---- MLB fetchers -----------------------------------------------------------
def _schedule(date_yyyy_mm_dd: str) -> Dict[str, Any]:
    r = requests.get(f"{MLB}/schedule?sportId=1&date={date_yyyy_mm_dd}", timeout=12)
    r.raise_for_status()
    return r.json() or {}

def _feed(game_pk: int) -> Dict[str, Any]:
    r = requests.get(f"{MLB}/game/{game_pk}/feed/live", timeout=15)
    r.raise_for_status()
    return r.json() or {}

def _box(game_pk: int) -> Dict[str, Any]:
    r = requests.get(f"{MLB}/game/{game_pk}/boxscore", timeout=15)
    r.raise_for_status()
    return r.json() or {}

# ---- main write loop --------------------------------------------------------
def upsert_labels_for_date(date_yyyy_mm_dd: str) -> Dict[str, Any]:
    if supabase is None:
        raise RuntimeError("supabase client is not available")

    sched = _schedule(date_yyyy_mm_dd)
    games = []
    for day in (sched.get("dates") or []):
        for g in (day.get("games") or []):
            # only finals
            if (g.get("status") or {}).get("detailedState") == "Final":
                games.append(g)

    inserted = 0
    for g in games:
        try:
            game_id = int(g.get("gamePk"))
        except Exception:
            continue

        # teams + abbr
        home_team = ((g.get("teams") or {}).get("home") or {}).get("team") or {}
        away_team = ((g.get("teams") or {}).get("away") or {}).get("team") or {}
        home_id = int(home_team.get("id") or 0) or None
        away_id = int(away_team.get("id") or 0) or None
        home_abbr = home_team.get("abbreviation")
        away_abbr = away_team.get("abbreviation")

        # game time (ET naive) + date
        gameDateUTC = g.get("gameDate")
        game_time_naive_et = _to_naive_et(gameDateUTC)
        game_date = (gameDateUTC[:10] if isinstance(gameDateUTC, str) else date_yyyy_mm_dd)

        # probable starters (so we can filter pitcher props to starters only)
        feed = {}
        try:
            feed = _feed(game_id)
        except Exception:
            pass
        prob = ((feed.get("gameData") or {}).get("probablePitchers") or {})
        sp_home = int(prob.get("home", {}).get("id") or 0) or None
        sp_away = int(prob.get("away", {}).get("id") or 0) or None

        # boxscore players
        try:
            box = _box(game_id)
        except Exception:
            continue

        # team blocks
        teams_box = (box.get("teams") or {})
        team_blocks = []
        if "home" in teams_box:
            team_blocks.append(("home", teams_box["home"], home_id, home_abbr, away_id, away_abbr, sp_home))
        if "away" in teams_box:
            team_blocks.append(("away", teams_box["away"], away_id, away_abbr, home_id, home_abbr, sp_away))

        for side, block, team_id, team_abbr, opp_id, opp_abbr, sp_id in team_blocks:
            players = (block.get("players") or {})
            for _key, pdata in players.items():
                person = pdata.get("person") or {}
                pid = person.get("id")
                name = (person.get("fullName") or "").strip()
                if not pid or not name:
                    continue
                pid = int(pid)

                bat = (pdata.get("stats") or {}).get("batting") or {}
                pit = (pdata.get("stats") or {}).get("pitching") or {}
                has_bat = bool(bat)
                has_pit = bool(pit)
                is_home = (side == "home")

                # pitcher props only if this player is the probable starter for that side
                eligible = []
                if has_bat:
                    eligible += BATTER_PROPS
                if has_pit and sp_id and pid == sp_id:
                    eligible += PITCHER_PROPS
                if not eligible:
                    continue

                # shared fields for MTP row
                home_away = "home" if is_home else "away"
                dow = _dow_3(game_date)
                bucket = _bucket(game_time_naive_et)
                is_pitcher_flag = bool(has_pit and pid == sp_id)

                for p_raw in eligible:
                    ptype = _canon_prop(p_raw)

                    # actual value
                    actual = (
                        _extract_batter_actual(bat, ptype)
                        if ptype in BATTER_PROPS
                        else _extract_pitcher_actual(pit, ptype)
                    )
                    if actual is None:
                        continue

                    # construct a half-step training label around actual
                    if actual == 0:
                        line = 0.5
                    else:
                        line = actual + (0.5 if random.random() < 0.5 else -0.5)
                    # round to .0 or .5 only
                    line = round(line * 2) / 2
                    over_under = "over" if random.random() < 0.5 else "under"

                    outcome = _grade(over_under, line, actual)
                    label_num = 1.0 if outcome == "win" else 0.0

                    row = {
                        "game_id": game_id,
                        "player_id": pid,
                        "player_name": name,
                        "team": team_abbr,
                        "opponent": opp_abbr,
                        "team_id": team_id,
                        "opponent_team_id": opp_id,
                        "is_home": is_home,
                        "home_away": home_away,

                        "prop_type": ptype,
                        "prop_value": float(actual),
                        "line": float(line),
                        "over_under": over_under,

                        "outcome": outcome,         # 'win' / 'loss'
                        "result": label_num,        # numeric label 1/0 for training
                        "status": "resolved",

                        "game_date": game_date,
                        "game_time": game_time_naive_et,  # naive ts (matches your column type)
                        "game_day_of_week": dow,
                        "time_of_day_bucket": bucket,

                        "is_pitcher": is_pitcher_flag,
                        "prop_source": "mlb_api",
                    }

                    try:
                        res = (
                            supabase
                            .from_("model_training_props")
                            .upsert(row, on_conflict="player_id,game_id,prop_type,prop_source")
                            .execute()
                        )
                        err = getattr(res, "error", None)
                        if err:
                            # keep going; noisy rows happen occasionally
                            continue
                        inserted += 1
                    except Exception:
                        continue

        # be gentle with MLB API
        time.sleep(0.2)

    return {"date": date_yyyy_mm_dd, "inserted": inserted, "games": len(games)}

# ---- CLI --------------------------------------------------------------------
def _date_range(start: str, end: str) -> List[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out = []
    d = s
    while d <= e:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out

if __name__ == "__main__":
    # Defaults to “yesterday”
    today = datetime.now(ZoneInfo("America/New_York")).date()
    yday = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    start = os.getenv("START_DATE") or (sys.argv[1] if len(sys.argv) > 1 else yday)
    end   = os.getenv("END_DATE")   or (sys.argv[2] if len(sys.argv) > 2 else start)

    total = 0
    for d in _date_range(start, end):
        try:
            res = upsert_labels_for_date(d)
            print(f"✔ {d}: inserted={res['inserted']} games={res['games']}")
            total += res["inserted"]
        except Exception as e:
            print(f"✖ {d}: {e}")
    print(f"\nDone. Total inserted: {total}")

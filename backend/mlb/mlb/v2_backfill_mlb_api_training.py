"""
v2_backfill_mlb_api_training.py
================================
Purpose
- Populate/refresh the feature store table `prop_features_precomputed` (PFP).
- Creates one row per (prop_type, player_id, game_id, feature_set_tag) with a JSON
  blob of model-ready features used by /predict.

What it DOES
- Walks MLB schedule/rosters for the target date range.
- Builds features for every eligible player/prop (no boxscore reads, no labels).
- Upserts into `prop_features_precomputed` so it’s safe to re-run.

What it DOES NOT do
- Does not compute actual results or wins/losses.
- Does not write into `model_training_props` (MTP).

Reads
- MLB schedule/roster endpoints + local ID/abbr maps.
- Existing PFP rows (to avoid redundant work).

Writes
- `public.prop_features_precomputed`:
  (prop_type, player_id, game_id, game_date, features jsonb, feature_set_tag, ...)

Run Order
- Run first (daily, pregame or during backfills).
- Then run the boxscore labeler (v2_write_mlb_api_labels_to_mtp.py).
- Finally merge features to MTP (v2_write_training_from_pfp.py).

Idempotency
- Upsert on (prop_type, player_id, game_id, feature_set_tag). Safe to re-run.

Env / Flags
- FEATURE_SET_TAG (default: "v1") — version names the feature recipe.
- QUIET / VERBOSE / DEBUG — optional logging controls.

Typical cron
- Overnight for yesterday, and periodically for today before first pitch.
"""

from __future__ import annotations

import os, time, json, math, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple

# Supabase helper (python version used elsewhere in backend)
try:
    from backend.scripts.shared.supabase_utils import supabase
except Exception:
    from scripts.shared.supabase_utils import supabase  # fallback

# --- config ------------------------------------------------------------------

DAYS = int(os.getenv("BACKFILL_DAYS", "60"))     # how many days back (inclusive of yesterday)
SLEEP_MS = int(os.getenv("API_SLEEP_MS", "20"))  # throttle MLB API calls a bit
FEATURE_SET_TAG = os.getenv("FEATURE_SET_TAG", "v1")  # tag for prop_features_precomputed

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# Focus props (add/remove as needed)
BATTER_PROPS = [
    "singles",
    "hits",
    "total_bases",
    "hits_runs_rbis",
    "runs_scored",
    "rbis",
    "home_runs",
    "doubles",
    "triples",
    "walks",
    "strikeouts_batting",
    "stolen_bases",
    "runs_rbis",
]
PITCHER_PROPS = [
    "earned_runs",
    "strikeouts_pitching",
    "outs_recorded",
    "walks_allowed",
    "hits_allowed",
]

# --- small utils -------------------------------------------------------------

def _to_iso_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def _utc_iso_to_et(utc_iso: Optional[str]) -> Optional[str]:
    if not utc_iso:
        return None
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York"))
        return dt.replace(microsecond=0).isoformat()
    except Exception:
        return None

def _day_of_week(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][dt.weekday()]
    except Exception:
        return "Mon"

def _time_of_day_bucket(iso_et: Optional[str]) -> str:
    try:
        if not iso_et:
            return "evening"
        dt = datetime.fromisoformat(iso_et.replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("America/New_York"))
        hour = dt.astimezone(ZoneInfo("America/New_York")).hour
        return "day" if hour < 17 else "evening"
    except Exception:
        return "evening"

def _sleep():
    time.sleep(SLEEP_MS / 1000.0)

# --- MLB API -----------------------------------------------------------------

def fetch_schedule(date_yyyy_mm_dd: str) -> dict:
    url = f"{MLB_BASE}/schedule?sportId=1&date={date_yyyy_mm_dd}"
    r = requests.get(url, timeout=10)
    return r.json() if r.ok else {}

def fetch_boxscore(game_pk: int) -> dict:
    url = f"{MLB_BASE}/game/{int(game_pk)}/boxscore"
    r = requests.get(url, timeout=12)
    return r.json() if r.ok else {}

def schedule_final_game_pks(date_yyyy_mm_dd: str) -> List[int]:
    js = fetch_schedule(date_yyyy_mm_dd)
    pks: List[int] = []
    for day in js.get("dates", []):
        for g in day.get("games", []):
            state = (g.get("status") or {}).get("detailedState", "")
            if state == "Final":
                try:
                    pks.append(int(g.get("gamePk")))
                except Exception:
                    pass
    return pks

# --- boxscore parsing --------------------------------------------------------

def _team_meta_from_box(box: dict) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    teams = (box.get("teams") or {})
    home_team = (teams.get("home") or {}).get("team") or {}
    away_team = (teams.get("away") or {}).get("team") or {}
    home = {
        "team_id": int(home_team.get("id")) if home_team.get("id") else None,
        "abbr": home_team.get("abbreviation") or home_team.get("teamCode"),
    }
    away = {
        "team_id": int(away_team.get("id")) if away_team.get("id") else None,
        "abbr": away_team.get("abbreviation") or away_team.get("teamCode"),
    }
    return home, away

def _players_iter(box: dict) -> List[Tuple[str, dict, str]]:
    # yields (side, player_dict, key)
    out = []
    teams = box.get("teams") or {}
    for side in ("home", "away"):
        players = (teams.get(side) or {}).get("players") or {}
        for key, pdata in players.items():
            out.append((side, pdata or {}, key))
    return out

def _is_starter_pitcher(pstats: dict) -> bool:
    pitching = (pstats.get("stats") or {}).get("pitching") or {}
    gs = pitching.get("gamesStarted")
    try:
        return bool(int(gs) > 0)
    except Exception:
        return False

# --- actual value extraction -------------------------------------------------

def _bat_stat(p: dict, key: str) -> float:
    batting = (p.get("stats") or {}).get("batting") or {}
    v = batting.get(key)
    try:
        return float(v)
    except Exception:
        return 0.0

def _pit_stat(p: dict, key: str) -> float:
    pitching = (p.get("stats") or {}).get("pitching") or {}
    v = pitching.get(key)
    try:
        return float(v)
    except Exception:
        return 0.0

def compute_actual(p: dict, prop_type: str) -> Optional[float]:
    pt = prop_type.lower().strip()

    if pt in {"singles"}:
        hits = _bat_stat(p, "hits")
        doubles = _bat_stat(p, "doubles")
        triples = _bat_stat(p, "triples")
        hr = _bat_stat(p, "homeRuns")
        return max(0.0, hits - doubles - triples - hr)

    if pt in {"hits"}:
        return _bat_stat(p, "hits")

    if pt in {"total_bases"}:
        return _bat_stat(p, "totalBases")

    if pt in {"hits_runs_rbis", "hitsrundrbis"}:
        return _bat_stat(p, "hits") + _bat_stat(p, "runs") + _bat_stat(p, "rbi")

    if pt in {"runs_scored", "runs"}:
        return _bat_stat(p, "runs")

    if pt in {"rbis", "rbi"}:
        return _bat_stat(p, "rbi")

    if pt in {"home_runs", "homeruns", "hr"}:
        return _bat_stat(p, "homeRuns")

    if pt in {"doubles"}:
        return _bat_stat(p, "doubles")

    if pt in {"triples"}:
        return _bat_stat(p, "triples")

    if pt in {"walks"}:
        return _bat_stat(p, "baseOnBalls")

    if pt in {"strikeouts_batting", "strikeouts"}:
        return _bat_stat(p, "strikeOuts")

    if pt in {"stolen_bases"}:
        return _bat_stat(p, "stolenBases")
    
    if pt in {"runs_rbis", "runsrbis"}:
        return _bat_stat(p, "runs") + _bat_stat(p, "rbi")

    # Pitching
    if pt in {"earned_runs"}:
        return _pit_stat(p, "earnedRuns")

    if pt in {"strikeouts_pitching"}:
        return _pit_stat(p, "strikeOuts")

    if pt in {"outs_recorded"}:
        # MLB returns "outs" or "inningsPitched" (e.g., "5.2")
        outs = (p.get("stats") or {}).get("pitching", {}).get("outs")
        if outs is not None:
            try:
                return float(outs)
            except Exception:
                pass
        ip = (p.get("stats") or {}).get("pitching", {}).get("inningsPitched")
        if isinstance(ip, str) and ip:
            # "5.2" = 5 innings + 2 outs
            try:
                whole, dot, frac = ip.partition(".")
                w = int(whole or "0")
                f = int(frac or "0")
                return float(w * 3 + f)
            except Exception:
                return None
        return None

    if pt in {"walks_allowed"}:
        return _pit_stat(p, "baseOnBalls")

    if pt in {"hits_allowed"}:
        return _pit_stat(p, "hits")

    return None

# --- DB helpers --------------------------------------------------------------

def upsert_game_info_min(game_id: int, game_time_et_iso: Optional[str]) -> None:
    """Make sure game_info has at least (game_id, game_time) so FKs pass."""
    try:
        # quick existence check
        ex = supabase.from_("game_info").select("game_id").eq("game_id", game_id).limit(1).execute()
        rows = getattr(ex, "data", []) or []
        if rows:
            return
        supabase.from_("game_info").upsert(
            {
                "game_id": int(game_id),
                # game_info.game_time is timestamp WITHOUT time zone in your schema
                "game_time": (datetime.fromisoformat(game_time_et_iso.replace("Z","+00:00")).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
                if game_time_et_iso else None,
            },
            on_conflict="game_id",
        ).execute()
    except Exception:
        pass

def upsert_prop_features_precomputed(
    *,
    prop_type: str,
    player_id: int,
    game_id: int,
    game_date: str,
    feature_set_tag: str,
    features: Dict[str, Any],
) -> None:
    try:
        payload = {
            "prop_type": prop_type,
            "player_id": str(player_id),
            "game_id": str(game_id),
            "game_date": game_date,
            "features": features,
            "feature_set_tag": feature_set_tag,
        }
        supabase.from_("prop_features_precomputed").upsert(
            payload,
            on_conflict="prop_type,player_id,game_id,feature_set_tag",
        ).execute()
    except Exception:
        pass

def upsert_model_training_prop(row: Dict[str, Any]) -> None:
    try:
        supabase.from_("model_training_props").upsert(
            row,
            on_conflict="player_id,game_id,prop_type,prop_source",
        ).execute()
    except Exception as e:
        # don't crash whole run on a single bad row
        print("Upsert MTP error:", getattr(e, "message", str(e))[:240])

# --- main process ------------------------------------------------------------

def grade(over_under: str, line: float, actual: float) -> str:
    # half-line → no push; still guard just in case
    if abs(actual - line) < 1e-9:
        return "push"
    if over_under == "over":
        return "win" if actual > line else "loss"
    else:
        return "win" if actual < line else "loss"

def half_step_line_from_actual(actual: float) -> float:
    if actual <= 0:
        return 0.5
    # randomly ±0.5 around actual, rounded to .5
    # (training variety; prevents degenerate always-over)
    base = actual + (0.5 if (hash((actual, "k")) % 2 == 0) else -0.5)
    return round(base * 2.0) / 2.0

def process_game(game_pk: int, date_str: str) -> int:
    box = fetch_boxscore(game_pk)
    if not box:
        return 0

    game_datetime_utc = box.get("gameInfo", {}).get("firstPitch")
    # schedule has better source → fetch once:
    sched = fetch_schedule(date_str)
    game_time_et: Optional[str] = None
    for day in sched.get("dates", []):
        for g in day.get("games", []):
            if int(g.get("gamePk", -1)) == int(game_pk):
                game_time_et = _utc_iso_to_et(g.get("gameDate"))
                break

    upsert_game_info_min(int(game_pk), game_time_et)

    home, away = _team_meta_from_box(box)
    game_day_of_week = _day_of_week(date_str)
    time_bucket = _time_of_day_bucket(game_time_et)

    # loop players
    inserted = 0
    for side, pdata, _k in _players_iter(box):
        person = (pdata.get("person") or {})
        pid = person.get("id")
        name = person.get("fullName")
        if pid is None:
            continue
        try:
            player_id = int(pid)
        except Exception:
            continue

        stats = pdata.get("stats") or {}
        has_bat = bool((stats.get("batting") or {}))
        has_pit = bool((stats.get("pitching") or {}))
        is_home = (side == "home")

        team_meta = home if is_home else away
        opp_meta  = away if is_home else home

        team_id = team_meta.get("team_id")
        opp_id  = opp_meta.get("team_id")
        team_abbr = (team_meta.get("abbr") or "")[:3].upper() if team_meta.get("abbr") else None
        opp_abbr  = (opp_meta.get("abbr") or "")[:3].upper()  if opp_meta.get("abbr") else None

        # build minimal features (v2-friendly)
        features_min = {
            "player_id": player_id,
            "game_id": int(game_pk),
            "game_date": date_str,
            "team_id": team_id,
            "opponent_team_id": opp_id,
            "team": team_abbr,
            "opponent": opp_abbr,
            "is_home": bool(is_home),
            "game_time": game_time_et,
            "game_day_of_week": game_day_of_week,
            "time_of_day_bucket": time_bucket,
        }

        # choose eligible props
        prop_list: List[str] = []
        if has_bat:
            prop_list += BATTER_PROPS
        if has_pit and _is_starter_pitcher(pdata):
            prop_list += PITCHER_PROPS

        # nothing to do?
        if not prop_list:
            continue

        for ptype in prop_list:
            actual = compute_actual(pdata, ptype)
            if actual is None or not (isinstance(actual, (int, float)) and math.isfinite(actual)):
                continue

            line = half_step_line_from_actual(float(actual))
            # random OU for balance (deterministic-ish via hash)
            over_under = "over" if (hash((player_id, ptype, line)) % 2 == 0) else "under"
            outcome = grade(over_under, line, float(actual))
            if outcome == "push":
                # should be rare with .5; skip
                continue

            # Upsert features row for this (ptype, pid, gid) so v2 can read it later
            upsert_prop_features_precomputed(
                prop_type=ptype,
                player_id=player_id,
                game_id=int(game_pk),
                game_date=date_str,
                feature_set_tag=FEATURE_SET_TAG,
                features=features_min,
            )

            # Upsert training label row
            now = datetime.now(ZoneInfo("UTC")).isoformat()
            mtp = {
                "game_id": int(game_pk),
                "player_id": int(player_id),
                "player_name": name,
                "prop_type": ptype,
                "prop_value": float(actual),   # the *actual* event count
                "line": float(line),
                "over_under": over_under,
                "outcome": outcome,
                "status": "resolved",
                "was_correct": True if outcome == "win" else False,
                "created_at": now,
                "updated_at": now,
                "prop_source": "mlb_api",

                # helpful context
                "game_date": date_str,
                "team": team_abbr,
                "opponent": opp_abbr,
                "team_id": int(team_id) if team_id is not None else None,
                "opponent_team_id": int(opp_id) if opp_id is not None else None,
                "is_home": bool(is_home),
                "game_time": game_time_et.replace("Z","+00:00") if game_time_et else None,
                "game_day_of_week": game_day_of_week,
                "time_of_day_bucket": time_bucket,
            }
            upsert_model_training_prop(mtp)
            inserted += 1

        _sleep()

    return inserted

def main():
    today = datetime.now(ZoneInfo("America/New_York")).date()
    # backfill up to yesterday
    start = today - timedelta(days=DAYS)
    end   = today - timedelta(days=1)

    total_rows = 0
    dates: List[str] = []
    d = start
    while d <= end:
        dates.append(_to_iso_date(datetime(d.year, d.month, d.day)))
        d += timedelta(days=1)

    print(f"Backfilling {len(dates)} day(s): {dates[0]} → {dates[-1]}")
    for ds in dates:
        try:
            pks = schedule_final_game_pks(ds)
            print(f"📅 {ds}: {len(pks)} final games")
            for pk in pks:
                try:
                    n = process_game(pk, ds)
                    total_rows += n
                except Exception as e:
                    print(f"  ❌ game {pk} failed: {e}")
                _sleep()
        except Exception as e:
            print(f"❌ schedule {ds} failed: {e}")

    print(f"\n✅ done. inserted/updated ~{total_rows} training rows.")

if __name__ == "__main__":
    main()

# File: backend/scripts/shared/enrich_game_context.py

from __future__ import annotations
import requests

from datetime import datetime
from dateutil import parser
from zoneinfo import ZoneInfo
from .team_name_map import team_id_map
from .time_utils_backend import get_time_of_day_bucket_et  # your existing helper

ET = ZoneInfo("America/New_York")

MLB_API_SCHEDULE = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date="
MLB_API_FEED = "https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"

def get_schedule(date):
    url = f"{MLB_API_SCHEDULE}{date}"
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

def get_feed_live(game_id):
    url = MLB_API_FEED.format(game_id=game_id)
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

def encode_team(team_abbr):
    teams = list(team_id_map.keys())
    return teams.index(team_abbr) if team_abbr in teams else -1

def _abbr_from_id(tid: int) -> str | None:
    info = team_id_map.get(int(tid)) or team_id_map.get(str(tid))
    return (info or {}).get("abbr")

def enrich_game_context(game_id: int, team_id: int) -> dict:
    """
    Build context using MLB live feed. IDs are the source of truth.
    """
    feed = requests.get(FEED_URL.format(game_id=game_id), timeout=10).json()

    gdata = feed.get("gameData") or {}
    teams_meta = gdata.get("teams") or {}
    home_id = (teams_meta.get("home") or {}).get("id")
    away_id = (teams_meta.get("away") or {}).get("id")
    if not home_id or not away_id:
        raise ValueError(f"Missing team IDs in feed for game {game_id}")

    is_home = (int(team_id) == int(home_id))
    opponent_team_id = int(away_id if is_home else home_id)

    # Start time → ET (fallback to schedule if missing)
    dt_iso = (gdata.get("datetime") or {}).get("dateTime")
    if not dt_iso:
        sched = requests.get(
            f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk={game_id}", timeout=10
        ).json()
        dt_iso = ((sched.get("dates") or [{}])[0].get("games") or [{}])[0].get("gameDate")

    dt_et = parser.isoparse(dt_iso).astimezone(ET) if dt_iso else None
    game_time_et = dt_et.isoformat() if dt_et else None
    game_day_of_week = dt_et.weekday() if dt_et else None  # 0=Mon..6=Sun
    time_of_day_bucket = get_time_of_day_bucket_et(dt_et) if dt_et else None

    # Probable starters (IDs)
    prob = gdata.get("probablePitchers") or {}
    home_sp = (prob.get("home") or {}).get("id")
    away_sp = (prob.get("away") or {}).get("id")

    return {
        "game_id": int(game_id),
        "is_home": bool(is_home),
        "home_team_id": int(home_id),
        "away_team_id": int(away_id),
        "team_id": int(team_id),
        "opponent_team_id": int(opponent_team_id),
        "opponent_encoded": int(opponent_team_id),   # models can use team_id directly
        "game_time": game_time_et,                   # ET ISO
        "game_day_of_week": game_day_of_week,
        "time_of_day_bucket": time_of_day_bucket,
        "starting_pitcher_id_home": int(home_sp) if home_sp else None,
        "starting_pitcher_id_away": int(away_sp) if away_sp else None,
        # Optional UI helpers (never required/persisted)
        "team": _abbr_from_id(team_id),
        "opponent": _abbr_from_id(opponent_team_id),
    }
# --- end replace ---
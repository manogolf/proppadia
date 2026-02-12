# backend/scripts/shared/mlb_api_v2.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

@dataclass
class GameLite:
    game_id: int
    game_date: str          # YYYY-MM-DD (ET)
    game_time: Optional[str]  # ISO datetime (ET) or None
    home_team_id: int
    away_team_id: int
    home_abbr: Optional[str]
    away_abbr: Optional[str]
    sp_home_id: Optional[int]  # probable starter
    sp_away_id: Optional[int]

def _get_json(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def fetch_schedule_by_date(game_date: str) -> List[GameLite]:
    # v1 used schedule → gamePk. (JS analogue: fetchSchedule) :contentReference[oaicite:3]{index=3}
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={game_date}"
    js = _get_json(url)
    games = (js.get("dates") or [{}])[0].get("games") or []
    out: List[GameLite] = []
    for g in games:
        gd_iso = g.get("gameDate")  # Zulu
        # Convert to ET ISO
        game_time = None
        if gd_iso:
            dt = datetime.fromisoformat(gd_iso.replace("Z", "+00:00")).astimezone(ET)
            game_time = dt.isoformat()

        home = g.get("teams", {}).get("home", {})
        away = g.get("teams", {}).get("away", {})
        home_team = home.get("team", {}) or {}
        away_team = away.get("team", {}) or {}

        # Sometimes abbreviations appear under "team" in schedule; if missing, we’ll fill later
        out.append(GameLite(
            game_id=int(g.get("gamePk")),
            game_date=game_date,
            game_time=game_time,
            home_team_id=int(home_team.get("id")),
            away_team_id=int(away_team.get("id")),
            home_abbr=home_team.get("abbreviation"),
            away_abbr=away_team.get("abbreviation"),
            sp_home_id=(home.get("probablePitcher") or {}).get("id"),
            sp_away_id=(away.get("probablePitcher") or {}).get("id"),
        ))
    return out

def get_game_time_et(game_id: int) -> Optional[str]:
    # v1 had two fallbacks (schedule → boxscore). (JS analogue: getGameStartTimeET) :contentReference[oaicite:4]{index=4}
    # 1) schedule by gamePk
    try:
        j = _get_json(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk={game_id}")
        iso = (j.get("dates") or [{}])[0].get("games", [{}])[0].get("gameDate")
        if iso:
            return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(ET).isoformat()
    except Exception:
        pass
    # 2) boxscore datetime
    try:
        j = _get_json(f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore")
        iso = j.get("gameData", {}).get("datetime", {}).get("dateTime")
        if iso:
            return datetime.fromisoformat(iso).astimezone(ET).isoformat()
    except Exception:
        pass
    return None

def resolve_game_for_team(team_id: int, game_date: str) -> Optional[GameLite]:
    games = fetch_schedule_by_date(game_date)
    # choose the game where this team is home or away; if doubleheader, pick earliest ET
    candidates = [g for g in games if g.home_team_id == team_id or g.away_team_id == team_id]
    if not candidates:
        return None
    def _key(g: GameLite):
        return g.game_time or f"{g.game_date}T00:00:00-05:00"
    return sorted(candidates, key=_key)[0]

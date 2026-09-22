# backend/scripts/shared/mlb_api_v2.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import hashlib
import json
import requests
from datetime import datetime, timezone
from pathlib import Path
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
    game_type: Optional[str] = None
    game_number: Optional[int] = None
    doubleheader_indicator: Optional[str] = None

def _get_json(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def _parse_schedule(game_date: str, js: Dict[str, Any]) -> List[GameLite]:
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
            game_type=(str(g.get("gameType") or "").strip().upper() or None),
            game_number=(int(g["gameNumber"]) if g.get("gameNumber") is not None else None),
            doubleheader_indicator=(str(g.get("doubleHeader")) if g.get("doubleHeader") is not None else None),
        ))
    return out


def fetch_schedule_by_date(game_date: str) -> List[GameLite]:
    # v1 used schedule → gamePk. (JS analogue: fetchSchedule) :contentReference[oaicite:3]{index=3}
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={game_date}"
    return _parse_schedule(game_date, _get_json(url))


def fetch_schedule_by_date_with_evidence(game_date: str, retain_path: Path) -> tuple[List[GameLite], Dict[str, str]]:
    """Perform the existing one schedule request and immutably retain its exact bytes."""

    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={game_date}"
    observed_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    raw = response.content
    digest = hashlib.sha256(raw).hexdigest()
    retain_path.parent.mkdir(parents=True, exist_ok=True)
    if retain_path.exists():
        if retain_path.read_bytes() != raw:
            raise RuntimeError(f"immutable schedule evidence conflict: {retain_path}")
    else:
        with retain_path.open("xb") as handle:
            handle.write(raw)
    payload = json.loads(raw)
    return _parse_schedule(game_date, payload), {
        "path": str(retain_path.resolve()),
        "sha256": digest,
        "request_url": url,
        "observed_at_utc": observed_at_utc,
    }

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

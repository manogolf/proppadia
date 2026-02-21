"""MLB game context domain logic."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from backend.mlb.shared.mlb_api_v2 import resolve_game_for_team
from backend.mlb.shared.team_name_map import getFullTeamAbbreviationFromID
from backend.mlb.shared.time_utils_backend import get_time_of_day_bucket_et

ET = ZoneInfo("America/New_York")


def _abbr(team_id: int) -> Optional[str]:
    return getFullTeamAbbreviationFromID(team_id)


def build_game_context(*, team_id: int, game_date: str) -> Optional[Dict[str, Any]]:
    """Resolve game metadata for one team/date from MLB schedule."""
    game = resolve_game_for_team(team_id=int(team_id), game_date=game_date)
    if not game:
        return None

    is_home = int(team_id) == int(game.home_team_id)
    opponent_team_id = int(game.away_team_id if is_home else game.home_team_id)

    game_time = game.game_time
    day_of_week: Optional[int] = None
    time_bucket: Optional[str] = None
    if game_time:
        dt = datetime.fromisoformat(game_time)
        day_of_week = dt.weekday()
        time_bucket = get_time_of_day_bucket_et(dt.astimezone(ET))

    opposing_pitcher_id = game.sp_away_id if is_home else game.sp_home_id

    return {
        "team_id": int(team_id),
        "team_abbr": _abbr(int(team_id)),
        "for_date": game_date,
        "game_id": int(game.game_id),
        "game_type": game.game_type,
        "game_time": game_time,
        "is_home": bool(is_home),
        "opponent_team_id": opponent_team_id,
        "opponent": _abbr(opponent_team_id),
        "opponent_encoded": opponent_team_id,
        "game_day_of_week": day_of_week,
        "time_of_day_bucket": time_bucket,
        "starting_pitcher_id": int(opposing_pitcher_id) if opposing_pitcher_id else None,
    }

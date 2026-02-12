"""MLB game-context application service."""

from __future__ import annotations

from typing import Any, Dict, Optional

from backend.domains.mlb.game_context import build_game_context


def get_game_context(*, team_id: int, game_date: str) -> Optional[Dict[str, Any]]:
    """Resolve game context from MLB schedule for a team/date."""
    return build_game_context(team_id=team_id, game_date=game_date)


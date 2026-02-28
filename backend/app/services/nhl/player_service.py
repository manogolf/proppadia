"""NHL player application service."""

from __future__ import annotations

from typing import Any, Dict

from backend.domains.nhl.repository import fetch_player_profile


def player_profile(*, player_id: int) -> Dict[str, Any]:
    return fetch_player_profile(player_id)

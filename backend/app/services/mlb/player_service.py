"""MLB player application service."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from backend.domains.mlb.player_directory import (
    list_players_mlb as list_players_mlb_directory,
    list_players as list_players_directory,
    lookup_player as lookup_player_directory,
    player_profile as player_profile_directory,
    search_players as search_players_directory,
)
from backend.domains.mlb.player_resolver import resolve_player_candidate


def resolve_player(
    *,
    player_id: Optional[int],
    name: Optional[str],
    team_abbr: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Resolve a player using domain-level repository logic."""
    return resolve_player_candidate(
        player_id=player_id,
        name=name,
        team_abbr=team_abbr,
    )


def lookup_player(*, player_id: int) -> Optional[Dict[str, Any]]:
    return lookup_player_directory(player_id=player_id)


def search_players(*, q: str, limit: int = 10) -> List[Dict[str, Any]]:
    return search_players_directory(q=q, limit=limit)


def list_players(*, limit: int = 2000) -> List[Dict[str, Any]]:
    return list_players_directory(limit=limit)


def list_players_mlb(*, limit: int = 2000) -> List[Dict[str, Any]]:
    return list_players_mlb_directory(limit=limit)


def player_profile(*, player_id: int, sections: Optional[Set[str]] = None) -> Dict[str, Any]:
    return player_profile_directory(player_id=player_id, sections=sections)

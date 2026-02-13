"""MLB player resolution domain logic."""

from __future__ import annotations

from typing import Any, Dict, Optional

from backend.domains.mlb.repository.player_repository import (
    _decorate as repo_decorate,
    resolve_by_name,
    resolve_by_player_id,
)


def _decorate(row: Dict[str, Any], *, source: str) -> Optional[Dict[str, Any]]:
    # Compatibility shim for existing tests/callers.
    return repo_decorate(row, source)


def _resolve_by_player_id(player_id: int) -> Optional[Dict[str, Any]]:
    return resolve_by_player_id(player_id)


def _resolve_by_name(name: str, team_abbr: Optional[str]) -> Optional[Dict[str, Any]]:
    return resolve_by_name(name=name, team_abbr=team_abbr)


def resolve_player_candidate(
    *,
    player_id: Optional[int],
    name: Optional[str],
    team_abbr: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Resolve best-match MLB player identity from current tables."""
    if player_id is not None:
        by_id = _resolve_by_player_id(player_id)
        if by_id:
            return by_id

    query_name = (name or "").strip()
    if query_name:
        return _resolve_by_name(query_name, team_abbr)
    return None

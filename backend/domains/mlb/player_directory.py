"""MLB player directory/profile query helpers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.domains.mlb.repository.player_repository import (
    _decorate as repo_decorate,
    fetch_player_profile_rows,
    list_players_mlb as repo_list_players_mlb,
    list_players as repo_list_players,
    lookup_player as repo_lookup_player,
    search_players as repo_search_players,
)


def _decorate(row: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
    # Compatibility shim for existing tests/callers.
    return repo_decorate(row, source)


def lookup_player(player_id: int) -> Optional[Dict[str, Any]]:
    return repo_lookup_player(player_id)


def search_players(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    return repo_search_players(q=q, limit=limit)


def list_players(limit: int = 2000) -> List[Dict[str, Any]]:
    return repo_list_players(limit=limit)


def list_players_mlb(limit: int = 2000) -> List[Dict[str, Any]]:
    return repo_list_players_mlb(limit=limit)


def player_profile(player_id: int) -> Dict[str, Any]:
    info = lookup_player(player_id) or {"player_id": player_id}
    rows = fetch_player_profile_rows(player_id)

    return {
        "player_info": {
            "player_id": info.get("player_id"),
            "player_name": info.get("player_name"),
            "team": info.get("team_abbr"),
            "team_id": info.get("team_id"),
        },
        "streaks": rows["streaks"],
        "recent_props": rows["recent_props"],
        "stat_derived": rows["stat_derived"],
        "training_summary": rows["training_summary"],
        "freshness_metadata": rows.get("freshness_metadata") or {},
        # Kept for frontend shape compatibility; can be filled in later.
        "season_stats": {},
        "career_stats": {},
    }

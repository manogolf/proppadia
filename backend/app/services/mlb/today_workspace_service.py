"""Application service for MLB /today workspace."""

from __future__ import annotations

from typing import Any, Dict, Optional

from backend.domains.mlb.today_workspace import fetch_today_workspace_rows as domain_fetch_today_workspace_rows


def fetch_today_workspace(
    *,
    slate_date: Optional[str] = None,
    prop_type: Optional[str] = None,
    team: Optional[str] = None,
    side: Optional[str] = None,
    timing_signal: Optional[str] = None,
    player_query: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> Dict[str, Any]:
    return domain_fetch_today_workspace_rows(
        slate_date=slate_date,
        prop_type=prop_type,
        team=team,
        side=side,
        timing_signal=timing_signal,
        player_query=player_query,
        limit=limit,
        offset=offset,
    )

"""Domain helpers for MLB /today workspace."""

from __future__ import annotations

from typing import Any, Dict, Optional

from backend.domains.mlb.repository.today_workspace_repository import (
    fetch_today_workspace_rows as repo_fetch_today_workspace_rows,
)


def fetch_today_workspace_rows(
    *,
    prop_type: Optional[str] = None,
    team: Optional[str] = None,
    timing_signal: Optional[str] = None,
    player_query: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> Dict[str, Any]:
    rows = repo_fetch_today_workspace_rows(
        prop_type=prop_type,
        team=team,
        timing_signal=timing_signal,
        player_query=player_query,
        limit=limit,
        offset=offset,
    )
    total = int(rows[0].get("total_rows") or 0) if rows else 0
    cleaned = []
    for r in rows:
        row = dict(r)
        row.pop("total_rows", None)
        cleaned.append(row)
    return {
        "ok": True,
        "count": len(cleaned),
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "rows": cleaned,
    }

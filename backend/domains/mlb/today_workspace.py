"""Domain helpers for MLB /today workspace."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from backend.domains.mlb.repository.today_workspace_repository import (
    fetch_today_workspace_last_updated as repo_fetch_today_workspace_last_updated,
    fetch_today_workspace_rows as repo_fetch_today_workspace_rows,
)

ET = ZoneInfo("America/New_York")


def _resolve_requested_slate_date(slate_date: Optional[str]) -> str:
    if slate_date:
        # Router validates format; keep this as a defensive guard.
        date.fromisoformat(str(slate_date))
        return str(slate_date)
    return datetime.now(ET).date().isoformat()


def fetch_today_workspace_rows(
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
    requested_slate_date = _resolve_requested_slate_date(slate_date)
    rows = repo_fetch_today_workspace_rows(
        slate_date=requested_slate_date,
        prop_type=prop_type,
        team=team,
        side=side,
        timing_signal=timing_signal,
        player_query=player_query,
        limit=limit,
        offset=offset,
    )
    last_updated = repo_fetch_today_workspace_last_updated(slate_date=requested_slate_date)
    total = int(rows[0].get("total_rows") or 0) if rows else 0
    cleaned = []
    for r in rows:
        row = dict(r)
        row.pop("total_rows", None)
        cleaned.append(row)
    is_ready = len(cleaned) > 0
    return {
        "ok": True,
        "count": len(cleaned),
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "requested_slate_date": requested_slate_date,
        "active_slate_date": requested_slate_date if is_ready else None,
        "is_ready": is_ready,
        "last_updated": last_updated,
        "rows": cleaned,
    }

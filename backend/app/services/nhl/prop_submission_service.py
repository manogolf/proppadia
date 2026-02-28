"""NHL prop add/history application services."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from backend.domains.nhl.repository.prop_repository import (
    DuplicatePropError,
    count_prop_history_rows,
    fetch_prop_history_rows,
    find_duplicate_prop_id,
    insert_prop_row,
)

ET = ZoneInfo("America/New_York")


def _normalize_prop_type(prop_type: str) -> str:
    raw = str(prop_type or "").strip().lower().replace(" ", "_")
    aliases = {
        "sog": "shots_on_goal",
        "shots_on_goal": "shots_on_goal",
        "goalie_saves": "goalie_saves",
        "saves": "goalie_saves",
    }
    return aliases.get(raw, raw)


def _normalize_prop_source(prop_source: Optional[str]) -> str:
    value = str(prop_source or "nhl_user_added").strip().lower()
    if not value:
        value = "nhl_user_added"
    if not value.startswith("nhl_"):
        value = f"nhl_{value}"
    return value


def _to_json_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def add_prop(payload: Dict[str, Any]) -> Dict[str, Any]:
    player_id = int(payload.get("player_id") or 0)
    game_id = int(payload.get("game_id") or 0)
    if player_id <= 0:
        raise ValueError("player_id must be a positive integer")
    if game_id <= 0:
        raise ValueError("game_id must be a positive integer")

    prop_type = _normalize_prop_type(payload.get("prop_type") or "")
    if not prop_type:
        raise ValueError("prop_type is required")

    prop_value = float(payload.get("prop_value"))
    over_under = str(payload.get("over_under") or "over").strip().lower()
    if over_under not in {"over", "under"}:
        raise ValueError("over_under must be over or under")

    probability = float(payload.get("probability") if payload.get("probability") is not None else 0.5)
    probability = max(0.0, min(1.0, probability))
    recommendation = "over" if probability >= 0.5 else "under"
    prop_source = _normalize_prop_source(payload.get("prop_source"))
    user_id = str(payload.get("user_id") or "").strip() or None
    game_date = str(payload.get("game_date") or datetime.now(ET).date().isoformat())

    dup_id = find_duplicate_prop_id(
        player_id=player_id,
        game_id=game_id,
        prop_type=prop_type,
        over_under=over_under,
        prop_value=prop_value,
        prop_source=prop_source,
    )
    if dup_id:
        return {"ok": True, "saved": False, "duplicate": True, "id": dup_id}

    try:
        insert_prop_row(
            player_id=player_id,
            player_name=str(payload.get("player_name") or "").strip() or None,
            team=str(payload.get("team") or "").strip() or None,
            team_id=int(payload.get("team_id")) if payload.get("team_id") is not None else None,
            game_id=game_id,
            game_date=game_date,
            prop_type=prop_type,
            prop_value=prop_value,
            over_under=over_under,
            prop_source=prop_source,
            recommendation=recommendation,
            probability=probability,
            user_id=user_id,
        )
    except DuplicatePropError:
        dup_id = find_duplicate_prop_id(
            player_id=player_id,
            game_id=game_id,
            prop_type=prop_type,
            over_under=over_under,
            prop_value=prop_value,
            prop_source=prop_source,
        )
        return {"ok": True, "saved": False, "duplicate": True, "id": dup_id}

    return {"ok": True, "saved": True, "duplicate": False}


def get_prop_history(payload: Dict[str, Any]) -> Dict[str, Any]:
    limit = int(payload.get("limit") or 50)
    offset = int(payload.get("offset") or 0)
    user_id = str(payload.get("user_id") or "").strip() or None
    from_date = str(payload.get("from_date") or "").strip() or None
    to_date = str(payload.get("to_date") or "").strip() or None
    prop_source = payload.get("prop_source")
    prop_source = _normalize_prop_source(prop_source) if prop_source else None
    status = str(payload.get("status") or "").strip() or None

    rows = fetch_prop_history_rows(
        limit=limit,
        offset=offset,
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        prop_source_prefix="nhl_",
        status=status,
    )
    total = count_prop_history_rows(
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        prop_source_prefix="nhl_",
        status=status,
    )

    out_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = {k: _to_json_scalar(v) for k, v in row.items()}
        if normalized.get("id") is not None:
            normalized["id"] = str(normalized["id"])
        if normalized.get("user_id") is not None:
            normalized["user_id"] = str(normalized["user_id"])
        out_rows.append(normalized)

    return {
        "ok": True,
        "count": len(out_rows),
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "rows": out_rows,
    }

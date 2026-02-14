"""NHL prop lifecycle resolution helpers (ops-driven)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from backend.shared.db import pg_fetchall, pg_fetchone

ET = ZoneInfo("America/New_York")
_PLAYER_PROPS_COLUMNS_CACHE: Optional[set[str]] = None


def _player_props_columns() -> set[str]:
    global _PLAYER_PROPS_COLUMNS_CACHE
    if _PLAYER_PROPS_COLUMNS_CACHE is not None:
        return _PLAYER_PROPS_COLUMNS_CACHE
    rows = pg_fetchall(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='player_props'
        """
    )
    _PLAYER_PROPS_COLUMNS_CACHE = {str(r.get("column_name") or "").strip() for r in rows}
    return _PLAYER_PROPS_COLUMNS_CACHE


def _parse_iso_date(value: Optional[str], label: str) -> Optional[str]:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as e:
        raise ValueError(f"{label} must be YYYY-MM-DD") from e
    return parsed.isoformat()


def _build_where(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    only_past_games: bool,
    today_et: str,
) -> tuple[str, List[Any]]:
    where = [
        "prop_source LIKE %s",
        "LOWER(COALESCE(status, 'pending')) = 'pending'",
        "game_date IS NOT NULL",
    ]
    params: List[Any] = ["nhl_%"]
    if from_date:
        where.append("game_date >= %s")
        params.append(from_date)
    if to_date:
        where.append("game_date <= %s")
        params.append(to_date)
    if only_past_games:
        where.append("game_date < %s")
        params.append(today_et)
    return " AND ".join(where), params


def resolve_nhl_pending_props(
    *,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    dry_run: bool = True,
    only_past_games: bool = True,
    outcome: str = "dnp",
) -> Dict[str, Any]:
    from_date_norm = _parse_iso_date(from_date, "from_date")
    to_date_norm = _parse_iso_date(to_date, "to_date")
    if from_date_norm and to_date_norm and from_date_norm > to_date_norm:
        raise ValueError("from_date must be <= to_date")

    outcome_norm = str(outcome or "dnp").strip().lower()
    if outcome_norm not in {"dnp", "push", "win", "loss"}:
        raise ValueError("outcome must be one of: dnp,push,win,loss")

    today_et = datetime.now(ET).date().isoformat()
    where_sql, params = _build_where(
        from_date=from_date_norm,
        to_date=to_date_norm,
        only_past_games=bool(only_past_games),
        today_et=today_et,
    )

    preview_sql = f"""
        SELECT
          COUNT(*)::int AS pending_count,
          MIN(game_date)::text AS min_game_date,
          MAX(game_date)::text AS max_game_date
        FROM player_props
        WHERE {where_sql}
    """
    preview = pg_fetchone(preview_sql, tuple(params)) or {}
    pending_count = int(preview.get("pending_count") or 0)

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "matched": pending_count,
            "updated": 0,
            "from_date": from_date_norm,
            "to_date": to_date_norm,
            "only_past_games": bool(only_past_games),
            "outcome": outcome_norm,
            "range": {
                "min_game_date": preview.get("min_game_date"),
                "max_game_date": preview.get("max_game_date"),
                "today_et": today_et,
            },
        }

    set_clauses = ["status = %s", "outcome = %s"]
    update_params: List[Any] = [outcome_norm, outcome_norm]
    columns = _player_props_columns()
    if "updated_at" in columns:
        set_clauses.append("updated_at = NOW()")

    update_sql = f"""
        WITH targets AS (
            SELECT id
            FROM player_props
            WHERE {where_sql}
        ),
        updated AS (
            UPDATE player_props p
            SET {", ".join(set_clauses)}
            WHERE p.id IN (SELECT id FROM targets)
            RETURNING p.id
        )
        SELECT
            (SELECT COUNT(*)::int FROM targets) AS matched_count,
            (SELECT COUNT(*)::int FROM updated) AS updated_count
    """
    row = pg_fetchone(update_sql, tuple(update_params + params)) or {}
    return {
        "ok": True,
        "dry_run": False,
        "matched": int(row.get("matched_count") or 0),
        "updated": int(row.get("updated_count") or 0),
        "from_date": from_date_norm,
        "to_date": to_date_norm,
        "only_past_games": bool(only_past_games),
        "outcome": outcome_norm,
        "range": {
            "min_game_date": preview.get("min_game_date"),
            "max_game_date": preview.get("max_game_date"),
            "today_et": today_et,
        },
    }

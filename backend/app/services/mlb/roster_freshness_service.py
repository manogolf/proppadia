from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from backend.shared.db.pg import pg_fetchone


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def get_roster_freshness(*, require_min: int = 1, stale_after_hours: int = 30) -> Dict[str, Any]:
    col_row = pg_fetchone(
        """
        SELECT
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='mlb' AND table_name='player_ids' AND column_name='active'
          ) AS has_active,
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='mlb' AND table_name='player_ids' AND column_name='updated_at'
          ) AS has_updated_at
        """
    ) or {}
    has_active = bool(col_row.get("has_active"))
    has_updated_at = bool(col_row.get("has_updated_at"))

    total_row = pg_fetchone("SELECT COUNT(*)::int AS n FROM mlb.player_ids") or {}
    total_players = int(total_row.get("n") or 0)

    active_players = None
    if has_active:
        active_row = pg_fetchone("SELECT COUNT(*)::int AS n FROM mlb.player_ids WHERE active = TRUE") or {}
        active_players = int(active_row.get("n") or 0)

    latest_updated_at = None
    stale = None
    age_hours = None
    if has_updated_at:
        updated_row = pg_fetchone(
            "SELECT MAX(updated_at)::text AS latest_updated_at FROM mlb.player_ids"
        ) or {}
        latest_updated_at = updated_row.get("latest_updated_at")
        latest_dt = _parse_dt(latest_updated_at)
        if latest_dt is not None:
            age = datetime.now(timezone.utc) - latest_dt
            age_hours = round(age.total_seconds() / 3600, 2)
            stale = age > timedelta(hours=int(stale_after_hours))

    minimum_ok = total_players >= int(require_min)
    freshness_ok = stale is not True
    ok = minimum_ok and freshness_ok
    return {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "table": "player_ids",
        "total_players": total_players,
        "active_players": active_players,
        "latest_updated_at": latest_updated_at,
        "age_hours": age_hours,
        "stale": stale,
        "stale_after_hours": int(stale_after_hours),
        "require_min": int(require_min),
    }

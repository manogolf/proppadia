#!/usr/bin/env python3
"""Emit a compact MLB pipeline readiness snapshot from DB signals."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Sequence

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


def _stat_derived_check(days: int, require_min: int) -> Dict[str, Any]:
    row = pg_fetchone(
        """
        SELECT
          COUNT(*)::int AS n,
          MAX(game_date)::text AS latest_game_date
        FROM model_training_props
        WHERE prop_source = 'stat_derived'
          AND game_date >= (CURRENT_DATE - (%s::int || ' days')::interval)::date
        """,
        (int(days),),
    ) or {}
    count = int(row.get("n") or 0)
    latest_game_date = row.get("latest_game_date")
    ok = count >= int(require_min)
    return {
        "status": "pass" if ok else "fail",
        "window_days": int(days),
        "count": count,
        "latest_game_date": latest_game_date,
        "require_min": int(require_min),
    }


def _roster_check(require_min: int, stale_after_hours: int) -> Dict[str, Any]:
    col_row = pg_fetchone(
        """
        SELECT
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='player_ids' AND column_name='active'
          ) AS has_active,
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='player_ids' AND column_name='updated_at'
          ) AS has_updated_at
        """
    ) or {}
    has_active = bool(col_row.get("has_active"))
    has_updated_at = bool(col_row.get("has_updated_at"))

    total_row = pg_fetchone("SELECT COUNT(*)::int AS n FROM public.player_ids") or {}
    total_players = int(total_row.get("n") or 0)

    active_players = None
    if has_active:
        active_row = pg_fetchone("SELECT COUNT(*)::int AS n FROM public.player_ids WHERE active = TRUE") or {}
        active_players = int(active_row.get("n") or 0)

    latest_updated_at = None
    stale = None
    age_hours = None
    if has_updated_at:
        updated_row = pg_fetchone(
            "SELECT MAX(updated_at)::text AS latest_updated_at FROM public.player_ids"
        ) or {}
        latest_updated_at = updated_row.get("latest_updated_at")
        latest_dt = _parse_dt(latest_updated_at)
        if latest_dt is not None:
            age = datetime.now(timezone.utc) - latest_dt
            age_hours = round(age.total_seconds() / 3600, 2)
            stale = age > timedelta(hours=int(stale_after_hours))

    minimum_ok = total_players >= int(require_min)
    freshness_ok = (stale is not True)
    ok = minimum_ok and freshness_ok
    return {
        "status": "pass" if ok else "fail",
        "total_players": total_players,
        "active_players": active_players,
        "latest_updated_at": latest_updated_at,
        "age_hours": age_hours,
        "stale": stale,
        "stale_after_hours": int(stale_after_hours),
        "require_min": int(require_min),
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit MLB readiness snapshot (JSON).")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    args = ap.parse_args(list(argv) if argv is not None else [])

    checks: Dict[str, Dict[str, Any]] = {}
    errors: Dict[str, str] = {}

    try:
        checks["stat_derived"] = _stat_derived_check(args.stat_days, args.stat_require_min)
    except Exception as exc:
        errors["stat_derived"] = f"{type(exc).__name__}: {exc}"
        checks["stat_derived"] = {"status": "fail"}

    try:
        checks["roster"] = _roster_check(args.roster_require_min, args.roster_stale_hours)
    except Exception as exc:
        errors["roster"] = f"{type(exc).__name__}: {exc}"
        checks["roster"] = {"status": "fail"}

    ok = all((c.get("status") == "pass") for c in checks.values()) and not errors
    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass" if ok else "fail",
        "ok": ok,
        "checks": checks,
        "errors": errors,
    }
    print(json.dumps(payload, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Emit a compact MLB pipeline readiness snapshot from DB signals."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Sequence

from backend.app.services.mlb.roster_freshness_service import get_roster_freshness
from backend.shared.db.pg import pg_fetchone


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
    payload = get_roster_freshness(require_min=require_min, stale_after_hours=stale_after_hours)
    payload.pop("table", None)
    payload.pop("ok", None)
    return payload


def collect_snapshot(
    *,
    stat_days: int,
    stat_require_min: int,
    roster_require_min: int,
    roster_stale_hours: int,
) -> Dict[str, Any]:
    checks: Dict[str, Dict[str, Any]] = {}
    errors: Dict[str, str] = {}

    try:
        checks["stat_derived"] = _stat_derived_check(stat_days, stat_require_min)
    except Exception as exc:
        errors["stat_derived"] = f"{type(exc).__name__}: {exc}"
        checks["stat_derived"] = {"status": "fail"}

    try:
        checks["roster"] = _roster_check(roster_require_min, roster_stale_hours)
    except Exception as exc:
        errors["roster"] = f"{type(exc).__name__}: {exc}"
        checks["roster"] = {"status": "fail"}

    ok = all((c.get("status") == "pass") for c in checks.values()) and not errors
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass" if ok else "fail",
        "ok": ok,
        "checks": checks,
        "errors": errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit MLB readiness snapshot (JSON).")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    args = ap.parse_args(list(argv) if argv is not None else [])

    payload = collect_snapshot(
        stat_days=args.stat_days,
        stat_require_min=args.stat_require_min,
        roster_require_min=args.roster_require_min,
        roster_stale_hours=args.roster_stale_hours,
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

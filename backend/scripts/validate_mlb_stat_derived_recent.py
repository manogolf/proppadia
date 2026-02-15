#!/usr/bin/env python3
"""Report recent MLB stat-derived row volume in model_training_props."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Optional

from backend.shared.db.pg import pg_fetchone


def _valid_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    datetime.strptime(s, "%Y-%m-%d")
    return s


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate recent MLB stat-derived row volume.")
    ap.add_argument(
        "--days",
        type=int,
        default=7,
        help="Rolling window in days ending today (ET not required; DB date semantics).",
    )
    ap.add_argument(
        "--from-date",
        default=None,
        help="Optional lower bound override (YYYY-MM-DD).",
    )
    ap.add_argument(
        "--to-date",
        default=None,
        help="Optional upper bound override (YYYY-MM-DD).",
    )
    ap.add_argument(
        "--require-min",
        type=int,
        default=0,
        help="Fail when count is below this value.",
    )
    args = ap.parse_args()

    from_date = _valid_date(args.from_date)
    to_date = _valid_date(args.to_date)
    days = max(1, int(args.days))

    if from_date and to_date:
        sql = """
            SELECT COUNT(*)::int AS n
            FROM model_training_props
            WHERE prop_source = 'stat_derived'
              AND game_date >= %s::date
              AND game_date <= %s::date
        """
        row = pg_fetchone(sql, (from_date, to_date)) or {}
        label = f"{from_date}..{to_date}"
    elif from_date:
        sql = """
            SELECT COUNT(*)::int AS n
            FROM model_training_props
            WHERE prop_source = 'stat_derived'
              AND game_date >= %s::date
        """
        row = pg_fetchone(sql, (from_date,)) or {}
        label = f"{from_date}..now"
    elif to_date:
        sql = """
            SELECT COUNT(*)::int AS n
            FROM model_training_props
            WHERE prop_source = 'stat_derived'
              AND game_date <= %s::date
        """
        row = pg_fetchone(sql, (to_date,)) or {}
        label = f"start..{to_date}"
    else:
        sql = """
            SELECT COUNT(*)::int AS n
            FROM model_training_props
            WHERE prop_source = 'stat_derived'
              AND game_date >= (CURRENT_DATE - (%s::int || ' days')::interval)::date
        """
        row = pg_fetchone(sql, (days,)) or {}
        label = f"last_{days}_days"

    n = int(row.get("n") or 0)
    print(f"MLB stat_derived rows ({label}): {n}")

    require_min = max(0, int(args.require_min))
    if n < require_min:
        print(f"FAIL stat_derived row count {n} < required minimum {require_min}")
        return 1
    print("PASS stat_derived volume check")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


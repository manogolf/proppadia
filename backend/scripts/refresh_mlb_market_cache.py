#!/usr/bin/env python3
"""
Warm MLB OddsAPI market cache for one or more ET dates.

Examples:
  .venv/bin/python backend/scripts/refresh_mlb_market_cache.py
  .venv/bin/python backend/scripts/refresh_mlb_market_cache.py --days 2
  .venv/bin/python backend/scripts/refresh_mlb_market_cache.py --date 2026-04-01 --date 2026-04-02
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

from backend.app.services.mlb.market_odds_service import refresh_market_cache_for_date

ET = ZoneInfo("America/New_York")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh MLB market cache dates")
    parser.add_argument(
        "--date",
        action="append",
        default=[],
        help="Explicit YYYY-MM-DD date (repeat for multiple)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="When --date is omitted, refresh N consecutive ET dates starting today (default: 1)",
    )
    return parser.parse_args()


def _target_dates(args: argparse.Namespace) -> List[str]:
    if args.date:
        return [str(d) for d in args.date]
    days = max(1, int(args.days or 1))
    today = datetime.now(ET).date()
    return [(today + timedelta(days=i)).isoformat() for i in range(days)]


def main() -> int:
    args = _parse_args()
    targets = _target_dates(args)

    failed = 0
    for d in targets:
        try:
            # Validate date format early for clear output.
            date.fromisoformat(d)
        except ValueError:
            failed += 1
            print(f"FAIL market_cache_refresh date={d} detail=invalid YYYY-MM-DD")
            continue

        out = refresh_market_cache_for_date(game_date=d)
        if out.get("ok"):
            print(
                "PASS market_cache_refresh "
                f"date={d} rows_cached={out.get('rows_cached', 0)} "
                f"cache_hit={str(bool(out.get('cache_hit'))).lower()} "
                f"age_seconds={out.get('age_seconds')}"
            )
        else:
            failed += 1
            print(
                "FAIL market_cache_refresh "
                f"date={d} detail={out.get('reason') or 'unknown error'}"
            )

    total = len(targets)
    print(f"Summary: {total - failed}/{total} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())


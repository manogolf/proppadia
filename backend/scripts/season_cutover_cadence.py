#!/usr/bin/env python3
"""Print the intended in-season MLB cadence plan (cron + command lanes)."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def build_plan(
    *,
    timezone: str,
    market_every_hours: int,
    roster_hour_local: int,
    stat_hour_local: int,
    ops_hour_local: int,
) -> dict[str, Any]:
    market_every_hours = max(1, int(market_every_hours))
    roster_hour_local = max(0, min(23, int(roster_hour_local)))
    stat_hour_local = max(0, min(23, int(stat_hour_local)))
    ops_hour_local = max(0, min(23, int(ops_hour_local)))
    return {
        "ok": True,
        "status": "pass",
        "timezone": timezone,
        "lanes": [
            {
                "name": "market_cache_refresh",
                "when": f"every {market_every_hours} hours",
                "cron": f"0 */{market_every_hours} * * *",
                "command": "make mlb-market-cache-refresh MLB_MARKET_DAYS=1",
            },
            {
                "name": "roster_refresh_daily",
                "when": f"{roster_hour_local:02d}:00 daily ({timezone})",
                "cron": f"0 {roster_hour_local} * * *",
                "command": "make mlb-roster-refresh-all MLB_ROSTER_DATE=$(date +%F)",
            },
            {
                "name": "stat_derived_refresh_daily",
                "when": f"{stat_hour_local:02d}:00 daily ({timezone})",
                "cron": f"0 {stat_hour_local} * * *",
                "command": (
                    "make mlb-stat-derived-refresh MLB_STAT_DAYS_AGO=2 "
                    "MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7 MLB_STAT_DERIVED_MIN=1"
                ),
            },
            {
                "name": "ops_daily_gate",
                "when": f"{ops_hour_local:02d}:00 daily ({timezone})",
                "cron": f"0 {ops_hour_local} * * *",
                "command": "make ops-daily-check",
            },
        ],
        "pre_cutover_gate": "make season-cutover-ready",
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Show intended in-season MLB cadence plan.")
    ap.add_argument("--timezone", default="America/New_York")
    ap.add_argument("--market-every-hours", type=int, default=8)
    ap.add_argument("--roster-hour-local", type=int, default=9)
    ap.add_argument("--stat-hour-local", type=int, default=11)
    ap.add_argument("--ops-hour-local", type=int, default=12)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    payload = build_plan(
        timezone=args.timezone,
        market_every_hours=args.market_every_hours,
        roster_hour_local=args.roster_hour_local,
        stat_hour_local=args.stat_hour_local,
        ops_hour_local=args.ops_hour_local,
    )

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"In-season cadence plan ({payload['timezone']}):")
        print(f"Pre-cutover gate: {payload['pre_cutover_gate']}")
        for lane in payload["lanes"]:
            print(f"- {lane['name']}: {lane['when']} | cron=\"{lane['cron']}\"")
            print(f"  {lane['command']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

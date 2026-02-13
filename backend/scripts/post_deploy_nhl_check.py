#!/usr/bin/env python3
"""
Fast post-deploy NHL checks against a running backend URL.

This is intentionally lightweight and safe:
- no DB write operations
- validates core NHL API health and key read endpoints
"""

from __future__ import annotations

import argparse
from typing import List, Sequence

from backend.scripts.check_output_utils import print_check_rows, print_summary, print_warn_rows
from backend.scripts.check_validators import expect_list_or_error_object, expect_ok, expect_ok_count_rows, expect_ping_sport
from backend.scripts.http_check_utils import CheckResult, HttpClient, run_check
from backend.scripts.sparse_warning_utils import find_sparse_warnings
from backend.scripts.strict_data_gate import enforce_strict_data_gate

def run(base_url: str, *, date: str, require_data: bool, allow_sparse: bool) -> int:
    client = HttpClient(base_url)
    checks: List[CheckResult] = []

    checks.append(
        run_check(
            client, name="health", method="GET", path="/api/health", expected_status=[200], validate=expect_ok
        )
    )
    checks.append(
        run_check(
            client,
            name="nhl_ping",
            method="GET",
            path="/api/nhl/ping",
            expected_status=[200],
            validate=expect_ping_sport("nhl"),
        )
    )
    checks.append(
        run_check(
            client,
            name="nhl_ping_db",
            method="GET",
            path="/api/nhl/ping-db",
            expected_status=[200],
            validate=expect_ok,
        )
    )
    checks.append(
        run_check(
            client,
            name="games_today",
            method="GET",
            path="/api/nhl/games/today",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=expect_ok_count_rows,
        )
    )
    checks.append(
        run_check(
            client,
            name="props_today",
            method="GET",
            path="/api/nhl/props/today",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=expect_ok_count_rows,
        )
    )
    checks.append(
        run_check(
            client,
            name="sog",
            method="GET",
            path="/api/nhl/sog",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=expect_list_or_error_object,
        )
    )
    checks.append(
        run_check(
            client,
            name="saves",
            method="GET",
            path="/api/nhl/saves",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=expect_list_or_error_object,
        )
    )

    passes = sum(1 for c in checks if c.ok)
    warns: List[str] = []
    if require_data:
        warns = find_sparse_warnings(
            checks,
            [
                ("games_today", "contains", '"count": 0', "games_today returned count=0"),
                ("props_today", "contains", '"count": 0', "props_today returned count=0"),
                ("sog", "contains", "list(len=0)", "sog returned list(len=0)"),
                ("saves", "contains", "list(len=0)", "saves returned list(len=0)"),
            ],
        )

    print_check_rows(checks, name_width=24, path_width=30)
    print_warn_rows(warns, label="data-richness")
    print_summary(passed=passes, total=len(checks))
    if passes != len(checks):
        return 1
    return enforce_strict_data_gate(require_data=require_data, allow_sparse=allow_sparse, warns=warns)


def main() -> int:
    ap = argparse.ArgumentParser(description="Fast post-deploy NHL checks")
    ap.add_argument("--base-url", required=True, help="Running backend URL, e.g. https://baseball-streaks-sq44.onrender.com")
    ap.add_argument("--date", default="2025-11-20", help="Probe date for NHL data endpoints")
    ap.add_argument(
        "--require-data",
        action="store_true",
        help="Fail if games/props/sog/saves are sparse for the probe date",
    )
    ap.add_argument(
        "--allow-sparse",
        action="store_true",
        help="When used with --require-data, keep warnings but do not fail on sparse probe data",
    )
    args = ap.parse_args()
    return run(
        args.base_url,
        date=args.date,
        require_data=args.require_data,
        allow_sparse=args.allow_sparse,
    )


if __name__ == "__main__":
    raise SystemExit(main())

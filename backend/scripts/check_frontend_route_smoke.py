#!/usr/bin/env python3
"""Lightweight frontend route smoke check.

Verifies critical route and nav link surfaces in AppRouter without browser deps.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Sequence, Tuple


APP_ROUTER = Path("frontend/src/routes/AppRouter.jsx")

REQUIRED_NAV_HREFS = {
    "/",
    "/props",
    "/watchlist",
    "/players/mlb",
    "/players/nhl",
    "/ops",
}

REQUIRED_ROUTE_PATHS = {
    "/",
    "/props",
    "/watchlist",
    "/players/mlb",
    "/players/nhl",
    "/ops",
}


def _extract_hrefs(text: str) -> set[str]:
    return set(re.findall(r'href="([^"]+)"', text))


def _extract_route_paths(text: str) -> set[str]:
    return set(re.findall(r'<Route\s+path="([^"]+)"', text))


def _missing(required: set[str], present: set[str]) -> List[str]:
    return sorted(required - present)


def main(argv: Sequence[str] | None = None) -> int:
    if not APP_ROUTER.exists():
        print(f"FAIL route smoke: missing {APP_ROUTER}")
        return 1

    text = APP_ROUTER.read_text(encoding="utf-8")
    hrefs = _extract_hrefs(text)
    paths = _extract_route_paths(text)

    missing_hrefs = _missing(REQUIRED_NAV_HREFS, hrefs)
    missing_paths = _missing(REQUIRED_ROUTE_PATHS, paths)
    problems: List[Tuple[str, str]] = []
    for h in missing_hrefs:
        problems.append(("href", h))
    for p in missing_paths:
        problems.append(("route", p))

    if problems:
        print("FAIL frontend route smoke:")
        for kind, value in problems:
            print(f"- missing {kind}: {value}")
        return 1

    print("PASS frontend route smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


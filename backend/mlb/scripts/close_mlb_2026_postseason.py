#!/usr/bin/env python3
"""Plan (default) or write the 2026 MLB postseason closeout package.

Plan mode prints the package and writes nothing. --execute writes the
write-once package and requires --authorized-by. Moneyline is read through
MLB_RESEARCH_DATABASE_URL (restricted research login); without it the run stops.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from backend.mlb.season_transition.postseason_closeout_v1 import DEFAULT_OUTPUT, build_package, execute
from backend.mlb.season_transition.regular_season_close_inventory_v1 import CloseInventoryError
from backend.shared.db.mlb_research import MLB_RESEARCH_DATABASE_URL_ENV


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--authorized-by", default="")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    if not os.environ.get(MLB_RESEARCH_DATABASE_URL_ENV, "").strip():
        print(json.dumps({"decision": "BLOCKED", "error": "RESEARCH_LOGIN_ROUTE_MISSING"}))
        return 2
    if args.execute and not args.authorized_by.strip():
        parser.error("--execute requires --authorized-by")
    try:
        if args.execute:
            body = execute(args.authorized_by, args.output)
            print(json.dumps({"decision": "POSTSEASON_CLOSEOUT_PUBLISHED", "path": str(args.output),
                              "package_sha256": body["package_sha256"]}, indent=2))
        else:
            body = build_package()
            body = {**body, "population": {k: v for k, v in body["population"].items() if k != "games"}}
            print(json.dumps({"mode": "PLAN_NO_WRITE", **body}, indent=2, sort_keys=True))
            return 0 if body["decision"] == "POSTSEASON_CLOSEOUT_READY" else 1
    except CloseInventoryError as exc:
        print(json.dumps({"decision": "BLOCKED", "error": str(exc)}))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Plan/check the pinned 2026 regular-season close; execute only with authorization."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.mlb.season_transition.regular_season_close_operation_v1 import (
    DEFAULT_OUTPUT, close_inputs, execute_close,
)
from backend.mlb.season_transition.regular_season_close_inventory_v1 import CloseInventoryError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Publish close (requires --authorization)")
    parser.add_argument("--authorization", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    if args.execute and args.authorization is None:
        parser.error("--execute requires --authorization")
    if not args.execute and args.authorization is not None:
        parser.error("--authorization is only accepted with --execute")
    try:
        inputs, rows = close_inputs()
        if args.execute:
            result = execute_close(args.authorization, args.output)
            print(json.dumps({"decision": "CLOSE_PUBLISHED", "path": str(args.output),
                              "close_artifact_sha256": result["close_artifact_sha256"]},
                             indent=2, sort_keys=True))
        else:
            print(json.dumps({"decision": "CLOSE_PLAN_READY", "mutating": False,
                              "publication": "NOT_PERFORMED", "inputs": inputs,
                              "game_count": len(rows)}, indent=2, sort_keys=True))
    except CloseInventoryError as exc:
        print(json.dumps({"decision": "CLOSE_BLOCKED", "error": str(exc)}, indent=2, sort_keys=True))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

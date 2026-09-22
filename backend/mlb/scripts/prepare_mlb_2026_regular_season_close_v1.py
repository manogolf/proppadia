#!/usr/bin/env python3
"""Check the one provenance-bound 2026 regular-season close inventory.

This command has no caller-supplied inventory, authorization token, execution
mode, or package writer.  A successful future check still performs no close.
"""

from __future__ import annotations

import json
import sys

from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    CloseInventoryError,
    validate_close_inventory_package,
)


def main() -> int:
    if len(sys.argv) != 1:
        print("CLOSE_CHECK_ACCEPTS_NO_CALLER_ARGUMENTS", file=sys.stderr)
        return 2
    try:
        report = validate_close_inventory_package()
    except CloseInventoryError as exc:
        print(json.dumps({"decision": "REGULAR_SEASON_CLOSE_BLOCKED", "error": str(exc)}, indent=2, sort_keys=True))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["integrity_passed"] or not report["close_ready"]:
        return 1
    print("CHECK_ONLY_READY: no close package written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

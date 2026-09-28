#!/usr/bin/env python3
"""Check the pinned V2 2026 regular-season close inventory.

This command has no caller-supplied inventory, authorization token, execution
mode, or package writer. A successful readiness check still performs no close.
The historical `_v1` filename is retained for compatibility; its authority
input is the explicitly pinned V2 reconciliation package.
"""

from __future__ import annotations

import json
import sys

from backend.mlb.season_transition.regular_season_close_inventory_v1 import CloseInventoryError
from backend.mlb.season_transition.regular_season_close_inventory_v2 import (
    validate_close_readiness_package,
)


def main() -> int:
    if len(sys.argv) != 1:
        print("CLOSE_CHECK_ACCEPTS_NO_CALLER_ARGUMENTS", file=sys.stderr)
        return 2
    try:
        report = validate_close_readiness_package()
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

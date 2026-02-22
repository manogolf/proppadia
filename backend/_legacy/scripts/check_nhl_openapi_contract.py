#!/usr/bin/env python3
"""
Check NHL OpenAPI contract drift against snapshot.

Compares:
- NHL path set
- Per-method request/response schema refs for NHL paths
- Referenced component schemas (deep, via $ref traversal)

Exit codes:
- 0: no drift
- 1: drift detected or snapshot missing
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

from backend.scripts.openapi_contract_utils import diff_contract, load_json

SNAPSHOT_DEFAULT = Path("docs/openapi/openapi.snapshot.json")


def _nhl_path(path: str) -> bool:
    return path.startswith("/api/nhl")
def main() -> int:
    ap = argparse.ArgumentParser(description="Check NHL OpenAPI contract drift")
    ap.add_argument("--snapshot", default=str(SNAPSHOT_DEFAULT))
    args = ap.parse_args()

    snapshot_path = Path(args.snapshot)
    if not snapshot_path.exists():
        print(f"FAIL snapshot not found: {snapshot_path}")
        print("Hint: generate it first (see docs/NHL OpenAPI Review.md).")
        return 1

    from backend.app.api_server import app

    current = app.openapi()
    snapshot = load_json(snapshot_path)
    errors, notes = diff_contract(snapshot=snapshot, current=current, include_path=_nhl_path, label="NHL")
    if errors:
        print("FAIL NHL OpenAPI contract drift detected:")
        for e in errors:
            print(f"- {e}")
        for n in notes:
            print(f"  {n}")
        return 1

    print("PASS NHL OpenAPI contract matches snapshot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

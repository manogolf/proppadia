#!/usr/bin/env python3
"""
Check MLB OpenAPI contract drift against snapshot.

Compares:
- MLB path set
- Per-method request/response schema refs for MLB paths
- Referenced component schemas (deep, via $ref traversal)

Exit codes:
- 0: no drift
- 1: drift detected or snapshot missing
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

from backend.shared.scripts.openapi_contract_utils import diff_contract, load_json

SNAPSHOT_DEFAULT = Path("docs/openapi/openapi.snapshot.json")


def _mlb_path(path: str) -> bool:
    return (
        path.startswith("/api/mlb")
        or path.startswith("/api/players")
        or path.startswith("/api/player-profile")
        or path.startswith("/api/games/context")
        or path.startswith("/api/prepareProp")
        or path.startswith("/api/predict")
        or path.startswith("/api/props/")
        or path.startswith("/api/model")
        or path.startswith("/api/user-vs-model")
    )
def main() -> int:
    ap = argparse.ArgumentParser(description="Check MLB OpenAPI contract drift")
    ap.add_argument("--snapshot", default=str(SNAPSHOT_DEFAULT))
    args = ap.parse_args()

    snapshot_path = Path(args.snapshot)
    if not snapshot_path.exists():
        print(f"FAIL snapshot not found: {snapshot_path}")
        print("Hint: generate it first (see docs/MLB OpenAPI Review.md).")
        return 1

    from backend.app.api_server import app

    current = app.openapi()
    snapshot = load_json(snapshot_path)
    errors, notes = diff_contract(snapshot=snapshot, current=current, include_path=_mlb_path, label="MLB")
    if errors:
        print("FAIL MLB OpenAPI contract drift detected:")
        for e in errors:
            print(f"- {e}")
        for n in notes:
            print(f"  {n}")
        return 1

    print("PASS MLB OpenAPI contract matches snapshot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

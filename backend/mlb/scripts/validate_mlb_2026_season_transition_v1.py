#!/usr/bin/env python3
"""Offline deterministic validator for the 2026 MLB season transition."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.mlb.season_transition.contract_v1 import validate_close_inventory, validate_fixture_suite

DEFAULT_FIXTURES = Path("backend/mlb/season_transition/fixtures/phase_and_close_cases_v1.json")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument("--close-inventory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.fixtures.read_text(encoding="utf-8"))
    report = validate_fixture_suite(payload)
    if args.close_inventory:
        close_payload = json.loads(args.close_inventory.read_text(encoding="utf-8"))
        report["close_inventory_validation"] = validate_close_inventory(close_payload)
        report["passed"] = report["passed"] and report["close_inventory_validation"]["passed"]
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

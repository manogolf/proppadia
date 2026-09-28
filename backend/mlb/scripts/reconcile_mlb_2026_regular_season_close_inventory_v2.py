"""Build or validate the offline V2 regular-season close reconciliation."""
from __future__ import annotations

import argparse
import json

from backend.mlb.season_transition.regular_season_close_inventory_v2 import (
    PACKAGE, initialize_package, validate_package,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="Pin retained evidence and build the versioned inventory")
    args = parser.parse_args()
    if args.build:
        PACKAGE.mkdir(parents=True, exist_ok=True)
        report = initialize_package()
    else:
        report = validate_package()
        (PACKAGE / "validation_report.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("integrity_passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())

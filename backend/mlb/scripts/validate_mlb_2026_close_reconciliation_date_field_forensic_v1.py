"""Build/validate the bounded offline close date-field forensic package."""
from __future__ import annotations

import argparse
import json

from backend.mlb.season_transition.close_date_field_forensic_v1 import (
    PACKAGE, build_report, validate_report,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="Write the deterministic forensic evidence report")
    args = parser.parse_args()
    if args.build:
        PACKAGE.mkdir(parents=True, exist_ok=True)
        report = build_report()
        (PACKAGE / "forensic_report.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n")
        result = {"integrity_passed": report["integrity_passed"]}
    else:
        result = validate_report()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["integrity_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

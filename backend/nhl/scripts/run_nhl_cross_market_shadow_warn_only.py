#!/usr/bin/env python3
"""WARN-only orchestration hook for the gated NHL cross-market shadow lane."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from backend.nhl.cross_market_shadow.core import ACTIVATION_PATH, CONTROL_NAME, daily_status


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--simulate-failure", action="store_true")
    args = parser.parse_args()
    status_root = args.output_root / "orchestration_status" / args.slate_date
    status_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = status_root / f"{timestamp}.json"
    activation = json.loads(ACTIVATION_PATH.read_text())
    failed = False
    try:
        if args.simulate_failure:
            raise RuntimeError("SIMULATED_CROSS_MARKET_SHADOW_FAILURE")
        daily = daily_status(args.output_root, args.slate_date)
        result = {
            "status": "READY_NOT_ACTIVATED" if not activation["capture_enabled"] else "ACTIVE",
            "warning_only": True, "control_name": CONTROL_NAME,
            "capture_enabled": activation["capture_enabled"], "daily_status": daily,
            "player_prop_execution_allowed": True, "failure": None,
        }
    except Exception as error:
        failed = True
        result = {
            "status": "FAILED_WARN_ONLY", "warning_only": True, "control_name": CONTROL_NAME,
            "capture_enabled": False, "player_prop_execution_allowed": True,
            "failure": f"{type(error).__name__}:{error}",
        }
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

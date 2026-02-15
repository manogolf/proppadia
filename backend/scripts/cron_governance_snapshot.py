#!/usr/bin/env python3
"""Emit a single JSON snapshot for cron/workflow governance checks."""

from __future__ import annotations

import json

from backend.scripts import check_nhl_workflow_compat
from backend.scripts import check_workflow_command_paths
from backend.scripts import check_workflow_schedule_inventory
from backend.scripts import json_check_runner
from backend.scripts import season_activation_report


def main() -> int:
    inv_rc, inventory = json_check_runner.run_json_check(
        check_workflow_schedule_inventory.main, ["--strict", "--json"]
    )
    path_rc, path_audit = json_check_runner.run_json_check(
        check_workflow_command_paths.main, ["--strict", "--json"]
    )
    nhl_rc, nhl_compat = json_check_runner.run_json_check(check_nhl_workflow_compat.main, ["--json"])
    activation_rc, activation_report = json_check_runner.run_json_check(
        season_activation_report.main, ["--strict"]
    )

    governance_ok = inv_rc == 0 and path_rc == 0 and nhl_rc == 0
    activation_ok = activation_rc == 0 and bool(activation_report.get("ok", False))
    ok = governance_ok and activation_ok
    snapshot = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "governance_ok": governance_ok,
        "season_activation_ok": activation_ok,
        "checks": {
            "workflow_inventory": inventory,
            "workflow_path_audit": path_audit,
            "nhl_workflow_compat": nhl_compat,
            "season_activation_report": activation_report,
        },
    }
    print(json.dumps(snapshot, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

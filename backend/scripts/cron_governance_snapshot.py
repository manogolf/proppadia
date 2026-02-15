#!/usr/bin/env python3
"""Emit a single JSON snapshot for cron/workflow governance checks."""

from __future__ import annotations

import json
import sys
from contextlib import redirect_stdout
from io import StringIO
from typing import Callable

from backend.scripts import check_nhl_workflow_compat
from backend.scripts import check_workflow_command_paths
from backend.scripts import check_workflow_schedule_inventory
from backend.scripts import phase_status_snapshot
from backend.scripts import season_activation_status


def _run_json_check(fn: Callable[[list[str]], int], args: list[str]) -> tuple[int, dict]:
    out = StringIO()
    with redirect_stdout(out):
        rc = fn(args)
    payload = json.loads(out.getvalue())
    return rc, payload


def main() -> int:
    inv_rc, inventory = _run_json_check(
        check_workflow_schedule_inventory.main, ["--strict", "--json"]
    )
    path_rc, path_audit = _run_json_check(
        check_workflow_command_paths.main, ["--strict", "--json"]
    )
    nhl_rc, nhl_compat = _run_json_check(check_nhl_workflow_compat.main, ["--json"])
    phase_rc, phase_status = _run_json_check(phase_status_snapshot.main, [])
    activation_rc, activation_status = _run_json_check(season_activation_status.main, [])

    governance_ok = inv_rc == 0 and path_rc == 0 and nhl_rc == 0 and phase_rc == 0
    activation_ok = activation_rc == 0 and bool(activation_status.get("ok", False))
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
            "phase_status": phase_status,
            "season_activation": activation_status,
        },
    }
    print(json.dumps(snapshot, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

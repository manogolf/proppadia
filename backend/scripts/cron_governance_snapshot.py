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

    ok = inv_rc == 0 and path_rc == 0 and nhl_rc == 0
    snapshot = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "checks": {
            "workflow_inventory": inventory,
            "workflow_path_audit": path_audit,
            "nhl_workflow_compat": nhl_compat,
        },
    }
    print(json.dumps(snapshot, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

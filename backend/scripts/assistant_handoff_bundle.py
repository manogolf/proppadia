#!/usr/bin/env python3
"""Emit a single assistant-ready JSON bundle for support handoffs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Sequence

from backend.scripts import check_nhl_workflow_compat
from backend.scripts import check_workflow_command_paths
from backend.scripts import check_workflow_schedule_inventory
from backend.scripts import phase_status_snapshot
from backend.scripts.mlb_readiness_last import _load_history, _regressions
from backend.scripts.mlb_readiness_snapshot import collect_snapshot


def _run_json_check(fn: Callable[[list[str]], int], args: list[str]) -> tuple[int, dict[str, Any]]:
    out = StringIO()
    with redirect_stdout(out):
        rc = fn(args)
    return rc, json.loads(out.getvalue())


def _default_ops_vars() -> dict[str, Any]:
    return {
        "BASE_URL": os.getenv("BASE_URL", "http://127.0.0.1:8001"),
        "MLB_DATE": os.getenv("MLB_DATE", "2025-08-15"),
        "MLB_MARKET_DAYS": int(os.getenv("MLB_MARKET_DAYS", "1")),
        "MLB_ROSTER_DATE": os.getenv("MLB_ROSTER_DATE", ""),
        "MLB_STAT_DERIVED_DAYS": int(os.getenv("MLB_STAT_DERIVED_DAYS", "7")),
        "MLB_STAT_DERIVED_MIN": int(os.getenv("MLB_STAT_DERIVED_MIN", "0")),
    }


def _history_tail(input_path: str, limit: int) -> dict[str, Any]:
    path = Path(input_path)
    history = _load_history(path)
    tail = history[-max(1, int(limit)) :]
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "stat_derived_count": (((item.get("checks") or {}).get("stat_derived") or {}).get("count")),
                "roster_total_players": (((item.get("checks") or {}).get("roster") or {}).get("total_players")),
                "roster_stale": (((item.get("checks") or {}).get("roster") or {}).get("stale")),
                "regressions": _regressions(prev, item) if prev else [],
            }
        )
    return {
        "input": str(path),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }


def _season_activation_tail(input_path: str, limit: int) -> dict[str, Any]:
    path = Path(input_path)
    history = _load_history(path)
    tail = history[-max(1, int(limit)) :]
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        prev_blockers = set((((prev.get("readiness") or {}).get("blockers")) or [])) if prev else set()
        cur_blockers = set((((item.get("readiness") or {}).get("blockers")) or []))
        rows.append(
            {
                "status": item.get("status"),
                "ok": item.get("ok"),
                "phase6_count": len(item.get("phase6_tracker") or []),
                "has_mlb_baseline": (((item.get("baseline_artifacts") or {}).get("has_mlb"))),
                "has_nhl_baseline": (((item.get("baseline_artifacts") or {}).get("has_nhl"))),
                "blockers": sorted(list(cur_blockers)),
                "new_blockers": sorted(list(cur_blockers - prev_blockers)),
            }
        )
    return {
        "input": str(path),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit assistant-ready support handoff bundle (JSON).")
    ap.add_argument("--history-input", default="artifacts/mlb_readiness_history.jsonl")
    ap.add_argument("--history-limit", type=int, default=5)
    ap.add_argument("--season-activation-input", default="artifacts/season_activation_history.jsonl")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    args = ap.parse_args(list(argv) if argv is not None else [])

    inv_rc, inventory = _run_json_check(
        check_workflow_schedule_inventory.main, ["--strict", "--json"]
    )
    path_rc, path_audit = _run_json_check(
        check_workflow_command_paths.main, ["--strict", "--json"]
    )
    nhl_rc, nhl_compat = _run_json_check(check_nhl_workflow_compat.main, ["--json"])
    phase_rc, phase_status = _run_json_check(phase_status_snapshot.main, [])

    readiness = collect_snapshot(
        stat_days=args.stat_days,
        stat_require_min=args.stat_require_min,
        roster_require_min=args.roster_require_min,
        roster_stale_hours=args.roster_stale_hours,
    )
    history = _history_tail(args.history_input, args.history_limit)
    season_activation_history = _season_activation_tail(args.season_activation_input, args.history_limit)

    governance_ok = inv_rc == 0 and path_rc == 0 and nhl_rc == 0 and phase_rc == 0
    readiness_ok = bool(readiness.get("ok"))
    ok = governance_ok and readiness_ok

    bundle = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "ops_vars": _default_ops_vars(),
        "governance": {
            "ok": governance_ok,
            "checks": {
                "workflow_inventory": inventory,
                "workflow_path_audit": path_audit,
                "nhl_workflow_compat": nhl_compat,
                "phase_status": phase_status,
            },
        },
        "mlb_readiness": readiness,
        "mlb_readiness_history": history,
        "season_activation_history": season_activation_history,
    }
    print(json.dumps(bundle, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

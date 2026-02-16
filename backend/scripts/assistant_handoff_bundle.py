#!/usr/bin/env python3
"""Emit a single assistant-ready JSON bundle for support handoffs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Sequence

from backend.scripts import check_nhl_workflow_compat
from backend.scripts import check_workflow_command_paths
from backend.scripts import check_workflow_schedule_inventory
from backend.scripts import json_check_runner
from backend.scripts import mlb_pipeline_check
from backend.scripts import mlb_pipeline_last
from backend.scripts import season_activation_report
from backend.scripts.mlb_readiness_last import _load_history, _regressions
from backend.scripts.mlb_readiness_snapshot import collect_snapshot


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


def _pipeline_history_tail(input_path: str, limit: int) -> dict[str, Any]:
    history = mlb_pipeline_last._load_history(Path(input_path))
    tail = history[-max(1, int(limit)) :]
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "ok": item.get("ok"),
                "failures": item.get("failures") or [],
                "regressions": mlb_pipeline_last._regressions(prev, item),
            }
        )
    return {
        "input": str(input_path),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit assistant-ready support handoff bundle (JSON).")
    ap.add_argument("--history-input", default="artifacts/mlb_readiness_history.jsonl")
    ap.add_argument("--history-limit", type=int, default=5)
    ap.add_argument("--pipeline-history-input", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--pipeline-history-limit", type=int, default=5)
    ap.add_argument("--season-activation-input", default="artifacts/season_activation_history.jsonl")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    args = ap.parse_args(list(argv) if argv is not None else [])

    inv_rc, inventory = json_check_runner.run_json_check(
        check_workflow_schedule_inventory.main, ["--strict", "--json"]
    )
    path_rc, path_audit = json_check_runner.run_json_check(
        check_workflow_command_paths.main, ["--strict", "--json"]
    )
    nhl_rc, nhl_compat = json_check_runner.run_json_check(check_nhl_workflow_compat.main, ["--json"])
    mlb_pipeline_rc, mlb_pipeline = json_check_runner.run_json_check(
        mlb_pipeline_check.main,
        [
            "--date",
            os.getenv("MLB_DATE", "2025-08-15"),
            "--sample-size",
            os.getenv("MLB_PREDICT_SAMPLE", "10"),
            "--require-min-success",
            os.getenv("MLB_PREDICT_MIN_SUCCESS", "1"),
            "--prop-types",
            os.getenv("MLB_PREDICT_PROP_TYPES", "hits"),
            "--quality-window-mode",
            os.getenv("MLB_QUALITY_WINDOW_MODE", "days"),
            "--quality-window-days",
            os.getenv("MLB_QUALITY_WINDOW_DAYS", "120"),
            "--quality-games-back",
            os.getenv("MLB_QUALITY_GAMES_BACK", "30"),
            "--quality-min-total",
            os.getenv("MLB_QUALITY_MIN_TOTAL", "1"),
            "--quality-min-accuracy",
            os.getenv("MLB_QUALITY_MIN_ACCURACY", "0"),
            "--coverage-window-mode",
            os.getenv("MLB_PROP_COVERAGE_WINDOW_MODE", "days"),
            "--coverage-window-days",
            os.getenv("MLB_PROP_COVERAGE_WINDOW_DAYS", "30"),
            "--coverage-games-back",
            os.getenv("MLB_PROP_COVERAGE_GAMES_BACK", "30"),
            "--coverage-required-props",
            os.getenv("MLB_PROP_COVERAGE_REQUIRED", ""),
            "--coverage-min-graded-per-prop",
            os.getenv("MLB_PROP_COVERAGE_MIN_GRADED", "0"),
        ],
    )
    report_rc, season_report = json_check_runner.run_json_check(
        season_activation_report.main,
        [
            "--strict",
            "--history-input",
            args.season_activation_input,
            "--history-limit",
            str(args.history_limit),
        ],
    )

    readiness = collect_snapshot(
        stat_days=args.stat_days,
        stat_require_min=args.stat_require_min,
        roster_require_min=args.roster_require_min,
        roster_stale_hours=args.roster_stale_hours,
    )
    history = _history_tail(args.history_input, args.history_limit)
    pipeline_history = _pipeline_history_tail(args.pipeline_history_input, args.pipeline_history_limit)
    season_activation_history = (season_report.get("season_activation_history") or {})
    season_baseline_latest = (season_report.get("baseline_latest") or {})

    governance_ok = (
        inv_rc == 0 and path_rc == 0 and nhl_rc == 0 and mlb_pipeline_rc == 0 and report_rc == 0
    )
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
                "mlb_pipeline_check": mlb_pipeline,
                "season_activation_report": season_report,
            },
        },
        "mlb_readiness": readiness,
        "mlb_readiness_history": history,
        "mlb_pipeline_history": pipeline_history,
        "season_activation_history": season_activation_history,
        "season_baseline_latest": season_baseline_latest,
    }
    print(json.dumps(bundle, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

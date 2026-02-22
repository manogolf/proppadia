#!/usr/bin/env python3
"""Emit one incident-ready payload: current compact ops + recent history tail."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from backend.scripts import ops_operator_last
from backend.scripts import ops_operator_summary
from backend.scripts import mlb_pipeline_last
from backend.mlb.scripts.mlb_readiness_last import _load_history


def collect_incident_snapshot(
    *,
    stat_days: int,
    stat_require_min: int,
    roster_require_min: int,
    roster_stale_hours: int,
    season_history_input: str,
    season_history_limit: int,
    season_history_max_age_hours: int,
    season_max_age_hours: int,
    season_cutover_history_input: str,
    season_cutover_history_limit: int,
    ops_history_input: str,
    ops_history_limit: int,
    pipeline_history_input: str,
    pipeline_history_limit: int,
) -> dict[str, Any]:
    summary_full = ops_operator_summary.collect_summary(
        stat_days=stat_days,
        stat_require_min=stat_require_min,
        roster_require_min=roster_require_min,
        roster_stale_hours=roster_stale_hours,
        season_history_input=season_history_input,
        season_history_limit=season_history_limit,
        season_history_max_age_hours=season_history_max_age_hours,
        season_max_age_hours=season_max_age_hours,
        season_cutover_history_input=season_cutover_history_input,
        season_cutover_history_limit=season_cutover_history_limit,
        pipeline_history_input=pipeline_history_input,
        pipeline_history_limit=pipeline_history_limit,
    )
    summary = ops_operator_summary.compact_summary(summary_full)

    history = _load_history(Path(ops_history_input))
    tail = history[-max(1, int(ops_history_limit)) :]
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        regressions = ops_operator_last._regressions(prev, item) if prev else []
        rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "ok": item.get("ok"),
                "governance_ok": (((item.get("governance") or {}).get("ok"))),
                "readiness_ok": (((item.get("mlb_readiness") or {}).get("ok"))),
                "season_ok": (((item.get("season_activation") or {}).get("ok"))),
                "stat_count": (((item.get("mlb_readiness") or {}).get("stat_count"))),
                "roster_stale": (((item.get("mlb_readiness") or {}).get("roster_stale"))),
                "blocker_count": (((item.get("season_activation") or {}).get("blocker_count"))),
                "top_blocker": (((item.get("season_activation") or {}).get("top_blocker"))),
                "regressions": regressions,
            }
        )

    latest_row = rows[-1] if rows else {}
    latest_regressions = latest_row.get("regressions") or []
    regressed = len(latest_regressions) > 0
    pipeline_history = mlb_pipeline_last._load_history(Path(pipeline_history_input))
    pipeline_tail = pipeline_history[-max(1, int(pipeline_history_limit)) :]
    pipeline_rows: list[dict[str, Any]] = []
    for idx, item in enumerate(pipeline_tail):
        prev = pipeline_tail[idx - 1] if idx > 0 else None
        pipeline_rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "ok": item.get("ok"),
                "failures": item.get("failures") or [],
                "regressions": mlb_pipeline_last._regressions(prev, item),
            }
        )
    latest_pipeline = pipeline_rows[-1] if pipeline_rows else {}
    latest_pipeline_regressions = latest_pipeline.get("regressions") or []
    return {
        "captured_at": summary.get("captured_at"),
        "ok": bool(summary.get("ok")),
        "status": summary.get("status"),
        "history_available": len(history) > 0,
        "pipeline_history_available": len(pipeline_history) > 0,
        "latest_regressions": latest_regressions,
        "regressed": regressed,
        "latest_pipeline_regressions": latest_pipeline_regressions,
        "pipeline_regressed": len(latest_pipeline_regressions) > 0,
        "summary": summary,
        "history_tail": {
            "input": str(ops_history_input),
            "history_count": len(history),
            "returned": len(rows),
            "rows": rows,
        },
        "pipeline_history_tail": {
            "input": str(pipeline_history_input),
            "history_count": len(pipeline_history),
            "returned": len(pipeline_rows),
            "rows": pipeline_rows,
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit incident-ready ops snapshot JSON.")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    ap.add_argument("--season-history-input", default="artifacts/season_activation_history.jsonl")
    ap.add_argument("--season-history-limit", type=int, default=10)
    ap.add_argument("--season-history-max-age-hours", type=int, default=0)
    ap.add_argument("--season-max-age-hours", type=int, default=0)
    ap.add_argument("--season-cutover-history-input", default="artifacts/season_cutover_history.jsonl")
    ap.add_argument("--season-cutover-history-limit", type=int, default=10)
    ap.add_argument("--ops-history-input", default="artifacts/ops_operator_history.jsonl")
    ap.add_argument("--ops-history-limit", type=int, default=10)
    ap.add_argument("--pipeline-history-input", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--pipeline-history-limit", type=int, default=10)
    ap.add_argument("--strict", action="store_true", help="Exit non-zero when snapshot status is fail")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    payload = collect_incident_snapshot(
        stat_days=args.stat_days,
        stat_require_min=args.stat_require_min,
        roster_require_min=args.roster_require_min,
        roster_stale_hours=args.roster_stale_hours,
        season_history_input=args.season_history_input,
        season_history_limit=args.season_history_limit,
        season_history_max_age_hours=args.season_history_max_age_hours,
        season_max_age_hours=args.season_max_age_hours,
        season_cutover_history_input=args.season_cutover_history_input,
        season_cutover_history_limit=args.season_cutover_history_limit,
        ops_history_input=args.ops_history_input,
        ops_history_limit=args.ops_history_limit,
        pipeline_history_input=args.pipeline_history_input,
        pipeline_history_limit=args.pipeline_history_limit,
    )
    print(json.dumps(payload, indent=2))
    if args.strict and not payload.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

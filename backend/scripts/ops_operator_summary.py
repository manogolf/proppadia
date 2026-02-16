#!/usr/bin/env python3
"""Compact operator-facing summary across governance and readiness checks."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from backend.scripts import cron_governance_snapshot
from backend.scripts import mlb_pipeline_last
from backend.scripts import mlb_readiness_snapshot
from backend.scripts import season_activation_report


def collect_summary(
    *,
    stat_days: int,
    stat_require_min: int,
    roster_require_min: int,
    roster_stale_hours: int,
    season_history_input: str,
    season_history_limit: int,
    season_max_age_hours: int,
    pipeline_history_input: str,
    pipeline_history_limit: int,
) -> dict[str, Any]:
    governance = cron_governance_snapshot.build_snapshot()
    readiness = mlb_readiness_snapshot.collect_snapshot(
        stat_days=stat_days,
        stat_require_min=stat_require_min,
        roster_require_min=roster_require_min,
        roster_stale_hours=roster_stale_hours,
    )
    season = season_activation_report.build_report(
        history_input=Path(season_history_input),
        history_limit=season_history_limit,
        max_age_hours=season_max_age_hours,
    )
    pipeline_history = mlb_pipeline_last._load_history(Path(pipeline_history_input))
    pipeline_tail = pipeline_history[-max(1, int(pipeline_history_limit)) :]
    pipeline_latest = pipeline_tail[-1] if pipeline_tail else {}
    pipeline_prev = pipeline_tail[-2] if len(pipeline_tail) >= 2 else None
    pipeline_regressions = mlb_pipeline_last._regressions(pipeline_prev, pipeline_latest) if pipeline_tail else []
    pipeline = {
        "history_available": bool(pipeline_history),
        "history_count": len(pipeline_history),
        "latest": {
            "captured_at": pipeline_latest.get("captured_at"),
            "status": pipeline_latest.get("status"),
            "ok": pipeline_latest.get("ok"),
            "failures": pipeline_latest.get("failures") or [],
            "regressions": pipeline_regressions,
        },
    }
    pipeline_ok = bool(pipeline_latest.get("ok")) if pipeline_history else True
    overall_ok = (
        bool(governance.get("ok"))
        and bool(readiness.get("ok"))
        and bool(season.get("ok"))
        and pipeline_ok
    )
    return {
        "ok": overall_ok,
        "status": "pass" if overall_ok else "fail",
        "governance": governance,
        "mlb_readiness": readiness,
        "season_activation_report": season,
        "mlb_pipeline": pipeline,
    }


def _print_text(summary: dict[str, Any]) -> None:
    governance = summary.get("governance") or {}
    readiness = summary.get("mlb_readiness") or {}
    season = summary.get("season_activation_report") or {}
    season_baseline_latest = (season.get("baseline_latest") or {}).get("latest") or {}
    mlb_baseline = season_baseline_latest.get("mlb") or {}
    nhl_baseline = season_baseline_latest.get("nhl") or {}
    pipeline = summary.get("mlb_pipeline") or {}
    pipeline_latest = pipeline.get("latest") or {}
    stat = ((readiness.get("checks") or {}).get("stat_derived")) or {}
    roster = ((readiness.get("checks") or {}).get("roster")) or {}
    blockers = (((season.get("season_activation") or {}).get("blockers")) or [])
    latest_game_date = stat.get("latest_game_date") or "-"
    print(f"OPS STATUS: {summary.get('status')}")
    print(
        "governance: "
        f"{governance.get('status')} "
        f"(core={governance.get('governance_ok')}, season={governance.get('season_activation_ok')})"
    )
    print(
        "mlb_readiness: "
        f"{readiness.get('status')} "
        f"(stat_count={stat.get('count', 0)} latest_game={latest_game_date})"
    )
    print(
        "mlb_roster: "
        f"{roster.get('status')} "
        f"(players={roster.get('total_players', 0)} stale={roster.get('stale', False)})"
    )
    print(
        "season_activation: "
        f"{season.get('status')} "
        f"(blockers={len(blockers)}"
        + (f" top={blockers[0]}" if blockers else "")
        + ")"
    )
    print(
        "season_baseline: "
        f"mlb_age_h={mlb_baseline.get('age_hours')} nhl_age_h={nhl_baseline.get('age_hours')}"
    )
    print(
        "mlb_pipeline: "
        + (
            f"{pipeline_latest.get('status')} "
            f"(failures={len(pipeline_latest.get('failures') or [])}"
            f" history={pipeline.get('history_count', 0)})"
            if pipeline.get("history_available")
            else "unknown (no history)"
        )
    )


def compact_summary(summary: dict[str, Any]) -> dict[str, Any]:
    governance = summary.get("governance") or {}
    readiness = summary.get("mlb_readiness") or {}
    season = summary.get("season_activation_report") or {}
    season_baseline_latest = (season.get("baseline_latest") or {}).get("latest") or {}
    mlb_baseline = season_baseline_latest.get("mlb") or {}
    nhl_baseline = season_baseline_latest.get("nhl") or {}
    pipeline = summary.get("mlb_pipeline") or {}
    pipeline_latest = pipeline.get("latest") or {}
    stat = ((readiness.get("checks") or {}).get("stat_derived")) or {}
    roster = ((readiness.get("checks") or {}).get("roster")) or {}
    blockers = (((season.get("season_activation") or {}).get("blockers")) or [])
    captured_at = readiness.get("captured_at") or datetime.now(timezone.utc).isoformat()
    return {
        "captured_at": captured_at,
        "ok": bool(summary.get("ok")),
        "status": summary.get("status"),
        "governance": {
            "ok": bool(governance.get("ok")),
            "status": governance.get("status"),
            "core_ok": bool(governance.get("governance_ok")),
            "season_ok": bool(governance.get("season_activation_ok")),
        },
        "mlb_readiness": {
            "ok": bool(readiness.get("ok")),
            "status": readiness.get("status"),
            "stat_count": int(stat.get("count") or 0),
            "latest_game_date": stat.get("latest_game_date"),
            "roster_players": int(roster.get("total_players") or 0),
            "roster_stale": bool(roster.get("stale")),
        },
        "season_activation": {
            "ok": bool(season.get("ok")),
            "status": season.get("status"),
            "blocker_count": len(blockers),
            "top_blocker": blockers[0] if blockers else None,
            "mlb_baseline_age_hours": mlb_baseline.get("age_hours"),
            "nhl_baseline_age_hours": nhl_baseline.get("age_hours"),
        },
        "mlb_pipeline": {
            "history_available": bool(pipeline.get("history_available")),
            "history_count": int(pipeline.get("history_count") or 0),
            "latest_ok": bool(pipeline_latest.get("ok"))
            if pipeline.get("history_available")
            else None,
            "latest_status": pipeline_latest.get("status") if pipeline.get("history_available") else None,
            "latest_failure_count": len(pipeline_latest.get("failures") or []),
            "latest_regressions": pipeline_latest.get("regressions") or [],
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit compact operator summary.")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    ap.add_argument("--season-history-input", default="artifacts/season_activation_history.jsonl")
    ap.add_argument("--season-history-limit", type=int, default=10)
    ap.add_argument("--season-max-age-hours", type=int, default=0)
    ap.add_argument("--pipeline-history-input", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--pipeline-history-limit", type=int, default=10)
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    ap.add_argument("--compact", action="store_true", help="Emit compact JSON shape")
    ap.add_argument("--strict", action="store_true", help="Exit non-zero when overall status is fail")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    summary = collect_summary(
        stat_days=args.stat_days,
        stat_require_min=args.stat_require_min,
        roster_require_min=args.roster_require_min,
        roster_stale_hours=args.roster_stale_hours,
        season_history_input=args.season_history_input,
        season_history_limit=args.season_history_limit,
        season_max_age_hours=args.season_max_age_hours,
        pipeline_history_input=args.pipeline_history_input,
        pipeline_history_limit=args.pipeline_history_limit,
    )
    if args.compact:
        print(json.dumps(compact_summary(summary), indent=2))
    elif args.json:
        print(json.dumps(summary, indent=2))
    else:
        _print_text(summary)
    if args.strict and not summary.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

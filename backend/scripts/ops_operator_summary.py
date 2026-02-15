#!/usr/bin/env python3
"""Compact operator-facing summary across governance and readiness checks."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from backend.scripts import cron_governance_snapshot
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
    overall_ok = bool(governance.get("ok")) and bool(readiness.get("ok")) and bool(season.get("ok"))
    return {
        "ok": overall_ok,
        "status": "pass" if overall_ok else "fail",
        "governance": governance,
        "mlb_readiness": readiness,
        "season_activation_report": season,
    }


def _print_text(summary: dict[str, Any]) -> None:
    governance = summary.get("governance") or {}
    readiness = summary.get("mlb_readiness") or {}
    season = summary.get("season_activation_report") or {}
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


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit compact operator summary.")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    ap.add_argument("--season-history-input", default="artifacts/season_activation_history.jsonl")
    ap.add_argument("--season-history-limit", type=int, default=10)
    ap.add_argument("--season-max-age-hours", type=int, default=0)
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of text")
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
    )
    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        _print_text(summary)
    if args.strict and not summary.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


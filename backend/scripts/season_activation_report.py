#!/usr/bin/env python3
"""Emit one combined JSON report for season activation readiness and history."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Sequence

from backend.scripts import check_season_baseline_artifacts
from backend.scripts import phase_status_snapshot
from backend.scripts import season_activation_last
from backend.scripts import season_activation_status


def _history_tail(path: Path, limit: int) -> Dict[str, Any]:
    history = season_activation_last._load_history(path)
    tail = history[-max(1, int(limit)) :]
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        rows.append(
            {
                "status": item.get("status"),
                "ok": item.get("ok"),
                "phase6_count": len(item.get("phase6_tracker") or []),
                "has_mlb_baseline": (((item.get("baseline_artifacts") or {}).get("has_mlb"))),
                "has_nhl_baseline": (((item.get("baseline_artifacts") or {}).get("has_nhl"))),
                "blockers": (((item.get("readiness") or {}).get("blockers")) or []),
                "new_blockers": season_activation_last._new_blockers(prev, item) if prev else [],
            }
        )
    return {
        "input": str(path),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }


def build_report(history_input: Path, history_limit: int, max_age_hours: int) -> Dict[str, Any]:
    phase = phase_status_snapshot.build_snapshot()
    activation = season_activation_status.build_status()
    baseline = check_season_baseline_artifacts.build_payload(max_age_hours=max_age_hours)
    history = _history_tail(history_input, history_limit)
    ok = bool(phase.get("ok")) and bool(activation.get("ok")) and bool(baseline.get("ok"))
    return {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "phase_status": phase,
        "season_activation": activation,
        "baseline_check": baseline,
        "season_activation_history": history,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Combined season activation report.")
    ap.add_argument("--history-input", default="artifacts/season_activation_history.jsonl")
    ap.add_argument("--history-limit", type=int, default=10)
    ap.add_argument("--max-age-hours", type=int, default=0)
    ap.add_argument("--strict", action="store_true", help="Exit non-zero when report status is fail.")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])
    payload = build_report(
        history_input=Path(args.history_input),
        history_limit=args.history_limit,
        max_age_hours=args.max_age_hours,
    )
    print(json.dumps(payload, indent=2))
    if args.strict and not payload["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

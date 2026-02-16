#!/usr/bin/env python3
"""Append compact ops operator summary snapshots to local JSONL history."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from backend.scripts.ops_operator_summary import collect_summary, compact_summary


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append compact ops summary to JSONL history.")
    ap.add_argument("--output", default="artifacts/ops_operator_history.jsonl")
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
    ap.add_argument("--pipeline-history-input", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--pipeline-history-limit", type=int, default=10)
    args = ap.parse_args(list(argv) if argv is not None else [])

    summary = collect_summary(
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
        pipeline_history_input=args.pipeline_history_input,
        pipeline_history_limit=args.pipeline_history_limit,
    )
    payload = compact_summary(summary)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")

    print(
        json.dumps(
            {
                "captured_at": payload.get("captured_at"),
                "status": payload.get("status"),
                "ok": payload.get("ok"),
                "output": str(out_path),
                "governance_ok": (((payload.get("governance") or {}).get("ok"))),
                "season_ok": (((payload.get("season_activation") or {}).get("ok"))),
                "pipeline_history_available": (((payload.get("mlb_pipeline") or {}).get("history_available"))),
                "pipeline_latest_ok": (((payload.get("mlb_pipeline") or {}).get("latest_ok"))),
                "pipeline_latest_failure_count": (
                    ((payload.get("mlb_pipeline") or {}).get("latest_failure_count"))
                ),
            },
            indent=2,
        )
    )
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

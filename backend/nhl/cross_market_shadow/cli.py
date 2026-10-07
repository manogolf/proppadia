#!/usr/bin/env python3
"""Command line interface for the NHL V2 cross-market prospective shadow."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .core import daily_status, fetch_markets, grade_capture, run_capture
from backend.nhl.odds_regions import NHL_ODDS_REGIONS_CSV


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    fetch = commands.add_parser("fetch-markets")
    fetch.add_argument("--output", type=Path, required=True)
    fetch.add_argument("--regions", default=NHL_ODDS_REGIONS_CSV)
    run = commands.add_parser("run")
    run.add_argument("--schedule-csv", type=Path, required=True)
    run.add_argument("--history-csv", type=Path, required=True)
    run.add_argument("--odds-json", type=Path)
    run.add_argument("--sog-csv", type=Path)
    run.add_argument("--points-csv", type=Path)
    run.add_argument("--saves-csv", type=Path)
    run.add_argument("--output-root", type=Path, default=Path("artifacts/operational/nhl/cross_market_shadow"))
    run.add_argument("--slate-date", required=True)
    run.add_argument("--run-timestamp-utc", required=True)
    run.add_argument("--run-type", choices=["MIDDAY", "FINAL_PREGAME"], required=True)
    run.add_argument("--preseason-canary", action="store_true")
    grade = commands.add_parser("grade")
    grade.add_argument("--run-dir", type=Path, required=True)
    grade.add_argument("--outcomes-csv", type=Path, required=True)
    grade.add_argument("--grade-root", type=Path, default=Path("artifacts/operational/nhl/cross_market_shadow/grades"))
    grade.add_argument("--grading-timestamp-utc", required=True)
    status = commands.add_parser("status")
    status.add_argument("--slate-date", required=True)
    status.add_argument("--root", type=Path, default=Path("artifacts/operational/nhl/cross_market_shadow"))
    status.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "fetch-markets":
        key = os.environ.get("ODDS_API_KEY", "").strip()
        result = fetch_markets(key, args.output, tuple(x.strip() for x in args.regions.split(",") if x.strip()))
        print(result)
    elif args.command == "run":
        print(run_capture(
            args.schedule_csv, args.history_csv, args.odds_json, args.output_root,
            args.slate_date, args.run_timestamp_utc, args.run_type,
            args.sog_csv, args.points_csv, args.saves_csv, args.preseason_canary,
        ))
    elif args.command == "grade":
        print(grade_capture(args.run_dir, args.outcomes_csv, args.grade_root, args.grading_timestamp_utc))
    else:
        result = daily_status(args.root, args.slate_date)
        text = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            if args.output.exists():
                raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text)
        print(text, end="")


if __name__ == "__main__":
    main()

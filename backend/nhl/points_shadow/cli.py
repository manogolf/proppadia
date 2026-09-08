#!/usr/bin/env python3
"""Manual Points shadow CLI; no upload or execution action exists."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .core import grade_run, run_shadow, verify_fixed_input_parity, verify_frozen_identity


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("verify-frozen")
    run = commands.add_parser("run")
    run.add_argument("--game-spine-csv", type=Path, required=True)
    run.add_argument("--game-spine-manifest", type=Path, required=True)
    run.add_argument("--player-inputs-csv", type=Path, required=True)
    run.add_argument("--player-inputs-manifest", type=Path, required=True)
    run.add_argument("--quote-run-dir", type=Path, required=True)
    run.add_argument("--output-root", type=Path, default=Path("backend/nhl/exports/points_shadow_runs"))
    run.add_argument("--slate-date", required=True)
    run.add_argument("--run-timestamp-utc", required=True)
    run.add_argument("--run-type", choices=["MIDDAY", "FINAL_PREGAME"], required=True)
    run.add_argument("--effective-policy-json", type=Path)
    grade = commands.add_parser("grade")
    grade.add_argument("--run-dir", type=Path, required=True)
    grade.add_argument("--outcomes-csv", type=Path, required=True)
    grade.add_argument("--grade-root", type=Path, default=Path("backend/nhl/exports/points_shadow_grades"))
    grade.add_argument("--grading-timestamp-utc", required=True)
    grade.add_argument("--correction-of-grade-id")
    args = parser.parse_args()
    if args.command == "verify-frozen":
        print(json.dumps({"identity": verify_frozen_identity(), "parity": verify_fixed_input_parity()}, indent=2, sort_keys=True))
    elif args.command == "run":
        print(run_shadow(game_spine_csv=args.game_spine_csv, game_spine_manifest=args.game_spine_manifest, player_inputs_csv=args.player_inputs_csv, player_inputs_manifest=args.player_inputs_manifest, quote_run_dir=args.quote_run_dir, output_root=args.output_root, slate_date=args.slate_date, run_timestamp_utc=args.run_timestamp_utc, run_type=args.run_type, effective_policy_json=args.effective_policy_json))
    else:
        print(grade_run(args.run_dir, args.outcomes_csv, args.grade_root, args.grading_timestamp_utc, args.correction_of_grade_id))


if __name__ == "__main__":
    main()

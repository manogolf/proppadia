#!/usr/bin/env python3
"""Check required NHL workflow compatibility scripts are present and syntactically valid."""

from __future__ import annotations

import argparse
import json
import py_compile
import sys
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[3]

REQUIRED = [
    Path("backend/nhl/scripts/backfill_game_manpower_segments.py"),
    Path("backend/nhl/scripts/build_points_with_market.py"),
    Path("backend/nhl/scripts/build_saves_with_market.py"),
    Path("backend/nhl/scripts/build_sog_with_market.py"),
    Path("backend/nhl/scripts/calibrate_sog_segmented_recency.py"),
    Path("backend/nhl/scripts/evaluate_sog_predictions.py"),
    Path("backend/nhl/scripts/fill_pp_toi_minutes_for_date.py"),
    Path("backend/nhl/scripts/import_roster_today.py"),
    Path("backend/nhl/scripts/import_schedule_today.py"),
    Path("backend/nhl/scripts/ingest_shiftcharts_for_date.py"),
    Path("backend/nhl/scripts/load_nhl_predictions_generic.py"),
    Path("backend/nhl/scripts/load_sog_predictions_denali.py"),
    Path("backend/nhl/scripts/refresh_all_team_rosters.py"),
    Path("backend/nhl/scripts/refresh_players_and_roster_today.py"),
    Path("backend/nhl/scripts/score_nhl_props.py"),
    Path("backend/nhl/scripts/score_points_phoenix.py"),
    Path("backend/nhl/scripts/score_sog_denali_pairings_ordinal_lgbm.py"),
    Path("backend/nhl/scripts/seed_goalie_logs_for_date.py"),
    Path("backend/nhl/scripts/seed_skater_logs_for_date.py"),
    Path("backend/nhl/scripts/sog_integrity_report.py"),
]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check required NHL workflow compatibility scripts."
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-file rows; print summary only.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON summary instead of text output.",
    )
    args = parser.parse_args(argv)

    missing: list[Path] = []
    compile_fail: list[tuple[Path, str]] = []
    checked = 0

    if not args.quiet and not args.json:
        print("NHL workflow compatibility check:")
    for rel in REQUIRED:
        checked += 1
        path = ROOT / rel
        if not path.exists():
            missing.append(rel)
            if not args.quiet and not args.json:
                print(f"- MISSING {rel}")
            continue
        if not args.quiet and not args.json:
            print(f"- OK {rel}")
        try:
            py_compile.compile(str(path), doraise=True)
        except Exception as exc:  # pragma: no cover
            compile_fail.append((rel, str(exc)))

    result = {
        "required_files": len(REQUIRED),
        "files_checked": checked,
        "missing_files": len(missing),
        "compile_failures": len(compile_fail),
        "status": "pass",
    }

    if missing:
        result["status"] = "fail"
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print("\nSummary:")
            print(f"- required files: {len(REQUIRED)}")
            print(f"- files checked: {checked}")
            print(f"- missing files: {len(missing)}")
            print(f"- compile failures: {len(compile_fail)}")
            print(f"FAIL missing required files: {len(missing)}")
        return 1

    if compile_fail:
        result["status"] = "fail"
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print("\nSummary:")
            print(f"- required files: {len(REQUIRED)}")
            print(f"- files checked: {checked}")
            print(f"- missing files: {len(missing)}")
            print(f"- compile failures: {len(compile_fail)}")
            print("FAIL py_compile errors:")
            for rel, err in compile_fail:
                print(f"- {rel}: {err}")
        return 1

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print("\nSummary:")
        print(f"- required files: {len(REQUIRED)}")
        print(f"- files checked: {checked}")
        print(f"- missing files: {len(missing)}")
        print(f"- compile failures: {len(compile_fail)}")
        print("PASS nhl workflow compatibility check")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

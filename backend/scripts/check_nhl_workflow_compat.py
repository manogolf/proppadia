#!/usr/bin/env python3
"""Check required NHL workflow compatibility scripts are present and syntactically valid."""

from __future__ import annotations

import argparse
import py_compile
import sys
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[2]

REQUIRED = [
    Path("backend/nhl/scripts/attach_names.py"),
    Path("backend/nhl/scripts/import_skater_logs.py"),
    Path("backend/nhl/scripts/import_skater_logs_for_date.py"),
    Path("backend/nhl/scripts/score_goalie_saves.py"),
    Path("backend/nhl/scripts/score_sog_phoenix.py"),
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
    args = parser.parse_args(argv)

    missing: list[Path] = []
    compile_fail: list[tuple[Path, str]] = []
    checked = 0

    if not args.quiet:
        print("NHL workflow compatibility check:")
    for rel in REQUIRED:
        checked += 1
        path = ROOT / rel
        if not path.exists():
            missing.append(rel)
            if not args.quiet:
                print(f"- MISSING {rel}")
            continue
        if not args.quiet:
            print(f"- OK {rel}")
        try:
            py_compile.compile(str(path), doraise=True)
        except Exception as exc:  # pragma: no cover
            compile_fail.append((rel, str(exc)))

    print("\nSummary:")
    print(f"- required files: {len(REQUIRED)}")
    print(f"- files checked: {checked}")
    print(f"- missing files: {len(missing)}")
    print(f"- compile failures: {len(compile_fail)}")

    if missing:
        print(f"FAIL missing required files: {len(missing)}")
        return 1

    if compile_fail:
        print("FAIL py_compile errors:")
        for rel, err in compile_fail:
            print(f"- {rel}: {err}")
        return 1

    print("PASS nhl workflow compatibility check")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

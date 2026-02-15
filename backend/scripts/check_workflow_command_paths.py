#!/usr/bin/env python3
"""Audit Python command references inside GitHub workflow files."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS_DIR = ROOT / ".github" / "workflows"

PATH_RE = re.compile(r"\b((?:backend|scripts)/[A-Za-z0-9_./-]+\.py)\b")
MODULE_RE = re.compile(
    r"\bpython(?:3)?\s+-m\s+((?:backend|scripts)\.[A-Za-z_][A-Za-z0-9_\.]*)\b"
)


def module_to_candidate_paths(module_name: str) -> list[Path]:
    parts = module_name.split(".")
    base = ROOT.joinpath(*parts)
    candidates = [base.with_suffix(".py"), base / "__init__.py"]
    return candidates


def collect_missing_references(text: str) -> list[str]:
    missing: list[str] = []

    for match in PATH_RE.finditer(text):
        rel = match.group(1)
        if not (ROOT / rel).exists():
            missing.append(f"path:{rel}")

    for match in MODULE_RE.finditer(text):
        module_name = match.group(1)
        candidates = module_to_candidate_paths(module_name)
        if not any(candidate.exists() for candidate in candidates):
            missing.append(f"module:{module_name}")

    # Keep output stable and deduplicated.
    return sorted(set(missing))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit workflow python command/module references."
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any missing command references are detected.",
    )
    args = parser.parse_args()

    if not WORKFLOWS_DIR.exists():
        print(f"FAIL workflows directory not found: {WORKFLOWS_DIR}")
        return 2

    workflow_files = sorted(WORKFLOWS_DIR.glob("*.yml"))
    print("Workflow command path audit:")

    missing_total = 0
    for workflow_file in workflow_files:
        text = workflow_file.read_text(encoding="utf-8")
        missing = collect_missing_references(text)
        if missing:
            missing_total += len(missing)
            print(f"- MISSING {workflow_file.name}")
            for item in missing:
                print(f"  - {item}")
        else:
            print(f"- OK {workflow_file.name}")

    if missing_total == 0:
        print("PASS workflow command path audit")
        return 0

    print(f"Found {missing_total} missing workflow command reference(s).")
    if args.strict:
        print("FAIL workflow command path audit (strict)")
        return 1
    print("WARN workflow command path audit")
    return 0


if __name__ == "__main__":
    sys.exit(main())

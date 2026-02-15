#!/usr/bin/env python3
"""Inventory scheduled GitHub workflows and flag unexpected schedulers.

Default mode is report-only (never fails). Use --strict to fail when
scheduled workflow files differ from the expected allowlist.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Sequence, Tuple


WORKFLOWS_DIR = Path(".github/workflows")
EXPECTED_SCHEDULED = {
    "mlb-refresh-player-ids.yml",
    "nhl-daily-refresh.yml",
}


def _find_workflows() -> List[Path]:
    if not WORKFLOWS_DIR.exists():
        return []
    return sorted([p for p in WORKFLOWS_DIR.iterdir() if p.suffix in {".yml", ".yaml"}])


def _extract_schedule_info(text: str) -> Tuple[bool, List[str]]:
    has_schedule = bool(re.search(r"(?m)^\s*schedule\s*:", text))
    crons: List[str] = []
    for line in text.splitlines():
        if "cron" not in line or ":" not in line:
            continue
        m = re.search(r"\bcron\s*:\s*(.+)$", line)
        if not m:
            continue
        value = m.group(1).split("#", 1)[0].strip().strip("[]{}").strip()
        value = value.strip("'").strip('"')
        if value:
            crons.append(value)
    return has_schedule, crons


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Inventory scheduled workflow files.")
    ap.add_argument("--strict", action="store_true", help="Fail when scheduled set differs from expected.")
    args = ap.parse_args(argv)

    files = _find_workflows()
    if not files:
        print("No workflow files found under .github/workflows")
        return 0

    scheduled_files: List[str] = []
    print("Workflow schedule inventory:")
    for wf in files:
        text = wf.read_text(encoding="utf-8", errors="replace")
        has_schedule, crons = _extract_schedule_info(text)
        if has_schedule:
            scheduled_files.append(wf.name)
            cron_txt = ", ".join(crons) if crons else "(schedule block, no cron lines parsed)"
            print(f"- SCHEDULED {wf.name}: {cron_txt}")
        else:
            print(f"- manual-only {wf.name}")

    scheduled_set = set(scheduled_files)
    unexpected = sorted(scheduled_set - EXPECTED_SCHEDULED)
    missing = sorted(EXPECTED_SCHEDULED - scheduled_set)

    print("\nSummary:")
    print(f"- scheduled files in repo: {len(scheduled_files)}")
    print(f"- expected scheduled files: {len(EXPECTED_SCHEDULED)}")
    if unexpected:
        print(f"- unexpected scheduled: {', '.join(unexpected)}")
    else:
        print("- unexpected scheduled: none")
    if missing:
        print(f"- missing expected schedule: {', '.join(missing)}")
    else:
        print("- missing expected schedule: none")

    if args.strict and (unexpected or missing):
        print("FAIL workflow schedule inventory (strict mode)")
        return 1

    print("PASS workflow schedule inventory")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

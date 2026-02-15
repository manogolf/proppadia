#!/usr/bin/env python3
"""Emit machine-readable Phase Status Tracker summary from docs/Execution Plan.md."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[2]
PLAN_PATH = ROOT / "docs" / "Execution Plan.md"

_LINE_RE = re.compile(r"^- (Phase [0-9]+\.[0-9]+)\s+(.+?):\s+(.+)$")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _extract_tracker_lines(plan_text: str) -> List[str]:
    in_tracker = False
    lines: List[str] = []
    for raw in plan_text.splitlines():
        line = raw.rstrip()
        if line.startswith("## Phase Status Tracker"):
            in_tracker = True
            continue
        if in_tracker and line.startswith("## "):
            break
        if in_tracker and line.startswith("- "):
            lines.append(line)
    return lines


def _normalize_status(value: str) -> str:
    s = value.strip().lower()
    if s in {"complete", "completed", "done"}:
        return "complete"
    if s in {"in progress", "in-progress", "active"}:
        return "in_progress"
    if s in {"pending", "not started", "todo"}:
        return "pending"
    return s.replace(" ", "_")


def build_snapshot(plan_path: Path = PLAN_PATH) -> Dict[str, object]:
    plan_text = _read_text(plan_path)
    tracker_lines = _extract_tracker_lines(plan_text)
    phases: List[Dict[str, str]] = []
    for line in tracker_lines:
        m = _LINE_RE.match(line)
        if not m:
            continue
        phase_id, label, raw_status = m.groups()
        phases.append(
            {
                "phase": phase_id,
                "label": label.strip(),
                "status": _normalize_status(raw_status),
                "raw_status": raw_status.strip(),
            }
        )

    counts: Dict[str, int] = {}
    for item in phases:
        status = item["status"]
        counts[status] = counts.get(status, 0) + 1

    try:
        source_label = str(plan_path.relative_to(ROOT)) if plan_path.is_absolute() else str(plan_path)
    except ValueError:
        source_label = str(plan_path)
    payload: Dict[str, object] = {
        "ok": True,
        "status": "pass",
        "source": source_label,
        "total": len(phases),
        "counts": counts,
        "phases": phases,
    }
    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Machine-readable phase status tracker snapshot.")
    _ = ap.parse_args(argv if argv is not None else sys.argv[1:])
    print(json.dumps(build_snapshot(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

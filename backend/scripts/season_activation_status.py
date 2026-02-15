#!/usr/bin/env python3
"""Report current season-activation readiness state from local artifacts/docs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[2]
PLAN_PATH = ROOT / "docs" / "Execution Plan.md"
BASELINE_DIR = ROOT / "artifacts" / "season_baselines"


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _phase6_status_lines(plan_text: str) -> List[str]:
    out: List[str] = []
    for line in plan_text.splitlines():
        s = line.strip()
        if s.startswith("- Phase 6."):
            out.append(s[2:].strip())
    return out


def _list_baselines(root: Path) -> Dict[str, List[str]]:
    if not root.exists():
        return {"mlb": [], "nhl": []}
    mlb = sorted([p.name for p in root.glob("mlb_quality_*.json")])
    nhl = sorted([p.name for p in root.glob("nhl_quality_*.json")])
    return {"mlb": mlb, "nhl": nhl}


def _next_steps(phase6: List[str], baselines: Dict[str, List[str]]) -> List[str]:
    has_mlb = len(baselines.get("mlb") or []) > 0
    has_nhl = len(baselines.get("nhl") or []) > 0
    lines = " ".join(phase6).lower()
    needs_dry_run = "6.1 preseason dry run: complete" not in lines
    needs_cutover = "6.2 in-season cadence cutover: complete" not in lines
    steps: List[str] = []
    if needs_dry_run:
        steps.append("Run: make mlb-season-kickoff-check BASE_URL=<url> MLB_DATE=YYYY-MM-DD")
    if not (has_mlb and has_nhl):
        steps.append(
            "Run: make season-baseline-capture MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 "
            "NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD"
        )
    if needs_cutover:
        steps.append("Apply intended in-season schedule windows and keep cron-governance-check green.")
    if not steps:
        steps.append("Phase 6 appears complete from local status + baseline artifacts.")
    return steps


def _readiness_state(phase6: List[str], baselines: Dict[str, List[str]]) -> Dict[str, object]:
    lines = " ".join(phase6).lower()
    has_mlb = len(baselines.get("mlb") or []) > 0
    has_nhl = len(baselines.get("nhl") or []) > 0
    needs_dry_run = "6.1 preseason dry run: complete" not in lines
    needs_cutover = "6.2 in-season cadence cutover: complete" not in lines
    needs_baseline = not (has_mlb and has_nhl)
    blockers: List[str] = []
    if needs_dry_run:
        blockers.append("phase_6_1_incomplete")
    if needs_cutover:
        blockers.append("phase_6_2_incomplete")
    if needs_baseline:
        blockers.append("baseline_artifacts_missing")
    return {"ready": len(blockers) == 0, "blockers": blockers}


def build_status(plan_path: Path = PLAN_PATH, baseline_dir: Path = BASELINE_DIR) -> Dict[str, object]:
    plan_text = _read_text(plan_path)
    phase6 = _phase6_status_lines(plan_text)
    baselines = _list_baselines(baseline_dir)
    readiness = _readiness_state(phase6, baselines)
    try:
        baseline_dir_label = str(baseline_dir.relative_to(ROOT)) if baseline_dir.is_absolute() else str(baseline_dir)
    except ValueError:
        baseline_dir_label = str(baseline_dir)
    payload: Dict[str, object] = {
        "ok": bool(readiness["ready"]),
        "status": "pass" if readiness["ready"] else "fail",
        "phase6_tracker": phase6,
        "baseline_artifacts": {
            "dir": baseline_dir_label,
            "mlb_files": baselines.get("mlb") or [],
            "nhl_files": baselines.get("nhl") or [],
            "has_mlb": len(baselines.get("mlb") or []) > 0,
            "has_nhl": len(baselines.get("nhl") or []) > 0,
        },
        "readiness": readiness,
        "next_steps": _next_steps(phase6, baselines),
    }
    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report season activation readiness status.")
    ap.add_argument("--json", action="store_true", help="Print JSON (default true behavior).")
    ap.add_argument("--strict", action="store_true", help="Exit non-zero when readiness is not complete.")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    payload = build_status()
    print(json.dumps(payload, indent=2))
    if args.strict and not payload.get("ok", False):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

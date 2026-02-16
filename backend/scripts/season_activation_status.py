#!/usr/bin/env python3
"""Report current season-activation readiness state from local artifacts/docs."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[2]
PLAN_PATH = ROOT / "docs" / "Execution Plan.md"
BASELINE_DIR = ROOT / "artifacts" / "season_baselines"
SEASON_CUTOVER_HISTORY_PATH = ROOT / "artifacts" / "season_cutover_history.jsonl"
SEASON_ACTIVATION_HISTORY_PATH = ROOT / "artifacts" / "season_activation_history.jsonl"


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
    needs_baseline = not (has_mlb and has_nhl)
    lines = " ".join(phase6).lower()
    needs_dry_run = "6.1 preseason dry run: complete" not in lines
    needs_cutover = "6.2 in-season cadence cutover: complete" not in lines
    needs_baseline_lock = "6.3 baseline lock: complete" not in lines
    steps: List[str] = []
    if needs_dry_run or needs_cutover or needs_baseline_lock or needs_baseline:
        steps.append(
            "Run: make season-activation-check BASE_URL=<url> MLB_DATE=YYYY-MM-DD "
            "NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD"
        )
    if needs_dry_run:
        steps.append("Run: make mlb-season-kickoff-check BASE_URL=<url> MLB_DATE=YYYY-MM-DD")
    if not (has_mlb and has_nhl):
        steps.append(
            "Run: make season-baseline-capture MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 "
            "NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD"
        )
    if needs_cutover:
        steps.append("Apply intended in-season schedule windows and keep cron-governance-check green.")
    if has_mlb and has_nhl:
        steps.append("Review: make season-baseline-last")
    if needs_baseline_lock and has_mlb and has_nhl:
        steps.append("Update docs/Execution Plan.md phase tracker: mark Phase 6.3 complete after baseline review.")
    if not steps:
        steps.append("Phase 6 appears complete from local status + baseline artifacts.")
    return steps


def _has_cutover_history(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            for raw in f:
                if raw.strip():
                    return True
    except Exception:
        return False
    return False


def _activation_history_meta(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"input": str(path), "history_count": 0, "latest_captured_at": None, "latest_age_hours": None}
    rows: List[dict] = []
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                rows.append(item)
    except Exception:
        return {"input": str(path), "history_count": 0, "latest_captured_at": None, "latest_age_hours": None}
    latest = rows[-1] if rows else {}
    latest_captured_at = latest.get("captured_at")
    latest_age_hours = None
    if isinstance(latest_captured_at, str):
        try:
            dt = datetime.fromisoformat(latest_captured_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            latest_age_hours = round((datetime.now(timezone.utc) - dt).total_seconds() / 3600.0, 3)
        except Exception:
            latest_age_hours = None
    return {
        "input": str(path),
        "history_count": len(rows),
        "latest_captured_at": latest_captured_at,
        "latest_age_hours": latest_age_hours,
    }


def _readiness_state(
    phase6: List[str],
    baselines: Dict[str, List[str]],
    *,
    has_cutover_history: bool,
    activation_history: Dict[str, object],
    activation_history_max_age_hours: int,
) -> Dict[str, object]:
    lines = " ".join(phase6).lower()
    has_mlb = len(baselines.get("mlb") or []) > 0
    has_nhl = len(baselines.get("nhl") or []) > 0
    needs_dry_run = "6.1 preseason dry run: complete" not in lines
    needs_cutover = "6.2 in-season cadence cutover: complete" not in lines
    needs_baseline_lock = "6.3 baseline lock: complete" not in lines
    needs_baseline = not (has_mlb and has_nhl)
    blockers: List[str] = []
    if needs_dry_run:
        blockers.append("phase_6_1_incomplete")
    if needs_cutover:
        blockers.append("phase_6_2_incomplete")
    if not has_cutover_history:
        blockers.append("season_cutover_history_missing")
    if int(activation_history.get("history_count") or 0) == 0:
        blockers.append("season_activation_history_missing")
    latest_age = activation_history.get("latest_age_hours")
    if (
        int(activation_history_max_age_hours) > 0
        and isinstance(latest_age, (int, float))
        and float(latest_age) > float(activation_history_max_age_hours)
    ):
        blockers.append("season_activation_history_stale")
    if needs_baseline_lock:
        blockers.append("phase_6_3_incomplete")
    if needs_baseline:
        blockers.append("baseline_artifacts_missing")
    return {"ready": len(blockers) == 0, "blockers": blockers}


def build_status(
    plan_path: Path = PLAN_PATH,
    baseline_dir: Path = BASELINE_DIR,
    season_cutover_history_path: Path = SEASON_CUTOVER_HISTORY_PATH,
    season_activation_history_path: Path = SEASON_ACTIVATION_HISTORY_PATH,
    season_activation_history_max_age_hours: int = 0,
) -> Dict[str, object]:
    plan_text = _read_text(plan_path)
    phase6 = _phase6_status_lines(plan_text)
    baselines = _list_baselines(baseline_dir)
    activation_history = _activation_history_meta(season_activation_history_path)
    has_cutover_history = _has_cutover_history(season_cutover_history_path)
    readiness = _readiness_state(
        phase6,
        baselines,
        has_cutover_history=has_cutover_history,
        activation_history=activation_history,
        activation_history_max_age_hours=season_activation_history_max_age_hours,
    )
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
        "season_cutover": {
            "history_path": str(season_cutover_history_path),
            "has_history": has_cutover_history,
        },
        "season_activation_history": activation_history,
        "readiness": readiness,
        "next_steps": _next_steps(phase6, baselines),
    }
    if not has_cutover_history:
        payload["next_steps"].append("Run: make season-cutover-log")
    if int(activation_history.get("history_count") or 0) == 0:
        payload["next_steps"].append("Run: make season-activation-log")
    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report season activation readiness status.")
    ap.add_argument("--json", action="store_true", help="Print JSON (default true behavior).")
    ap.add_argument("--strict", action="store_true", help="Exit non-zero when readiness is not complete.")
    ap.add_argument(
        "--history-max-age-hours",
        type=int,
        default=0,
        help="When >0, fail readiness if latest season activation history snapshot is older than this many hours.",
    )
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    payload = build_status(season_activation_history_max_age_hours=args.history_max_age_hours)
    print(json.dumps(payload, indent=2))
    if args.strict and not payload.get("ok", False):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

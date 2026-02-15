#!/usr/bin/env python3
"""Validate season baseline artifact presence (and optional freshness)."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from backend.scripts.season_activation_status import BASELINE_DIR


def _latest_file(pattern_root: Path, pattern: str) -> Path | None:
    files = sorted(pattern_root.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _age_hours(path: Path, now: datetime) -> float:
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return max(0.0, (now - modified).total_seconds() / 3600.0)


def build_payload(root: Path = BASELINE_DIR, max_age_hours: int = 0) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    latest_mlb = _latest_file(root, "mlb_quality_*.json")
    latest_nhl = _latest_file(root, "nhl_quality_*.json")

    mlb_age = _age_hours(latest_mlb, now) if latest_mlb else None
    nhl_age = _age_hours(latest_nhl, now) if latest_nhl else None

    errors: list[str] = []
    if latest_mlb is None:
        errors.append("missing_mlb_baseline")
    if latest_nhl is None:
        errors.append("missing_nhl_baseline")
    if max_age_hours > 0:
        if mlb_age is not None and mlb_age > max_age_hours:
            errors.append("mlb_baseline_stale")
        if nhl_age is not None and nhl_age > max_age_hours:
            errors.append("nhl_baseline_stale")

    return {
        "ok": len(errors) == 0,
        "status": "pass" if len(errors) == 0 else "fail",
        "baseline_dir": str(root),
        "max_age_hours": max_age_hours,
        "latest": {
            "mlb": str(latest_mlb) if latest_mlb else None,
            "nhl": str(latest_nhl) if latest_nhl else None,
            "mlb_age_hours": round(mlb_age, 2) if mlb_age is not None else None,
            "nhl_age_hours": round(nhl_age, 2) if nhl_age is not None else None,
        },
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate season baseline artifacts.")
    ap.add_argument("--baseline-dir", default=str(BASELINE_DIR))
    ap.add_argument("--max-age-hours", type=int, default=0, help="0 disables freshness enforcement.")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    payload = build_payload(Path(args.baseline_dir), max_age_hours=args.max_age_hours)
    print(json.dumps(payload, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


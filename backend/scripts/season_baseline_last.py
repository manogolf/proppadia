#!/usr/bin/env python3
"""Show latest season baseline artifacts with compact quality summary."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.scripts.check_season_baseline_artifacts import _latest_file
from backend.scripts.season_activation_status import BASELINE_DIR


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _age_hours(path: Path, now: datetime) -> float:
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return max(0.0, (now - modified).total_seconds() / 3600.0)


def _artifact_summary(path: Path | None, now: datetime) -> dict[str, Any]:
    if path is None:
        return {
            "exists": False,
            "path": None,
            "modified_at": None,
            "age_hours": None,
            "status": None,
            "overall_total": None,
            "overall_accuracy_pct": None,
        }

    payload = _load_json(path)
    overall = payload.get("overall") or {}
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return {
        "exists": True,
        "path": str(path),
        "modified_at": modified.isoformat(),
        "age_hours": round(_age_hours(path, now), 2),
        "status": payload.get("status"),
        "overall_total": overall.get("total"),
        "overall_accuracy_pct": overall.get("accuracy_pct"),
    }


def build_payload(root: Path = BASELINE_DIR) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    mlb = _artifact_summary(_latest_file(root, "mlb_quality_*.json"), now)
    nhl = _artifact_summary(_latest_file(root, "nhl_quality_*.json"), now)
    ok = bool(mlb.get("exists")) and bool(nhl.get("exists"))
    return {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "captured_at": now.isoformat(),
        "baseline_dir": str(root),
        "latest": {
            "mlb": mlb,
            "nhl": nhl,
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Show latest season baseline artifacts.")
    ap.add_argument("--baseline-dir", default=str(BASELINE_DIR))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    payload = build_payload(Path(args.baseline_dir))
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"Season baseline latest: {payload['baseline_dir']}")
        for league in ("mlb", "nhl"):
            row = (payload.get("latest") or {}).get(league) or {}
            if not row.get("exists"):
                print(f"- {league.upper()}: missing")
                continue
            print(
                f"- {league.upper()}: status={row.get('status')} total={row.get('overall_total')} "
                f"acc={row.get('overall_accuracy_pct')} age_h={row.get('age_hours')} path={row.get('path')}"
            )
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

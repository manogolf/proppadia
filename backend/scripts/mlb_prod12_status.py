#!/usr/bin/env python3
"""Show compact current status for MLB prod12 daily + weekly lanes."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _latest_row(path: Path) -> dict[str, Any] | None:
    rows = _load_jsonl(path)
    return rows[-1] if rows else None


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Current MLB prod12 status summary.")
    ap.add_argument("--pipeline-history", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--phase2-history", default="artifacts/mlb_prod12_phase2_history.jsonl")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    pipeline_path = Path(args.pipeline_history)
    phase2_path = Path(args.phase2_history)
    pipeline = _latest_row(pipeline_path)
    phase2 = _latest_row(phase2_path)

    payload = {
        "status": "pass",
        "ok": True,
        "inputs": {
            "pipeline_history": str(pipeline_path),
            "phase2_history": str(phase2_path),
        },
        "daily": {
            "captured_at": (pipeline or {}).get("captured_at"),
            "status": (pipeline or {}).get("status"),
            "ok": (pipeline or {}).get("ok"),
            "failures": (pipeline or {}).get("failures") or [],
        },
        "weekly": {
            "captured_at": (phase2 or {}).get("captured_at"),
            "status": (phase2 or {}).get("status"),
            "ok": (phase2 or {}).get("ok"),
            "failures": (phase2 or {}).get("failures") or [],
        },
        "warnings": [],
    }

    if pipeline is None:
        payload["warnings"].append("missing_daily_pipeline_history")
    if phase2 is None:
        payload["warnings"].append("missing_weekly_phase2_history")

    if pipeline is None or not bool((pipeline or {}).get("ok")):
        payload["ok"] = False
    if phase2 is None or not bool((phase2 or {}).get("ok")):
        payload["ok"] = False
    payload["status"] = "pass" if payload["ok"] else "fail"

    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

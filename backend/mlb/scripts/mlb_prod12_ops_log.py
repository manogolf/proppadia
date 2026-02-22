#!/usr/bin/env python3
"""Append MLB prod12 operator snapshot (status + health + incident) to JSONL history."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from backend.scripts import mlb_prod12_health_report
from backend.scripts import mlb_prod12_incident
from backend.scripts import mlb_prod12_status
from backend.shared.scripts.json_check_runner import run_json_check


def collect_snapshot(
    *,
    pipeline_history: str,
    phase2_history: str,
    daily_max_age_hours: float,
    weekly_max_age_hours: float,
    daily_window: int,
    weekly_window: int,
) -> dict[str, Any]:
    status_args = [
        "--pipeline-history",
        str(pipeline_history),
        "--phase2-history",
        str(phase2_history),
        "--daily-max-age-hours",
        str(float(daily_max_age_hours)),
        "--weekly-max-age-hours",
        str(float(weekly_max_age_hours)),
        "--strict",
    ]
    health_args = [
        "--pipeline-history",
        str(pipeline_history),
        "--phase2-history",
        str(phase2_history),
        "--daily-window",
        str(int(daily_window)),
        "--weekly-window",
        str(int(weekly_window)),
    ]
    incident_args = [
        "--pipeline-history",
        str(pipeline_history),
        "--phase2-history",
        str(phase2_history),
    ]

    status_rc, status_payload = run_json_check(mlb_prod12_status.main, status_args)
    health_rc, health_payload = run_json_check(mlb_prod12_health_report.main, health_args)
    incident_rc, incident_payload = run_json_check(mlb_prod12_incident.main, incident_args)

    checks = {
        "status": {
            "ok": bool(status_payload.get("ok")),
            "status": status_payload.get("status"),
            "exit_code": int(status_rc),
            "payload": status_payload,
        },
        "health": {
            "ok": bool(health_payload.get("ok")),
            "status": health_payload.get("status"),
            "exit_code": int(health_rc),
            "payload": health_payload,
        },
        "incident": {
            "ok": bool(incident_payload.get("ok")),
            "status": incident_payload.get("status"),
            "exit_code": int(incident_rc),
            "payload": incident_payload,
        },
    }
    failures = [name for name, check in checks.items() if not bool(check.get("ok"))]
    ok = len(failures) == 0
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok,
        "status": "pass" if ok else "fail",
        "failures": failures,
        "checks": checks,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append MLB prod12 operator snapshot to JSONL history.")
    ap.add_argument("--output", default="artifacts/mlb_prod12_ops_history.jsonl")
    ap.add_argument("--pipeline-history", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--phase2-history", default="artifacts/mlb_prod12_phase2_history.jsonl")
    ap.add_argument("--daily-max-age-hours", type=float, default=30.0)
    ap.add_argument("--weekly-max-age-hours", type=float, default=240.0)
    ap.add_argument("--daily-window", type=int, default=14)
    ap.add_argument("--weekly-window", type=int, default=8)
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    payload = collect_snapshot(
        pipeline_history=str(args.pipeline_history),
        phase2_history=str(args.phase2_history),
        daily_max_age_hours=float(args.daily_max_age_hours),
        weekly_max_age_hours=float(args.weekly_max_age_hours),
        daily_window=int(args.daily_window),
        weekly_window=int(args.weekly_window),
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, default=str))
        fh.write("\n")

    summary = {
        "captured_at": payload.get("captured_at"),
        "status": payload.get("status"),
        "ok": payload.get("ok"),
        "output": str(out_path),
        "failures": payload.get("failures") or [],
    }
    print(json.dumps(summary, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

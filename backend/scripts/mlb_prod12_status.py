#!/usr/bin/env python3
"""Show compact current status for MLB prod12 daily + weekly lanes."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
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


def _parse_iso8601_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _age_hours(captured_at: Any, now: datetime) -> float | None:
    dt = _parse_iso8601_utc(captured_at)
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds() / 3600.0)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Current MLB prod12 status summary.")
    ap.add_argument("--pipeline-history", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--phase2-history", default="artifacts/mlb_prod12_phase2_history.jsonl")
    ap.add_argument("--daily-max-age-hours", type=float, default=0.0, help="0 disables staleness check.")
    ap.add_argument("--weekly-max-age-hours", type=float, default=0.0, help="0 disables staleness check.")
    ap.add_argument(
        "--scope",
        choices=("all", "daily", "weekly"),
        default="all",
        help="Status scope to enforce when computing pass/fail.",
    )
    ap.add_argument("--strict", action="store_true", help="Exit non-zero when overall status is fail.")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    pipeline_path = Path(args.pipeline_history)
    phase2_path = Path(args.phase2_history)
    pipeline = _latest_row(pipeline_path)
    phase2 = _latest_row(phase2_path)
    now = datetime.now(timezone.utc)
    daily_age = _age_hours((pipeline or {}).get("captured_at"), now)
    weekly_age = _age_hours((phase2 or {}).get("captured_at"), now)

    payload = {
        "status": "pass",
        "ok": True,
        "inputs": {
            "pipeline_history": str(pipeline_path),
            "phase2_history": str(phase2_path),
            "daily_max_age_hours": float(args.daily_max_age_hours),
            "weekly_max_age_hours": float(args.weekly_max_age_hours),
            "scope": args.scope,
        },
        "daily": {
            "captured_at": (pipeline or {}).get("captured_at"),
            "age_hours": round(daily_age, 2) if daily_age is not None else None,
            "status": (pipeline or {}).get("status"),
            "ok": (pipeline or {}).get("ok"),
            "failures": (pipeline or {}).get("failures") or [],
        },
        "weekly": {
            "captured_at": (phase2 or {}).get("captured_at"),
            "age_hours": round(weekly_age, 2) if weekly_age is not None else None,
            "status": (phase2 or {}).get("status"),
            "ok": (phase2 or {}).get("ok"),
            "failures": (phase2 or {}).get("failures") or [],
        },
        "warnings": [],
        "failures": [],
    }

    if pipeline is None:
        payload["warnings"].append("missing_daily_pipeline_history")
    if phase2 is None:
        payload["warnings"].append("missing_weekly_phase2_history")

    if args.scope in {"all", "daily"}:
        if pipeline is None or not bool((pipeline or {}).get("ok")):
            payload["ok"] = False
            payload["failures"].append("daily_failed_or_missing")
    if args.scope in {"all", "weekly"}:
        if phase2 is None or not bool((phase2 or {}).get("ok")):
            payload["ok"] = False
            payload["failures"].append("weekly_failed_or_missing")

    daily_max = float(args.daily_max_age_hours)
    weekly_max = float(args.weekly_max_age_hours)
    if args.scope in {"all", "daily"} and daily_max > 0:
        if daily_age is None or daily_age > daily_max:
            payload["ok"] = False
            payload["failures"].append("daily_stale")
    if args.scope in {"all", "weekly"} and weekly_max > 0:
        if weekly_age is None or weekly_age > weekly_max:
            payload["ok"] = False
            payload["failures"].append("weekly_stale")

    payload["status"] = "pass" if payload["ok"] else "fail"

    print(json.dumps(payload, indent=2))
    if args.strict and not payload.get("ok"):
        return 1
    return 0 if payload.get("ok") else 0


if __name__ == "__main__":
    raise SystemExit(main())

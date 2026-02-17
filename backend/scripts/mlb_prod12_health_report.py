#!/usr/bin/env python3
"""Summarize recent MLB prod12 daily/weekly history health."""

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
        except Exception:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _last_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return rows[-max(1, int(limit)) :]


def _pass_rate(rows: list[dict[str, Any]]) -> float | None:
    if not rows:
        return None
    passed = sum(1 for row in rows if bool(row.get("ok")))
    return round((passed / len(rows)) * 100.0, 2)


def _parse_dt(value: Any) -> datetime | None:
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


def _age_hours(value: Any) -> float | None:
    dt = _parse_dt(value)
    if dt is None:
        return None
    return round(max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0), 2)


def _latest_failure(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in reversed(rows):
        if not bool(row.get("ok")):
            return {
                "captured_at": row.get("captured_at"),
                "failures": row.get("failures") or [],
                "status": row.get("status"),
            }
    return None


def build_report(
    *,
    pipeline_history: Path,
    phase2_history: Path,
    daily_window: int,
    weekly_window: int,
) -> dict[str, Any]:
    daily_all = _load_jsonl(pipeline_history)
    weekly_all = _load_jsonl(phase2_history)
    daily_recent = _last_rows(daily_all, daily_window)
    weekly_recent = _last_rows(weekly_all, weekly_window)

    daily_latest = daily_recent[-1] if daily_recent else None
    weekly_latest = weekly_recent[-1] if weekly_recent else None

    daily_rate = _pass_rate(daily_recent)
    weekly_rate = _pass_rate(weekly_recent)
    ok = bool((daily_latest or {}).get("ok")) and bool((weekly_latest or {}).get("ok"))

    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok,
        "status": "pass" if ok else "fail",
        "windows": {
            "daily_window": int(daily_window),
            "weekly_window": int(weekly_window),
        },
        "daily": {
            "history_rows": len(daily_all),
            "window_rows": len(daily_recent),
            "pass_rate_pct": daily_rate,
            "latest": {
                "captured_at": (daily_latest or {}).get("captured_at"),
                "age_hours": _age_hours((daily_latest or {}).get("captured_at")),
                "ok": (daily_latest or {}).get("ok"),
                "failures": (daily_latest or {}).get("failures") or [],
            },
            "latest_failure": _latest_failure(daily_recent),
        },
        "weekly": {
            "history_rows": len(weekly_all),
            "window_rows": len(weekly_recent),
            "pass_rate_pct": weekly_rate,
            "latest": {
                "captured_at": (weekly_latest or {}).get("captured_at"),
                "age_hours": _age_hours((weekly_latest or {}).get("captured_at")),
                "ok": (weekly_latest or {}).get("ok"),
                "failures": (weekly_latest or {}).get("failures") or [],
            },
            "latest_failure": _latest_failure(weekly_recent),
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="MLB prod12 recent health report.")
    ap.add_argument("--pipeline-history", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--phase2-history", default="artifacts/mlb_prod12_phase2_history.jsonl")
    ap.add_argument("--daily-window", type=int, default=14)
    ap.add_argument("--weekly-window", type=int, default=8)
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    payload = build_report(
        pipeline_history=Path(args.pipeline_history),
        phase2_history=Path(args.phase2_history),
        daily_window=max(1, int(args.daily_window)),
        weekly_window=max(1, int(args.weekly_window)),
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

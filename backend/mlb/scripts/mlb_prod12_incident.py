#!/usr/bin/env python3
"""Produce compact incident summary for latest MLB prod12 daily/weekly snapshots."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, dict):
            out.append(parsed)
    return out


def _latest(path: Path) -> dict[str, Any] | None:
    rows = _load_jsonl(path)
    return rows[-1] if rows else None


def _top_daily_failure(daily: dict[str, Any]) -> dict[str, Any] | None:
    checks = daily.get("checks") or []
    if not isinstance(checks, list):
        return None
    for check in checks:
        if not isinstance(check, dict):
            continue
        if not bool(check.get("ok", False)):
            return {
                "name": check.get("name"),
                "status": check.get("status"),
                "exit_code": check.get("exit_code"),
                "failures": (check.get("payload") or {}).get("failures") or [],
            }
    return None


def _phase2_failures(weekly: dict[str, Any]) -> list[dict[str, Any]]:
    checks = weekly.get("checks") or {}
    if not isinstance(checks, dict):
        return []
    out: list[dict[str, Any]] = []
    for name in ("release_manifest", "replay_latency", "candidate_eval"):
        item = checks.get(name) or {}
        if not isinstance(item, dict):
            continue
        if bool(item.get("ok", False)):
            continue
        payload = item.get("payload") or {}
        out.append(
            {
                "name": name,
                "status": item.get("status"),
                "failures": (payload.get("failures") or []),
                "error": payload.get("error"),
            }
        )
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Latest MLB prod12 incident summary.")
    ap.add_argument("--pipeline-history", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--phase2-history", default="artifacts/mlb_prod12_phase2_history.jsonl")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    daily = _latest(Path(args.pipeline_history))
    weekly = _latest(Path(args.phase2_history))

    daily_ok = bool((daily or {}).get("ok"))
    weekly_ok = bool((weekly or {}).get("ok"))
    ok = bool(daily is not None and weekly is not None and daily_ok and weekly_ok)

    payload = {
        "status": "pass" if ok else "fail",
        "ok": ok,
        "daily": {
            "captured_at": (daily or {}).get("captured_at"),
            "ok": daily_ok if daily is not None else None,
            "failures": (daily or {}).get("failures") or [],
            "top_check_failure": _top_daily_failure(daily or {}) if daily is not None else None,
        },
        "weekly": {
            "captured_at": (weekly or {}).get("captured_at"),
            "ok": weekly_ok if weekly is not None else None,
            "failures": (weekly or {}).get("failures") or [],
            "failed_checks": _phase2_failures(weekly or {}) if weekly is not None else [],
        },
        "next_actions": [],
    }

    if daily is None:
        payload["next_actions"].append("run: make mlb-prod12-daily-gate")
    elif not daily_ok:
        payload["next_actions"].append("inspect: make mlb-pipeline-check-prod12 MLB_BASE_URL=<url> MLB_DATE=<date>")
        payload["next_actions"].append("inspect: make mlb-pipeline-last")

    if weekly is None:
        payload["next_actions"].append("run: make mlb-prod12-phase2-weekly-gate MLB_BASE_URL=<url>")
    elif not weekly_ok:
        payload["next_actions"].append("inspect: make mlb-prod12-phase2-last")
        payload["next_actions"].append("re-run: make mlb-prod12-phase2-weekly-gate MLB_BASE_URL=<url>")

    print(json.dumps(payload, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

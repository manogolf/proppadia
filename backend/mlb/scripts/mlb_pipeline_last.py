#!/usr/bin/env python3
"""Show recent MLB pipeline check history and detect regressions."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


def _load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
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


def _regressions(prev: dict[str, Any] | None, cur: dict[str, Any]) -> list[str]:
    if not prev:
        return []
    out: list[str] = []
    if bool(prev.get("ok", False)) and not bool(cur.get("ok", False)):
        out.append("overall_became_fail")

    prev_failures = set(prev.get("failures") or [])
    cur_failures = set(cur.get("failures") or [])
    new_failures = sorted(cur_failures - prev_failures)
    if new_failures:
        out.append("new_failures:" + ",".join(new_failures))
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Read recent MLB pipeline check history.")
    ap.add_argument("--input", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    history = _load_history(Path(args.input))
    tail = history[-max(1, int(args.limit)) :]
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "ok": item.get("ok"),
                "failures": item.get("failures") or [],
                "regressions": _regressions(prev, item),
            }
        )

    payload = {
        "captured_at": rows[-1].get("captured_at") if rows else None,
        "input": str(args.input),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

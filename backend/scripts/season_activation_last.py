#!/usr/bin/env python3
"""Show last N season activation status snapshots with blocker regressions."""

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
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    return rows


def _new_blockers(prev: dict[str, Any], cur: dict[str, Any]) -> list[str]:
    prev_blockers = set((((prev.get("readiness") or {}).get("blockers")) or []))
    cur_blockers = set((((cur.get("readiness") or {}).get("blockers")) or []))
    return sorted(list(cur_blockers - prev_blockers))


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Display last season activation status snapshots.")
    ap.add_argument("--input", default="artifacts/season_activation_history.jsonl")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else [])

    path = Path(args.input)
    history = _load_history(path)
    tail = history[-max(1, int(args.limit)) :]

    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "ok": item.get("ok"),
                "phase6_count": len(item.get("phase6_tracker") or []),
                "has_mlb_baseline": (((item.get("baseline_artifacts") or {}).get("has_mlb"))),
                "has_nhl_baseline": (((item.get("baseline_artifacts") or {}).get("has_nhl"))),
                "blockers": (((item.get("readiness") or {}).get("blockers")) or []),
                "new_blockers": _new_blockers(prev, item) if prev else [],
            }
        )

    payload = {
        "input": str(path),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"Season activation history: {path} ({len(history)} rows total)")
        for row in rows:
            new_blockers = ",".join(row["new_blockers"]) if row["new_blockers"] else "none"
            blockers = ",".join(row["blockers"]) if row["blockers"] else "none"
            print(
                f"- {row['captured_at'] or '-'} {row['status']} ok={row['ok']} phase6={row['phase6_count']} "
                f"mlb={row['has_mlb_baseline']} nhl={row['has_nhl_baseline']} "
                f"blockers={blockers} new_blockers={new_blockers}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

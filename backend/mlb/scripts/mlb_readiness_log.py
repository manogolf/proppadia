#!/usr/bin/env python3
"""Append MLB readiness snapshots to a local JSONL history file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from backend.mlb.scripts.mlb_readiness_snapshot import collect_snapshot


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append MLB readiness snapshot to JSONL history.")
    ap.add_argument("--output", default="artifacts/mlb_readiness_history.jsonl")
    ap.add_argument("--stat-days", type=int, default=30)
    ap.add_argument("--stat-require-min", type=int, default=0)
    ap.add_argument("--roster-require-min", type=int, default=1)
    ap.add_argument("--roster-stale-hours", type=int, default=30)
    args = ap.parse_args(list(argv) if argv is not None else [])

    payload = collect_snapshot(
        stat_days=args.stat_days,
        stat_require_min=args.stat_require_min,
        roster_require_min=args.roster_require_min,
        roster_stale_hours=args.roster_stale_hours,
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")

    print(
        json.dumps(
            {
                "status": payload.get("status"),
                "ok": payload.get("ok"),
                "output": str(out_path),
                "captured_at": payload.get("captured_at"),
            },
            indent=2,
        )
    )
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

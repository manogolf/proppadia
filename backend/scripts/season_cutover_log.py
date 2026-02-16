#!/usr/bin/env python3
"""Append season cutover cadence plan snapshots to a local JSONL history file."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from backend.scripts.season_cutover_cadence import build_plan


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append season cutover cadence plan to JSONL history.")
    ap.add_argument("--output", default="artifacts/season_cutover_history.jsonl")
    ap.add_argument("--timezone", default="America/New_York")
    ap.add_argument("--market-every-hours", type=int, default=8)
    ap.add_argument("--roster-hour-local", type=int, default=9)
    ap.add_argument("--stat-hour-local", type=int, default=11)
    ap.add_argument("--ops-hour-local", type=int, default=12)
    args = ap.parse_args(list(argv) if argv is not None else [])

    payload = build_plan(
        timezone=args.timezone,
        market_every_hours=args.market_every_hours,
        roster_hour_local=args.roster_hour_local,
        stat_hour_local=args.stat_hour_local,
        ops_hour_local=args.ops_hour_local,
    )
    payload["captured_at"] = datetime.now(timezone.utc).isoformat()

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
                "timezone": payload.get("timezone"),
                "lanes": len(payload.get("lanes") or []),
            },
            indent=2,
        )
    )
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

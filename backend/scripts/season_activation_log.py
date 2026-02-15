#!/usr/bin/env python3
"""Append season activation status snapshots to a local JSONL history file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from backend.scripts.season_activation_status import build_status


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append season activation status snapshot to JSONL history.")
    ap.add_argument("--output", default="artifacts/season_activation_history.jsonl")
    args = ap.parse_args(list(argv) if argv is not None else [])

    payload = build_status()

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
                "phase6_count": len(payload.get("phase6_tracker") or []),
            },
            indent=2,
        )
    )
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


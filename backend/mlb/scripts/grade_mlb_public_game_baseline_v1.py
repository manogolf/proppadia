#!/usr/bin/env python3
"""Append official-final outcomes for frozen public-game predictions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.mlb.public_game_predictions.baseline_v1 import RUNTIME_ROOT, append_grading_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mlb-date", required=True)
    ap.add_argument("--grading-rows-json", type=Path, required=True)
    ap.add_argument("--ledger", type=Path)
    args = ap.parse_args()
    rows = json.loads(args.grading_rows_json.read_text())
    path = args.ledger or RUNTIME_ROOT / args.mlb_date / "outcomes.jsonl"
    print(json.dumps({"appended": append_grading_rows(rows, path), "ledger": str(path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

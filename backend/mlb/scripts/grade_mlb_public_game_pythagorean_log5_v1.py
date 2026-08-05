#!/usr/bin/env python3
"""Append official-final grades for frozen Pythagorean/Log5 predictions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.mlb.public_game_predictions.pythagorean_log5_v1 import RUNTIME_ROOT, append_grading_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mlb-date", required=True)
    parser.add_argument("--grading-rows-json", type=Path, required=True)
    parser.add_argument("--ledger", type=Path)
    args = parser.parse_args()
    rows = json.loads(args.grading_rows_json.read_text(encoding="utf-8"))
    path = args.ledger or RUNTIME_ROOT / args.mlb_date / "pythagorean_log5_outcomes.jsonl"
    print(json.dumps({"model_version": "MLB_GAME_PYTHAGOREAN_LOG5_V1",
                      "appended": append_grading_rows(rows, path), "ledger": str(path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

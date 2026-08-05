#!/usr/bin/env python3
"""Append official-final grades for frozen Pythagorean/Log5 predictions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.mlb.public_game_predictions.durable_store_v1 import append_outcome_grade
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import RUNTIME_ROOT, append_grading_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mlb-date", required=True)
    parser.add_argument("--grading-rows-json", type=Path, required=True)
    parser.add_argument("--ledger", type=Path)
    parser.add_argument("--write-durable", action="store_true")
    args = parser.parse_args()
    rows = json.loads(args.grading_rows_json.read_text(encoding="utf-8"))
    if args.write_durable:
        appended=sum(int(append_outcome_grade(row)) for row in rows)
        destination="POSTGRES_MLB_PUBLIC_GAME_MONEYLINE_OUTCOMES"
    else:
        path = args.ledger or RUNTIME_ROOT / args.mlb_date / "pythagorean_log5_outcomes.jsonl"
        appended=append_grading_rows(rows,path);destination=str(path)
    print(json.dumps({"model_version": "MLB_GAME_PYTHAGOREAN_LOG5_V1",
                      "mode":"DURABLE_WRITE" if args.write_durable else "LOCAL_TEST_ONLY",
                      "appended": appended, "destination":destination}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

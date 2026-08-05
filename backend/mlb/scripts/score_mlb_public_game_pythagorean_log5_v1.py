#!/usr/bin/env python3
"""Score one official schedule with the frozen Pythagorean/Log5 candidate."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

from backend.app.services.mlb.schedule_service import fetch_schedule
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import (
    RUNTIME_ROOT, append_prediction_rows, score_schedule_payload,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mlb-date", default=date.today().isoformat())
    parser.add_argument("--schedule-json", type=Path)
    parser.add_argument("--prediction-timestamp-utc")
    parser.add_argument("--ledger", type=Path)
    args = parser.parse_args()
    payload = (json.loads(args.schedule_json.read_text(encoding="utf-8"))
               if args.schedule_json else fetch_schedule(game_date=args.mlb_date))
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    timestamp = args.prediction_timestamp_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows = score_schedule_payload(payload, prediction_timestamp_utc=timestamp,
                                  source_schedule_hash=hashlib.sha256(raw).hexdigest())
    ledger = args.ledger or RUNTIME_ROOT / args.mlb_date / "pythagorean_log5_shadow_predictions.jsonl"
    appended = append_prediction_rows(rows, ledger)
    print(json.dumps({"model_version": "MLB_GAME_PYTHAGOREAN_LOG5_V1", "rows": len(rows),
                      "appended": appended, "ledger": str(ledger)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

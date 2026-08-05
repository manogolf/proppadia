#!/usr/bin/env python3
"""Score one schedule with the immutable public baseline; shadow by default."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

from backend.app.services.mlb.schedule_service import fetch_schedule
from backend.mlb.public_game_predictions.baseline_v1 import (
    RUNTIME_ROOT, append_prediction_rows, score_schedule_payload,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mlb-date", default=date.today().isoformat())
    ap.add_argument("--schedule-json", type=Path)
    ap.add_argument("--prediction-timestamp-utc")
    ap.add_argument("--ledger", type=Path)
    args = ap.parse_args()
    payload = json.loads(args.schedule_json.read_text()) if args.schedule_json else fetch_schedule(game_date=args.mlb_date)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    timestamp = args.prediction_timestamp_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows = score_schedule_payload(payload, prediction_timestamp_utc=timestamp, source_schedule_hash=hashlib.sha256(raw).hexdigest())
    ledger = args.ledger or RUNTIME_ROOT / args.mlb_date / "shadow_predictions.jsonl"
    appended = append_prediction_rows(rows, ledger)
    print(json.dumps({"rows": len(rows), "appended": appended, "ledger": str(ledger)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Feature-flagged adapter for the frozen public MLB game baseline."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from backend.app.services.mlb.schedule_service import fetch_schedule
from backend.mlb.public_game_predictions.baseline_v1 import (
    authority_status,
    feature_enabled,
    load_candidate,
    score_schedule_payload,
)


def get_public_game_prediction_status() -> dict[str, Any]:
    candidate = load_candidate()
    return {
        "ok": True,
        "enabled": feature_enabled(),
        **authority_status(),
        "model_hash": candidate["model_hash"],
        "required_disclosure": candidate["disclosure"],
    }


def get_public_game_predictions(game_date: str) -> dict[str, Any]:
    status = get_public_game_prediction_status()
    if not status["enabled"]:
        return {**status, "rows": [], "count": 0}
    schedule = fetch_schedule(game_date=game_date)
    source_hash = hashlib.sha256(
        __import__("json").dumps(schedule, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows = score_schedule_payload(
        schedule, prediction_timestamp_utc=now, source_schedule_hash=source_hash
    )
    return {**status, "rows": rows, "count": len(rows)}

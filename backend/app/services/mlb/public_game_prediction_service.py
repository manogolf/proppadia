"""Feature-flagged adapter for the frozen MLB Pythagorean/Log5 candidate."""
from __future__ import annotations

from typing import Any

from backend.mlb.public_game_predictions.durable_store_v1 import fetch_prediction_rows
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import (
    authority_status,
    feature_enabled,
    load_candidate,
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
    # Production serves only immutable durable rows created by the designated
    # pregame lifecycle. Requests never synthesize retrospective predictions.
    rows = fetch_prediction_rows(game_date)
    return {**status, "rows": rows, "count": len(rows)}

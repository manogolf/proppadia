"""Feature-flagged adapter for the frozen MLB Pythagorean/Log5 candidate."""
from __future__ import annotations

from typing import Any

from backend.mlb.public_game_predictions.durable_store_v1 import fetch_prediction_rows
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import (
    DISCLOSURE,
    MODEL_NAME,
    MODEL_VERSION,
    PublicGamePredictionError,
    authority_status,
    feature_enabled,
    load_candidate,
)

CERTIFIED_MODEL_HASH = "804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6"
CERTIFICATION_STATUS = "MONEYLINE_STANDALONE_PREDICTION_CERTIFIED"
DISPLAY_STATUS = "MONEYLINE_PUBLIC_PREDICTION_READY"
CONFIDENCE_LABELS = {"STRONG", "MODERATE", "LEAN", "NEAR_EVEN"}


def _public_row(row: dict[str, Any], requested_date: str) -> dict[str, Any]:
    """Validate one immutable lifecycle payload and expose prediction-only fields."""
    if row.get("winner_model_version") != MODEL_VERSION:
        raise PublicGamePredictionError("PUBLIC_PREDICTION_MODEL_VERSION_MISMATCH")
    if row.get("winner_model_hash") != CERTIFIED_MODEL_HASH:
        raise PublicGamePredictionError("PUBLIC_PREDICTION_MODEL_HASH_MISMATCH")
    if row.get("game_date") != requested_date:
        raise PublicGamePredictionError("PUBLIC_PREDICTION_DATE_SCOPE_MISMATCH")
    if row.get("admission_status") != "ADMITTED_SHADOW":
        raise PublicGamePredictionError("PUBLIC_PREDICTION_NOT_ADMITTED")
    if row.get("confidence_band") not in CONFIDENCE_LABELS:
        raise PublicGamePredictionError("PUBLIC_PREDICTION_CONFIDENCE_INVALID")
    home = float(row["home_win_probability"])
    away = float(row["away_win_probability"])
    if not (0.0 <= home <= 1.0 and 0.0 <= away <= 1.0 and abs(home + away - 1.0) <= 1e-12):
        raise PublicGamePredictionError("PUBLIC_PREDICTION_PROBABILITY_INVALID")
    winner = row.get("predicted_winner")
    if winner not in {row.get("home_team"), row.get("away_team")}:
        raise PublicGamePredictionError("PUBLIC_PREDICTION_WINNER_INVALID")
    identity = "|".join(str(row.get(key) or "") for key in (
        "game_date", "game_id", "winner_model_version", "prediction_snapshot_class"
    ))
    if not all(identity.split("|")):
        raise PublicGamePredictionError("PUBLIC_PREDICTION_IDENTITY_INCOMPLETE")
    return {
        "immutable_prediction_identity": identity,
        "game_id": int(row["game_id"]),
        "game_date": row["game_date"],
        "scheduled_start_utc": row["scheduled_start_utc"],
        "home_team": row["home_team"],
        "away_team": row["away_team"],
        "predicted_winner": winner,
        "home_win_probability": home,
        "away_win_probability": away,
        "picked_side_probability": home if winner == row["home_team"] else away,
        "confidence_band": row["confidence_band"],
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "model_hash": CERTIFIED_MODEL_HASH,
        "prediction_timestamp_utc": row["prediction_timestamp_utc"],
    }


def _certified_rows(rows: list[dict[str, Any]], requested_date: str) -> list[dict[str, Any]]:
    public = [_public_row(row, requested_date) for row in rows]
    identities = [row["immutable_prediction_identity"] for row in public]
    if len(identities) != len(set(identities)):
        raise PublicGamePredictionError("DUPLICATE_PUBLIC_PREDICTION_IDENTITY")
    return public


def get_public_game_prediction_status() -> dict[str, Any]:
    candidate = load_candidate()
    return {
        "ok": True,
        "enabled": feature_enabled(),
        **authority_status(),
        "model_hash": candidate["model_hash"],
        "certification_status": CERTIFICATION_STATUS,
        "display_status": DISPLAY_STATUS,
        "required_disclosure": DISCLOSURE,
    }


def get_public_game_predictions(game_date: str) -> dict[str, Any]:
    status = get_public_game_prediction_status()
    if not status["enabled"]:
        return {**status, "rows": [], "count": 0}
    # Production serves only immutable durable rows created by the designated
    # pregame lifecycle. Requests never synthesize retrospective predictions.
    rows = _certified_rows(fetch_prediction_rows(game_date), game_date)
    return {**status, "rows": rows, "count": len(rows)}

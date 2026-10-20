"""Feature-flagged adapter for the frozen MLB Pythagorean/Log5 candidate."""
from __future__ import annotations

from typing import Any

from backend.mlb.public_game_predictions.durable_store_v1 import fetch_prediction_rows
from backend.mlb.public_game_predictions.phase_gating_v1 import (
    MoneylinePhaseGateError,
    classify_moneyline_row,
    snapshot_verified_moneyline_phase_authority,
)
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import (
    DISCLOSURE,
    MODEL_NAME,
    MODEL_VERSION,
    PublicGamePredictionError,
    authority_status,
    feature_enabled,
    load_candidate,
)
from backend.mlb.season_transition.game_phase_authority_v1 import GamePhaseAuthorityError
from backend.mlb.season_transition.phase_authority_db_chain_v1 import extend_public_authority

CERTIFIED_MODEL_HASH = "804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6"
CERTIFICATION_STATUS = "MONEYLINE_STANDALONE_PREDICTION_CERTIFIED"
DISPLAY_STATUS = "MONEYLINE_PUBLIC_PREDICTION_READY"
PHASE_AUTHORITY_UNAVAILABLE = "PHASE_AUTHORITY_EVIDENCE_UNAVAILABLE"
GAME_NOT_IN_SNAPSHOT = "PHASE_AUTHORITY_GAME_NOT_IN_COMMITTED_SNAPSHOT"
CONFIDENCE_LABELS = {"STRONG", "MODERATE", "LEAN", "NEAR_EVEN"}


def _public_row(row: dict[str, Any], requested_date: str) -> tuple[dict[str, Any], dict[str, Any]]:
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
    return row, {
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


def _certified_rows(
    rows: list[dict[str, Any]], requested_date: str
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    certified = [_public_row(row, requested_date) for row in rows]
    identities = [public["immutable_prediction_identity"] for _, public in certified]
    if len(identities) != len(set(identities)):
        raise PublicGamePredictionError("DUPLICATE_PUBLIC_PREDICTION_IDENTITY")
    return certified


def _phase_joined_rows(
    certified: list[tuple[dict[str, Any], dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None, dict[str, Any] | None]:
    if not certified:
        return [], [], None, None
    # Hosts lack the git-ignored raw schedule sources, so the projection reads
    # the committed hash-chained snapshot. Row admission was fully verified.
    # Games added since the last deploy come from the signed DB chain, which
    # is re-verified from the committed anchor on every request.
    authority, extension = extend_public_authority(snapshot_verified_moneyline_phase_authority())
    joined = []
    excluded = []
    for source, public in certified:
        try:
            phase = classify_moneyline_row(source, authority=authority)
        except MoneylinePhaseGateError as exc:
            # A game admitted locally through the run-bound runtime overlay is
            # absent until a governed snapshot child or the verified DB chain
            # adds it. Withhold only that row; every other phase failure still
            # fails the request.
            if exc.code != "MONEYLINE_PHASE_AUTHORITY_INVALID" or exc.detail != "GAME_PHASE_ABSENT":
                raise
            excluded.append({"game_id": public["game_id"], "excluded_code": GAME_NOT_IN_SNAPSHOT})
            continue
        # These values are joined at read time and remain absent from the
        # immutable Moneyline prediction ledger.
        joined.append({
            **public,
            "authoritative_game_type": phase.source_game_type,
            "normalized_phase": phase.normalized_phase,
            "postseason_round": phase.postseason_round,
        })
    return joined, excluded, authority.source_verification, extension


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
    certified = _certified_rows(fetch_prediction_rows(game_date), game_date)
    try:
        rows, excluded, verification, extension = _phase_joined_rows(certified)
    except GamePhaseAuthorityError as exc:
        # Committed snapshot evidence failed verification; no row can be
        # phase-joined, so publish nothing and report a typed reason.
        return {
            **status,
            "rows": [],
            "count": 0,
            "unavailable_status": PHASE_AUTHORITY_UNAVAILABLE,
            "unavailable_code": exc.code,
        }
    payload = {**status, "rows": rows, "count": len(rows)}
    if verification is not None:
        payload["phase_authority_verification"] = verification
    if extension is not None:
        payload["phase_authority_extension"] = extension
    if excluded:
        payload["excluded_rows"] = excluded
    return payload

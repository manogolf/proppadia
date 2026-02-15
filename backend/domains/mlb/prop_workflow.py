"""MLB prop workflow domain logic: prepare, predict, persist."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from backend.app.services.mlb.commit_tokens import sign_commit_payload, verify_commit_token
from backend.domains.mlb.game_context import build_game_context
from backend.domains.mlb.repository.prop_repository import (
    DuplicatePropError,
    find_duplicate_prop_id,
    insert_prop_row,
)
from backend.domains.mlb.player_resolver import resolve_player_candidate
from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    getTeamIdFromAbbr,
    normalizeTeamAbbreviation,
)

ET = ZoneInfo("America/New_York")


def normalize_prop_type(prop_type: str) -> str:
    return (
        (prop_type or "")
        .strip()
        .lower()
        .replace("(", "")
        .replace(")", "")
        .replace(" + ", "_")
        .replace(" ", "_")
        .strip("_")
    )


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def prepare_prop(payload: Dict[str, Any]) -> Dict[str, Any]:
    game_date = str(payload.get("game_date") or "").strip()
    if not game_date:
        game_date = datetime.now(ET).date().isoformat()
    date.fromisoformat(game_date)  # raises on bad format

    prop_type = normalize_prop_type(str(payload.get("prop_type") or ""))
    if not prop_type:
        raise ValueError("prop_type is required")

    prop_value = _to_float(payload.get("prop_value"), 0.0)
    over_under = str(payload.get("over_under") or "over").strip().lower()
    if over_under not in {"over", "under"}:
        over_under = "over"

    player_id = payload.get("player_id")
    team_id = payload.get("team_id")
    team_abbr = normalizeTeamAbbreviation(payload.get("team_abbr") or payload.get("team"))
    player_name = str(payload.get("player_name") or payload.get("name") or "").strip() or None

    warnings = []

    # Resolve identity/team from inputs and tables.
    resolved = resolve_player_candidate(
        player_id=int(player_id) if player_id is not None else None,
        name=player_name,
        team_abbr=team_abbr,
    )
    if resolved:
        resolved_team_id = resolved.get("team_id")
        resolved_team_abbr = resolved.get("team_abbr")
        if (
            team_id is not None
            and resolved_team_id is not None
            and int(team_id) != int(resolved_team_id)
        ):
            warnings.append("input team_id mismatched resolved player team; using resolved team")
        if (
            team_abbr
            and resolved_team_abbr
            and normalizeTeamAbbreviation(team_abbr) != normalizeTeamAbbreviation(resolved_team_abbr)
        ):
            warnings.append("input team_abbr mismatched resolved player team; using resolved team")
        player_id = resolved.get("player_id")
        player_name = player_name or resolved.get("player_name")
        if resolved_team_id is not None:
            team_id = resolved_team_id
        if resolved_team_abbr:
            team_abbr = normalizeTeamAbbreviation(resolved_team_abbr)

    if team_id is None and team_abbr:
        team_id = getTeamIdFromAbbr(team_abbr)
    if team_abbr is None and team_id is not None:
        team_abbr = normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(int(team_id)))

    if player_id is None:
        raise ValueError("unable to resolve player_id")
    if team_id is None:
        raise ValueError("unable to resolve team_id/team_abbr")

    try:
        context = build_game_context(team_id=int(team_id), game_date=game_date)
    except Exception:
        context = None
    if not context:
        # Offseason/no-network fallback: keep pipeline alive with neutral defaults.
        warnings.append("game context unavailable; using fallback context")
        game_id_fallback = payload.get("game_id")
        try:
            game_id_fallback = int(game_id_fallback) if game_id_fallback is not None else None
        except Exception:
            game_id_fallback = None
        context = {
            "team_id": int(team_id),
            "team_abbr": team_abbr,
            "for_date": game_date,
            "game_id": game_id_fallback,
            "game_time": None,
            "is_home": bool(payload.get("is_home", False)),
            "opponent_team_id": payload.get("opponent_team_id"),
            "opponent": payload.get("opponent"),
            "opponent_encoded": payload.get("opponent_encoded"),
            "game_day_of_week": payload.get("game_day_of_week"),
            "time_of_day_bucket": payload.get("time_of_day_bucket"),
            "starting_pitcher_id": payload.get("starting_pitcher_id"),
        }

    rolling_result_avg_7 = _to_float(payload.get("rolling_result_avg_7"), 0.0)
    line_diff = _to_float(payload.get("line_diff"), rolling_result_avg_7 - prop_value)
    market_odds_american = payload.get("market_odds_american")
    market_implied_probability = payload.get("market_implied_probability")
    market_odds_american = _to_float(market_odds_american, None) if market_odds_american not in (None, "") else None
    market_implied_probability = (
        _to_float(market_implied_probability, None)
        if market_implied_probability not in (None, "")
        else None
    )

    features = {
        "player_id": int(player_id),
        "player_name": player_name,
        "team_id": int(team_id),
        "team": team_abbr,
        "game_date": game_date,
        "prop_type": prop_type,
        "prop_value": prop_value,
        "over_under": over_under,
        "rolling_result_avg_7": rolling_result_avg_7,
        "hit_streak": _to_float(payload.get("hit_streak"), 0.0),
        "win_streak": _to_float(payload.get("win_streak"), 0.0),
        "line_diff": line_diff,
        "market_odds_american": market_odds_american,
        "market_implied_probability": market_implied_probability,
        **context,
    }
    if warnings:
        features["_warnings"] = warnings
    return features


def _heuristic_probability(features: Dict[str, Any]) -> float:
    line_diff = _to_float(features.get("line_diff"), 0.0)
    hit_streak = _to_float(features.get("hit_streak"), 0.0)
    win_streak = _to_float(features.get("win_streak"), 0.0)
    is_home = 1.0 if bool(features.get("is_home")) else 0.0

    z = 0.0
    z += 0.9 * math.tanh(line_diff)
    z += 0.07 * max(-8.0, min(8.0, hit_streak))
    z += 0.05 * max(-8.0, min(8.0, win_streak))
    z += 0.10 * (1.0 if is_home else -1.0)
    return 1.0 / (1.0 + math.exp(-z))


def predict_prop(prop_type: str, features: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_prop_type(prop_type)
    if not normalized:
        raise ValueError("prop_type is required")

    probability: Optional[float] = None
    model_name = "heuristic_fallback_v1"
    model_meta: Dict[str, Any] = {"strategy": "heuristic"}

    # Try canonical model pipeline first; fallback if unavailable.
    try:
        from backend.mlb.prediction.make_prediction import predict as model_predict

        result = model_predict(prop_type=normalized, features=features)
        probability = _to_float(result.get("probability"), None)  # type: ignore[arg-type]
        if probability is None:
            probability = _to_float(result.get("probability_over"), None)  # type: ignore[arg-type]
        model_name = str(result.get("blend", {}).get("strategy") or "model_pipeline")
        model_meta = {
            "strategy": "model_pipeline",
            "components": result.get("components"),
            "blend": result.get("blend"),
        }
    except Exception:
        probability = None

    if probability is None:
        probability = _heuristic_probability(features)

    probability = max(0.0, min(1.0, float(probability)))
    recommendation = "over" if probability >= 0.5 else "under"
    commit_payload = {
        "flow": "mlb_prop_v1",
        "prop_type": normalized,
        "features": features,
        "probability": probability,
        "recommendation": recommendation,
    }
    commit_token = sign_commit_payload(commit_payload)

    return {
        "prop_type": normalized,
        "probability": probability,
        "probability_over": probability,
        "probability_under": 1.0 - probability,
        "recommendation": recommendation,
        "predicted_outcome": recommendation,
        "commit_token": commit_token,
        "model": model_name,
        "model_meta": model_meta,
    }


def add_prop_from_commit(
    *,
    commit_token: str,
    prop_source: str = "user_added",
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload = verify_commit_token(commit_token)
    features = payload.get("features") or {}
    prop_type = normalize_prop_type(payload.get("prop_type") or "")
    probability = _to_float(payload.get("probability"), 0.5)
    recommendation = str(payload.get("recommendation") or ("over" if probability >= 0.5 else "under"))

    if features.get("player_id") is None:
        raise ValueError("player_id missing in committed features")
    if features.get("game_id") is None:
        raise ValueError("game_id missing in committed features")
    game_date_raw = str(features.get("game_date") or "").strip()
    if not game_date_raw:
        raise ValueError("game_date missing in committed features")
    try:
        date.fromisoformat(game_date_raw)
    except ValueError as e:
        raise ValueError("game_date must be YYYY-MM-DD") from e
    context_date = str(features.get("for_date") or "").strip()
    if context_date and context_date != game_date_raw:
        raise ValueError("game_date mismatch with context for_date")

    player_id = int(features.get("player_id"))
    game_id = int(features.get("game_id"))
    if game_id <= 0:
        raise ValueError("game_id must be a positive integer")
    over_under = str(features.get("over_under") or "over").lower()
    prop_value = _to_float(features.get("prop_value"), 0.0)
    game_date = game_date_raw
    team = normalizeTeamAbbreviation(features.get("team") or features.get("team_abbr"))
    team_id = features.get("team_id")
    player_name = features.get("player_name")

    dup_id = find_duplicate_prop_id(
        player_id=player_id,
        game_id=game_id,
        prop_type=prop_type,
        over_under=over_under,
        prop_value=prop_value,
        prop_source=prop_source,
    )
    if dup_id:
        return {"ok": True, "saved": False, "duplicate": True, "id": dup_id}

    try:
        insert_prop_row(
            player_id=player_id,
            player_name=player_name,
            team=team,
            team_id=int(team_id) if team_id is not None else None,
            game_id=game_id,
            game_date=game_date,
            prop_type=prop_type,
            prop_value=prop_value,
            over_under=over_under,
            prop_source=prop_source,
            recommendation=recommendation,
            probability=probability,
            user_id=user_id,
        )
    except DuplicatePropError:
        dup_id = find_duplicate_prop_id(
            player_id=player_id,
            game_id=game_id,
            prop_type=prop_type,
            over_under=over_under,
            prop_value=prop_value,
            prop_source=prop_source,
        )
        return {"ok": True, "saved": False, "duplicate": True, "id": dup_id}
    return {"ok": True, "saved": True, "duplicate": False}

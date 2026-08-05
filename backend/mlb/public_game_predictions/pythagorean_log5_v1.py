"""Frozen, game-only scorer for MLB_GAME_PYTHAGOREAN_LOG5_V1.

The scorer is intentionally isolated from player-prop, retired-model, market,
EV, ranking, upload, routing, and wagering code.  It reads only its immutable
candidate manifest and an official schedule payload.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"
RUNTIME_ROOT = REPO_ROOT / "backend/mlb/data/runtime/public_game_predictions/v1"
MODEL_VERSION = "MLB_GAME_PYTHAGOREAN_LOG5_V1"
MODEL_NAME = "MLB Pythagorean/Log5"
SNAPSHOT_CLASS = "DESIGNATED_DAILY_PUBLIC_SNAPSHOT"
PUBLIC_PENDING = "MLB_PUBLIC_MONEYLINE_PREDICTION_CANDIDATE_PENDING_ENABLEMENT"
PUBLIC_ACTIVE = "MLB_PUBLIC_MONEYLINE_PREDICTION_MODEL_ACTIVE"
BETTING_AUTHORITY = "NO_QUALIFIED_MLB_BETTING_MODEL"
PROP_AUTHORITY = "NO_QUALIFIED_MLB_PROP_MODEL"
SCORE_STATUS = "UNAVAILABLE_NO_QUALIFIED_SCORE_MODEL"
DISCLOSURE = "MODEL PREDICTION — BETTING EDGE NOT DEMONSTRATED"


class PublicGamePredictionError(RuntimeError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def scorer_sha256() -> str:
    return sha256_bytes(Path(__file__).read_bytes())


def load_candidate(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    identity = payload.get("model_identity")
    if payload.get("model_hash") != sha256_bytes(_canonical_bytes(identity)):
        raise PublicGamePredictionError("PYTHAGOREAN_LOG5_MODEL_HASH_MISMATCH")
    if identity.get("model_version") != MODEL_VERSION:
        raise PublicGamePredictionError("PYTHAGOREAN_LOG5_VERSION_MISMATCH")
    if payload.get("score_prediction_status") != SCORE_STATUS:
        raise PublicGamePredictionError("UNQUALIFIED_SCORE_MODEL_BINDING")
    return payload


def _frozen_team_states(config: dict[str, Any]) -> dict[int, dict[str, float]]:
    source = config["frozen_team_state_source"]
    path = REPO_ROOT / source["path"]
    if sha256_bytes(path.read_bytes()) != source["sha256"]:
        raise PublicGamePredictionError("FROZEN_TEAM_STATE_SOURCE_HASH_MISMATCH")
    totals: dict[int, dict[str, float]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            team_id = int(row["team_mlb_id"])
            totals[team_id] = {
                "games": int(row["games"]), "runs_scored": float(row["runs_scored"]),
                "runs_allowed": float(row["runs_allowed"]),
            }
    for state in totals.values():
        state["runs_scored_per_game"] = state["runs_scored"] / state["games"]
        state["runs_allowed_per_game"] = state["runs_allowed"] / state["games"]
    return totals


def feature_enabled(env: dict[str, str] | None = None) -> bool:
    source = os.environ if env is None else env
    return str(source.get("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }


def authority_status(env: dict[str, str] | None = None) -> dict[str, str]:
    return {
        "public_game_prediction_authority": PUBLIC_ACTIVE if feature_enabled(env) else PUBLIC_PENDING,
        "betting_authority": BETTING_AUTHORITY,
        "player_prop_authority": PROP_AUTHORITY,
        "model_version": MODEL_VERSION,
        "score_prediction_status": SCORE_STATUS,
    }


def pythagorean_strength(runs_scored: float, runs_allowed: float, exponent: float) -> float:
    if runs_scored < 0 or runs_allowed < 0:
        raise PublicGamePredictionError("INVALID_TEAM_RUN_STATE")
    if runs_scored + runs_allowed == 0:
        return 0.5
    numerator = runs_scored ** exponent
    return numerator / (numerator + runs_allowed ** exponent)


def log5_probability(home_strength: float, away_strength: float) -> float:
    denominator = home_strength + away_strength - 2 * home_strength * away_strength
    if abs(denominator) <= 1e-9:
        return 0.5
    return (home_strength - home_strength * away_strength) / denominator


def matchup_probability(home_strength: float, away_strength: float, home_logit: float,
                        lower_bound: float, upper_bound: float) -> float:
    raw = min(upper_bound, max(lower_bound, log5_probability(home_strength, away_strength)))
    adjusted = 1.0 / (1.0 + math.exp(-(math.log(raw / (1.0 - raw)) + home_logit)))
    return min(1.0 - 1e-6, max(1e-6, adjusted))


def confidence_band(home_probability: float, bands: dict[str, float]) -> str:
    distance = abs(home_probability - 0.5)
    if distance <= bands["near_even_max_distance"]:
        return "NEAR_EVEN"
    if distance <= bands["lean_max_distance"]:
        return "LEAN"
    if distance <= bands["moderate_max_distance"]:
        return "MODERATE"
    return "STRONG"


def _schedule_games(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for date_block in payload.get("dates") or []:
        yield from date_block.get("games") or []


def _team(game: dict[str, Any], side: str) -> tuple[int | None, str | None]:
    team = (((game.get("teams") or {}).get(side) or {}).get("team") or {})
    return team.get("id"), team.get("name")


def _unavailable_fields() -> dict[str, None]:
    return {key: None for key in (
        "expected_home_runs", "expected_away_runs", "expected_total_runs", "expected_home_margin",
        "home_minus_1_5_probability", "away_plus_1_5_probability",
        "away_minus_1_5_probability", "home_plus_1_5_probability",
    )}


def score_schedule_payload(payload: dict[str, Any], *, prediction_timestamp_utc: str,
                           source_schedule_hash: str, team_state_snapshot: dict[str, Any] | None = None,
                           production_mode: bool = False,
                           snapshot_class: str = SNAPSHOT_CLASS) -> list[dict[str, Any]]:
    """Score strict pregame schedule identities without reading outcomes or props."""
    config = load_candidate()
    identity = config["model_identity"]
    if production_mode and team_state_snapshot is None:
        raise PublicGamePredictionError("PRODUCTION_SCORING_REQUIRES_ADVANCED_TEAM_STATE")
    if team_state_snapshot is None:
        states = _frozen_team_states(config)
        state_hash_value = config["frozen_team_state_source"]["sha256"]
        state_through_game_date = identity["frozen_state_cutoff"]
        prediction_cutoff_utc = prediction_timestamp_utc
        state_quality = "CERTIFIED_INITIALIZATION_STATE"
    else:
        from .state_v1 import scoring_team_states
        states = scoring_team_states(team_state_snapshot)
        state_hash_value = team_state_snapshot["state_hash"]
        state_through_game_date = team_state_snapshot["state_through_game_date"]
        prediction_cutoff_utc = team_state_snapshot["prediction_cutoff_utc"]
        state_quality = "STRICT_PRIOR_ADVANCED_STATE_CERTIFIED"
    now = datetime.fromisoformat(prediction_timestamp_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    rows: list[dict[str, Any]] = []
    for game in _schedule_games(payload):
        game_id = game.get("gamePk")
        home_id, home_name = _team(game, "home")
        away_id, away_name = _team(game, "away")
        start_raw = game.get("gameDate")
        reason = None
        try:
            start = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            start = None
            reason = "SCHEDULED_START_TIME_UNRESOLVED"
        if not game_id or not home_id or not away_id or not home_name or not away_name:
            reason = reason or "CORE_GAME_IDENTITY_UNRESOLVED"
        if start is not None and now >= start:
            reason = reason or "PREGAME_CUTOFF_FAILED"
        hs, aws = states.get(home_id), states.get(away_id)
        if hs is None or aws is None:
            reason = reason or "TEAM_STRICT_PRIOR_STATE_UNAVAILABLE"
        game_date = str(game.get("officialDate") or (start.date().isoformat() if start else ""))
        row = {
            "league": "MLB", "game_date": game_date, "game_id": game_id,
            "scheduled_start_utc": start.isoformat().replace("+00:00", "Z") if start else start_raw,
            "prediction_timestamp_utc": now.isoformat().replace("+00:00", "Z"),
            "away_team": away_name, "home_team": home_name,
            "prediction_snapshot_class": snapshot_class,
            "winner_model_name": MODEL_NAME, "winner_model_version": MODEL_VERSION,
            "winner_model_hash": config["model_hash"], "scorer_hash": scorer_sha256(),
            "source_schedule_hash": source_schedule_hash,
            "team_state_hash": state_hash_value,
            "state_through_game_date": state_through_game_date,
            "prediction_cutoff_utc": prediction_cutoff_utc,
            "historical_validation_accuracy": config["historical_evaluation"]["validation"]["accuracy"],
            "historical_validation_brier": config["historical_evaluation"]["validation"]["brier"],
            "historical_validation_log_loss": config["historical_evaluation"]["validation"]["log_loss"],
            "historical_holdout_accuracy": config["historical_evaluation"]["late_2026_holdout"]["accuracy"],
            "historical_holdout_brier": config["historical_evaluation"]["late_2026_holdout"]["brier"],
            "historical_holdout_log_loss": config["historical_evaluation"]["late_2026_holdout"]["log_loss"],
            "betting_edge_status": "NOT_DEMONSTRATED", "disclosure": DISCLOSURE,
            "score_prediction_status": SCORE_STATUS, **_unavailable_fields(),
            "generated_at_utc": now.isoformat().replace("+00:00", "Z"),
            "admission_status": "REJECTED_FAIL_CLOSED" if reason else "ADMITTED_SHADOW",
            "failure_reason": reason,
            "lead_time_minutes": (start - now).total_seconds() / 60 if start else None,
        }
        if reason:
            rows.append(row)
            continue
        exponent = float(identity["pythagorean_exponent"])
        hp = pythagorean_strength(float(hs["runs_scored_per_game"]), float(hs["runs_allowed_per_game"]), exponent)
        ap = pythagorean_strength(float(aws["runs_scored_per_game"]), float(aws["runs_allowed_per_game"]), exponent)
        home_p = matchup_probability(hp, ap, float(identity["home_logit_adjustment"]),
                                     float(identity["log5_clip_lower"]), float(identity["log5_clip_upper"]))
        row.update({
            "home_win_probability": home_p, "away_win_probability": 1.0 - home_p,
            "predicted_winner": home_name if home_p >= 0.5 else away_name,
            "confidence_band": confidence_band(home_p, identity["confidence_bands"]),
            "data_quality_status": state_quality,
            "home_team_games": int(hs["games"]), "away_team_games": int(aws["games"]),
            "home_pythagorean_strength": hp, "away_pythagorean_strength": ap,
            "home_field_logit_adjustment": float(identity["home_logit_adjustment"]),
        })
        rows.append(row)
    return rows


def _identity(row: dict[str, Any]) -> str:
    return "|".join(str(row.get(k) or "") for k in (
        "game_date", "game_id", "winner_model_version", "prediction_snapshot_class"
    ))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")
    return len(rows)


def append_prediction_rows(rows: list[dict[str, Any]], path: Path) -> int:
    seen = {_identity(row) for row in _read_jsonl(path)}
    admitted = []
    for row in rows:
        key = _identity(row)
        if not all(key.split("|")):
            raise PublicGamePredictionError("PREDICTION_LEDGER_IDENTITY_INCOMPLETE")
        if key not in seen:
            seen.add(key)
            admitted.append(row)
    return _append_jsonl(path, admitted)


def append_grading_rows(rows: list[dict[str, Any]], path: Path) -> int:
    seen = {_identity(row) for row in _read_jsonl(path)}
    admitted = []
    for row in rows:
        if row.get("official_status") != "Final":
            raise PublicGamePredictionError("GRADING_REQUIRES_OFFICIAL_FINAL")
        key = _identity(row)
        if key not in seen:
            seen.add(key)
            admitted.append(row)
    return _append_jsonl(path, admitted)


def build_official_final_grade(prediction: dict[str, Any], *, official_home_runs: int,
                               official_away_runs: int, official_source_path: str,
                               official_source_sha256: str, grading_timestamp_utc: str) -> dict[str, Any]:
    """Construct one grade without mutating or recomputing the frozen prediction."""
    if official_home_runs == official_away_runs:
        raise PublicGamePredictionError("OFFICIAL_FINAL_WINNER_UNRESOLVED")
    home_won = int(official_home_runs > official_away_runs)
    probability = float(prediction["home_win_probability"])
    selected_home = prediction["predicted_winner"] == prediction["home_team"]
    clipped = min(1.0 - 1e-15, max(1e-15, probability))
    return {
        **prediction,
        "official_status": "Final",
        "official_home_runs": int(official_home_runs),
        "official_away_runs": int(official_away_runs),
        "official_winner": prediction["home_team"] if home_won else prediction["away_team"],
        "prediction_correct": bool(selected_home == bool(home_won)),
        "observed_outcome_probability": probability if home_won else 1.0 - probability,
        "brier_contribution": (probability - home_won) ** 2,
        "log_loss_contribution": -(home_won * math.log(clipped) + (1 - home_won) * math.log(1 - clipped)),
        "official_source_path": official_source_path,
        "official_source_sha256": official_source_sha256,
        "grading_timestamp_utc": grading_timestamp_utc,
    }

"""Deterministic strict-prior team-state advancement for Pythagorean/Log5 v1."""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .pythagorean_log5_v1 import CONFIG_PATH, REPO_ROOT, PublicGamePredictionError, sha256_bytes

FINAL_STATES = {"Final", "Game Over"}
NONFINAL_EXPLICIT = {
    "Scheduled", "Pre-Game", "Warmup", "In Progress", "Delayed", "Postponed",
    "Suspended", "Cancelled",
}


@dataclass(frozen=True)
class OfficialFinalGame:
    game_pk: int
    game_date: str
    scheduled_start_utc: str
    game_number: int
    home_team_id: int
    away_team_id: int
    home_runs: int
    away_runs: int
    official_status: str
    observed_final_at_utc: str
    source_identity: str
    source_sha256: str

    @property
    def order_key(self) -> tuple[str, str, int, int]:
        return (self.game_date, self.scheduled_start_utc, self.game_number, self.game_pk)

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(json.dumps(self.__dict__, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise PublicGamePredictionError("STATE_TIMESTAMP_MUST_BE_TIMEZONE_AWARE")
    return parsed.astimezone(timezone.utc)


def load_initialization_state(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    source = config["frozen_team_state_source"]
    path = REPO_ROOT / source["path"]
    if sha256_bytes(path.read_bytes()) != source["sha256"]:
        raise PublicGamePredictionError("INITIALIZATION_STATE_HASH_MISMATCH")
    states: dict[str, dict[str, int]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            states[str(int(row["team_mlb_id"]))] = {
                "games": int(row["games"]), "runs_scored": int(row["runs_scored"]),
                "runs_allowed": int(row["runs_allowed"]),
            }
    return {
        "schema_version": "MLB_PYTHAGOREAN_TEAM_STATE_V1",
        "model_version": config["model_identity"]["model_version"],
        "initialization_source_sha256": source["sha256"],
        "state_through_game_date": config["model_identity"]["frozen_state_cutoff"],
        "applied_game_ids": [], "applied_game_hashes": {}, "teams": states,
    }


def state_hash(snapshot: dict[str, Any]) -> str:
    stable = {k: snapshot[k] for k in (
        "schema_version", "model_version", "initialization_source_sha256",
        "state_through_game_date", "applied_game_ids", "applied_game_hashes", "teams",
    )}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def reconstruct_state(final_games: Iterable[OfficialFinalGame], *, prediction_cutoff_utc: str,
                      state_generated_at_utc: str) -> dict[str, Any]:
    cutoff = _utc(prediction_cutoff_utc)
    generated = _utc(state_generated_at_utc)
    if generated > cutoff:
        raise PublicGamePredictionError("STATE_GENERATED_AFTER_PREDICTION_CUTOFF")
    snapshot = load_initialization_state()
    initialization_through_date = snapshot["state_through_game_date"]
    by_game: dict[int, OfficialFinalGame] = {}
    unresolved: list[dict[str, Any]] = []
    for game in final_games:
        if game.official_status not in FINAL_STATES:
            unresolved.append({"game_pk": game.game_pk, "status": game.official_status,
                               "reason": "OFFICIAL_STATUS_NOT_FINAL"})
            continue
        if _utc(game.observed_final_at_utc) >= cutoff:
            unresolved.append({"game_pk": game.game_pk, "status": game.official_status,
                               "reason": "FINAL_OBSERVED_AT_OR_AFTER_CUTOFF"})
            continue
        prior = by_game.get(game.game_pk)
        if prior and prior.content_hash != game.content_hash:
            raise PublicGamePredictionError(f"CONFLICTING_OFFICIAL_FINAL_GAME:{game.game_pk}")
        by_game[game.game_pk] = game
    for game in sorted(by_game.values(), key=lambda item: item.order_key):
        if game.game_date <= initialization_through_date:
            continue
        if game.home_team_id == game.away_team_id or min(game.home_runs, game.away_runs) < 0:
            raise PublicGamePredictionError(f"INVALID_OFFICIAL_FINAL_IDENTITY:{game.game_pk}")
        for team_id, scored, allowed in (
            (game.home_team_id, game.home_runs, game.away_runs),
            (game.away_team_id, game.away_runs, game.home_runs),
        ):
            team = snapshot["teams"].get(str(team_id))
            if team is None:
                raise PublicGamePredictionError(f"TEAM_INITIALIZATION_STATE_UNAVAILABLE:{team_id}")
            team["games"] += 1
            team["runs_scored"] += scored
            team["runs_allowed"] += allowed
        snapshot["applied_game_ids"].append(game.game_pk)
        snapshot["applied_game_hashes"][str(game.game_pk)] = game.content_hash
        snapshot["state_through_game_date"] = max(snapshot["state_through_game_date"], game.game_date)
    snapshot.update({
        "state_generated_at_utc": generated.isoformat().replace("+00:00", "Z"),
        "prediction_cutoff_utc": cutoff.isoformat().replace("+00:00", "Z"),
        "unresolved_games": unresolved,
    })
    snapshot["state_hash"] = state_hash(snapshot)
    return snapshot


def scoring_team_states(snapshot: dict[str, Any]) -> dict[int, dict[str, float]]:
    if snapshot.get("state_hash") != state_hash(snapshot):
        raise PublicGamePredictionError("TEAM_STATE_HASH_MISMATCH")
    result = {}
    for key, value in snapshot["teams"].items():
        games = int(value["games"])
        if games <= 0:
            raise PublicGamePredictionError(f"TEAM_STATE_EMPTY:{key}")
        result[int(key)] = {
            **value,
            "runs_scored_per_game": float(value["runs_scored"]) / games,
            "runs_allowed_per_game": float(value["runs_allowed"]) / games,
        }
    return result

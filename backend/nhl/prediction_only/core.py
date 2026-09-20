"""Immutable Points and Saves prediction-only publication.

This module deliberately has no market client, quote-run, provider-event, price,
candidate, upload, or execution dependency.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.points_shadow.core import (
    IDENTITY_PATH as POINTS_IDENTITY_PATH,
    PLAYER_IDENTITY_COLUMNS,
    _validate_inputs as validate_points_inputs,
    evaluate_ladder_coherence,
    score_frozen as score_points,
    verify_fixed_input_parity as verify_points_parity,
    verify_frozen_identity as verify_points_identity,
)
from backend.nhl.saves_shadow.core import (
    IDENTITY_COLUMNS as SAVES_IDENTITY_COLUMNS,
    IDENTITY_PATH as SAVES_IDENTITY_PATH,
    _validate_inputs as validate_saves_inputs,
    score_frozen as score_saves,
    verify_historical_parity as verify_saves_parity,
    verify_operational_amendment,
    verify_frozen_identity as verify_saves_identity,
)


CONTRACT = "NHL_GENERAL_PREDICTION_ONLY_V1"
PHASES = {"MIDDAY", "FINAL_PREGAME"}
LANES = {"POINTS", "SAVES"}
PROSPECTIVE_NOT_BEFORE = "2026-09-21"
ELIGIBLE_LADDER_STATES = {"PASS_LADDER_COHERENCE", "WARNING_MINOR_LADDER_INCOHERENCE"}
_RUN_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{1,160}$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("TIMESTAMP_MUST_BE_TIMEZONE_AWARE")
    return parsed.astimezone(timezone.utc)


def _iso(value: str) -> str:
    return parse_utc(value).isoformat().replace("+00:00", "Z")


def _manifest(directory: Path) -> None:
    files = sorted(path for path in directory.iterdir() if path.is_file() and path.name != "SHA256SUMS")
    (directory / "SHA256SUMS").write_text("".join(f"{sha256_file(path)}  {path.name}\n" for path in files))


def verify_manifest(directory: Path) -> None:
    complete, manifest = directory / "RUN_COMPLETE.json", directory / "SHA256SUMS"
    if not complete.is_file() or not manifest.is_file():
        raise RuntimeError("PREDICTION_ONLY_RUN_INCOMPLETE")
    for raw in manifest.read_text().splitlines():
        expected, name = raw.split("  ", 1)
        if sha256_file(directory / name) != expected:
            raise RuntimeError(f"PREDICTION_ONLY_MANIFEST_MISMATCH:{name}")


def _validate_common(*, games: pd.DataFrame, season: int, slate_date: str, phase: str,
                     observation_timestamp_utc: str, input_cutoff_timestamp_utc: str,
                     canonical_run_identifier: str) -> tuple[str, str]:
    if season != 2026:
        raise RuntimeError("FROZEN_MODEL_SEASON_MISMATCH")
    if slate_date < PROSPECTIVE_NOT_BEFORE:
        raise RuntimeError(f"RETROSPECTIVE_PREDICTION_FORBIDDEN:NOT_BEFORE_{PROSPECTIVE_NOT_BEFORE}")
    if phase not in PHASES:
        raise RuntimeError("INVALID_PREDICTION_PHASE")
    if not _RUN_TOKEN.fullmatch(canonical_run_identifier):
        raise RuntimeError("INVALID_CANONICAL_RUN_IDENTIFIER")
    observation, cutoff = _iso(observation_timestamp_utc), _iso(input_cutoff_timestamp_utc)
    if parse_utc(cutoff) > parse_utc(observation):
        raise RuntimeError("INPUT_CUTOFF_AFTER_OBSERVATION")
    required = {"canonical_season", "slate_date", "game_id", "scheduled_start_time_utc", "game_type_code"}
    if required - set(games):
        raise RuntimeError(f"CANONICAL_SLATE_SCHEMA_INCOMPLETE:{sorted(required-set(games))}")
    if games.empty:
        raise RuntimeError("VALID_EMPTY_SLATE_NO_PREDICTION")
    if games.game_id.duplicated().any():
        raise RuntimeError("DUPLICATE_CANONICAL_GAME_IDENTITY")
    if not games.canonical_season.astype(int).eq(season).all() or not games.slate_date.astype(str).eq(slate_date).all():
        raise RuntimeError("CANONICAL_SLATE_IDENTITY_MISMATCH")
    starts = pd.to_datetime(games.scheduled_start_time_utc, utc=True, errors="coerce")
    if starts.isna().any() or (pd.Timestamp(parse_utc(observation)) >= starts).any():
        raise RuntimeError("WHOLE_SLATE_PRESTART_GATE_FAILED_PARTIAL_SLATE_FORBIDDEN")
    return observation, cutoff


def _run_id(lane: str, season: int, slate_date: str, phase: str,
            canonical_run_identifier: str) -> str:
    identity = digest({"contract": CONTRACT, "lane": lane, "season": season,
                       "slate_date": slate_date, "phase": phase,
                       "canonical_run_identifier": canonical_run_identifier})[:20]
    return f"nhl{lane.lower()}prediction_s{season}_d{slate_date.replace('-', '')}_{phase}_{identity}_v1"


@contextlib.contextmanager
def prediction_lock(root: Path, lane: str, slate_date: str, phase: str):
    lock_path = root / ".locks" / f"{lane.lower()}_{slate_date}_{phase}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"{lane}_PREDICTION_ONLY_ALREADY_RUNNING") from error
        yield


def _existing(root: Path, season: int, slate_date: str, phase: str,
              canonical_run_identifier: str) -> tuple[Path | None, str | None]:
    phase_root = root / f"season={season}" / f"slate_date={slate_date}" / f"phase={phase}"
    runs = sorted(phase_root.glob("run_id=*"))
    if len(runs) > 1:
        raise RuntimeError("MULTIPLE_IMMUTABLE_PREDICTION_RUNS_FOR_PHASE")
    if not runs:
        return None, None
    verify_manifest(runs[0])
    metadata = json.loads((runs[0] / "run_metadata.json").read_text())
    if metadata.get("canonical_run_identifier") != canonical_run_identifier:
        raise RuntimeError("CONFLICTING_PREDICTION_IDENTITY")
    return runs[0], "IDEMPOTENT_EXISTING_ZERO_INSERTS"


def _empty_surface(run_id: str) -> pd.DataFrame:
    return pd.DataFrame(columns=["run_id", "status", "reason"])


def publish_points(*, games: pd.DataFrame, players: pd.DataFrame, output_root: Path,
                   season: int, slate_date: str, phase: str,
                   observation_timestamp_utc: str, input_cutoff_timestamp_utc: str,
                   canonical_run_identifier: str) -> tuple[Path, str]:
    observation, cutoff = _validate_common(
        games=games, season=season, slate_date=slate_date, phase=phase,
        observation_timestamp_utc=observation_timestamp_utc,
        input_cutoff_timestamp_utc=input_cutoff_timestamp_utc,
        canonical_run_identifier=canonical_run_identifier,
    )
    with prediction_lock(output_root, "POINTS", slate_date, phase):
        old, disposition = _existing(output_root, season, slate_date, phase, canonical_run_identifier)
        if old is not None:
            return old, disposition or "IDEMPOTENT_EXISTING_ZERO_INSERTS"
        validate_points_inputs(games, players, slate_date, observation)
        parity, identity = verify_points_parity(), verify_points_identity()
        raw = score_points(players, identity)
        ladder = evaluate_ladder_coherence(raw, identity)
        predictions = raw.merge(players[PLAYER_IDENTITY_COLUMNS], on=["game_id", "player_id"], validate="many_to_one")
        predictions = predictions.merge(
            ladder[["game_id", "player_id", "ladder_coherence_decision", "ladder_coherence_reason",
                    "maximum_adjacent_crossing_probability", "maximum_adjacent_crossing_pp"]],
            on=["game_id", "player_id"], validate="many_to_one",
        )
        run_id = _run_id("POINTS", season, slate_date, phase, canonical_run_identifier)
        predictions.insert(0, "run_id", run_id)
        predictions["phase"] = phase
        predictions["prediction_timestamp_utc"] = observation
        predictions["input_cutoff_timestamp_utc"] = cutoff
        predictions["model_version"] = identity["model_version"]
        predictions["prediction_eligible"] = predictions.ladder_coherence_decision.isin(ELIGIBLE_LADDER_STATES)
        predictions["market_qualified"] = False
        predictions["price"] = pd.NA
        predictions["provider_event_id"] = pd.NA
        predictions["prediction_identity"] = predictions.apply(
            lambda row: digest({"run_id": run_id, "game_id": int(row.game_id),
                                "player_id": int(row.player_id), "line": float(row.line),
                                "model_version": identity["model_version"]}), axis=1)
        exclusions = ladder.loc[~ladder.ladder_coherence_decision.isin(ELIGIBLE_LADDER_STATES)].copy()
        input_exclusions = pd.DataFrame(
            players.attrs.get("input_exclusions", []),
            columns=["game_id", "player_id", "reason"],
        )
        if predictions.prediction_identity.duplicated().any():
            raise RuntimeError("DUPLICATE_POINTS_PREDICTION_IDENTITY")
        phase_root = output_root / f"season={season}" / f"slate_date={slate_date}" / f"phase={phase}"
        destination = phase_root / f"run_id={run_id}"
        phase_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".points.incomplete.", dir=phase_root))
        try:
            games.sort_values("game_id").to_csv(staging / "canonical_game_spine.csv", index=False)
            players.sort_values(["game_id", "player_id"]).to_csv(staging / "feature_input_snapshot.csv", index=False)
            predictions.sort_values(["game_id", "player_id", "line"]).to_csv(staging / "immutable_predictions.csv", index=False)
            ladder.sort_values(["game_id", "player_id"]).to_csv(staging / "ladder_coherence.csv", index=False)
            exclusions.sort_values(["game_id", "player_id"]).to_csv(staging / "prediction_exclusions.csv", index=False)
            input_exclusions.to_csv(staging / "input_exclusions.csv", index=False)
            for name in ("market_qualified_population.csv", "candidate_population.csv", "upload_population.csv", "execution_population.csv"):
                _empty_surface(run_id).to_csv(staging / name, index=False)
            written = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            metadata = {
                "contract_version": CONTRACT, "lane": "POINTS", "canonical_season": season,
                "slate_date": slate_date, "phase": phase, "run_id": run_id,
                "canonical_run_identifier": canonical_run_identifier,
                "observation_timestamp_utc": observation, "input_cutoff_timestamp_utc": cutoff,
                "actual_write_timestamp_utc": written, "games": len(games),
                "player_games": int(players[["game_id", "player_id"]].drop_duplicates().shape[0]),
                "prediction_rows": len(predictions), "eligible_prediction_rows": int(predictions.prediction_eligible.sum()),
                "ladder_excluded_player_games": len(exclusions),
                "input_excluded_player_games": len(input_exclusions),
                "market_attachment": "ABSENT_OPTIONAL_DOWNSTREAM", "market_qualified_rows": 0,
                "candidate_rows": 0, "provider_event_dependency": "NONE",
                "bookmaker_credential_access": False, "market_requests": 0, "paid_credits": 0,
                "model_fixture_status": parity["status"], "status": "COMPLETE_PREDICTION_ONLY",
                "first_eligible_live_date": PROSPECTIVE_NOT_BEFORE,
            }
            (staging / "run_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
            (staging / "RUN_COMPLETE.json").write_text(json.dumps({"run_id": run_id, "status": "COMPLETE"}, sort_keys=True) + "\n")
            _manifest(staging)
            os.replace(staging, destination)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    return destination, "COMPLETE_NEW_APPEND_ONLY"


def publish_saves(*, games: pd.DataFrame, goalies: pd.DataFrame, output_root: Path,
                  season: int, slate_date: str, phase: str,
                  observation_timestamp_utc: str, input_cutoff_timestamp_utc: str,
                  canonical_run_identifier: str) -> tuple[Path, str]:
    observation, cutoff = _validate_common(
        games=games, season=season, slate_date=slate_date, phase=phase,
        observation_timestamp_utc=observation_timestamp_utc,
        input_cutoff_timestamp_utc=input_cutoff_timestamp_utc,
        canonical_run_identifier=canonical_run_identifier,
    )
    with prediction_lock(output_root, "SAVES", slate_date, phase):
        old, disposition = _existing(output_root, season, slate_date, phase, canonical_run_identifier)
        if old is not None:
            return old, disposition or "IDEMPOTENT_EXISTING_ZERO_INSERTS"
        validate_saves_inputs(games, goalies, slate_date, observation)
        identity, parity, amendment = verify_saves_identity(), verify_saves_parity(), verify_operational_amendment()
        scoring = goalies.copy()
        scoring["player_id"] = scoring.goalie_id
        predictions, _ = score_saves(scoring, 1.0, identity)
        if len(predictions) != len(goalies) * len(identity["lines"]):
            raise RuntimeError("SAVES_PREDICTION_POPULATION_COLLAPSE")
        predictions = predictions.merge(
            goalies[["game_id", "goalie_id", "goalie_name", "team", "opponent",
                     "scheduled_start_time_utc", "game_type_code"]],
            on=["game_id", "goalie_id"], validate="many_to_one",
        )
        run_id = _run_id("SAVES", season, slate_date, phase, canonical_run_identifier)
        predictions.insert(0, "run_id", run_id)
        predictions["phase"] = phase
        predictions["prediction_timestamp_utc"] = observation
        predictions["input_cutoff_timestamp_utc"] = cutoff
        predictions["prediction_semantics"] = identity["operational_amendment"]["semantic_contract"]
        predictions["starter_state"] = "UNKNOWN_NO_AUTHORIZED_PREGAME_STARTER_SOURCE"
        predictions["selected_starter"] = False
        predictions["market_qualified"] = False
        predictions["price"] = pd.NA
        predictions["provider_event_id"] = pd.NA
        predictions["prediction_identity"] = predictions.apply(
            lambda row: digest({"run_id": run_id, "game_id": int(row.game_id),
                                "goalie_id": int(row.goalie_id), "line": float(row.line),
                                "model_version": identity["model_version"]}), axis=1)
        if predictions.prediction_identity.duplicated().any():
            raise RuntimeError("DUPLICATE_SAVES_PREDICTION_IDENTITY")
        input_exclusions = pd.DataFrame(
            goalies.attrs.get("input_exclusions", []),
            columns=["game_id", "player_id", "reason"],
        ).rename(columns={"player_id": "goalie_id"})
        phase_root = output_root / f"season={season}" / f"slate_date={slate_date}" / f"phase={phase}"
        destination = phase_root / f"run_id={run_id}"
        phase_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".saves.incomplete.", dir=phase_root))
        try:
            games.sort_values("game_id").to_csv(staging / "canonical_game_spine.csv", index=False)
            goalies.sort_values(["game_id", "goalie_id"]).to_csv(staging / "feature_input_snapshot.csv", index=False)
            predictions.sort_values(["game_id", "goalie_id", "line"]).to_csv(staging / "immutable_conditional_predictions.csv", index=False)
            input_exclusions.to_csv(staging / "input_exclusions.csv", index=False)
            pd.DataFrame(columns=["game_id", "team", "selected_goalie_id", "reason"]).to_csv(staging / "starter_selections.csv", index=False)
            for name in ("market_qualified_population.csv", "candidate_population.csv", "upload_population.csv", "execution_population.csv"):
                _empty_surface(run_id).to_csv(staging / name, index=False)
            written = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            metadata = {
                "contract_version": CONTRACT, "lane": "SAVES", "canonical_season": season,
                "slate_date": slate_date, "phase": phase, "run_id": run_id,
                "canonical_run_identifier": canonical_run_identifier,
                "observation_timestamp_utc": observation, "input_cutoff_timestamp_utc": cutoff,
                "actual_write_timestamp_utc": written, "games": len(games),
                "goalie_games": int(goalies[["game_id", "goalie_id"]].drop_duplicates().shape[0]),
                "prediction_rows": len(predictions), "starter_selections": 0,
                "input_excluded_goalie_games": len(input_exclusions),
                "starter_contract": "CONDITIONAL_DISTRIBUTION_STARTER_UNKNOWN",
                "market_attachment": "ABSENT_OPTIONAL_DOWNSTREAM", "market_qualified_rows": 0,
                "candidate_rows": 0, "provider_event_dependency": "NONE",
                "bookmaker_credential_access": False, "market_requests": 0, "paid_credits": 0,
                "model_fixture_status": parity["status"], "operational_amendment": amendment["status"],
                "status": "COMPLETE_PREDICTION_ONLY", "first_eligible_live_date": PROSPECTIVE_NOT_BEFORE,
            }
            (staging / "run_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
            (staging / "RUN_COMPLETE.json").write_text(json.dumps({"run_id": run_id, "status": "COMPLETE"}, sort_keys=True) + "\n")
            _manifest(staging)
            os.replace(staging, destination)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    return destination, "COMPLETE_NEW_APPEND_ONLY"

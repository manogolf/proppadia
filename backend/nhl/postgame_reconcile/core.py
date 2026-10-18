"""Pure validation, outcome construction, grading, and append-only publication."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import math
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from backend.nhl.cross_market_shadow.core import (
    ACTIVATION_PATH as CROSS_MARKET_ACTIVATION_PATH,
    FEATURES as CROSS_MARKET_FEATURES,
    PARAMETER_PATH as MONEYLINE_PARAMETER_PATH,
    PUCK_PARAMETER_PATH,
    digest_value,
    moneyline_substantive_identity,
)
from backend.nhl.sog_cold_start.core import (
    CONTRACT_PATH as SOG_CONTRACT_PATH,
    digest as sog_digest,
    grade_predictions,
)
from backend.nhl.prediction_only.core import digest as prediction_only_digest
from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.game_phase import phase_for_game_type, regular_season_evaluation_eligible
from backend.nhl.postgame_reconcile.resilient_finalization import (
    canonical_atomic_identity, select_latest_atomic_sources, validate_atomic_group,
)


CONTRACT = "NHL_POSTGAME_RECONCILIATION_V1"
FINAL_STATES = {"FINAL", "OFF"}
SUPPORTED_GAME_TYPES = {1, 2, 3}
PROSPECTIVE_PROP_NOT_BEFORE = "2026-09-21"
SEPTEMBER_20_REQUIRED_FAILED_ANCESTORS = [
    "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6",
    "nhlpostgame_20260920_20260922T161724179739Z_cef0bc8b",
    "nhlpostgame_20260920_20260922T171356619916Z_cd2ac1d9",
    "nhlpostgame_20260920_20260922T181726727181Z_70f0280d",
    "nhlpostgame_20260920_20260922T204231536641Z_75b7fb99",
]


def validate_request_lineage(lineage: dict[str, Any], *, slate_date: str) -> None:
    """Validate typed response sources and all required failed ancestors."""
    version = lineage.get("contract_version")
    if version in {"NHL_POSTGAME_REQUEST_LINEAGE_V3",
                   "NHL_POSTGAME_REQUEST_LINEAGE_V4",
                   "NHL_POSTGAME_REQUEST_LINEAGE_V5",
                   "NHL_POSTGAME_REQUEST_LINEAGE_V6"}:
        response_sources = lineage.get("response_sources") or []
        failed_ancestors = lineage.get("failed_ancestors") or []
        source_ids = [row.get("source_run_id") for row in response_sources]
        failed_ids = [row.get("run_id") for row in failed_ancestors]
        roles = [row.get("role") for row in response_sources]
        allowed_role_sets = ([
            ["AUTHORITY_RESPONSE_SOURCE", "ROSTER_RESPONSE_SOURCE"],
            ["AUTHORITY_RESPONSE_SOURCE", "PLAYER_IDENTITY_RESPONSE_SOURCE",
             "ROSTER_RESPONSE_SOURCE"],
        ] if version.endswith("V6") else [[
            "AUTHORITY_RESPONSE_SOURCE", "ROSTER_RESPONSE_SOURCE"]]
            if version.endswith("V3") else [[
                "AUTHORITY_RESPONSE_SOURCE", "PLAYER_IDENTITY_RESPONSE_SOURCE",
                "ROSTER_RESPONSE_SOURCE"]])
        source_role_ids = list(zip(roles, source_ids))
        if (len(source_role_ids) != len(set(source_role_ids))
                or len(failed_ids) != len(set(failed_ids))
                or sorted(roles) not in allowed_role_sets
                or any(row.get("role") != "FAILED_EXECUTION_ANCESTOR"
                       for row in failed_ancestors)):
            raise RuntimeError("REQUEST_LINEAGE_INVALID")
        if slate_date == "2026-09-20":
            if (version != "NHL_POSTGAME_REQUEST_LINEAGE_V5"
                    or failed_ids != SEPTEMBER_20_REQUIRED_FAILED_ANCESTORS):
                raise RuntimeError("SEPTEMBER_20_ALL_FIVE_FAILED_ANCESTORS_REQUIRED")
        elif slate_date >= "2026-09-21" and version != "NHL_POSTGAME_REQUEST_LINEAGE_V6":
            raise RuntimeError("FRESH_DATE_REQUEST_LINEAGE_V6_REQUIRED")
        return
    ancestors = lineage.get("ancestors") or []
    ancestor_ids = [row.get("source_run_id") or row.get("run_id")
                    for row in ancestors]
    if (len(ancestor_ids) != len(set(ancestor_ids))
            or sum(row.get("role") == "AUTHORITY_RESPONSE_SOURCE"
                   for row in ancestors) != 1):
        raise RuntimeError("REQUEST_LINEAGE_INVALID")


def _verify_manifest(run: Path) -> dict[str, str]:
    manifest = run / "SHA256SUMS"
    if not manifest.is_file():
        raise RuntimeError(f"IMMUTABLE_SOURCE_MANIFEST_MISSING:{run}")
    entries: dict[str, str] = {}
    for line in manifest.read_text().splitlines():
        try:
            digest, name = line.split("  ", 1)
        except ValueError as error:
            raise RuntimeError(f"IMMUTABLE_SOURCE_MANIFEST_INVALID:{run}") from error
        if Path(name).name != name or name in entries or len(digest) != 64:
            raise RuntimeError(f"IMMUTABLE_SOURCE_MANIFEST_INVALID:{run}")
        path = run / name
        if not path.is_file() or _sha(path) != digest:
            raise RuntimeError(f"IMMUTABLE_SOURCE_HASH_MISMATCH:{path}")
        entries[name] = digest
    return entries


def _one_run(pattern: Path, label: str) -> Path:
    runs = sorted(pattern.parent.glob(pattern.name))
    if len(runs) != 1:
        raise RuntimeError(f"IMMUTABLE_{label}_RUN_CARDINALITY:{len(runs)}")
    return runs[0]


def _select_cross_market_final_run(*, runs: list[Path], schedule: pd.DataFrame,
                                  activation: dict[str, Any],
                                  allow_missing_active_final: bool = False,
                                  allow_missing_active_final_for_resilient: bool = False,
                                  ) -> tuple[Path | None, dict[str, Any]]:
    """Require a final card for active evaluation except prior-learning handoff.

    A preseason slate is explicitly non-evaluation for Moneyline/Puck Line, so
    no FINAL_PREGAME card is required. Outside preseason, a disabled lane may
    also have no card. An active lane retains the exact-one invariant for direct
    reconciliation; prior-learning may proceed reference-only without a card.
    """
    if len(runs) > 1:
        raise RuntimeError(f"IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:{len(runs)}")
    if len(runs) == 1:
        return runs[0], {"status": "REQUIRED_CARD_PRESENT"}
    game_types = pd.to_numeric(schedule.game_type_code, errors="coerce")
    if game_types.isna().any() or not game_types.isin([1, 2, 3]).all():
        raise RuntimeError("IMMUTABLE_CROSS_MARKET_GAME_TYPE_UNRESOLVED")
    if game_types.eq(1).all():
        return None, {"status": "NOT_REQUIRED_PRESEASON", "reason": "PRESEASON_NON_EVALUATION"}
    if activation.get("capture_enabled") is False:
        return None, {"status": "NOT_REQUIRED_LANE_INACTIVE", "reason": "CAPTURE_DISABLED"}
    if allow_missing_active_final:
        return None, {
            "status": ("MISSING_ACTIVE_CARD_RESILIENT_POPULATION"
                       if allow_missing_active_final_for_resilient else
                       "MISSING_ACTIVE_CARD_PRIOR_LEARNING"),
            "reason": ("NO_IMMUTABLE_CROSS_MARKET_SOURCE_RESILIENT_POPULATION"
                       if allow_missing_active_final_for_resilient else
                       "NO_IMMUTABLE_CROSS_MARKET_SOURCE"),
        }
    raise RuntimeError("IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:0")


def _attach_retained_schedule_codes(schedule: pd.DataFrame, *, path: Path,
                                   slate_date: str) -> tuple[pd.DataFrame, str]:
    """Bind team codes from the retained official slate response by exact game identity."""
    payload = json.loads(path.read_text())
    rows = [game for day in payload.get("gameWeek", [])
            if str(day.get("date")) == slate_date for game in day.get("games", [])]
    by_id: dict[int, dict[str, Any]] = {}
    for game in rows:
        try:
            game_id = int(game["id"])
            home_id = int(game["homeTeam"]["id"])
            away_id = int(game["awayTeam"]["id"])
            game_type = int(game["gameType"])
            start = pd.to_datetime(game["startTimeUTC"], utc=True)
            home_code = str(game["homeTeam"]["abbrev"]).upper()
            away_code = str(game["awayTeam"]["abbrev"]).upper()
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_IDENTITY_INCOMPLETE") from error
        if game_id in by_id:
            raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_DUPLICATE_GAME")
        by_id[game_id] = {"home_team_id": home_id, "away_team_id": away_id,
                          "game_type_code": game_type,
                          "scheduled_start_time_utc": start,
                          "home_team": home_code, "away_team": away_code}
    expected = set(schedule.game_id.astype(int))
    if set(by_id) != expected:
        raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_GAME_SET_MISMATCH")
    bound = schedule.copy()
    for column in ("home_team_id", "away_team_id", "game_type_code"):
        source_values = {game_id: values[column] for game_id, values in by_id.items()}
        if any(int(getattr(row, column)) != int(source_values[int(row.game_id)])
               for row in bound.itertuples(index=False)):
            raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_IDENTITY_CONFLICT")
    source_starts = {game_id: values["scheduled_start_time_utc"]
                     for game_id, values in by_id.items()}
    bound_starts = pd.to_datetime(bound.scheduled_start_time_utc, utc=True)
    if any(start != source_starts[int(game_id)]
           for game_id, start in zip(bound.game_id.astype(int), bound_starts)):
        raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_START_CONFLICT")
    bound["home_team"] = bound.game_id.astype(int).map({
        game_id: values["home_team"] for game_id, values in by_id.items()})
    bound["away_team"] = bound.game_id.astype(int).map({
        game_id: values["away_team"] for game_id, values in by_id.items()})
    return bound, _sha(path)


def _retained_schedule_spine(*, operational_root: Path, slate_date: str) -> tuple[pd.DataFrame, dict[str, str]]:
    """Build canonical schedule identity from the retained official response."""
    path = operational_root / "slates" / slate_date / "raw_schedule_response.json"
    if not path.is_file():
        raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_SOURCE_MISSING")
    payload = json.loads(path.read_text())
    rows = [game for day in payload.get("gameWeek", []) if str(day.get("date")) == slate_date
            for game in (day.get("games") or [])]
    spine = []
    for game in rows:
        try:
            spine.append({
                "canonical_season": 2026, "slate_date": slate_date,
                "game_id": int(game["id"]),
                "scheduled_start_time_utc": pd.to_datetime(game["startTimeUTC"], utc=True),
                "home_team_id": int(game["homeTeam"]["id"]),
                "away_team_id": int(game["awayTeam"]["id"]),
                "game_type_code": int(game["gameType"]),
            })
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_IDENTITY_INCOMPLETE") from error
    if not spine or len({row["game_id"] for row in spine}) != len(spine):
        raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_GAME_SET_INVALID")
    bound, digest = _attach_retained_schedule_codes(pd.DataFrame(spine), path=path,
                                                     slate_date=slate_date)
    return _local_spine(bound, slate_date), {"path": str(path), "sha256": digest}


def _verify_resilient_prop_package(*, run: Path, lane: str, slate_date: str,
                                  schedule: pd.DataFrame,
                                  durable_publication_timestamp_utc: str | None = None) -> pd.DataFrame:
    """Validate immutable prop rows without imposing the legacy whole-slate gate."""
    files = _verify_manifest(run)
    metadata = json.loads((run / "run_metadata.json").read_text())
    marker_path = run / "RUN_COMPLETE.json"
    marker = json.loads(marker_path.read_text()) if marker_path.is_file() else {}
    prediction_file = ("immutable_conditional_predictions.csv" if lane == "SAVES"
                       else "immutable_predictions.csv")
    if prediction_file not in files or (marker and marker.get("status") != "COMPLETE"):
        raise RuntimeError(f"RESILIENT_{lane}_SOURCE_COMPLETION_INVALID")
    if (metadata.get("slate_date") != slate_date
            or run.name != f"run_id={metadata.get('run_id')}"
            or marker.get("run_id") not in {None, metadata.get("run_id")}):
        raise RuntimeError(f"RESILIENT_{lane}_SOURCE_IDENTITY_MISMATCH")
    if lane in {"POINTS", "SAVES"} and metadata.get("lane") != lane:
        raise RuntimeError(f"RESILIENT_{lane}_SOURCE_LANE_MISMATCH")
    phase = str(metadata.get("phase") or "")
    if phase not in {"MIDDAY", "FINAL_PREGAME"}:
        raise RuntimeError(f"RESILIENT_{lane}_PHASE_INELIGIBLE:{phase}")
    frame = pd.read_csv(run / prediction_file, float_precision="round_trip")
    if frame.empty:
        return frame
    if lane in {"POINTS", "SAVES"}:
        id_column = "player_id" if lane == "POINTS" else "goalie_id"
        required = {"run_id", "game_id", id_column, "line", "prob_over", "phase",
                    "prediction_timestamp_utc", "input_cutoff_timestamp_utc", "model_version",
                    "prediction_identity", "scheduled_start_time_utc", "game_type_code"}
        if lane == "POINTS":
            required.add("feature_cutoff_timestamp_utc")
        required |= ({"probability_construction", "prediction_eligible", "market_qualified", "price"}
                     if lane == "POINTS" else {"prediction_semantics", "market_qualified", "price"})
        _require_columns(frame, required, f"RESILIENT_{lane}")
        if (set(frame.run_id.astype(str)) != {str(metadata.get("run_id"))}
                or set(frame.phase.astype(str)) != {phase}
                or frame.duplicated(["game_id", id_column, "line"]).any()
                or frame.prediction_identity.duplicated().any()):
            raise RuntimeError(f"RESILIENT_{lane}_PREDICTION_IDENTITY_DUPLICATE_OR_MIXED")
        if frame.market_qualified.fillna(False).astype(bool).any() or frame.price.notna().any():
            raise RuntimeError(f"RESILIENT_{lane}_UNEXPECTED_MARKET_ATTACHMENT")
        probability = pd.to_numeric(frame.prob_over, errors="coerce")
        if probability.isna().any() or (~probability.between(0, 1)).any():
            raise RuntimeError(f"RESILIENT_{lane}_PROBABILITY_INVALID")
        spine = pd.read_csv(run / "canonical_game_spine.csv")
        _require_columns(spine, {"game_id", "scheduled_start_time_utc"},
                         f"RESILIENT_{lane}_CANONICAL_SPINE")
        if spine.game_id.duplicated().any() or set(spine.game_id.astype(int)) != set(schedule.game_id.astype(int)):
            raise RuntimeError(f"RESILIENT_{lane}_CANONICAL_GAME_SET_MISMATCH")
        frame = _pregame(frame, "prediction_timestamp_utc", schedule)
        cutoff_column = ("feature_cutoff_timestamp_utc" if "feature_cutoff_timestamp_utc" in frame
                         else "input_cutoff_timestamp_utc")
        cutoff = pd.to_datetime(frame[cutoff_column], utc=True, errors="coerce")
        published = pd.to_datetime(frame.prediction_timestamp_utc, utc=True, errors="coerce")
        durable = pd.to_datetime(marker.get("completed_at_utc")
                                  or metadata.get("actual_write_timestamp_utc"),
                                  utc=True, errors="coerce")
        starts = pd.to_datetime(frame.canonical_scheduled_start_time_utc, utc=True)
        if (cutoff.isna().any() or pd.isna(durable) or (cutoff > published).any()
                or (cutoff >= starts).any() or (published >= starts).any()
                or (durable >= starts).any()):
            raise RuntimeError(f"RESILIENT_{lane}_TIMESTAMP_NOT_PREGAME")
        for row in frame.itertuples(index=False):
            identity_payload = {
                "run_id": row.run_id, "game_id": int(row.game_id),
                id_column: int(getattr(row, id_column)), "line": float(row.line),
                "model_version": row.model_version,
            }
            if lane == "POINTS":
                identity_payload["probability_construction"] = row.probability_construction
            if prediction_only_digest(identity_payload) != row.prediction_identity:
                raise RuntimeError(f"RESILIENT_{lane}_PREDICTION_IDENTITY_MISMATCH")
        group_columns = (["game_id", "player_id", "model_version", "probability_construction"]
                         if lane == "POINTS" else
                         ["game_id", "goalie_id", "model_version", "prediction_semantics"])
        for _, group in frame.groupby(group_columns, dropna=False):
            validate_atomic_group(lane, group.to_dict(orient="records"))
        return frame

    # Legacy SOG producers use shorter cutoff field names and keep run_id in
    # run_metadata.json. Accept those equivalent hash-bound fields without
    # requiring the newer prop-observer CSV schema.
    if "run_id" not in frame and metadata.get("run_id"):
        frame["run_id"] = str(metadata["run_id"])
    if "input_cutoff_timestamp_utc" not in frame:
        cutoff_name = next((name for name in ("feature_cutoff_timestamp_utc",
                                               "feature_cutoff_utc", "input_cutoff_utc")
                            if name in frame), None)
        if cutoff_name:
            frame["input_cutoff_timestamp_utc"] = frame[cutoff_name]
    required_sog = {"run_id", "canonical_season", "slate_date", "game_id", "player_id", "line", "phase",
                    "prediction_timestamp_utc", "prediction_identity", "contract_arm", "contract_sha256",
                    "input_cutoff_timestamp_utc"}
    _require_columns(frame, required_sog, "RESILIENT_SOG")
    if (set(frame.run_id.astype(str)) != {str(metadata.get("run_id"))}
            or set(frame.phase.astype(str)) != {phase} or not frame.slate_date.astype(str).eq(slate_date).all()
            or frame.duplicated(["game_id", "player_id", "contract_arm", "line"]).any()
            or frame.prediction_identity.duplicated().any()
            or set(frame.contract_sha256.astype(str)) != {_sha(SOG_CONTRACT_PATH)}):
        raise RuntimeError("RESILIENT_SOG_SOURCE_IDENTITY_INVALID")
    spine = pd.read_csv(run / "canonical_game_spine.csv")
    _require_columns(spine, {"game_id", "scheduled_start_time_utc"}, "RESILIENT_SOG_CANONICAL_SPINE")
    if spine.game_id.duplicated().any() or set(spine.game_id.astype(int)) != set(schedule.game_id.astype(int)):
        raise RuntimeError("RESILIENT_SOG_CANONICAL_GAME_SET_MISMATCH")
    frame = _pregame(frame, "prediction_timestamp_utc", schedule)
    durable = pd.to_datetime(durable_publication_timestamp_utc
                              or marker.get("completed_at_utc")
                              or metadata.get("actual_write_timestamp_utc"),
                              utc=True, errors="coerce")
    starts = pd.to_datetime(frame.canonical_scheduled_start_time_utc, utc=True)
    cutoff = pd.to_datetime(frame.input_cutoff_timestamp_utc, utc=True, errors="coerce")
    published = pd.to_datetime(frame.prediction_timestamp_utc, utc=True, errors="coerce")
    if cutoff.isna().any() or pd.isna(durable) or (cutoff > published).any() or (durable >= starts).any():
        raise RuntimeError("RESILIENT_SOG_TIMESTAMP_NOT_PREGAME")
    for row in frame.itertuples(index=False):
        identity = sog_digest({
            "contract": row.contract_sha256, "arm": row.contract_arm, "phase": row.phase,
            "slate_date": row.slate_date, "game_id": int(row.game_id),
            "player_id": int(row.player_id), "line": float(row.line),
        })
        if identity != row.prediction_identity:
            raise RuntimeError("RESILIENT_SOG_PREDICTION_IDENTITY_MISMATCH")
    return frame


def _verify_prop_source(*, run: Path, lane: str, slate_date: str,
                        schedule: pd.DataFrame) -> dict[str, Any]:
    """Verify one immutable Points/Saves prediction-only publication."""
    files = _verify_manifest(run)
    metadata = json.loads((run / "run_metadata.json").read_text())
    expected_file = ("immutable_predictions.csv" if lane == "POINTS"
                     else "immutable_conditional_predictions.csv")
    required_manifest_files = {
        "RUN_COMPLETE.json", "run_metadata.json", "canonical_game_spine.csv",
        expected_file, "input_exclusions.csv",
    }
    coverage_contract = metadata.get("coverage_contract")
    if coverage_contract not in {None, "NHL_GAME_LEVEL_PREGAME_COVERAGE_V1"}:
        raise RuntimeError(f"IMMUTABLE_{lane}_COVERAGE_CONTRACT_UNSUPPORTED")
    if coverage_contract == "NHL_GAME_LEVEL_PREGAME_COVERAGE_V1":
        required_manifest_files.add("game_exclusions.csv")
    if lane == "POINTS":
        required_manifest_files.add("prediction_exclusions.csv")
    if not required_manifest_files.issubset(files):
        raise RuntimeError(f"IMMUTABLE_{lane}_MANIFEST_COVERAGE_MISMATCH")
    completion = json.loads((run / "RUN_COMPLETE.json").read_text())
    frame = pd.read_csv(run / expected_file)
    id_column = "player_id" if lane == "POINTS" else "goalie_id"
    required = {"run_id", "game_id", id_column, "line", "prob_over", "phase",
                "prediction_timestamp_utc", "input_cutoff_timestamp_utc",
                "model_version", "prediction_identity", "scheduled_start_time_utc",
                "game_type_code", "market_qualified", "price"}
    if lane == "POINTS":
        required |= {"prediction_eligible", "ladder_coherence_decision"}
    else:
        required |= {"starter_state", "selected_starter", "prediction_semantics"}
    _require_columns(frame, required, lane)
    source_spine = pd.read_csv(run / "canonical_game_spine.csv")
    _require_columns(source_spine, {"game_id", "scheduled_start_time_utc"},
                     f"{lane}_CANONICAL_GAME_SPINE")
    canonical_ids = set(schedule.game_id.astype(int))
    if (source_spine.game_id.duplicated().any()
            or set(source_spine.game_id.astype(int)) != canonical_ids
            or int(metadata.get("games", -1)) != len(canonical_ids)):
        raise RuntimeError(f"IMMUTABLE_{lane}_CANONICAL_GAME_SET_MISMATCH")
    if (metadata.get("lane") != lane or metadata.get("slate_date") != slate_date
            or metadata.get("phase") != "FINAL_PREGAME"
            or run.name != f"run_id={metadata.get('run_id')}"
            or set(frame.run_id.astype(str)) != {str(metadata.get("run_id"))}
            or set(frame.phase.astype(str)) != {"FINAL_PREGAME"}
            or completion.get("status") != "COMPLETE"
            or completion.get("run_id") != metadata.get("run_id")):
        raise RuntimeError(f"IMMUTABLE_{lane}_RUN_IDENTITY_MISMATCH")
    natural = ["game_id", id_column, "line"]
    if frame.duplicated(natural).any() or frame.prediction_identity.duplicated().any():
        raise RuntimeError(f"IMMUTABLE_{lane}_PREDICTION_IDENTITY_DUPLICATE")
    if set(frame.game_id.astype(int)) - canonical_ids:
        raise RuntimeError(f"IMMUTABLE_{lane}_GAME_IDENTITY_CONFLICT")
    qualified = frame.market_qualified.fillna(False).astype(bool)
    if qualified.any() or frame.loc[qualified, "price"].notna().any():
        raise RuntimeError(f"IMMUTABLE_{lane}_UNEXPECTED_MARKET_ATTACHMENT")
    _pregame(frame, "prediction_timestamp_utc", schedule)
    cutoff = pd.to_datetime(frame.input_cutoff_timestamp_utc, utc=True, errors="coerce")
    observed = pd.to_datetime(frame.prediction_timestamp_utc, utc=True, errors="coerce")
    if cutoff.isna().any() or observed.isna().any() or (cutoff > observed).any():
        raise RuntimeError(f"IMMUTABLE_{lane}_TIMESTAMP_INVALID")
    metadata_observed = pd.to_datetime(
        metadata.get("observation_timestamp_utc"), utc=True, errors="coerce")
    durable_write = pd.to_datetime(
        metadata.get("actual_write_timestamp_utc"), utc=True, errors="coerce")
    if pd.isna(metadata_observed) or pd.isna(durable_write):
        raise RuntimeError(f"IMMUTABLE_{lane}_DURABLE_WRITE_NOT_PREGAME")
    prediction_games = set(frame.game_id.astype(int))
    if coverage_contract == "NHL_GAME_LEVEL_PREGAME_COVERAGE_V1":
        if durable_write < metadata_observed or not observed.eq(metadata_observed).all():
            raise RuntimeError(f"IMMUTABLE_{lane}_OBSERVATION_IDENTITY_MISMATCH")
        declared_canonical = sorted(map(int, metadata.get("canonical_game_ids") or []))
        eligible_ids = sorted(map(int, metadata.get("eligible_game_ids") or []))
        prediction_ids = sorted(map(int, metadata.get("prediction_game_ids") or []))
        started_ids = sorted(map(int, metadata.get("started_excluded_game_ids") or []))
        schedule_starts = schedule.assign(_game_id=schedule.game_id.astype(int)).set_index(
            "_game_id").scheduled_start_time_utc
        expected_eligible = {
            game_id for game_id in canonical_ids
            if pd.to_datetime(schedule_starts.loc[game_id], utc=True, errors="coerce")
            > metadata_observed
        }
        expected_started = canonical_ids - expected_eligible
        if (declared_canonical != sorted(canonical_ids)
                or metadata.get("canonical_game_set_hash") != canonical_game_set_hash(canonical_ids)
                or metadata.get("eligible_game_set_hash") != canonical_game_set_hash(eligible_ids)
                or len(eligible_ids) != len(set(eligible_ids))
                or len(prediction_ids) != len(set(prediction_ids))
                or len(started_ids) != len(set(started_ids))
                or set(eligible_ids) != expected_eligible
                or set(started_ids) != expected_started
                or set(prediction_ids) != prediction_games
                or not prediction_games.issubset(set(eligible_ids))):
            raise RuntimeError(f"IMMUTABLE_{lane}_GAME_LEVEL_COVERAGE_IDENTITY_MISMATCH")
        exclusions = pd.read_csv(run / "game_exclusions.csv")
        _require_columns(exclusions, {"game_id", "reason"}, f"{lane}_GAME_EXCLUSIONS")
        exclusion_ids = exclusions.game_id.astype(int).tolist()
        reasons = exclusions.reason.astype(str).str.strip()
        if (len(exclusion_ids) != len(set(exclusion_ids))
                or reasons.eq("").any()
                or set(exclusion_ids) - canonical_ids
                or set(exclusion_ids) & prediction_games
                or not set(started_ids).issubset(set(exclusion_ids))
                or set(eligible_ids) - prediction_games - set(exclusion_ids)):
            raise RuntimeError(f"IMMUTABLE_{lane}_GAME_LEVEL_EXCLUSION_MISMATCH")
        reason_by_game = dict(zip(exclusion_ids, reasons))
        eligible_without_predictions = set(eligible_ids) - prediction_games
        allowed_reasons = {
            "GAME_ALREADY_STARTED", "GAME_STARTED_BEFORE_DURABLE_CAPTURE",
            "NO_ACTIVE_CUTOFF_BOUNDED_ROSTER_IDENTITIES",
            ("NO_POINTS_ROWS_PASS_LADDER_COHERENCE_OR_FEATURE_INPUTS"
             if lane == "POINTS" else "NO_QUALIFIED_SAVES_FEATURE_INPUTS"),
        }
        if (any(reason_by_game.get(game_id) != "GAME_ALREADY_STARTED"
                for game_id in started_ids)
                or any(reason_by_game.get(game_id) not in allowed_reasons
                       for game_id in eligible_without_predictions)):
            raise RuntimeError(f"IMMUTABLE_{lane}_GAME_LEVEL_EXCLUSION_REASON_INVALID")
        expected_coverage = (
            "FULL_SLATE"
            if len(eligible_ids) == len(canonical_ids) and exclusions.empty
            and int(metadata.get("input_excluded_player_games" if lane == "POINTS"
                                 else "input_excluded_goalie_games", 0)) == 0
            and int(metadata.get("ladder_excluded_player_games", 0)) == 0
            else "PARTIAL_GAME_OR_PLAYER_COVERAGE"
        )
        if metadata.get("coverage_status") != expected_coverage:
            raise RuntimeError(f"IMMUTABLE_{lane}_COVERAGE_STATUS_MISMATCH")
        for game_id in prediction_games:
            start = pd.to_datetime(schedule_starts.loc[game_id], utc=True, errors="coerce")
            if metadata_observed >= start or durable_write >= start:
                raise RuntimeError(f"IMMUTABLE_{lane}_DURABLE_WRITE_NOT_PREGAME:{game_id}")
    else:
        first_puck = pd.to_datetime(
            schedule.scheduled_start_time_utc, utc=True, errors="raise").min()
        if metadata_observed >= first_puck or durable_write >= first_puck:
            raise RuntimeError(f"IMMUTABLE_{lane}_DURABLE_WRITE_NOT_PREGAME")
    probabilities = pd.to_numeric(frame.prob_over, errors="coerce")
    if probabilities.isna().any() or (~probabilities.between(0.0, 1.0)).any():
        raise RuntimeError(f"IMMUTABLE_{lane}_PROBABILITY_INVALID")
    expected_lines = ({0.5, 1.5, 2.5} if lane == "POINTS" else
                      {18.5 + value for value in range(13)})
    if set(frame.line.astype(float)) != expected_lines:
        raise RuntimeError(f"IMMUTABLE_{lane}_LADDER_LINE_SET_MISMATCH")
    if lane == "POINTS" and not frame.prediction_eligible.fillna(False).astype(bool).all():
        raise RuntimeError("IMMUTABLE_POINTS_INELIGIBLE_ROW_IN_PREDICTION_SURFACE")
    if lane == "SAVES" and set(frame.starter_state.astype(str)) != {
            "UNKNOWN_NO_AUTHORIZED_PREGAME_STARTER_SOURCE"}:
        raise RuntimeError("IMMUTABLE_SAVES_STARTER_STATE_MISMATCH")
    if lane == "SAVES" and frame.selected_starter.fillna(False).astype(bool).any():
        raise RuntimeError("IMMUTABLE_SAVES_UNAUTHORIZED_PREGAME_STARTER_SELECTION")
    for row in frame.itertuples(index=False):
        identity_payload = {
            "run_id": row.run_id, "game_id": int(row.game_id),
            id_column: int(getattr(row, id_column)), "line": float(row.line),
            "model_version": row.model_version,
        }
        # Older immutable Points packages predate probability construction as
        # an identity component. Presence in the manifest-bound prediction CSV
        # identifies the newer identity shape; do not rewrite historical runs.
        if lane == "POINTS" and "probability_construction" in frame.columns:
            identity_payload["probability_construction"] = row.probability_construction
        identity = prediction_only_digest(identity_payload)
        if identity != row.prediction_identity:
            raise RuntimeError(f"IMMUTABLE_{lane}_PREDICTION_IDENTITY_MISMATCH")
    population = frame[["game_id", id_column]].drop_duplicates()
    expected_population_key = "player_games" if lane == "POINTS" else "goalie_games"
    if (len(frame) != int(metadata.get("prediction_rows", -1))
            or len(population) != int(metadata.get(expected_population_key, -1))):
        raise RuntimeError(f"IMMUTABLE_{lane}_ROW_COUNT_MISMATCH")
    return {
        "status": "PROSPECTIVE_SOURCE_BOUND", "run": str(run),
        "manifest_sha256": _sha(run / "SHA256SUMS"), "files": files,
        "prediction_rows": len(frame), expected_population_key: len(population),
        "observation_timestamp_utc": str(metadata.get("observation_timestamp_utc")),
        "coverage_status": metadata.get("coverage_status", "LEGACY_FULL_SLATE_CONTRACT"),
        "canonical_game_ids": sorted(canonical_ids),
        "eligible_game_ids": sorted(map(int, metadata.get("eligible_game_ids") or canonical_ids)),
        "prediction_game_ids": sorted(prediction_games),
        "game_exclusions": (pd.read_csv(run / "game_exclusions.csv").to_dict("records")
                            if (run / "game_exclusions.csv").is_file() else []),
    }


def _resolve_prop_prediction_source(*, runs: list[Path], lane: str,
                                    slate_date: str,
                                    schedule: pd.DataFrame,
                                    allow_missing_prediction: bool = False) -> dict[str, Any]:
    """Resolve a prospective Points/Saves card without hiding corrupt sources.

    An entirely absent prospective Points publication means that there is no
    immutable prediction to grade. It is an explicit zero-grade outcome, not
    a retrospective prediction opportunity. Saves retains its exact-one
    requirement outside prior-learning mode. Any present card is fully validated, and competing cards
    remain fatal for both lanes.
    """
    if len(runs) > 1:
        raise RuntimeError(f"IMMUTABLE_{lane}_FINAL_PREGAME_RUN_CARDINALITY:{len(runs)}")
    if not runs:
        if lane == "POINTS":
            return {
                "status": "NOT_GRADEABLE_NO_PREDICTION",
                "reason": "NO_VALID_IMMUTABLE_FINAL_PREGAME_PREDICTION",
            }
        if allow_missing_prediction:
            return {
                "status": "NOT_GRADEABLE_NO_PREDICTION",
                "reason": "NO_VALID_IMMUTABLE_FINAL_PREGAME_PREDICTION",
            }
        raise RuntimeError(f"IMMUTABLE_{lane}_FINAL_PREGAME_RUN_CARDINALITY:0")
    return _verify_prop_source(run=runs[0], lane=lane, slate_date=slate_date,
                               schedule=schedule)


def _require_columns(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame))
    if missing:
        raise RuntimeError(f"IMMUTABLE_{label}_SCHEMA_INCOMPLETE:{missing}")


def _local_spine(schedule: pd.DataFrame, slate_date: str) -> pd.DataFrame:
    required = {"canonical_season", "slate_date", "game_id", "scheduled_start_time_utc",
                "home_team_id", "away_team_id", "game_type_code"}
    _require_columns(schedule, required, "CANONICAL_GAME_SPINE")
    if schedule.empty or schedule.game_id.duplicated().any():
        raise RuntimeError("IMMUTABLE_CANONICAL_GAME_IDENTITY_INVALID")
    if not schedule.slate_date.astype(str).eq(slate_date).all():
        raise RuntimeError("IMMUTABLE_CANONICAL_SLATE_DATE_MISMATCH")
    if not schedule.canonical_season.astype(int).eq(2026).all():
        raise RuntimeError("IMMUTABLE_CANONICAL_SEASON_MISMATCH")
    return schedule


def _cross_market_final_runs(operational_root: Path, slate_date: str) -> list[Path]:
    """Return only FINAL_PREGAME states explicitly retained for this slate."""
    pattern = (Path(operational_root) / "cross_market_shadow" / "season=2026" /
               f"slate_date={slate_date}" / "run_type=FINAL_PREGAME" / "state=*")
    return sorted(pattern.parent.glob(pattern.name))


def _read_immutable_puck_predictions(path: Path) -> pd.DataFrame:
    """Read hash-bound CSV floats without changing their binary values."""
    return pd.read_csv(path, float_precision="round_trip")


def _verify_cross_market_identities(
    moneyline: pd.DataFrame,
    puck: pd.DataFrame,
    *,
    moneyline_feature_provenance: pd.DataFrame | None = None,
) -> None:
    provenance_by_game: dict[int, dict[str, Any]] = {}
    if moneyline_feature_provenance is not None:
        required = {"canonical_season", "slate_date", "game_id", "scheduled_start_time_utc",
                    "home_team", "away_team", "feature_vector_json"}
        _require_columns(moneyline_feature_provenance, required, "MONEYLINE_FEATURE_PROVENANCE")
        if moneyline_feature_provenance.game_id.duplicated().any():
            raise RuntimeError("IMMUTABLE_MONEYLINE_FEATURE_PROVENANCE_IDENTITY_CONFLICT")
        for provenance in moneyline_feature_provenance.itertuples(index=False):
            try:
                values = json.loads(provenance.feature_vector_json)
            except (TypeError, json.JSONDecodeError):
                raise RuntimeError("IMMUTABLE_MONEYLINE_FEATURE_PROVENANCE_INVALID") from None
            overlap = set(CROSS_MARKET_FEATURES).intersection(values)
            if not overlap:
                raise RuntimeError("IMMUTABLE_MONEYLINE_FEATURE_PROVENANCE_SCHEMA_INVALID")
            values = {name: values[name] for name in overlap}
            provenance_by_game[int(provenance.game_id)] = {
                "canonical_season": int(provenance.canonical_season),
                "slate_date": str(provenance.slate_date),
                "scheduled_start_time_utc": pd.Timestamp(
                    provenance.scheduled_start_time_utc).isoformat(),
                "home_team": str(provenance.home_team),
                "away_team": str(provenance.away_team),
                "features": values,
            }

    for row in moneyline.itertuples(index=False):
        raw = {name: getattr(row, name) for name in CROSS_MARKET_FEATURES}
        observed_identity = moneyline_substantive_identity(
            canonical_season=row.canonical_season, game_id=row.game_id,
            scheduled_start_time_utc=row.scheduled_start_time_utc,
            home_team=row.home_team, away_team=row.away_team,
            features=raw, parameter_sha256=_sha(MONEYLINE_PARAMETER_PATH),
        )
        if observed_identity != row.substantive_prediction_sha256:
            # Older packages used pandas' default CSV float formatting after
            # hashing the in-memory values. Accept a mismatch only when the
            # same manifest-bound provenance record supplies the exact values,
            # matches this row's full identity, and differs solely by tiny
            # float serialization error.
            provenance = provenance_by_game.get(int(row.game_id))
            if provenance is None or any((
                provenance[key] != expected for key, expected in (
                    ("canonical_season", int(row.canonical_season)),
                    ("slate_date", str(row.slate_date)),
                    ("scheduled_start_time_utc", pd.Timestamp(row.scheduled_start_time_utc).isoformat()),
                    ("home_team", str(row.home_team)),
                    ("away_team", str(row.away_team)),
                )
            )):
                raise RuntimeError("IMMUTABLE_MONEYLINE_SUBSTANTIVE_IDENTITY_MISMATCH")
            precise_features = provenance["features"]
            if any(not math.isclose(
                float(raw[name]), float(precise_features[name]), rel_tol=0.0, abs_tol=1e-15
            ) for name in precise_features):
                raise RuntimeError("IMMUTABLE_MONEYLINE_SUBSTANTIVE_IDENTITY_MISMATCH")
            verified_features = {**raw, **precise_features}
            if moneyline_substantive_identity(
                canonical_season=row.canonical_season, game_id=row.game_id,
                scheduled_start_time_utc=row.scheduled_start_time_utc,
                home_team=row.home_team, away_team=row.away_team,
                features=verified_features, parameter_sha256=_sha(MONEYLINE_PARAMETER_PATH),
            ) != row.substantive_prediction_sha256:
                raise RuntimeError("IMMUTABLE_MONEYLINE_SUBSTANTIVE_IDENTITY_MISMATCH")
    for row in puck.itertuples(index=False):
        raw = {name: getattr(row, name) for name in CROSS_MARKET_FEATURES}
        substantive = {
            "canonical_season": int(row.canonical_season), "game_id": int(row.game_id),
            "scheduled_start_time_utc": str(row.scheduled_start_time_utc),
            "home_team": row.home_team, "away_team": row.away_team,
            "features": raw, "control_artifact_sha256": _sha(PUCK_PARAMETER_PATH),
        }
        if digest_value(substantive) != row.substantive_prediction_sha256:
            provenance = provenance_by_game.get(int(row.game_id))
            if provenance is None or any((
                provenance[key] != expected for key, expected in (
                    ("canonical_season", int(row.canonical_season)),
                    ("slate_date", str(row.slate_date)),
                    ("scheduled_start_time_utc", pd.Timestamp(row.scheduled_start_time_utc).isoformat()),
                    ("home_team", str(row.home_team)),
                    ("away_team", str(row.away_team)),
                )
            )):
                raise RuntimeError("IMMUTABLE_PUCK_LINE_SUBSTANTIVE_IDENTITY_MISMATCH")
            precise_features = provenance["features"]
            if any(not math.isclose(
                float(raw[name]), float(precise_features[name]), rel_tol=0.0, abs_tol=1e-15
            ) for name in precise_features):
                raise RuntimeError("IMMUTABLE_PUCK_LINE_SUBSTANTIVE_IDENTITY_MISMATCH")
            substantive["features"].update(precise_features)
            if digest_value(substantive) != row.substantive_prediction_sha256:
                raise RuntimeError("IMMUTABLE_PUCK_LINE_SUBSTANTIVE_IDENTITY_MISMATCH")


def resolve_operational_sources(*, slate_date: str, operational_root: Path,
                                allow_missing_active_final_for_prior_learning: bool = False,
                                allow_missing_final_for_resilient: bool = False,
                                ) -> dict[str, Any]:
    """Resolve and fully validate immutable local inputs without I/O outside disk."""
    cross_runs = _cross_market_final_runs(operational_root, slate_date)
    if len(cross_runs) > 1:
        raise RuntimeError(f"IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:{len(cross_runs)}")
    # Preserve the established validation order for an existing card: its
    # manifest is checked before any other source cardinality is considered.
    cross_manifest = _verify_manifest(cross_runs[0]) if cross_runs else {}
    sog_runs = sorted((operational_root / "sog_prediction_only" / "season=2026" /
                       f"slate_date={slate_date}" / "phase=FINAL_PREGAME").glob("run_id=*"))
    if len(sog_runs) > 1:
        raise RuntimeError(f"IMMUTABLE_SOG_FINAL_PREGAME_RUN_CARDINALITY:{len(sog_runs)}")
    sog = sog_runs[0] if sog_runs else None
    if sog is None and not allow_missing_final_for_resilient:
        raise RuntimeError("IMMUTABLE_SOG_FINAL_PREGAME_RUN_CARDINALITY:0")
    if sog is not None:
        sog_manifest = _verify_manifest(sog)
        sog_meta = json.loads((sog / "run_metadata.json").read_text())
        if (sog_meta.get("slate_date") != slate_date or sog_meta.get("phase") != "FINAL_PREGAME"
                or sog.name != f"run_id={sog_meta.get('run_id')}"):
            raise RuntimeError("IMMUTABLE_SOG_RUN_IDENTITY_MISMATCH")
        sog_spine = pd.read_csv(sog / "canonical_game_spine.csv").rename(columns={"game_type": "game_type_code"})
        sog_spine = _local_spine(sog_spine, slate_date)
        retained_schedule_source = None
    else:
        sog_manifest = {}
        sog_meta = {}
        sog_spine, retained_schedule_source = _retained_schedule_spine(
            operational_root=operational_root, slate_date=slate_date)

    activation = json.loads(CROSS_MARKET_ACTIVATION_PATH.read_text())
    cross, cross_resolution = _select_cross_market_final_run(
        runs=cross_runs, schedule=sog_spine, activation=activation,
        allow_missing_active_final=(allow_missing_active_final_for_prior_learning
                                    or allow_missing_final_for_resilient),
        allow_missing_active_final_for_resilient=(allow_missing_final_for_resilient
                                                  and not allow_missing_active_final_for_prior_learning))
    cross_observed: pd.Timestamp | None = None
    retained_schedule_source: dict[str, str] | None = None
    if cross is None:
        # SOG's immutable, manifest-verified canonical spine supplies the slate
        # identity. No Moneyline/Puck Line prediction or market evidence is
        # invented when their card is absent in a permitted no-capture state.
        retained_schedule_path = operational_root / "slates" / slate_date / "raw_schedule_response.json"
        if not retained_schedule_path.is_file():
            raise RuntimeError("IMMUTABLE_RETAINED_SCHEDULE_SOURCE_MISSING")
        schedule, retained_schedule_hash = _attach_retained_schedule_codes(
            sog_spine, path=retained_schedule_path, slate_date=slate_date)
        retained_schedule_source = {
            "path": str(retained_schedule_path), "sha256": retained_schedule_hash,
        }
        moneyline = pd.DataFrame()
        puck = pd.DataFrame()
    else:
        status = json.loads((cross / "daily_execution_status.json").read_text())
        if (status.get("slate_date") != slate_date or status.get("run_type") != "FINAL_PREGAME"
                or cross.name != f"state={status.get('substantive_state_sha256')}"):
            raise RuntimeError("IMMUTABLE_CROSS_MARKET_RUN_IDENTITY_MISMATCH")
        promoted_capture = status.get("mode") == "PROSPECTIVE_PRODUCTION_WITH_SHADOW_CHALLENGER"
        schedule = pd.read_csv(cross / "schedule_event_identity.csv")
        schedule = _local_spine(schedule, slate_date)
        v3_production = cross / "moneyline_production_predictions.csv"
        if promoted_capture and not v3_production.is_file():
            raise RuntimeError("IMMUTABLE_MONEYLINE_PRODUCTION_ARTIFACT_MISSING")
        v2_shadow = pd.read_csv(cross / "v2_immutable_predictions.csv",
                                float_precision="round_trip")
        moneyline = pd.read_csv(v3_production if v3_production.is_file()
                                else cross / "v2_immutable_predictions.csv",
                                float_precision="round_trip")
        provenance_name = "moneyline_shot_finishing_challenger_v3_feature_provenance.csv"
        moneyline_provenance_path = cross / provenance_name
        moneyline_provenance = (
            pd.read_csv(moneyline_provenance_path)
            if provenance_name in cross_manifest and moneyline_provenance_path.is_file() else None
        )
        # These immutable rows hash the original in-memory float values.
        # pandas' default CSV parser can shift the last bit for a small subset
        # of decimals, causing valid stored identities to fail verification.
        puck = _read_immutable_puck_predictions(
            cross / "puck_line_v1_immutable_predictions.csv")
        common = {"canonical_season", "slate_date", "game_id", "scheduled_start_time_utc",
                  "home_team_id", "away_team_id", "home_team", "away_team",
                  "prediction_creation_time_utc", "substantive_prediction_sha256"} | set(CROSS_MARKET_FEATURES)
        _require_columns(v2_shadow, common, "MONEYLINE_SHADOW")
        _require_columns(puck, common, "PUCK_LINE")
        ids = set(schedule.game_id.astype(int))
        _verify_cross_market_identities(
            v2_shadow, puck, moneyline_feature_provenance=moneyline_provenance)
        if v3_production.is_file():
            v3_required = {"canonical_season", "slate_date", "game_id",
                           "scheduled_start_time_utc", "home_team_id", "away_team_id",
                           "home_team", "away_team", "prediction_creation_time_utc",
                           "model_version", "model_parameter_sha256",
                           "challenger_home_win_probability", "challenger_away_win_probability",
                           "challenger_favored_team", "operational_role"}
            _require_columns(moneyline, v3_required, "MONEYLINE_PRODUCTION")
            if (set(moneyline.model_version.astype(str)) != {"NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3"}
                    or set(moneyline.operational_role.astype(str)) != {"PRODUCTION_CHAMPION"}
                    or moneyline.game_id.duplicated().any()
                    or set(moneyline.game_id.astype(int)) != ids):
                raise RuntimeError("IMMUTABLE_MONEYLINE_PRODUCTION_IDENTITY_CONFLICT")
            from backend.nhl.cross_market_shadow.moneyline_challenger import load_model
            expected_model_sha = load_model()["parameter_file_sha256"]
            if set(moneyline.model_parameter_sha256.astype(str)) != {expected_model_sha}:
                raise RuntimeError("IMMUTABLE_MONEYLINE_PRODUCTION_MODEL_SHA_MISMATCH")
            if not np.allclose(
                    pd.to_numeric(moneyline.challenger_home_win_probability),
                    1 - pd.to_numeric(moneyline.challenger_away_win_probability),
                    rtol=0, atol=1e-12):
                raise RuntimeError("IMMUTABLE_MONEYLINE_PRODUCTION_PROBABILITY_PAIR_INVALID")
        for label, frame in (("MONEYLINE", moneyline), ("PUCK_LINE", puck)):
            if frame.game_id.duplicated().any() or set(frame.game_id.astype(int)) != ids:
                raise RuntimeError(f"IMMUTABLE_{label}_GAME_IDENTITY_CONFLICT")
            if not frame.slate_date.astype(str).eq(slate_date).all():
                raise RuntimeError(f"IMMUTABLE_{label}_SLATE_DATE_MISMATCH")
            _pregame(frame, "prediction_creation_time_utc", schedule)
        if not v3_production.is_file():
            _verify_cross_market_identities(
                moneyline, puck, moneyline_feature_provenance=moneyline_provenance)
        cross_observed = pd.to_datetime(status["run_timestamp_utc"], utc=True)

    ids = set(schedule.game_id.astype(int))
    spine_fields = ["game_id", "scheduled_start_time_utc", "home_team_id", "away_team_id"]
    left = schedule[spine_fields].copy(); right = sog_spine[spine_fields].copy()
    for frame in (left, right):
        frame["scheduled_start_time_utc"] = pd.to_datetime(frame.scheduled_start_time_utc, utc=True)
    if not left.sort_values("game_id").reset_index(drop=True).equals(right.sort_values("game_id").reset_index(drop=True)):
        raise RuntimeError("IMMUTABLE_SOURCE_CANONICAL_GAME_SET_CONFLICT")
    predictions = (pd.read_csv(sog / "immutable_predictions.csv") if sog is not None
                   else pd.DataFrame(columns=["canonical_season", "slate_date", "game_id",
                                              "player_id", "line", "phase", "prediction_timestamp_utc",
                                              "prediction_identity", "contract_arm", "contract_sha256"]))
    exclusions = (pd.read_csv(sog / "excluded_players.csv") if sog is not None
                  else pd.DataFrame(columns=["game_id", "player_id", "slate_date"]))
    required_sog = {"canonical_season", "slate_date", "game_id", "player_id", "line", "phase",
                    "prediction_timestamp_utc", "prediction_identity", "contract_arm", "contract_sha256"}
    if sog is not None:
        _require_columns(predictions, required_sog, "SOG")
    if predictions.prediction_identity.duplicated().any():
        raise RuntimeError("IMMUTABLE_SOG_PREDICTION_IDENTITY_DUPLICATE")
    if sog is not None and (set(predictions.phase.astype(str)) != {"FINAL_PREGAME"}
            or set(predictions.contract_sha256.astype(str)) != {_sha(SOG_CONTRACT_PATH)}):
        raise RuntimeError("IMMUTABLE_SOG_CONTRACT_IDENTITY_MISMATCH")
    if set(predictions.game_id.astype(int)) - ids or not predictions.slate_date.astype(str).eq(slate_date).all():
        raise RuntimeError("IMMUTABLE_SOG_GAME_IDENTITY_CONFLICT")
    if (exclusions.duplicated(["game_id", "player_id"]).any()
            or set(exclusions.game_id.astype(int)) - ids
            or not exclusions.slate_date.astype(str).eq(slate_date).all()):
        raise RuntimeError("IMMUTABLE_SOG_EXCLUSION_IDENTITY_CONFLICT")
    if sog is not None:
        _pregame(predictions, "prediction_timestamp_utc", schedule)
    for row in predictions.itertuples(index=False):
        identity = sog_digest({
            "contract": row.contract_sha256, "arm": row.contract_arm, "phase": row.phase,
            "slate_date": row.slate_date, "game_id": int(row.game_id),
            "player_id": int(row.player_id), "line": float(row.line),
        })
        if identity != row.prediction_identity:
            raise RuntimeError("IMMUTABLE_SOG_PREDICTION_IDENTITY_MISMATCH")
    if sog is not None and (len(predictions) != int(sog_meta.get("prediction_rows", -1))
            or predictions.player_id.nunique() != int(sog_meta.get("admitted_players", -1))
            or len(exclusions) != int(sog_meta.get("excluded_players", -1))):
        raise RuntimeError("IMMUTABLE_SOG_ROW_COUNT_MISMATCH")
    if slate_date == "2026-09-20" and (
            len(schedule) != 7 or len(moneyline) != 7 or len(puck) != 7
            or len(predictions) != 5517 or predictions.player_id.nunique() != 408
            or len(exclusions) != 45):
        raise RuntimeError("SEPTEMBER_20_IMMUTABLE_SOURCE_CARDINALITY_MISMATCH")

    if slate_date < PROSPECTIVE_PROP_NOT_BEFORE:
        early_points = list((operational_root / "points_prediction_only" / "season=2026" /
                             f"slate_date={slate_date}" / "phase=FINAL_PREGAME").glob("run_id=*"))
        early_saves = list((operational_root / "saves_prediction_only" / "season=2026" /
                            f"slate_date={slate_date}" / "phase=FINAL_PREGAME").glob("run_id=*"))
        if early_points or early_saves:
            raise RuntimeError("RETROSPECTIVE_POINTS_OR_SAVES_SOURCE_FORBIDDEN")
        points = {"status": "NO_PROSPECTIVE_POINTS_PREDICTIONS", "reason": "PROSPECTIVE_NOT_BEFORE_2026-09-21"}
        saves = {"status": "NO_PROSPECTIVE_SAVES_PREDICTIONS", "reason": "PROSPECTIVE_NOT_BEFORE_2026-09-21"}
    else:
        points_runs = list((operational_root / "points_prediction_only" / "season=2026" / f"slate_date={slate_date}" / "phase=FINAL_PREGAME").glob("run_id=*"))
        saves_runs = list((operational_root / "saves_prediction_only" / "season=2026" / f"slate_date={slate_date}" / "phase=FINAL_PREGAME").glob("run_id=*"))
        points = _resolve_prop_prediction_source(
            runs=points_runs, lane="POINTS", slate_date=slate_date,
            schedule=schedule)
        saves = _resolve_prop_prediction_source(
            runs=saves_runs, lane="SAVES", slate_date=slate_date,
            schedule=schedule,
            allow_missing_prediction=(allow_missing_active_final_for_prior_learning
                                      or allow_missing_final_for_resilient))

    observed = (pd.to_datetime(sog_meta["prediction_timestamp_utc"], utc=True)
                if sog is not None else None)
    first_puck = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True).min()
    if ((observed is not None and observed >= first_puck)
            or (cross_observed is not None and cross_observed >= first_puck)):
        raise RuntimeError("IMMUTABLE_SOURCE_OBSERVED_AFTER_FIRST_PUCK")
    return {
        "contract_version": "NHL_POSTGAME_LOCAL_SOURCE_BINDING_V1", "slate_date": slate_date,
        "operational_root": str(operational_root),
        "canonical_games": len(schedule), "game_ids": sorted(ids),
        "team_codes": sorted(set(schedule.home_team.astype(str))
                             | set(schedule.away_team.astype(str))),
        "canonical_game_set_hash": _hash_bytes(json.dumps(sorted(ids), separators=(",", ":")).encode()),
        "first_puck_utc": first_puck.isoformat(),
        "canonical_schedule_source": retained_schedule_source or {
            "path": str(cross / "schedule_event_identity.csv"),
            "sha256": cross_manifest.get("schedule_event_identity.csv"),
        },
        "cross_market": {"run": str(cross) if cross is not None else None,
                         "status": cross_resolution["status"],
                         "reason": cross_resolution.get("reason"),
                         "manifest_sha256": _sha(cross / "SHA256SUMS") if cross is not None else None,
                         "files": cross_manifest,
                         "observed_at_utc": cross_observed.isoformat() if cross_observed is not None else None,
                         "moneyline_rows": len(moneyline), "puck_line_rows": len(puck)},
        "sog": ({"run": str(sog), "manifest_sha256": _sha(sog / "SHA256SUMS"),
                 "files": sog_manifest, "observed_at_utc": observed.isoformat(),
                 "prediction_rows": len(predictions), "players": predictions.player_id.nunique(),
                 "exclusions": len(exclusions), "arms": sorted(predictions.contract_arm.unique())}
                if sog is not None else {
                    "run": None, "status": "NOT_GRADEABLE_NO_FINAL_PREGAME_PREDICTION",
                    "reason": "NO_IMMUTABLE_FINAL_PREGAME_SOURCE", "prediction_rows": 0,
                }),
        "points": points, "saves": saves,
    }


def _hash_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _frame_hash(frame: pd.DataFrame) -> str:
    ordered = frame.sort_values(list(frame.columns)).reset_index(drop=True) if len(frame) else frame
    return _hash_bytes(ordered.to_csv(index=False).encode())


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _seconds(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    parts = str(value).split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        return float(value) * 60
    except (TypeError, ValueError):
        return 0.0


def validate_final_slate(canonical: pd.DataFrame, official: pd.DataFrame, slate_date: str) -> pd.DataFrame:
    required = {"canonical_season", "slate_date", "game_id", "scheduled_start_time_utc",
                "home_team_id", "away_team_id", "game_type_code"}
    if required - set(canonical):
        raise RuntimeError(f"CANONICAL_SLATE_SCHEMA_INCOMPLETE:{sorted(required-set(canonical))}")
    official_required = {"game_id", "game_state", "home_team_id", "away_team_id", "home_score", "away_score"}
    if official_required - set(official):
        raise RuntimeError(f"OFFICIAL_SLATE_SCHEMA_INCOMPLETE:{sorted(official_required-set(official))}")
    if canonical.empty:
        raise RuntimeError("CANONICAL_ADMITTED_SLATE_EMPTY")
    if canonical.game_id.duplicated().any() or official.game_id.duplicated().any():
        raise RuntimeError("DUPLICATE_GAME_IDENTITY")
    if not canonical.slate_date.astype(str).eq(slate_date).all():
        raise RuntimeError("CANONICAL_SLATE_DATE_MISMATCH")
    if not canonical.canonical_season.astype(int).eq(2026).all():
        raise RuntimeError("CANONICAL_SEASON_MISMATCH")
    if not canonical.game_type_code.astype(int).isin(SUPPORTED_GAME_TYPES).all():
        raise RuntimeError("UNSUPPORTED_GAME_TYPE")
    if set(canonical.game_id.astype(int)) != set(official.game_id.astype(int)):
        raise RuntimeError("OFFICIAL_CANONICAL_GAME_SET_MISMATCH")
    merged = canonical.merge(official, on="game_id", how="left", suffixes=("_canonical", "_official"), validate="one_to_one")
    if not merged.game_state.astype(str).str.upper().isin(FINAL_STATES).all():
        unfinished = merged.loc[~merged.game_state.astype(str).str.upper().isin(FINAL_STATES), "game_id"].astype(int).tolist()
        raise RuntimeError(f"ADMITTED_GAMES_NOT_OFFICIAL_FINAL:{unfinished}")
    for side in ("home", "away"):
        canonical_col, official_col = f"{side}_team_id_canonical", f"{side}_team_id_official"
        if not merged[canonical_col].astype(int).eq(merged[official_col].astype(int)).all():
            raise RuntimeError(f"OFFICIAL_{side.upper()}_IDENTITY_MISMATCH")
    if pd.to_numeric(merged.home_score, errors="coerce").isna().any() or pd.to_numeric(merged.away_score, errors="coerce").isna().any():
        raise RuntimeError("OFFICIAL_FINAL_SCORE_MISSING")
    if pd.to_numeric(merged.home_score).eq(pd.to_numeric(merged.away_score)).any():
        raise RuntimeError("OFFICIAL_FINAL_SCORE_NOT_DECISIVE")
    return merged


def build_outcomes(validated: pd.DataFrame, boxscores: dict[int, dict], observed_at: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    games, skaters, goalies = [], [], []
    for game in validated.itertuples(index=False):
        gid = int(game.game_id)
        box = boxscores.get(gid)
        if not isinstance(box, dict):
            raise RuntimeError(f"OFFICIAL_BOXSCORE_MISSING:{gid}")
        home_score, away_score = int(game.home_score), int(game.away_score)
        games.append({
            "canonical_season": 2026, "slate_date": str(game.slate_date), "game_id": gid,
            "game_type_code": int(game.game_type_code), "home_team_id": int(game.home_team_id_canonical),
            "away_team_id": int(game.away_team_id_canonical), "official_final_home_goals": home_score,
            "official_final_away_goals": away_score,
            "official_full_game_winner": "HOME" if home_score > away_score else "AWAY",
            "official_final": True, "outcome_source": "NHL_OFFICIAL_GAMECENTER",
            "outcome_source_timestamp_utc": observed_at, "outcome_conflict_status": "NO_CONFLICT",
        })
        stats = box.get("playerByGameStats") or {}
        for side, team_id in (("homeTeam", int(game.home_team_id_canonical)),
                              ("awayTeam", int(game.away_team_id_canonical))):
            team = stats.get(side) or {}
            for group in ("forwards", "defense"):
                for row in team.get(group) or []:
                    player_id = row.get("playerId")
                    if player_id is None:
                        raise RuntimeError(f"SKATER_IDENTITY_MISSING:{gid}")
                    skaters.append({
                        "canonical_season": 2026, "slate_date": str(game.slate_date), "game_id": gid,
                        "player_id": int(player_id), "team_id": team_id,
                        "official_goals": int(row.get("goals") or 0),
                        "official_assists": int(row.get("assists") or 0),
                        "official_points": int(row.get("points") or ((row.get("goals") or 0) + (row.get("assists") or 0))),
                        "official_sog": int(row.get("sog") or row.get("shots") or 0),
                        "toi": row.get("toi"), "participation_state": "PARTICIPATED",
                        "official_final": True, "outcome_source": "NHL_OFFICIAL_GAMECENTER",
                        "outcome_source_timestamp_utc": observed_at,
                    })
            team_goalies = []
            for row in team.get("goalies") or []:
                player_id = row.get("playerId")
                if player_id is None:
                    raise RuntimeError(f"GOALIE_IDENTITY_MISSING:{gid}")
                saves = row.get("saves")
                shots = row.get("shotsAgainst")
                if saves is None and isinstance(row.get("savesToShots"), str) and "/" in row["savesToShots"]:
                    saves, shots = row["savesToShots"].split("/", 1)
                team_goalies.append({
                    "canonical_season": 2026, "slate_date": str(game.slate_date), "game_id": gid,
                    "goalie_id": int(player_id), "team_id": team_id,
                    "official_saves": None if saves is None else int(saves),
                    "official_shots_faced": None if shots is None else int(shots),
                    "toi": row.get("toi"), "_toi_seconds": _seconds(row.get("toi")),
                    "goalie_participation_state": "RELIEF_APPEARANCE",
                    "actual_start_flag": False, "outcome_source": "NHL_OFFICIAL_GAMECENTER",
                    "outcome_source_timestamp_utc": observed_at,
                })
            if not team_goalies:
                raise RuntimeError(f"OFFICIAL_GOALIE_TEAM_MEMBERSHIP_MISSING:{gid}:{team_id}")
            maximum_toi = max(item["_toi_seconds"] for item in team_goalies)
            winners = [index for index, item in enumerate(team_goalies)
                       if item["_toi_seconds"] == maximum_toi]
            if maximum_toi <= 0 or len(winners) != 1:
                raise RuntimeError(f"OFFICIAL_GOALIE_STARTER_NOT_UNIQUE:{gid}:{team_id}")
            starter = winners[0]
            team_goalies[starter]["goalie_participation_state"] = "STARTED"
            team_goalies[starter]["actual_start_flag"] = True
            for item in team_goalies:
                item["starter_identity_method"] = "UNIQUE_MAX_OFFICIAL_TOI_POSTGAME"
                item.pop("_toi_seconds")
                goalies.append(item)
    game_frame, skater_frame, goalie_frame = pd.DataFrame(games), pd.DataFrame(skaters), pd.DataFrame(goalies)
    if game_frame.game_id.duplicated().any() or skater_frame.duplicated(["game_id", "player_id"]).any() or goalie_frame.duplicated(["game_id", "goalie_id"]).any():
        raise RuntimeError("DUPLICATE_CANONICAL_OUTCOME_IDENTITY")
    return game_frame, skater_frame, goalie_frame


def validate_staging_identity_sets(
    *, expected_games: list[int], expected_skaters: list[tuple[int, int]],
    expected_goalies: list[tuple[int, int]], expected_starters: list[tuple[int, int]],
    actual_games: list[int], actual_skaters: list[tuple[int, int]],
    actual_goalies: list[tuple[int, int]], actual_starters: list[tuple[int, int]],
) -> dict[str, Any]:
    """Require exact official/stage identity equality before downstream work."""
    categories = {
        "games": (expected_games, actual_games),
        "skaters": (expected_skaters, actual_skaters),
        "goalies": (expected_goalies, actual_goalies),
        "starters": (expected_starters, actual_starters),
    }
    diagnostics: dict[str, Any] = {}
    failed = False
    for label, (expected_raw, actual_raw) in categories.items():
        expected = [tuple(value) if isinstance(value, tuple) else int(value)
                    for value in expected_raw]
        actual = [tuple(value) if isinstance(value, tuple) else int(value)
                  for value in actual_raw]
        expected_set, actual_set = set(expected), set(actual)
        duplicate_actual = sorted({value for value in actual if actual.count(value) > 1})
        missing = sorted(expected_set - actual_set)
        extra = sorted(actual_set - expected_set)
        diagnostics[label] = {
            "expected": len(expected_set), "actual": len(actual_set),
            "missing": missing, "extra": extra, "duplicates": duplicate_actual,
        }
        failed = failed or bool(missing or extra or duplicate_actual)
    if failed:
        raise RuntimeError(
            "NHL_POSTGAME_STAGING_IDENTITY_SET_MISMATCH:"
            + json.dumps(diagnostics, sort_keys=True, separators=(",", ":")))
    return {
        "contract_version": "NHL_POSTGAME_STAGING_COMPLETENESS_V1",
        "status": "EXACT_IDENTITY_SET_EQUALITY",
        "games": diagnostics["games"]["actual"],
        "skater_appearances": diagnostics["skaters"]["actual"],
        "goalie_appearances": diagnostics["goalies"]["actual"],
        "confirmed_starters": diagnostics["starters"]["actual"],
        "diagnostics": diagnostics,
    }


def _pregame(frame: pd.DataFrame, timestamp_col: str, schedule: pd.DataFrame) -> pd.DataFrame:
    spine = schedule[["game_id", "scheduled_start_time_utc", "game_type_code"]].rename(columns={
        "scheduled_start_time_utc": "canonical_scheduled_start_time_utc",
        "game_type_code": "canonical_game_type_code",
    })
    joined = frame.merge(spine, on="game_id", how="left", validate="many_to_one")
    if "scheduled_start_time_utc" in joined:
        supplied = pd.to_datetime(joined.scheduled_start_time_utc, utc=True, errors="coerce")
        canonical_starts = pd.to_datetime(joined.canonical_scheduled_start_time_utc, utc=True, errors="coerce")
        if supplied.isna().any() or not supplied.eq(canonical_starts).all():
            raise RuntimeError("PREDICTION_SCHEDULE_IDENTITY_MISMATCH")
    if "game_type_code" in joined:
        supplied_type = pd.to_numeric(joined.game_type_code, errors="coerce")
        canonical_type = pd.to_numeric(joined.canonical_game_type_code, errors="coerce")
        present = supplied_type.notna()
        if (canonical_type.isna().any()
                or not supplied_type.loc[present].astype(int).eq(
                    canonical_type.loc[present].astype(int)).all()):
            raise RuntimeError("PREDICTION_GAME_TYPE_IDENTITY_MISMATCH")
        joined["game_type_code"] = supplied_type.where(present, canonical_type).astype(int)
    else:
        joined["game_type_code"] = joined.canonical_game_type_code
    stamps = pd.to_datetime(joined[timestamp_col], utc=True, errors="coerce")
    starts = pd.to_datetime(joined.canonical_scheduled_start_time_utc, utc=True, errors="coerce")
    if stamps.isna().any() or starts.isna().any() or (stamps >= starts).any():
        raise RuntimeError("POST_START_PREDICTION_PRESENTED_AS_PREGAME")
    return joined


def _grade_game_lines(moneyline: pd.DataFrame, puck: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the canonical official game-type evaluation contract to ML/PL."""
    for frame in (moneyline, puck):
        if "game_type_code" not in frame:
            if not frame.empty:
                raise RuntimeError("CANONICAL_GAME_TYPE_MISSING_FROM_GRADE_ROWS")
            frame["game_type_code"] = pd.Series(dtype="Int64")
        frame["canonical_phase"] = frame.game_type_code.map(phase_for_game_type)
        eligible = frame.game_type_code.map(regular_season_evaluation_eligible)
        frame["grading_status"] = "PRESEASON_NON_EVALUATION"
        frame.loc[eligible, "grading_status"] = "REGULAR_SEASON_GRADED"
        frame.loc[frame.canonical_phase.eq("POSTSEASON"), "grading_status"] = "POSTSEASON_NON_REGULAR_SEASON_EVALUATION"
        frame.loc[frame.canonical_phase.eq("UNKNOWN_GAME_TYPE"), "grading_status"] = "GAME_TYPE_UNRESOLVED"
        frame["regular_season_evaluation_target"] = pd.NA
        winner = frame.official_full_game_winner
        frame.loc[eligible, "regular_season_evaluation_target"] = winner.map({"HOME": 1, "AWAY": 0})[eligible]
        if "model_favored_team" in frame and "home_team" in frame:
            home_won = winner.eq("HOME")
            frame["prediction_correct"] = frame.model_favored_team.eq(frame.home_team).eq(home_won).where(eligible)
        if frame is puck:
            margin = pd.to_numeric(frame.official_final_home_goals, errors="coerce") - pd.to_numeric(frame.official_final_away_goals, errors="coerce")
            frame["actual_margin_class"] = pd.Series(pd.NA, index=frame.index, dtype="string")
            frame.loc[eligible & margin.le(-2), "actual_margin_class"] = "AWAY_BY_2_PLUS"
            frame.loc[eligible & margin.abs().lt(2), "actual_margin_class"] = "ONE_GOAL_GAME"
            frame.loc[eligible & margin.ge(2), "actual_margin_class"] = "HOME_BY_2_PLUS"
            class_columns = ["away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability"]
            if set(class_columns).issubset(frame.columns):
                frame["predicted_margin_class"] = frame[class_columns].idxmax(axis=1).map({
                    "away_by_2_plus_probability": "AWAY_BY_2_PLUS",
                    "one_goal_game_probability": "ONE_GOAL_GAME",
                    "home_by_2_plus_probability": "HOME_BY_2_PLUS",
                })
                frame["prediction_correct"] = frame.predicted_margin_class.eq(frame.actual_margin_class).where(eligible)
    return moneyline, puck


def grade_catchup(prediction_root: Path, schedule: pd.DataFrame, games: pd.DataFrame,
                  skaters: pd.DataFrame, goalies: pd.DataFrame) -> dict[str, pd.DataFrame]:
    main_runs = list((prediction_root / "constructed/mainline").glob("season=2026/slate_date=*/run_type=*/state=*"))
    if len(main_runs) != 1:
        raise RuntimeError(f"IMMUTABLE_MAINLINE_RUN_CARDINALITY:{len(main_runs)}")
    main = main_runs[0]
    moneyline = _pregame(pd.read_csv(main / "v2_immutable_predictions.csv"), "prediction_creation_time_utc", schedule)
    puck = _pregame(pd.read_csv(main / "puck_line_v1_immutable_predictions.csv"), "prediction_creation_time_utc", schedule)
    moneyline = moneyline.merge(games[["game_id", "official_full_game_winner", "official_final_home_goals", "official_final_away_goals"]], on="game_id", validate="one_to_one")
    puck = puck.merge(games[["game_id", "official_full_game_winner", "official_final_home_goals", "official_final_away_goals"]], on="game_id", validate="one_to_one")
    moneyline, puck = _grade_game_lines(moneyline, puck)

    props = prediction_root / "constructed/independent_props"
    summary = json.loads((props / "lane_prediction_summary.json").read_text())
    observation = summary["observation_timestamp_utc"]
    points = pd.read_csv(props / "points_immutable_predictions.csv")
    points["prediction_timestamp_utc"] = observation
    points = _pregame(points, "prediction_timestamp_utc", schedule)
    points = points.merge(skaters[["game_id", "player_id", "official_points", "participation_state"]], on=["game_id", "player_id"], how="left", validate="many_to_one")
    points["timestamp_qualification"] = "RUN_SUMMARY_OBSERVATION_TIMESTAMP_PRESTART"
    points["grading_status"] = points.participation_state.map(
        lambda value: "PRESEASON_NON_EVALUATION" if value == "PARTICIPATED" else "NONPARTICIPANT_UNGRADED"
    )
    points["regular_season_evaluation_target"] = pd.NA

    saves = pd.read_csv(props / "saves_conditional_start_predictions.csv").rename(columns={"goalie_id": "goalie_id"})
    saves["prediction_timestamp_utc"] = observation
    saves = _pregame(saves, "prediction_timestamp_utc", schedule)
    saves = saves.merge(goalies[["game_id", "goalie_id", "official_saves", "actual_start_flag", "goalie_participation_state", "starter_identity_method"]], on=["game_id", "goalie_id"], how="left", validate="many_to_one")
    saves["grading_status"] = saves.actual_start_flag.map(
        lambda value: "PRESEASON_NON_EVALUATION_CONDITIONAL_STARTER"
        if pd.notna(value) and bool(value) else "NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION"
    )
    saves["regular_season_evaluation_target"] = pd.NA

    sog = skaters.copy()
    sog["grading_status"] = "NO_SEPTEMBER_19_PREDICTION_GRADE"
    return {"moneyline": moneyline, "puck_line": puck, "points": points, "saves": saves, "sog_outcomes_only": sog}


def _zero_evidence_surface(status: str, reason: str) -> pd.DataFrame:
    frame = pd.DataFrame(columns=[
        "canonical_season", "slate_date", "game_id", "player_id", "goalie_id",
        "prediction_identity", "grading_status", "reason",
    ]).astype({"grading_status": "object", "reason": "object"}).assign(
        grading_status=pd.Series(dtype="object"), reason=pd.Series(dtype="object")
    ).rename_axis(None)
    frame.attrs.update({"status": status, "reason": reason})
    return frame


def _settle_ladder(frame: pd.DataFrame, *, outcome_column: str) -> pd.DataFrame:
    result = frame.copy()
    actual = pd.to_numeric(result[outcome_column], errors="coerce")
    line = pd.to_numeric(result.line, errors="coerce")
    result["settled_side"] = pd.Series(pd.NA, index=result.index, dtype="string")
    result.loc[actual > line, "settled_side"] = "OVER"
    result.loc[actual < line, "settled_side"] = "UNDER"
    result.loc[actual.eq(line) & actual.notna() & line.notna(), "settled_side"] = "PUSH"
    result["model_side"] = result.prob_over.astype(float).map(
        lambda value: "OVER" if value >= 0.5 else "UNDER")
    result["prediction_correct"] = result.model_side.eq(result.settled_side).where(
        result.settled_side.ne("PUSH"), pd.NA)
    result["grading_status"] = "OUTCOME_UNRESOLVED"
    result.loc[result.settled_side.isin(["OVER", "UNDER"]), "grading_status"] = "SETTLED"
    result.loc[result.settled_side.eq("PUSH"), "grading_status"] = "PUSH"
    return result


def _grade_points(run: Path | None, schedule: pd.DataFrame,
                  skaters: pd.DataFrame, *, predictions: pd.DataFrame | None = None
                  ) -> dict[str, pd.DataFrame]:
    predictions = (predictions.copy() if predictions is not None else _pregame(
        pd.read_csv(run / "immutable_predictions.csv"), "prediction_timestamp_utc", schedule))
    outcomes = skaters.copy()
    if outcomes.duplicated(["game_id", "player_id"]).any():
        raise RuntimeError("POINTS_OUTCOME_IDENTITY_DUPLICATE")
    outcomes["official_points"] = (outcomes.official_goals.astype(int)
                                    + outcomes.official_assists.astype(int))
    joined = predictions.merge(
        outcomes[["game_id", "player_id", "official_points", "participation_state"]],
        on=["game_id", "player_id"], how="left", validate="many_to_one")
    participating = joined.loc[joined.participation_state.eq("PARTICIPATED")].copy()
    unresolved = joined.loc[~joined.participation_state.eq("PARTICIPATED")].copy()
    settled = _settle_ladder(participating, outcome_column="official_points")
    unresolved["grading_status"] = unresolved.participation_state.map(
        lambda state: "NONPARTICIPANT_UNGRADED" if pd.notna(state)
        else "PARTICIPATION_STATUS_UNRESOLVED")
    predicted = set(predictions[["game_id", "player_id"]].itertuples(index=False, name=None))
    missing = outcomes.loc[~outcomes[["game_id", "player_id"]].apply(tuple, axis=1).isin(predicted)].copy()
    missing["grading_status"] = "MISSING_PROSPECTIVE_PREDICTION_EXCLUDED"
    exclusions = pd.read_csv(run / "prediction_exclusions.csv") if run else pd.DataFrame()
    input_exclusions = pd.read_csv(run / "input_exclusions.csv") if run else pd.DataFrame()
    game_exclusions = (pd.read_csv(run / "game_exclusions.csv")
                       if run and (run / "game_exclusions.csv").is_file()
                       else pd.DataFrame(columns=["game_id", "reason"]))
    return {"points": pd.concat([settled, unresolved], ignore_index=True, sort=False),
            "points_settled": settled, "points_participating": participating,
            "points_nonparticipants": unresolved, "points_unresolved": unresolved,
            "points_missing_predictions": missing,
            "points_source_exclusions": exclusions,
            "points_input_exclusions": input_exclusions,
            "points_game_exclusions": game_exclusions}


def _grade_saves(run: Path | None, schedule: pd.DataFrame,
                 goalies: pd.DataFrame, *, predictions: pd.DataFrame | None = None
                 ) -> dict[str, pd.DataFrame]:
    predictions = (predictions.copy() if predictions is not None else _pregame(
        pd.read_csv(run / "immutable_conditional_predictions.csv"), "prediction_timestamp_utc", schedule))
    if goalies.duplicated(["game_id", "goalie_id"]).any():
        raise RuntimeError("SAVES_OUTCOME_IDENTITY_DUPLICATE")
    joined = predictions.merge(
        goalies[["game_id", "goalie_id", "team_id", "official_saves",
                 "actual_start_flag", "goalie_participation_state",
                 "starter_identity_method"]],
        on=["game_id", "goalie_id"], how="left", validate="many_to_one")
    starters = joined.loc[joined.actual_start_flag.astype("boolean").fillna(False).astype(bool)].copy()
    nonstarters = joined.loc[
        joined.actual_start_flag.eq(False) & joined.goalie_participation_state.notna()
    ].copy()
    unresolved = joined.loc[joined.actual_start_flag.isna()].copy()
    settled = _settle_ladder(starters, outcome_column="official_saves")
    nonstarters["grading_status"] = "DID_NOT_START_NOT_GRADEABLE_CONDITIONAL"
    unresolved["grading_status"] = "STARTER_STATUS_UNRESOLVED"
    predicted = set(predictions[["game_id", "goalie_id"]].itertuples(index=False, name=None))
    unpredicted = goalies.loc[
        ~goalies[["game_id", "goalie_id"]].apply(tuple, axis=1).isin(predicted)].copy()
    unpredicted["grading_status"] = unpredicted.actual_start_flag.map(
        lambda value: "UNPREDICTED_STARTER" if bool(value) else "UNPREDICTED_RELIEF_APPEARANCE")
    unpredicted_starters = unpredicted.loc[
        unpredicted.actual_start_flag.astype("boolean").fillna(False).astype(bool)].copy()
    unpredicted_relief = unpredicted.loc[
        ~unpredicted.actual_start_flag.astype("boolean").fillna(False).astype(bool)].copy()
    predicted_relief = nonstarters.loc[
        nonstarters.goalie_participation_state.eq("RELIEF_APPEARANCE")].copy()
    input_exclusions = pd.read_csv(run / "input_exclusions.csv") if run else pd.DataFrame()
    game_exclusions = (pd.read_csv(run / "game_exclusions.csv")
                       if run and (run / "game_exclusions.csv").is_file()
                       else pd.DataFrame(columns=["game_id", "reason"]))
    return {"saves": pd.concat([settled, nonstarters, unresolved], ignore_index=True, sort=False),
            "saves_settled_starters": settled,
            "saves_predicted_nonstarters": nonstarters,
            "saves_unresolved": unresolved,
            "saves_predicted_relief_appearances": predicted_relief,
            "saves_nonstarters": nonstarters,
            "saves_unpredicted_starters": unpredicted_starters,
            "saves_unpredicted_relief_appearances": unpredicted_relief,
            "saves_unpredicted_outcomes": unpredicted,
            "saves_input_exclusions": input_exclusions,
            "saves_game_exclusions": game_exclusions}


def _grade_points_source(points_status: dict[str, Any], schedule: pd.DataFrame,
                        skaters: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    if points_status["status"] in {
            "NO_PROSPECTIVE_POINTS_PREDICTIONS", "NOT_GRADEABLE_NO_PREDICTION"}:
        return _zero_evidence_surface(points_status["status"], points_status["reason"]), {}
    point_grades = _grade_points(Path(points_status["run"]), schedule, skaters)
    points = point_grades.pop("points")
    return points, point_grades


def _resilient_cross_market_grades(*, slate_date: str, operational_root: Path,
                                  schedule: pd.DataFrame, games: pd.DataFrame,
                                  skaters: pd.DataFrame, goalies: pd.DataFrame
                                  ) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Grade receipt-bound cross-market observations as a separate population.

    This adapter intentionally accepts only a daily parent receipt that names
    the immutable source artifact, prior-day certificate, and roster manifest.
    It does not infer lineage from timestamps or directory names.
    """
    from backend.nhl.reconciliation_certificate import verify_reconciliation_certificate
    from backend.nhl.daily_orchestration import verify_roster_observation_reuse

    daily_root = operational_root / "daily_runs"
    starts = {int(row.game_id): pd.Timestamp(row.scheduled_start_time_utc).isoformat()
              for row in schedule.itertuples(index=False)}
    game_ids = sorted(starts)
    candidates: dict[str, list[dict[str, Any]]] = {
        "MONEYLINE": [], "PUCK_LINE": [], "SOG": [], "POINTS": [], "SAVES": [],
    }
    exclusions: dict[str, list[dict[str, Any]]] = {
        "MONEYLINE": [], "PUCK_LINE": [], "SOG": [], "POINTS": [], "SAVES": [],
    }
    proof_notes: list[dict[str, Any]] = []
    verified_cross_packages: set[Path] = set()
    for receipt_path in sorted(daily_root.glob("run_id=*/parent_receipt.json")):
        linked_parent = False
        try:
            parent = json.loads(receipt_path.read_text())
            if str(parent.get("slate_date")) != slate_date:
                continue
            prior = parent.get("prior_day_learning") or {}
            certificate = prior.get("certificate") or {}
            prior_date = (datetime.fromisoformat(slate_date).date()
                          - __import__("datetime").timedelta(days=1)).isoformat()
            if certificate:
                cert_path = Path(str(certificate.get("package_path") or ""))
                expected_cert_manifest = certificate.get("package_manifest_sha256")
                certificate_status = certificate.get("status")
            else:
                # Earlier receipts bind the exact reused package path and its
                # successful FINAL reconciliation status instead of embedding
                # the later certificate object. Verify that retained package
                # directly; do not synthesize or infer a package path.
                cert_path = Path(str(prior.get("reconciliation_package") or ""))
                expected_cert_manifest = None
                certificate_status = (
                    "CERTIFIED_FINAL"
                    if prior.get("reconciliation_status") == "REUSED_VALID_PACKAGE"
                    and prior.get("official_outcomes_status") == "FINAL"
                    else None
                )
            verified_cert = verify_reconciliation_certificate(
                cert_path, prior_date)
            if (certificate_status != "CERTIFIED_FINAL"
                    or (expected_cert_manifest is not None
                        and expected_cert_manifest != verified_cert["package_manifest_sha256"])):
                continue
            roster = parent.get("roster_observation") or {}
            roster_path = Path(str(roster.get("path") or ""))
            if not roster_path.is_dir() or not (roster_path / "SHA256SUMS").is_file():
                continue
            roster_hash = _sha(roster_path / "SHA256SUMS")
            if roster.get("manifest_sha256") != roster_hash:
                continue
            roster_summary = json.loads((roster_path / "observation_summary.json").read_text())
            roster_season = int(roster_summary["season"])
            verified_roster = verify_roster_observation_reuse(
                roster_path, season=roster_season, slate_date=slate_date,
                phase=str(roster_summary["phase"]), canonical_game_ids=game_ids,
                canonical_game_set_hash=canonical_game_set_hash(game_ids))
            if verified_roster["manifest_sha256"] != roster_hash:
                continue
            for lane_name, lane in (parent.get("lanes") or {}).items():
                if lane_name not in {"moneyline", "cold_start_sog_reference"}:
                    continue
                for output in lane.get("outputs") or []:
                    output_path = Path(str(output.get("path") or ""))
                    source_files = {
                        "moneyline_production_predictions.csv": ("MONEYLINE", "PRODUCTION_CHAMPION"),
                        "moneyline_schedule_logit_v2_shadow_predictions.csv": ("MONEYLINE", "SHADOW"),
                        "v2_immutable_predictions.csv": ("MONEYLINE", "SHADOW"),
                        "puck_line_v1_immutable_predictions.csv": ("PUCK_LINE", "PUCK_LINE"),
                        "immutable_predictions.csv": ("SOG", "CONTRACT_ARM"),
                    }
                    # Output rows bind exact files; resolve the containing
                    # package and verify its full manifest before reading.
                    is_sog_package = lane_name == "cold_start_sog_reference" and output_path.is_dir()
                    if (not is_sog_package and output_path.name not in source_files):
                        continue
                    if (not is_sog_package and
                            (not output_path.is_file() or "cross_market_shadow" not in str(output_path))):
                        continue
                    linked_parent = True
                    source_run = output_path if is_sog_package else output_path.parent
                    phase = (json.loads((source_run / "run_metadata.json").read_text()).get("phase")
                             if is_sog_package else source_run.parent.name.removeprefix("run_type="))
                    if phase not in {"MIDDAY", "FINAL_PREGAME", "REFRESH"}:
                        continue
                    source_files_manifest = _verify_manifest(source_run)
                    if is_sog_package:
                        output_file = source_run / "immutable_predictions.csv"
                        if "immutable_predictions.csv" not in source_files_manifest:
                            raise RuntimeError("RESILIENT_SOG_PREDICTION_ARTIFACT_MISSING")
                    else:
                        output_file = output_path
                    if not output_file.is_file() or source_files_manifest.get(output_file.name) != _sha(output_file):
                        raise RuntimeError("RESILIENT_CROSS_MARKET_OUTPUT_HASH_MISMATCH")
                    expected_output_hash = output.get("sha256") or output.get("manifest_sha256")
                    actual_output_hash = _sha(output_path) if output_path.is_file() else _sha(source_run / "SHA256SUMS")
                    if expected_output_hash and expected_output_hash != actual_output_hash:
                        raise RuntimeError("RESILIENT_CROSS_MARKET_RECEIPT_HASH_MISMATCH")
                    parent_digest = _sha(receipt_path)
                    durable = str(lane.get("ended_at_utc") or parent.get("ended_at_utc") or "")
                    parent_feature_cutoff = str(parent.get("feature_cutoff_utc") or "")
                    source_key = "immutable_predictions.csv" if is_sog_package else output_path.name
                    if source_key not in source_files:
                        continue
                    lane_key, role = source_files[source_key]
                    status_file = "run_metadata.json" if is_sog_package else "daily_execution_status.json"
                    capture_status = json.loads((source_run / status_file).read_text())
                    if (capture_status.get("slate_date") != slate_date
                            or capture_status.get("phase" if is_sog_package else "run_type") != phase
                            or (not is_sog_package and source_run.name != f"state={capture_status.get('substantive_state_sha256')}")):
                        raise RuntimeError("RESILIENT_CROSS_MARKET_SOURCE_IDENTITY_MISMATCH")
                    if not is_sog_package and source_run not in verified_cross_packages:
                        shadow_path = next((path for path in (
                            source_run / "moneyline_schedule_logit_v2_shadow_predictions.csv",
                            source_run / "v2_immutable_predictions.csv",
                        ) if path.is_file()), None)
                        puck_path = source_run / "puck_line_v1_immutable_predictions.csv"
                        if shadow_path is None or not puck_path.is_file():
                            raise RuntimeError("RESILIENT_CROSS_MARKET_MODEL_ROLE_ARTIFACT_MISSING")
                        provenance_name = "moneyline_shot_finishing_challenger_v3_feature_provenance.csv"
                        provenance_path = source_run / provenance_name
                        provenance = (pd.read_csv(provenance_path)
                                      if provenance_name in source_files_manifest and provenance_path.is_file()
                                      else None)
                        _verify_cross_market_identities(
                            pd.read_csv(shadow_path, float_precision="round_trip"),
                            _read_immutable_puck_predictions(puck_path),
                            moneyline_feature_provenance=provenance)
                        production_path = source_run / "moneyline_production_predictions.csv"
                        if production_path.is_file():
                            production = pd.read_csv(production_path, float_precision="round_trip")
                            required_production = {
                                "game_id", "slate_date", "scheduled_start_time_utc",
                                "model_version", "model_parameter_sha256", "operational_role",
                                "challenger_home_win_probability", "challenger_away_win_probability",
                                "challenger_favored_team",
                            }
                            _require_columns(production, required_production, "RESILIENT_MONEYLINE_PRODUCTION")
                            if (production.game_id.duplicated().any()
                                    or set(production.game_id.astype(int)) != set(game_ids)
                                    or set(production.model_version.astype(str)) !=
                                    {"NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3"}
                                    or set(production.operational_role.astype(str)) != {"PRODUCTION_CHAMPION"}
                                    or not np.allclose(
                                        pd.to_numeric(production.challenger_home_win_probability),
                                        1 - pd.to_numeric(production.challenger_away_win_probability),
                                        rtol=0, atol=1e-12)):
                                raise RuntimeError("RESILIENT_MONEYLINE_PRODUCTION_IDENTITY_INVALID")
                            from backend.nhl.cross_market_shadow.moneyline_challenger import load_model
                            if set(production.model_parameter_sha256.astype(str)) != {
                                    load_model()["parameter_file_sha256"]}:
                                raise RuntimeError("RESILIENT_MONEYLINE_PRODUCTION_MODEL_SHA_MISMATCH")
                        verified_cross_packages.add(source_run)
                    if lane_key == "SOG":
                        frame = _verify_resilient_prop_package(
                            run=source_run, lane="SOG", slate_date=slate_date, schedule=schedule,
                            durable_publication_timestamp_utc=durable or None)
                    elif lane_key == "MONEYLINE":
                        frame = pd.read_csv(output_file, float_precision="round_trip")
                    else:
                        frame = _read_immutable_puck_predictions(output_file)
                    if ("game_id" not in frame
                            or (lane_key != "SOG" and frame.game_id.duplicated().any())):
                        raise RuntimeError("RESILIENT_CROSS_MARKET_GAME_IDENTITY_INVALID")
                    if (("slate_date" in frame and not frame.slate_date.astype(str).eq(slate_date).all())
                            or ("canonical_season" in frame
                                and not frame.canonical_season.astype(int).eq(2026).all())):
                        raise RuntimeError("RESILIENT_CROSS_MARKET_SLATE_IDENTITY_MISMATCH")
                    if ("slate_date" in frame and not frame.slate_date.astype(str).eq(slate_date).all()
                            or "canonical_season" in frame
                            and not frame.canonical_season.astype(int).eq(2026).all()):
                        raise RuntimeError("RESILIENT_CROSS_MARKET_SLATE_IDENTITY_MISMATCH")
                    for row in frame.to_dict(orient="records"):
                        game_id = int(row["game_id"])
                        if game_id not in starts:
                            raise RuntimeError("RESILIENT_CROSS_MARKET_GAME_OUTSIDE_SLATE")
                        row_feature_cutoff = next((row.get(name) for name in (
                            "feature_cutoff_timestamp_utc", "feature_cutoff_utc",
                        ) if row.get(name) is not None and not pd.isna(row.get(name))
                            and str(row.get(name)).strip()), None)
                        feature_cutoff = str(row_feature_cutoff or parent_feature_cutoff)
                        if not feature_cutoff:
                            proof_notes.append({
                                "source_path": str(output_file),
                                "parent_receipt": str(receipt_path),
                                "excluded_reason": "RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN",
                            })
                            continue
                        model_identity = str(
                            capture_status.get("moneyline_production_model") if role == "PRODUCTION_CHAMPION"
                            else capture_status.get("moneyline_shadow_model") if role == "SHADOW"
                            else capture_status.get("contract_sha256") if lane_key == "SOG"
                            else row.get("model_version") or row.get("model_parameter_sha256") or role)
                        feature_identity = str(
                            capture_status.get("moneyline_shot_finishing_challenger_v3_feature_policy")
                            if role == "PRODUCTION_CHAMPION" else
                            _hash_bytes(json.dumps(CROSS_MARKET_FEATURES, separators=(",", ":")).encode())
                            if role == "SHADOW" else
                            capture_status.get("puck_line_v2_shot_prior_feature_policy")
                            if lane_key == "PUCK_LINE" else row.get("feature_contract_identity")
                            or row.get("feature_hash") or output_file.name)
                        if lane_key == "SOG":
                            feature_identity = str(capture_status.get("contract_sha256") or row.get("contract_sha256"))
                            model_identity = str(row.get("contract_arm"))
                        identity_row = ({**row, "game_id": game_id}
                                        if lane_key == "SOG" else
                                        {"game_id": game_id, "model_role": role,
                                         "model_identity": model_identity,
                                         "feature_contract_identity": feature_identity})
                        atomic = canonical_atomic_identity(lane_key, identity_row)
                        timestamp = str(row.get("prediction_timestamp_utc")
                                       or row.get("prediction_creation_time_utc") or durable)
                        payload = _hash_bytes(json.dumps(row, sort_keys=True, default=str).encode())
                        item = {
                            "game_id": game_id, "atomic_prediction_identity": atomic,
                            "eligible": True, "source_phase": phase,
                            "source_path": str(output_file), "source_manifest_sha256": _sha(source_run / "SHA256SUMS"),
                            "observer_receipt_path": str(receipt_path), "observer_receipt_sha256": parent_digest,
                            "certificate_sha256": str(verified_cert["package_manifest_sha256"]),
                            "roster_source_sha256": roster_hash,
                            "model_identity": model_identity,
                            "feature_contract_identity": feature_identity,
                            "feature_cutoff_timestamp_utc": feature_cutoff,
                            "prediction_timestamp_utc": timestamp,
                            "durable_publication_timestamp_utc": durable or timestamp,
                            "prediction_payload_sha256": payload,
                            "source_run_id": source_run.name,
                            "source_row_identity": str(row.get("game_id")),
                        }
                        candidates[lane_key].append(item)
                        proof_notes.append({"source_path": str(source_run), "parent_receipt": str(receipt_path),
                                        "certificate_sha256": verified_cert["package_manifest_sha256"],
                                        "roster_source_sha256": roster_hash})
        except (OSError, ValueError, KeyError, json.JSONDecodeError, RuntimeError) as error:
            # A corrupt linked source fails closed. Unrelated daily receipts
            # without a link to an accepted output are ignored above.
            reason = str(error)
            source_not_eligible = any(token in reason for token in (
                "TIMESTAMP_NOT_PREGAME", "PHASE_INELIGIBLE", "DURABLE_WRITE_NOT_PROVEN",
            ))
            if linked_parent and not source_not_eligible:
                raise
            proof_notes.append({"source_path": str(receipt_path), "excluded_reason": reason[:240]})

    # Cross-market parent receipts historically listed the Moneyline output
    # files but omitted the sibling Puck Line CSV. The linked package manifest
    # still authenticates that sibling artifact, so discover it only when the
    # same parent receipt links another prediction file in the exact package.
    puck_source_keys = {
        (str(item.get("source_path")), int(item.get("game_id", -1)))
        for item in candidates["PUCK_LINE"]
    }
    for receipt_path in sorted(daily_root.glob("run_id=*/parent_receipt.json")):
        try:
            parent = json.loads(receipt_path.read_text())
            if str(parent.get("slate_date")) != slate_date:
                continue
            outputs = (parent.get("lanes") or {}).get("moneyline", {}).get("outputs") or []
            linked_packages: set[Path] = set()
            for output in outputs:
                linked_path = Path(str(output.get("path") or ""))
                if (linked_path.is_file() and "cross_market_shadow" in str(linked_path)
                        and linked_path.name in {
                            "moneyline_production_predictions.csv",
                            "moneyline_schedule_logit_v2_shadow_predictions.csv",
                            "v2_immutable_predictions.csv",
                        } and output.get("sha256") == _sha(linked_path)):
                    linked_packages.add(linked_path.parent)
            if not linked_packages:
                continue
            prior = parent.get("prior_day_learning") or {}
            certificate = prior.get("certificate") or {}
            prior_date = (datetime.fromisoformat(slate_date).date()
                          - __import__("datetime").timedelta(days=1)).isoformat()
            cert_path = Path(str(certificate.get("package_path")
                                 or prior.get("reconciliation_package") or ""))
            verified_cert = verify_reconciliation_certificate(cert_path, prior_date)
            modern_status = certificate.get("status")
            legacy_status = ("CERTIFIED_FINAL"
                             if prior.get("reconciliation_status") == "REUSED_VALID_PACKAGE"
                             and prior.get("official_outcomes_status") == "FINAL" else None)
            if modern_status != "CERTIFIED_FINAL" and legacy_status != "CERTIFIED_FINAL":
                continue
            if (certificate.get("package_manifest_sha256")
                    and certificate["package_manifest_sha256"]
                    != verified_cert["package_manifest_sha256"]):
                continue
            roster = parent.get("roster_observation") or {}
            roster_path = Path(str(roster.get("path") or ""))
            roster_summary = json.loads((roster_path / "observation_summary.json").read_text())
            roster_hash = _sha(roster_path / "SHA256SUMS")
            if roster.get("manifest_sha256") != roster_hash:
                continue
            verified_roster = verify_roster_observation_reuse(
                roster_path, season=int(roster_summary["season"]), slate_date=slate_date,
                phase=str(roster_summary["phase"]), canonical_game_ids=game_ids,
                canonical_game_set_hash=canonical_game_set_hash(game_ids))
            if verified_roster.get("manifest_sha256") != roster_hash:
                continue
            for source_run in sorted(linked_packages):
                manifest = _verify_manifest(source_run)
                puck_path = source_run / "puck_line_v1_immutable_predictions.csv"
                if puck_path.name not in manifest:
                    continue
                capture_status = json.loads((source_run / "daily_execution_status.json").read_text())
                phase = str(capture_status.get("run_type") or "")
                if (phase not in {"MIDDAY", "FINAL_PREGAME", "REFRESH"}
                        or capture_status.get("slate_date") != slate_date
                        or source_run.name != f"state={capture_status.get('substantive_state_sha256')}"):
                    raise RuntimeError("RESILIENT_PUCK_LINE_SOURCE_IDENTITY_MISMATCH")
                puck = _read_immutable_puck_predictions(puck_path)
                if (puck.game_id.duplicated().any()
                        or set(puck.game_id.astype(int)) != set(game_ids)
                        or ("slate_date" in puck
                            and not puck.slate_date.astype(str).eq(slate_date).all())):
                    raise RuntimeError("RESILIENT_PUCK_LINE_GAME_IDENTITY_INVALID")
                shadow_path = next((source_run / name for name in (
                    "moneyline_schedule_logit_v2_shadow_predictions.csv",
                    "v2_immutable_predictions.csv",
                ) if name in manifest), None)
                if shadow_path is None:
                    raise RuntimeError("RESILIENT_PUCK_LINE_MODEL_ROLE_ARTIFACT_MISSING")
                provenance_name = "moneyline_shot_finishing_challenger_v3_feature_provenance.csv"
                provenance_path = source_run / provenance_name
                provenance = (pd.read_csv(provenance_path)
                              if provenance_name in manifest and provenance_path.is_file() else None)
                _verify_cross_market_identities(
                    pd.read_csv(shadow_path, float_precision="round_trip"), puck,
                    moneyline_feature_provenance=provenance)
                lane_receipt = (parent.get("lanes") or {}).get("moneyline") or {}
                durable = str(lane_receipt.get("ended_at_utc")
                              or parent.get("ended_at_utc") or "")
                feature_cutoff = str(parent.get("feature_cutoff_utc") or "")
                parent_hash = _sha(receipt_path)
                for row in puck.to_dict(orient="records"):
                    puck_model_identity = str(row.get("model_version")
                                              or row.get("model_parameter_sha256") or "PUCK_LINE")
                    puck_feature_identity = str(
                        capture_status.get("puck_line_v2_shot_prior_feature_policy")
                        or row.get("feature_contract_identity") or row.get("feature_hash")
                        or "PUCK_LINE")
                    identity = canonical_atomic_identity("PUCK_LINE", {
                        "game_id": int(row["game_id"]), "model_role": "PUCK_LINE",
                        "model_identity": puck_model_identity,
                        "feature_contract_identity": puck_feature_identity,
                    })
                    key = (str(puck_path), int(row["game_id"]))
                    if key in puck_source_keys:
                        continue
                    puck_source_keys.add(key)
                    timestamp = str(row.get("prediction_timestamp_utc")
                                    or row.get("prediction_creation_time_utc") or durable)
                    row_cutoff = next((row.get(name) for name in (
                        "feature_cutoff_timestamp_utc", "feature_cutoff_utc",
                    ) if row.get(name) is not None and not pd.isna(row.get(name))
                        and str(row.get(name)).strip()), feature_cutoff)
                    if not row_cutoff:
                        proof_notes.append({"source_path": str(puck_path),
                                            "parent_receipt": str(receipt_path),
                                            "excluded_reason": "RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN"})
                        continue
                    candidates["PUCK_LINE"].append({
                        "game_id": int(row["game_id"]),
                        "atomic_prediction_identity": identity,
                        "eligible": True, "source_phase": phase,
                        "source_path": str(puck_path),
                        "source_manifest_sha256": _sha(source_run / "SHA256SUMS"),
                        "observer_receipt_path": str(receipt_path),
                        "observer_receipt_sha256": parent_hash,
                        "certificate_sha256": str(verified_cert["package_manifest_sha256"]),
                        "roster_source_sha256": roster_hash,
                        "model_identity": puck_model_identity,
                        "feature_contract_identity": puck_feature_identity,
                        "feature_cutoff_timestamp_utc": str(row_cutoff),
                        "prediction_timestamp_utc": timestamp,
                        "durable_publication_timestamp_utc": durable,
                        "prediction_payload_sha256": _hash_bytes(json.dumps(
                            row, sort_keys=True, default=str).encode()),
                        "source_run_id": source_run.name,
                        "source_row_identity": str(row["game_id"]),
                    })
        except (OSError, ValueError, KeyError, json.JSONDecodeError, RuntimeError) as error:
            proof_notes.append({"source_path": str(receipt_path),
                                "excluded_reason": str(error)[:240]})

    orchestration_root = operational_root / "cross_market_shadow" / "orchestration_status" / slate_date
    for receipt_path in sorted(orchestration_root.glob("*.json")):
        try:
            receipt = json.loads(receipt_path.read_text())
            run_value = receipt.get("run_dir")
            phase = str(receipt.get("phase") or receipt.get("requested_phase") or "")
            if receipt.get("slate_date") != slate_date or not run_value or phase not in {
                    "MIDDAY", "FINAL_PREGAME", "REFRESH"}:
                continue
            source_run = Path(str(run_value))
            already_bound = any(
                str(candidate.get("source_path", "")).startswith(str(source_run))
                for lane_candidates in candidates.values() for candidate in lane_candidates)
            if already_bound:
                continue
            # These observer receipts currently provide the prior certificate
            # and roster but do not provide a distinct feature cutoff. Do not
            # substitute the invocation or prediction timestamp for that proof.
            proof_notes.append({
                "source_path": str(source_run),
                "observer_receipt_path": str(receipt_path),
                "observer_receipt_sha256": _sha(receipt_path),
                "excluded_reason": "RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN",
            })
        except (OSError, ValueError, json.JSONDecodeError):
            continue

    # Prop captures are owned by the independent observer and linked from its
    # mainline parent receipt. Both receipts are retained in the index.
    for parent_path in sorted(orchestration_root.glob("*.json")):
        linked_prop = False
        try:
            parent = json.loads(parent_path.read_text())
            if parent.get("slate_date") != slate_date:
                continue
            links = parent.get("player_prop_observers") or {}
            if not links:
                continue
            cert = parent.get("prior_day_reconciliation_certificate") or {}
            prior_date = (datetime.fromisoformat(slate_date).date()
                          - __import__("datetime").timedelta(days=1)).isoformat()
            verified_cert = verify_reconciliation_certificate(
                Path(str(cert.get("package_path") or "")), prior_date)
            if (cert.get("status") != "CERTIFIED_FINAL"
                    or cert.get("package_manifest_sha256") != verified_cert["package_manifest_sha256"]):
                continue
            roster = parent.get("current_slate_roster_observation") or {}
            roster_path = Path(str(roster.get("package_path") or ""))
            roster_summary = json.loads((roster_path / "observation_summary.json").read_text())
            roster_verified = verify_roster_observation_reuse(
                roster_path, season=int(roster_summary["season"]), slate_date=slate_date,
                phase=str(roster_summary["phase"]), canonical_game_ids=game_ids,
                canonical_game_set_hash=canonical_game_set_hash(game_ids))
            roster_hash = str(roster_verified["manifest_sha256"])
            if (roster.get("status") != "VERIFIED_CURRENT_SLATE_ROSTER"
                    or roster.get("package_manifest_sha256") != roster_hash):
                continue
            for lane, lane_name in (("POINTS", "POINTS"), ("SAVES", "SAVES"),
                                    ("SOG", "SOG")):
                child_path = Path(str((links.get(lane) or {}).get("receipt_path") or ""))
                if not child_path.is_file() or (links.get(lane) or {}).get("status") != "CAPTURED":
                    continue
                linked_prop = True
                child = json.loads(child_path.read_text())
                if child.get("status") != "CAPTURED" or child.get("slate_date") != slate_date:
                    continue
                source_run = Path(str(child.get("run_dir") or ""))
                if not source_run.is_dir():
                    raise RuntimeError(f"RESILIENT_{lane}_SOURCE_PACKAGE_MISSING")
                manifest = _verify_manifest(source_run)
                metadata = json.loads((source_run / "run_metadata.json").read_text())
                phase = str(metadata.get("phase") or child.get("phase") or "")
                if phase not in {"MIDDAY", "FINAL_PREGAME"}:
                    continue
                if (metadata.get("slate_date") != slate_date
                        or source_run.name != f"run_id={metadata.get('run_id')}"):
                    raise RuntimeError(f"RESILIENT_{lane}_SOURCE_IDENTITY_MISMATCH")
                readiness = metadata.get("readiness_evidence")
                if readiness is not None:
                    source_cert = readiness.get("prior_day_reconciliation_certificate") or {}
                    source_roster = readiness.get("current_slate_roster_observation") or {}
                    if (source_cert.get("package_manifest_sha256")
                            != verified_cert["package_manifest_sha256"]
                            or source_roster.get("package_manifest_sha256") != roster_hash):
                        raise RuntimeError(f"RESILIENT_{lane}_SOURCE_READINESS_CONFLICT")
                expected_file = ("immutable_conditional_predictions.csv" if lane == "SAVES"
                                 else "immutable_predictions.csv")
                if expected_file not in manifest:
                    raise RuntimeError(f"RESILIENT_{lane}_SOURCE_ARTIFACT_MISSING")
                prediction_path = source_run / expected_file
                frame = _verify_resilient_prop_package(
                    run=source_run, lane=lane, slate_date=slate_date, schedule=schedule)
                if frame.empty:
                    continue
                marker = json.loads((source_run / "RUN_COMPLETE.json").read_text())
                if marker.get("status") != "COMPLETE":
                    raise RuntimeError(f"RESILIENT_{lane}_SOURCE_COMPLETION_INVALID")
                durable = str(marker.get("completed_at_utc") or "")
                timestamp_column = "prediction_timestamp_utc"
                if lane == "POINTS":
                    group_columns = ["game_id", "player_id", "model_version", "probability_construction"]
                elif lane == "SAVES":
                    group_columns = ["game_id", "goalie_id", "model_version", "prediction_semantics"]
                else:
                    group_columns = ["game_id", "player_id", "contract_arm", "contract_sha256", "line"]
                for _, group in frame.groupby(group_columns, dropna=False, sort=True):
                    group_records = group.to_dict(orient="records")
                    try:
                        if lane in {"POINTS", "SAVES"}:
                            validate_atomic_group(lane, group_records)
                        identity_row = group_records[0]
                        atomic = canonical_atomic_identity(lane, identity_row)
                        game_id = int(identity_row["game_id"])
                        if game_id not in starts:
                            raise RuntimeError("GAME_NOT_IN_CANONICAL_SLATE")
                        feature_cutoff = str(identity_row.get("feature_cutoff_timestamp_utc")
                                             or identity_row.get("input_cutoff_timestamp_utc") or "")
                        prediction_at = str(identity_row.get(timestamp_column) or "")
                        payload = _hash_bytes(json.dumps(sorted(group_records,
                            key=lambda row: float(row["line"])), sort_keys=True, default=str).encode())
                        model_identity = (str(identity_row["contract_arm"]) if lane == "SOG"
                                          else str(identity_row["model_version"]))
                        feature_identity = (str(identity_row["contract_sha256"]) if lane == "SOG"
                                            else str(identity_row.get("probability_construction")
                                                     or identity_row.get("prediction_semantics")))
                        candidate = {
                            "game_id": game_id, "atomic_prediction_identity": atomic,
                            "eligible": True, "source_phase": phase,
                            "source_path": str(prediction_path),
                            "source_manifest_sha256": _sha(source_run / "SHA256SUMS"),
                            "observer_receipt_path": str(child_path),
                            "observer_receipt_sha256": _sha(child_path),
                            "observer_parent_receipt_path": str(parent_path),
                            "observer_parent_receipt_sha256": _sha(parent_path),
                            "certificate_sha256": str(verified_cert["package_manifest_sha256"]),
                            "roster_source_sha256": roster_hash,
                            "model_identity": model_identity,
                            "feature_contract_identity": feature_identity,
                            "feature_cutoff_timestamp_utc": feature_cutoff,
                            "prediction_timestamp_utc": prediction_at,
                            "durable_publication_timestamp_utc": durable,
                            "prediction_payload_sha256": payload,
                            "source_run_id": source_run.name,
                            "source_row_identity": atomic,
                        }
                        candidates[lane].append(candidate)
                    except RuntimeError as error:
                        exclusions[lane].append({"game_id": int(group.iloc[0].game_id),
                                                 "reason": str(error),
                                                 "source_path": str(prediction_path)})
        except (OSError, KeyError, ValueError, json.JSONDecodeError, RuntimeError) as error:
            reason = str(error)
            source_not_eligible = any(token in reason for token in (
                "TIMESTAMP_NOT_PREGAME", "PHASE_INELIGIBLE", "DURABLE_WRITE_NOT_PROVEN",
            ))
            if linked_prop and not source_not_eligible:
                raise
            proof_notes.append({"source_path": str(parent_path), "excluded_reason": reason[:240]})

    indexes: dict[str, Any] = {}
    grade_frames: dict[str, pd.DataFrame] = {}
    unlinked_prop_source_counts: dict[str, int] = {lane: 0 for lane in ("SOG", "POINTS", "SAVES")}
    for lane in ("SOG", "POINTS", "SAVES"):
        status_root = operational_root / f"{lane.lower()}_prediction_only" / "status" / slate_date
        seen_packages: set[Path] = set()
        bound_packages = {
            Path(candidate["source_path"]).parent.resolve()
            for candidate in candidates[lane] if candidate.get("source_path")
        }
        for status_path in sorted(status_root.glob("*.json")):
            try:
                status = json.loads(status_path.read_text())
                if (status.get("status") != "CAPTURED"
                        or status.get("phase") not in {"MIDDAY", "FINAL_PREGAME"}
                        or status.get("slate_date") != slate_date):
                    continue
                source_run = Path(str(status.get("run_dir") or ""))
                if not source_run.is_dir() or source_run.resolve() in bound_packages:
                    continue
                if source_run in seen_packages:
                    continue
                seen_packages.add(source_run)
                unlinked_prop_source_counts[lane] += 1
                try:
                    manifest = _verify_manifest(source_run)
                    metadata = json.loads((source_run / "run_metadata.json").read_text())
                    prediction_name = ("immutable_conditional_predictions.csv" if lane == "SAVES"
                                       else "immutable_predictions.csv")
                    rows = (len(pd.read_csv(source_run / prediction_name))
                            if prediction_name in manifest else None)
                    reason = ("RESILIENT_PROP_PARENT_CERTIFICATE_ROSTER_LINKAGE_MISSING"
                              if not metadata.get("readiness_evidence")
                              else "RESILIENT_PROP_PARENT_OBSERVER_LINKAGE_MISSING")
                    manifest_hash = _sha(source_run / "SHA256SUMS")
                except (OSError, ValueError, KeyError, RuntimeError) as error:
                    rows = None
                    manifest_hash = None
                    reason = f"RESILIENT_{lane}_SOURCE_MANIFEST_INVALID:{str(error)[:160]}"
                proof_notes.append({
                    "lane": lane, "source_path": str(source_run),
                    "source_phase": status.get("phase"),
                    "source_manifest_sha256": manifest_hash,
                    "observer_receipt_path": str(status_path),
                    "observer_receipt_sha256": _sha(status_path),
                    "prediction_rows": rows, "excluded_reason": reason,
                })
            except (OSError, ValueError, json.JSONDecodeError):
                continue
    outcome_columns = ["game_id", "official_full_game_winner", "official_final_home_goals",
                       "official_final_away_goals"]
    for lane_key, output_name in (("MONEYLINE", "moneyline"), ("PUCK_LINE", "puck_line"),
                                 ("SOG", "sog"), ("POINTS", "points"), ("SAVES", "saves")):
        index = select_latest_atomic_sources(
            lane=lane_key, slate_date=slate_date, canonical_game_ids=game_ids,
            scheduled_starts_utc=starts, candidates=candidates[lane_key])
        index["discovered_candidate_count"] = len(candidates[lane_key])
        index["proof_valid_candidate_count"] = len(candidates[lane_key])
        unique_source_packages = {
            (str(Path(str(candidate.get("source_path") or "")).parent),
             str(candidate.get("source_manifest_sha256") or ""))
            for candidate in candidates[lane_key]
        }
        index["discovered_source_package_count"] = len(unique_source_packages)
        index["proof_valid_source_package_count"] = len(unique_source_packages)
        index["unlinked_source_package_count"] = unlinked_prop_source_counts.get(lane_key, 0)
        indexes[lane_key] = index
        index["excluded"].extend(exclusions[lane_key])
        index["excluded"] = sorted(index["excluded"], key=lambda row: (
            int(row.get("game_id") or -1), str(row.get("atomic_prediction_identity") or ""),
            str(row.get("reason") or ""), str(row.get("source_path") or "")))
        rows = []
        for selected in index["selected"]:
            # Re-read and select by canonical game and model role from the
            # exact winning artifact. Each cross-market source has one row per
            # game and immutable source manifest validation above.
            path = Path(selected["source_path"])
            if lane_key in {"POINTS", "SAVES"}:
                source = pd.read_csv(path, float_precision="round_trip")
                group_columns = (["game_id", "player_id", "model_version", "probability_construction"]
                                 if lane_key == "POINTS" else
                                 ["game_id", "goalie_id", "model_version", "prediction_semantics"])
                matching_group = None
                for _, group in source.groupby(group_columns, dropna=False, sort=True):
                    if canonical_atomic_identity(lane_key, group.iloc[0].to_dict()) == selected["atomic_prediction_identity"]:
                        matching_group = group
                        break
                if matching_group is None:
                    raise RuntimeError(f"RESILIENT_{lane_key}_SELECTED_GROUP_DISAPPEARED")
                for row_data in matching_group.to_dict(orient="records"):
                    row_data["evaluation_population"] = "RESILIENT_PREGAME"
                    row_data["resilient_source_phase"] = selected["source_phase"]
                    row_data["resilient_source_path"] = selected["source_path"]
                    row_data["resilient_source_manifest_sha256"] = selected["source_manifest_sha256"]
                    row_data["resilient_parent_receipt_path"] = selected["observer_receipt_path"]
                    row_data["resilient_parent_receipt_sha256"] = selected["observer_receipt_sha256"]
                    row_data["resilient_observer_parent_receipt_path"] = selected.get("observer_parent_receipt_path")
                    row_data["resilient_observer_parent_receipt_sha256"] = selected.get("observer_parent_receipt_sha256")
                    row_data["resilient_prior_certificate_sha256"] = selected["certificate_sha256"]
                    row_data["resilient_roster_manifest_sha256"] = selected["roster_source_sha256"]
                    row_data["resilient_prediction_payload_sha256"] = selected["prediction_payload_sha256"]
                    rows.append(row_data)
                continue
            if lane_key == "SOG":
                source = pd.read_csv(path, float_precision="round_trip")
                row = source.loc[
                    source.game_id.astype(int).eq(int(selected["game_id"]))
                    & source.apply(lambda value: canonical_atomic_identity("SOG", value.to_dict())
                                   == selected["atomic_prediction_identity"], axis=1)
                ].iloc[0].to_dict()
            elif path.name == "moneyline_production_predictions.csv":
                source = pd.read_csv(path, float_precision="round_trip")
                row = source.loc[source.game_id.astype(int).eq(int(selected["game_id"]))].iloc[0].to_dict()
                row["operational_role"] = "PRODUCTION_CHAMPION"
                row["model_favored_team"] = row.get("challenger_favored_team")
            elif path.name == "moneyline_schedule_logit_v2_shadow_predictions.csv":
                source = pd.read_csv(path, float_precision="round_trip")
                row = source.loc[source.game_id.astype(int).eq(int(selected["game_id"]))].iloc[0].to_dict()
                row["operational_role"] = "SHADOW"
            elif path.name == "v2_immutable_predictions.csv":
                source = pd.read_csv(path, float_precision="round_trip")
                row = source.loc[source.game_id.astype(int).eq(int(selected["game_id"]))].iloc[0].to_dict()
                row["operational_role"] = "SHADOW"
            else:
                source = _read_immutable_puck_predictions(path)
                row = source.loc[source.game_id.astype(int).eq(int(selected["game_id"]))].iloc[0].to_dict()
            row["evaluation_population"] = "RESILIENT_PREGAME"
            row["resilient_source_phase"] = selected["source_phase"]
            row["resilient_source_path"] = selected["source_path"]
            row["resilient_source_manifest_sha256"] = selected["source_manifest_sha256"]
            row["resilient_parent_receipt_path"] = selected["observer_receipt_path"]
            row["resilient_parent_receipt_sha256"] = selected["observer_receipt_sha256"]
            row["resilient_prior_certificate_sha256"] = selected["certificate_sha256"]
            row["resilient_roster_manifest_sha256"] = selected["roster_source_sha256"]
            row["resilient_prediction_payload_sha256"] = selected["prediction_payload_sha256"]
            rows.append(row)
        frame = pd.DataFrame(rows)
        if not frame.empty:
            timestamp_column = ("prediction_timestamp_utc"
                                if lane_key in {"SOG", "POINTS", "SAVES"}
                                else "prediction_creation_time_utc")
            frame = _pregame(frame, timestamp_column, schedule)
            if lane_key != "SOG":
                frame = frame.merge(games[outcome_columns], on="game_id", validate="many_to_one")
        else:
            frame = pd.DataFrame(columns=["game_id", "game_type_code", *outcome_columns[1:]])
        if lane_key == "MONEYLINE":
            # Preserve separate production and shadow populations.
            if frame.empty:
                for role in ("production_champion", "shadow"):
                    grade_frames[f"resilient_moneyline_{role}"] = pd.DataFrame(columns=[
                        "game_id", "operational_role", "evaluation_population",
                        *outcome_columns[1:],
                    ])
            else:
                for role, role_frame in frame.groupby("operational_role", dropna=False):
                    role_frame = role_frame.copy()
                    _grade_game_lines(role_frame, role_frame.iloc[0:0].copy())
                    grade_frames[f"resilient_moneyline_{str(role).lower()}"] = role_frame
        elif lane_key == "PUCK_LINE":
            _, puck_grade = _grade_game_lines(frame.iloc[0:0].copy(), frame)
            grade_frames["resilient_puck_line"] = puck_grade
        elif lane_key == "SOG":
            if frame.empty:
                grade_frames["resilient_sog"] = pd.DataFrame(columns=[
                    "canonical_season", "slate_date", "game_id", "player_id",
                    "grading_status", "evaluation_population", "reason",
                ])
                continue
            outcome = skaters.rename(columns={"participation_state": "participation_status"})[[
                "canonical_season", "slate_date", "game_id", "player_id", "official_final",
                "official_sog", "participation_status", "outcome_source", "outcome_source_timestamp_utc",
            ]]
            sog_grade = grade_predictions(frame, outcome, grading_timestamp_utc=datetime.now(timezone.utc).isoformat())
            arm = sog_grade.contract_arm.astype(str)
            if not arm.str.startswith(("A_", "B_", "C_", "D_", "F_", "G_")).all():
                raise RuntimeError("IMMUTABLE_SOG_UNKNOWN_CONTRACT_ARM")
            sog_grade["evaluation_lane"] = arm.str[0].map({
                "A": "OPERATIONAL_SHADOW_A", "B": "OPERATIONAL_SHADOW_B",
                "C": "OPERATIONAL_SHADOW_C", "D": "CHAMPION_D",
                "F": "UNQUALIFIED_SHADOW_DIAGNOSTIC_ONLY", "G": "OPERATIONAL_SHADOW_G",
            })
            sog_grade["evaluation_population"] = arm
            grade_frames["resilient_sog"] = sog_grade
            predicted_identity = set(frame[["game_id", "player_id"]].itertuples(index=False, name=None))
            missing_sog = skaters.loc[
                ~skaters[["game_id", "player_id"]].apply(tuple, axis=1).isin(predicted_identity)
            ].copy()
            missing_sog["grading_status"] = "MISSING_PROSPECTIVE_PREDICTION_EXCLUDED"
            missing_sog["reason"] = "NO_RESILIENT_PREGAME_SOG_ROW"
            grade_frames["resilient_sog_missing_predictions"] = missing_sog
        elif lane_key == "POINTS":
            if frame.empty:
                grade_frames["resilient_points"] = pd.DataFrame(columns=[
                    "game_id", "player_id", "line", "grading_status", "evaluation_population",
                ])
                continue
            point_grades = _grade_points(None, schedule, skaters, predictions=frame)
            grade_frames["resilient_points"] = point_grades.pop("points")
            for suffix, grade in point_grades.items():
                if suffix != "points":
                    grade_frames[f"resilient_{suffix}"] = grade
        else:
            if frame.empty:
                grade_frames["resilient_saves"] = pd.DataFrame(columns=[
                    "game_id", "goalie_id", "line", "grading_status", "evaluation_population",
                ])
                continue
            save_grades = _grade_saves(None, schedule, goalies, predictions=frame)
            grade_frames["resilient_saves"] = save_grades.pop("saves")
            for suffix, grade in save_grades.items():
                if suffix != "saves":
                    grade_frames[f"resilient_{suffix}"] = grade
    return grade_frames, {"contract_version": "NHL_RESILIENT_PREGAME_SOURCE_INDEX_V1",
                          "slate_date": slate_date, "lanes": indexes,
                          "proof_receipts": proof_notes}


def _resilient_grade_denominator(frame: pd.DataFrame) -> dict[str, Any]:
    if "grading_state" in frame:
        state = frame.grading_state.astype(str)
        graded_mask = state.isin({"WIN", "LOSS", "PUSH"})
        push_mask = state.eq("PUSH")
    elif "grading_status" in frame:
        state = frame.grading_status.astype(str)
        graded_mask = state.isin({"REGULAR_SEASON_GRADED", "SETTLED", "PUSH"})
        push_mask = state.eq("PUSH")
    else:
        state = pd.Series("UNGRADEABLE", index=frame.index)
        graded_mask = pd.Series(False, index=frame.index)
        push_mask = pd.Series(False, index=frame.index)
    result: dict[str, Any] = {
        "prediction_rows": int(len(frame)), "graded_rows": int(graded_mask.sum()),
        "push_rows": int(push_mask.sum()),
    }
    group_column = next((name for name in (
        "operational_role", "evaluation_lane", "contract_arm", "model_version",
        "probability_construction", "prediction_semantics",
    ) if name in frame), None)
    if group_column:
        result["grouping"] = group_column
        result["groups"] = {}
        for name, group in frame.groupby(group_column, dropna=False):
            labels = (group["grading_state"].astype(str) if "grading_state" in group
                      else group["grading_status"].astype(str) if "grading_status" in group
                      else pd.Series("UNGRADEABLE", index=group.index))
            result["groups"][str(name)] = {
                "prediction_rows": int(len(group)),
                "graded_rows": int(labels.isin({"WIN", "LOSS", "PUSH", "REGULAR_SEASON_GRADED", "SETTLED"}).sum()),
                "push_rows": int(labels.eq("PUSH").sum()),
            }
    return result


def grade_operational_sources(source_binding: dict[str, Any], schedule: pd.DataFrame,
                              games: pd.DataFrame, skaters: pd.DataFrame,
                              goalies: pd.DataFrame, observed_at: str) -> dict[str, pd.DataFrame]:
    outcome_columns = ["game_id", "official_full_game_winner", "official_final_home_goals",
                       "official_final_away_goals"]
    cross_binding = source_binding["cross_market"]
    if cross_binding["run"] is None:
        empty_columns = ["canonical_season", "slate_date", "game_id",
                         "grading_status", "regular_season_evaluation_target",
                         "game_type_code", *outcome_columns[1:]]
        moneyline = pd.DataFrame(columns=empty_columns)
        puck = pd.DataFrame(columns=empty_columns)
    else:
        cross = Path(cross_binding["run"])
        production_path = cross / "moneyline_production_predictions.csv"
        capture_status = json.loads((cross / "daily_execution_status.json").read_text())
        if (capture_status.get("mode") == "PROSPECTIVE_PRODUCTION_WITH_SHADOW_CHALLENGER"
                and not production_path.is_file()):
            raise RuntimeError("IMMUTABLE_MONEYLINE_PRODUCTION_ARTIFACT_MISSING")
        moneyline = _pregame(pd.read_csv(
            production_path if production_path.is_file() else cross / "v2_immutable_predictions.csv"),
            "prediction_creation_time_utc", schedule)
        if production_path.is_file():
            moneyline["operational_role"] = "PRODUCTION_CHAMPION"
            moneyline["model_favored_team"] = moneyline.challenger_favored_team
        puck = _pregame(pd.read_csv(cross / "puck_line_v1_immutable_predictions.csv"),
                        "prediction_creation_time_utc", schedule)
        moneyline = moneyline.merge(games[outcome_columns], on="game_id", validate="one_to_one")
        puck = puck.merge(games[outcome_columns], on="game_id", validate="one_to_one")
    moneyline, puck = _grade_game_lines(moneyline, puck)

    if source_binding["sog"].get("run") is None:
        graded_sog = _zero_evidence_surface("NOT_GRADEABLE_NO_PREDICTION",
                                            str(source_binding["sog"].get("reason")))
        missing = skaters.copy()
        missing["grading_status"] = "MISSING_PROSPECTIVE_PREDICTION_EXCLUDED"
        missing["reason"] = "NO_RESILIENT_OR_LEGACY_SOG_SOURCE"
        sog_exclusions = pd.DataFrame(columns=["game_id", "player_id", "reason"])
    else:
        sog_run = Path(source_binding["sog"]["run"])
        predictions = pd.read_csv(sog_run / "immutable_predictions.csv")
        outcome = skaters.rename(columns={"participation_state": "participation_status"})[[
            "canonical_season", "slate_date", "game_id", "player_id", "official_final",
            "official_sog", "participation_status", "outcome_source", "outcome_source_timestamp_utc",
        ]]
        outcome["participation_status"] = outcome.participation_status.replace({"PARTICIPATED": "APPEARED"})
        graded_sog = grade_predictions(predictions, outcome, grading_timestamp_utc=observed_at)
        arm = graded_sog.contract_arm.astype(str)
        allowed = arm.str.startswith(("A_", "B_", "C_", "D_", "F_", "G_"))
        if not allowed.all():
            raise RuntimeError("IMMUTABLE_SOG_UNKNOWN_CONTRACT_ARM")
        graded_sog["evaluation_lane"] = arm.str[0].map({
            "A": "OPERATIONAL_SHADOW_A", "B": "OPERATIONAL_SHADOW_B",
            "C": "OPERATIONAL_SHADOW_C", "D": "CHAMPION_D",
            "F": "UNQUALIFIED_SHADOW_DIAGNOSTIC_ONLY", "G": "OPERATIONAL_SHADOW_G",
        })
        graded_sog.loc[arm.str.startswith("D_"), "evaluation_lane"] = "CHAMPION_D"
        graded_sog.loc[arm.str.startswith("F_"), "evaluation_lane"] = "UNQUALIFIED_SHADOW_DIAGNOSTIC_ONLY"
        graded_sog["evaluation_population"] = graded_sog.contract_arm
        predicted_players = set(predictions[["game_id", "player_id"]].itertuples(index=False, name=None))
        missing = skaters.loc[
            ~skaters[["game_id", "player_id"]].apply(tuple, axis=1).isin(predicted_players)
        ].copy()
        missing["grading_status"] = "MISSING_PROSPECTIVE_PREDICTION_EXCLUDED"
        missing["reason"] = "NO_PROSPECTIVE_SOG_ROW"
        sog_exclusions = pd.read_csv(sog_run / "excluded_players.csv")

    points_status = source_binding["points"]
    saves_status = source_binding["saves"]
    extra: dict[str, pd.DataFrame] = {}
    points, point_grades = _grade_points_source(points_status, schedule, skaters)
    extra.update(point_grades)
    if saves_status["status"] in {
            "NO_PROSPECTIVE_SAVES_PREDICTIONS", "NOT_GRADEABLE_NO_PREDICTION"}:
        saves = _zero_evidence_surface(saves_status["status"], saves_status["reason"])
    else:
        save_grades = _grade_saves(Path(saves_status["run"]), schedule, goalies)
        saves = save_grades.pop("saves"); extra.update(save_grades)
    # Empty surfaces carry their status in the package summary/source lineage;
    # these columns remain schema-valid without inventing a prediction row.
    resilient_frames, resilient_index = _resilient_cross_market_grades(
        slate_date=str(source_binding["slate_date"]),
        operational_root=Path(source_binding["operational_root"]),
        schedule=schedule, games=games, skaters=skaters, goalies=goalies)
    source_binding["resilient_finalization_index"] = resilient_index
    return {"moneyline": moneyline, "puck_line": puck, "points": points, "saves": saves,
            "sog": graded_sog, "sog_missing_predictions": missing,
            "sog_source_exclusions": sog_exclusions, **extra, **resilient_frames}


@contextlib.contextmanager
def reconciliation_lock(root: Path, slate_date: str):
    path = root / ".locks" / f"{slate_date}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("NHL_POSTGAME_RECONCILIATION_ALREADY_RUNNING") from error
        yield path


def publish_reconciliation(*, canonical: pd.DataFrame, official: pd.DataFrame,
                           boxscores: dict[int, dict], slate_date: str,
                           prediction_root: Path, output_root: Path,
                           collector: Callable[[], None] | None = None,
                           observed_at: str | None = None,
                           request_journal: Path | None = None,
                           request_accounting_factory: Callable[[], dict[str, Any]] | None = None,
                           source_binding: dict[str, Any] | None = None,
                           request_lineage: dict[str, Any] | None = None,
                           ) -> tuple[Path, str]:
    observed_at = observed_at or datetime.now(timezone.utc).isoformat()
    validated = validate_final_slate(canonical, official, slate_date)
    games, skaters, goalies = build_outcomes(validated, boxscores, observed_at)
    identity = _hash_bytes(json.dumps({
        "contract": CONTRACT, "slate_date": slate_date,
        "games": _frame_hash(games), "skaters": _frame_hash(skaters), "goalies": _frame_hash(goalies),
    }, sort_keys=True).encode())
    day_root = output_root / slate_date
    destination = day_root / f"reconciliation={identity[:20]}"
    existing = sorted(day_root.glob("reconciliation=*")) if day_root.exists() else []
    if destination in existing:
        complete = destination / "RUN_COMPLETE.json"
        if not complete.is_file() or json.loads(complete.read_text()).get("substantive_identity") != identity:
            raise RuntimeError("RETAINED_RECONCILIATION_IDENTITY_CORRUPT")
        return destination, "IDEMPOTENT_EXISTING_ZERO_INSERTS"
    if existing:
        raise RuntimeError("CONFLICTING_RETAINED_OUTCOME")
    database_action_accounting = collector() if collector is not None else {
        "contract": "NHL_RECONCILIATION_DATABASE_ACTION_ACCOUNTING_V1",
        "status": "UNKNOWN",
        "reason": "no transactional collector action ledger supplied",
    }
    grades = (grade_operational_sources(source_binding, canonical, games, skaters, goalies, observed_at)
              if source_binding is not None else
              grade_catchup(prediction_root, canonical, games, skaters, goalies))
    request_accounting = request_accounting_factory() if request_accounting_factory else None
    if request_accounting is not None:
        expected_authority = 1 + len(validated)
        if request_journal is None or not request_journal.is_file():
            raise RuntimeError("OFFICIAL_REQUEST_JOURNAL_MISSING")
        if request_accounting.get("authority_boundary_logical_requests") != expected_authority:
            raise RuntimeError("OFFICIAL_REQUEST_AUTHORITY_TOTAL_MISMATCH")
        if request_accounting.get("unexpected_requests") != 0:
            raise RuntimeError("OFFICIAL_REQUEST_UNEXPECTED_IDENTITY")
        if request_accounting.get("total_logical_requests", 0) <= 0:
            raise RuntimeError("OFFICIAL_REQUEST_TOTAL_RECONCILIATION_FAILED")
    day_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".reconciliation.incomplete.", dir=day_root))
    try:
        canonical.to_csv(staging / "canonical_admitted_slate.csv", index=False)
        games.to_csv(staging / "canonical_game_outcomes.csv", index=False)
        skaters.to_csv(staging / "canonical_skater_outcomes.csv", index=False)
        goalies.to_csv(staging / "canonical_goalie_outcomes.csv", index=False)
        for lane, frame in grades.items():
            frame.to_csv(staging / f"graded_{lane}.csv", index=False)
        resilient_index = (source_binding or {}).get("resilient_finalization_index")
        if resilient_index is not None:
            (staging / "resilient_finalization_index.json").write_text(
                json.dumps(resilient_index, indent=2, sort_keys=True) + "\n")
        all_regular = canonical.game_type_code.astype(int).eq(2).all()
        all_preseason = canonical.game_type_code.astype(int).eq(1).all()
        if all_preseason:
            phase_grade_status = "PRESEASON_NON_EVALUATION"
        elif all_regular and source_binding and source_binding["cross_market"]["run"] is not None:
            phase_grade_status = "REGULAR_SEASON_GRADED"
        elif all_regular:
            phase_grade_status = "NO_IMMUTABLE_CROSS_MARKET_SOURCE"
        else:
            phase_grade_status = "MIXED_OR_UNRESOLVED_GAME_PHASE"
        points_missing_warning = (
            "NO_VALID_IMMUTABLE_FINAL_PREGAME_PREDICTION; Points grading skipped"
            if source_binding and source_binding["points"]["status"] ==
            "NOT_GRADEABLE_NO_PREDICTION" else None
        )
        summary = {
            "contract_version": CONTRACT, "slate_date": slate_date,
            "status": "COMPLETE", "substantive_identity": identity,
            "games": len(games), "skater_outcomes": len(skaters), "goalie_outcomes": len(goalies),
            "moneyline_status": phase_grade_status,
            "puck_line_status": phase_grade_status,
            "points_timestamp_qualification": (source_binding["points"]["status"] if source_binding else "RUN_SUMMARY_OBSERVATION_TIMESTAMP_PRESTART"),
            "saves_contract": (source_binding["saves"]["status"] if source_binding else "FROZEN_CONDITIONAL_STARTER_PARTICIPATION"),
            "sog_status": ("PROSPECTIVE_FINAL_PREGAME_GRADED_BY_CONTRACT_ARM" if source_binding else "NO_SEPTEMBER_19_PREDICTION_GRADE"),
            "strict_prior_update_status": "COMPLETE_AFTER_ALL_FINAL_AND_COLLECTOR_SUCCESS",
            "odds_api_requests": 0, "bookmaker_requests": 0, "paid_credits": 0,
            "database_action_accounting": database_action_accounting,
        }
        if request_accounting is not None:
            summary["official_request_accounting"] = request_accounting
            packaged_journal = staging / "official_request_journal.jsonl"
            shutil.copyfile(request_journal, packaged_journal)
            os.chmod(packaged_journal, 0o600)
            (staging / "official_request_accounting.json").write_text(
                json.dumps(request_accounting, indent=2, sort_keys=True) + "\n"
            )
        (staging / "database_action_accounting.json").write_text(
            json.dumps(database_action_accounting, indent=2, sort_keys=True) + "\n"
        )
        if source_binding is not None:
            (staging / "source_bindings.json").write_text(
                json.dumps(source_binding, indent=2, sort_keys=True) + "\n"
            )
            summary["points_reason"] = source_binding["points"].get("reason")
            summary["saves_reason"] = source_binding["saves"].get("reason")
            summary["points_status"] = source_binding["points"]["status"]
            summary["points_warning"] = points_missing_warning
            summary["saves_status"] = source_binding["saves"]["status"]
            resilient_lanes = (source_binding.get("resilient_finalization_index") or {}).get("lanes", {})
            summary["resilient_pregame"] = {
                "status": "COMPLETE_INDEXED",
                "index_file": "resilient_finalization_index.json",
                "lane_coverage": {
                    lane.lower(): {
                        "coverage_status": index.get("coverage_status"),
                        "selected_atomic_prediction_count": index.get("selected_atomic_prediction_count", 0),
                        "selected_game_count": len({row.get("game_id") for row in index.get("selected", [])}),
                        "canonical_game_count": len(index.get("canonical_game_ids", [])),
                    } for lane, index in resilient_lanes.items()
                },
                "grading_files": sorted(name for name in grades if name.startswith("resilient_")),
                "grading_denominators": {
                    name: _resilient_grade_denominator(frame)
                    for name, frame in grades.items() if name.startswith("resilient_")
                },
            }
            summary["sog_evaluation_populations"] = {
                str(key): int(value) for key, value in grades["sog"].evaluation_lane.value_counts().items()
            }
            summary["sog_missing_prospective_participants"] = len(grades["sog_missing_predictions"])
        if request_lineage is not None:
            lineage = json.loads(json.dumps(request_lineage))
            validate_request_lineage(lineage, slate_date=slate_date)
            if request_accounting is None or request_journal is None:
                raise RuntimeError("REQUEST_LINEAGE_REQUIRES_COMPLETED_JOURNAL")
            lineage["completed_request_run"] = {
                "role": "COMPLETED_EXECUTION",
                "run_id": request_accounting["run_id"],
                "journal_sha256": _sha(request_journal),
                "canonical_game_set_hash": lineage["canonical_game_set_hash"],
            }
            (staging / "request_lineage.json").write_text(
                json.dumps(lineage, indent=2, sort_keys=True) + "\n"
            )
            summary["request_lineage"] = lineage
        (staging / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        (staging / "report.md").write_text(
            f"# NHL postgame reconciliation — {slate_date}\n\n"
            f"Status: COMPLETE\n\nGames: {len(games)}; skaters: {len(skaters)}; goalies: {len(goalies)}.\n\n"
            + (("WARNING: no valid immutable FINAL_PREGAME Points prediction existed; Points grading was skipped. "
                if points_missing_warning else "")
             + ("Prospective SOG is graded by separate contract arm; Points/Saves are graded from their bound prospective sources. "
              if source_binding and source_binding["points"]["status"] == "PROSPECTIVE_SOURCE_BOUND"
              else "Prospective SOG is graded by separate contract arm; absent pre-activation Points/Saves remain zero-row evidence. "
              if source_binding else "September 19 SOG is outcome-only and has no prediction grade. ")
             + "All preseason lanes remain non-evaluation.\n")
            + ("\nOfficial NHL request accounting is reconciled end to end in "
               "official_request_accounting.json.\n" if request_accounting is not None else "")
        )
        manifest_files = sorted(path for path in staging.iterdir() if path.is_file())
        (staging / "SHA256SUMS").write_text("".join(f"{_sha(path)}  {path.name}\n" for path in manifest_files))
        (staging / "RUN_COMPLETE.json").write_text(json.dumps({
            "status": "COMPLETE", "substantive_identity": identity,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "database_action_accounting_sha256": _sha(staging / "database_action_accounting.json"),
        }, indent=2, sort_keys=True) + "\n")
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return destination, "COMPLETE_NEW_APPEND_ONLY"

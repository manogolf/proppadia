#!/usr/bin/env python3
"""Score the exact frozen Hits 0.5 model on a nonmarket pregame hitter board.

The scorer consumes the governed lineup/starter capture created by the current
nonmarket parent producer.  It does not consume that producer's model score,
the player-prop market population, odds, prices, selections, or outcomes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterator

import joblib
import numpy as np
import pandas as pd

from backend.domains.mlb import prop_workflow
from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger
from backend.mlb.prediction import make_prediction as prediction_runtime
from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    normalizeTeamAbbreviation,
)
from backend.shared.db.pg import pg_fetchall


ROOT = Path(__file__).resolve().parents[3]
MODEL_PATH = ROOT / "models_out/latest/hits.joblib"
SEMANTIC_MANIFEST = ROOT / "backend/mlb/config/semantic_models/manifests/MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json"
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3"
DEFAULT_SUMMARY_ROOT = ROOT / "artifacts/analysis/mlb/hits05_full_board_shadow"
PARENT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer"
BASELINE_POPULATION = 0.575713564031321
BASELINE_PSEUDO_GAMES = 8.0
LINE_SENSITIVITY_ALPHA = 0.90
FEATURE_CONSTRUCTION_CONTRACT = "EXACT_RUNTIME_PREPARE_PROP_BASEBALL_ONLY_V1"
ELIGIBILITY_CONTRACT = "HITS05_FULL_BOARD_CONFIRMED_LINEUP_STRICT_PREGAME_V1"
SCORE_CONTRACT = "HITS05_EXACT_ARTIFACT_DIRECT_AUC_BLEND_LINE05_V1"
MARKET_CONTEXT_KEYS = {
    "bookmaker_key",
    "implied_over",
    "implied_over_novig",
    "implied_under",
    "implied_under_novig",
    "market_hold",
    "market_implied_probability",
    "market_odds_american",
    "price_over_american",
    "price_under_american",
}
MODEL_BANNED_TOKENS = {
    "book",
    "bookmaker",
    "consensus",
    "implied",
    "market",
    "odds",
    "price",
    "sportsbook",
    "vig",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def integer(value: Any) -> int | None:
    text = clean(value)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _find_one(parent_dir: Path, pattern: str) -> Path:
    matches = sorted(path for path in parent_dir.glob(pattern) if path.is_file())
    if len(matches) != 1:
        raise RuntimeError(f"REQUIRED_PARENT_ARTIFACT_COUNT pattern={pattern} count={len(matches)}")
    return matches[0]


def latest_parent_dir(slate_date: str) -> Path | None:
    day = PARENT_ROOT / slate_date
    candidates = [path.parent for path in day.glob(f"*/hits05_lineup_source_ledger_{slate_date}.csv")]
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (path.stat().st_mtime, str(path)))[-1]


@lru_cache(maxsize=1)
def verified_model_bundle() -> dict[str, Any]:
    if sha256_file(MODEL_PATH) != ledger.MODEL_HASH:
        raise RuntimeError("FROZEN_HITS_MODEL_SHA256_MISMATCH")
    manifest = json.loads(SEMANTIC_MANIFEST.read_text(encoding="utf-8"))
    registered = manifest.get("registration_payload") or {}
    if registered.get("semantic_model_id") != ledger.MODEL_ID:
        raise RuntimeError("SEMANTIC_MODEL_ID_MISMATCH")
    if registered.get("loaded_artifact_sha256") != ledger.MODEL_HASH:
        raise RuntimeError("SEMANTIC_MODEL_HASH_BINDING_MISMATCH")
    bundle = joblib.load(MODEL_PATH)
    if not isinstance(bundle, dict) or bundle.get("lr") is None or bundle.get("rf") is None:
        raise RuntimeError("FROZEN_HITS_MODEL_BUNDLE_INVALID")
    meta = bundle.get("meta") or {}
    columns = [str(value) for value in meta.get("input_columns") or []]
    if not columns or len(columns) != 73:
        raise RuntimeError(f"FROZEN_HITS_FEATURE_ORDER_INVALID count={len(columns)}")
    contaminated = [name for name in columns if any(token in name.lower() for token in MODEL_BANNED_TOKENS)]
    if contaminated:
        raise RuntimeError(f"MARKET_FEATURE_IN_FROZEN_MODEL names={','.join(contaminated)}")
    for key in ("auc_lr", "auc_rf"):
        if key not in meta:
            raise RuntimeError(f"FROZEN_HITS_META_MISSING {key}")
    return bundle


def _predict_component(model: Any, frame: pd.DataFrame, name: str) -> float:
    if not hasattr(model, "predict_proba"):
        raise RuntimeError(f"FROZEN_COMPONENT_NO_PREDICT_PROBA component={name}")
    value = float(model.predict_proba(frame)[0][1])
    if not 0.0 <= value <= 1.0:
        raise RuntimeError(f"FROZEN_COMPONENT_PROBABILITY_INVALID component={name}")
    return value


def score_prepared_features(prepared: dict[str, Any]) -> dict[str, Any]:
    """Score one already prepared baseball-only context with the frozen bundle."""
    bundle = verified_model_bundle()
    meta = bundle["meta"]
    columns = [str(value) for value in meta["input_columns"]]
    frame = prediction_runtime._vectorize(prepared, columns)
    p_lr = _predict_component(bundle["lr"], frame, "lr")
    p_rf = _predict_component(bundle["rf"], frame, "rf")
    w_lr = max(float(meta["auc_lr"]) - 0.5, 0.0)
    w_rf = max(float(meta["auc_rf"]) - 0.5, 0.0)
    if w_lr + w_rf > 0:
        raw_probability = (p_lr * w_lr + p_rf * w_rf) / (w_lr + w_rf)
    else:
        raw_probability = (p_lr + p_rf) / 2.0
    mean_proxy = prediction_runtime._prop_mean_proxy("hits", prepared)
    scale_proxy = prediction_runtime._prop_scale_proxy("hits", prepared)
    probability = raw_probability
    if mean_proxy is not None:
        shift = LINE_SENSITIVITY_ALPHA * ((float(mean_proxy) - ledger.TARGET_LINE) / float(scale_proxy))
        probability = prediction_runtime._sigmoid(prediction_runtime._logit(raw_probability) + shift)
    probability = float(np.clip(probability, 0.0, 1.0))
    vector = {str(key): float(value) for key, value in frame.iloc[0].to_dict().items()}
    return {
        "probability_over": probability,
        "raw_auc_weighted_probability": float(raw_probability),
        "component_probability_lr": p_lr,
        "component_probability_rf": p_rf,
        "weight_lr": w_lr,
        "weight_rf": w_rf,
        "line_sensitivity_alpha": LINE_SENSITIVITY_ALPHA,
        "line_sensitivity_mean_proxy": mean_proxy,
        "line_sensitivity_scale_proxy": scale_proxy,
        "target_line": ledger.TARGET_LINE,
        "model_input_vector": vector,
        "model_input_columns": columns,
    }


@contextmanager
def _row_bound_prepare_context(row: dict[str, Any]) -> Iterator[None]:
    original_context = prop_workflow.build_game_context
    original_resolver = prop_workflow.resolve_player_candidate

    def no_implicit_game_context(*, team_id: int, game_date: str):
        _ = (team_id, game_date)
        return None

    def exact_player(*, player_id: int | None, name: str | None, team_abbr: str | None):
        _ = (name, team_abbr)
        if player_id is None or int(player_id) != int(row["player_id"]):
            return None
        return {
            "player_id": int(row["player_id"]),
            "player_name": row["player_name"],
            "team_id": int(row["team_id"]),
            "team_abbr": row["team"],
        }

    prop_workflow.build_game_context = no_implicit_game_context  # type: ignore[assignment]
    prop_workflow.resolve_player_candidate = exact_player  # type: ignore[assignment]
    try:
        yield
    finally:
        prop_workflow.build_game_context = original_context  # type: ignore[assignment]
        prop_workflow.resolve_player_candidate = original_resolver  # type: ignore[assignment]


def prepare_baseball_features(row: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "player_id": int(row["player_id"]),
        "player_name": row["player_name"],
        "team_id": int(row["team_id"]),
        "team_abbr": row["team"],
        "game_date": row["slate_date"],
        "game_id": int(row["game_id"]),
        "game_type": row.get("game_type") or "R",
        "game_time": row["scheduled_start_utc"],
        "is_home": bool(row["is_home"]),
        "home_team_code": row["home_team"],
        "away_team_code": row["away_team"],
        "opponent_team_id": int(row["opponent_team_id"]),
        "opponent": row["opponent"],
        "opponent_encoded": int(row["opponent_team_id"]),
        "game_day_of_week": parse_dt(row["scheduled_start_utc"]).weekday(),
        "time_of_day_bucket": row["time_of_day_bucket"],
        "starting_pitcher_id": int(row["opposing_starter_id"]),
        "prop_type": "hits",
        # This is the fixed target definition, not a sportsbook line.
        "prop_value": ledger.TARGET_LINE,
        "line": ledger.TARGET_LINE,
        "over_under": "over",
    }
    if set(payload) & MARKET_CONTEXT_KEYS:
        raise RuntimeError("MARKET_FIELD_ENTERED_FULL_BOARD_PREPARE_PAYLOAD")
    with _row_bound_prepare_context(row):
        prepared = prop_workflow.prepare_prop(payload)
    for key in MARKET_CONTEXT_KEYS:
        value = prepared.get(key)
        if value not in {None, ""}:
            raise RuntimeError(f"MARKET_VALUE_ENTERED_PREPARED_CONTEXT field={key}")
    return {key: value for key, value in prepared.items() if key not in MARKET_CONTEXT_KEYS}


def _team_from_schedule(team: dict[str, Any]) -> dict[str, Any]:
    data = team.get("team") or team
    team_id = integer(data.get("id"))
    abbreviation = normalizeTeamAbbreviation(
        data.get("abbreviation") or data.get("teamCode") or data.get("fileCode") or getFullTeamAbbreviationFromID(team_id or 0)
    )
    return {"team_id": team_id, "team": abbreviation or clean(data.get("name"))}


def load_schedule_context(parent_dir: Path, slate_date: str) -> tuple[dict[int, dict[str, Any]], Path]:
    raw_dir = parent_dir / "governed_lineup_capture/raw"
    paths = sorted(raw_dir.glob(f"statsapi_schedule_{slate_date}_*.json"))
    if len(paths) != 1:
        raise RuntimeError(f"SCHEDULE_SOURCE_COUNT_INVALID count={len(paths)}")
    path = paths[0]
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("SCHEDULE_SOURCE_ROOT_NOT_OBJECT")
    games: dict[int, dict[str, Any]] = {}
    for date_payload in payload.get("dates") or []:
        for game in date_payload.get("games") or []:
            game_id = integer(game.get("gamePk"))
            if game_id is None:
                continue
            teams = game.get("teams") or {}
            home = _team_from_schedule(teams.get("home") or {})
            away = _team_from_schedule(teams.get("away") or {})
            start = parse_dt(game.get("gameDate"))
            games[game_id] = {
                "game_id": game_id,
                "slate_date": slate_date,
                "scheduled_start_utc": iso(start) if start else "",
                "game_type": clean(game.get("gameType")),
                "doubleheader": clean(game.get("doubleHeader")),
                "game_number": integer(game.get("gameNumber")),
                "home_team_id": home["team_id"],
                "home_team": home["team"],
                "away_team_id": away["team_id"],
                "away_team": away["team"],
                "detailed_state": clean((game.get("status") or {}).get("detailedState")),
                "abstract_state": clean((game.get("status") or {}).get("abstractGameState")),
            }
    if not games:
        raise RuntimeError("SCHEDULE_SOURCE_ZERO_GAMES")
    return games, path


def _time_bucket(start: datetime) -> str:
    hour_et = start.astimezone(__import__("zoneinfo").ZoneInfo("America/New_York")).hour
    if hour_et < 12:
        return "morning"
    if hour_et < 17:
        return "afternoon"
    return "evening"


def classify_lineup_rows(
    parent_dir: Path,
    slate_date: str,
    capture_time: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Path]]:
    lineup_path = _find_one(parent_dir, f"hits05_lineup_source_ledger_{slate_date}.csv")
    team_status_candidates = sorted((parent_dir / "governed_lineup_capture").glob(f"lineup_team_status_{slate_date}.csv"))
    team_status_path = team_status_candidates[0] if len(team_status_candidates) == 1 else None
    machine_path = _find_one(parent_dir, f"machine_readable_hits05_current_nonmarket_parent_producer_{slate_date}.json")
    try:
        lineups = pd.read_csv(lineup_path, low_memory=False)
    except pd.errors.EmptyDataError:
        lineups = pd.DataFrame()
    schedule, schedule_path = load_schedule_context(parent_dir, slate_date)
    team_observations: list[dict[str, Any]] = []
    if team_status_path is not None:
        team_status = pd.read_csv(team_status_path, low_memory=False)
        for source in team_status.to_dict(orient="records"):
            if clean(source.get("lineup_status")) == "CONFIRMED_LINEUP":
                continue
            start = parse_dt(source.get("first_pitch_timestamp"))
            reason = clean(source.get("validation_reason")) or clean(source.get("lineup_status")) or "OFFICIAL_LINEUP_NOT_AVAILABLE"
            if start is not None and not capture_time < start:
                reason = "PREGAME_CUTOFF_FAILED"
            team_observations.append({
                "slate_date": slate_date,
                "game_id": integer(source.get("game_id")),
                "player_id": None,
                "player_name": "",
                "team": normalizeTeamAbbreviation(source.get("team")) or clean(source.get("team")),
                "run_tag": clean(source.get("run_tag")) or parent_dir.name,
                "eligibility_state": "TEAM_BOARD_PENDING_RETRY" if reason != "PREGAME_CUTOFF_FAILED" else "EXCLUDED_FAIL_CLOSED",
                "exclusion_reason": reason,
                "capture_timestamp_utc": iso(capture_time),
                "scheduled_start_utc": iso(start) if start else "",
                "eligibility_contract": ELIGIBILITY_CONTRACT,
                "lineup_status": clean(source.get("lineup_status")),
            })
    if lineups.empty:
        artifacts = {"lineup": lineup_path, "schedule": schedule_path, "machine": machine_path}
        if team_status_path:
            artifacts["team_status"] = team_status_path
        return [], team_observations, artifacts
    required = {
        "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "lineup_slot",
        "position", "opposing_starter_id", "lineup_status", "source_timestamp", "first_pitch_timestamp",
        "pregame_validity_state", "raw_response_path", "raw_response_sha256",
    }
    missing = required - set(lineups.columns)
    if missing:
        raise RuntimeError(f"LINEUP_SOURCE_SCHEMA_MISSING fields={','.join(sorted(missing))}")
    keyed = lineups.assign(
        game_id_int=pd.to_numeric(lineups.game_id, errors="coerce"),
        player_id_int=pd.to_numeric(lineups.player_id, errors="coerce"),
    )
    if keyed.duplicated(["game_id_int", "player_id_int"]).any():
        raise RuntimeError("DUPLICATE_PLAYER_GAME_IN_GOVERNED_LINEUP_SOURCE")
    eligible: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = list(team_observations)
    for source in lineups.to_dict(orient="records"):
        game_id = integer(source.get("game_id"))
        player_id = integer(source.get("player_id"))
        schedule_row = schedule.get(game_id or -1)
        start = parse_dt(source.get("first_pitch_timestamp")) or parse_dt((schedule_row or {}).get("scheduled_start_utc"))
        lineup_time = parse_dt(source.get("source_timestamp"))
        reason = ""
        if clean(source.get("slate_date")) != slate_date:
            reason = "SLATE_DATE_MISMATCH"
        elif game_id is None or player_id is None or schedule_row is None:
            reason = "PLAYER_GAME_IDENTITY_UNRESOLVED"
        elif clean(source.get("lineup_status")) != "CONFIRMED_LINEUP":
            reason = "OFFICIAL_CONFIRMED_LINEUP_REQUIRED"
        elif clean(source.get("pregame_validity_state")) != "VALID_PREGAME":
            reason = "LINEUP_SOURCE_NOT_VALID_PREGAME"
        elif start is None or lineup_time is None:
            reason = "TIMING_UNRESOLVED"
        elif not lineup_time < start:
            reason = "LINEUP_SOURCE_NOT_BEFORE_START"
        elif not capture_time < start:
            reason = "PREGAME_CUTOFF_FAILED"
        elif lineup_time > capture_time:
            reason = "LINEUP_SOURCE_AFTER_SCORE_TIMESTAMP"
        elif integer(source.get("opposing_starter_id")) is None:
            reason = "OPPOSING_STARTER_UNRESOLVED"
        elif schedule_row["abstract_state"] not in {"Preview", ""}:
            reason = "GAME_NOT_PREGAME"
        else:
            source_team = normalizeTeamAbbreviation(source.get("team"))
            if source_team == schedule_row["home_team"]:
                is_home = True
                team_id, opponent_id = schedule_row["home_team_id"], schedule_row["away_team_id"]
                opponent = schedule_row["away_team"]
            elif source_team == schedule_row["away_team"]:
                is_home = False
                team_id, opponent_id = schedule_row["away_team_id"], schedule_row["home_team_id"]
                opponent = schedule_row["home_team"]
            else:
                reason = "TEAM_SCHEDULE_IDENTITY_MISMATCH"
                team_id = opponent_id = None
                opponent = ""
        observation = {
            "slate_date": slate_date,
            "game_id": game_id,
            "player_id": player_id,
            "player_name": clean(source.get("player_name")),
            "team": normalizeTeamAbbreviation(source.get("team")) or clean(source.get("team")),
            "run_tag": clean(source.get("run_tag")) or parent_dir.name,
            "eligibility_state": "TECHNICALLY_ELIGIBLE" if not reason else "EXCLUDED_FAIL_CLOSED",
            "exclusion_reason": reason,
            "capture_timestamp_utc": iso(capture_time),
            "scheduled_start_utc": iso(start) if start else "",
            "eligibility_contract": ELIGIBILITY_CONTRACT,
        }
        observations.append(observation)
        if reason:
            continue
        assert schedule_row is not None and game_id is not None and player_id is not None and start is not None
        eligible.append({
            "slate_date": slate_date,
            "game_id": game_id,
            "player_id": player_id,
            "player_name": clean(source.get("player_name")),
            "team_id": int(team_id),
            "team": normalizeTeamAbbreviation(source.get("team")) or clean(source.get("team")),
            "opponent_team_id": int(opponent_id),
            "opponent": opponent,
            "is_home": bool(is_home),
            "home_team": schedule_row["home_team"],
            "away_team": schedule_row["away_team"],
            "scheduled_start_utc": iso(start),
            "game_type": schedule_row["game_type"],
            "doubleheader": schedule_row["doubleheader"],
            "game_number": schedule_row["game_number"],
            "lineup_slot": int(integer(source.get("lineup_slot")) or 0),
            "lineup_bucket": clean(source.get("lineup_bucket")),
            "position": clean(source.get("position")),
            "lineup_status": "CONFIRMED_LINEUP",
            "lineup_source_timestamp_utc": iso(lineup_time),
            "opposing_starter_id": int(integer(source.get("opposing_starter_id"))),
            "opposing_starter_name": clean(source.get("opposing_starter_name")),
            "time_of_day_bucket": _time_bucket(start),
            "raw_response_path": clean(source.get("raw_response_path")),
            "raw_response_sha256": clean(source.get("raw_response_sha256")),
            "source_url": clean(source.get("source_url")),
            "parser_version": clean(source.get("parser_version")),
        })
    artifacts = {"lineup": lineup_path, "schedule": schedule_path, "machine": machine_path}
    if team_status_path:
        artifacts["team_status"] = team_status_path
    return eligible, observations, artifacts


HistoryProvider = Callable[[list[int], str], dict[int, dict[str, Any]]]


def strict_prior_hitter_history(player_ids: list[int], slate_date: str) -> dict[int, dict[str, Any]]:
    if not player_ids:
        return {}
    rows = pg_fetchall(
        """
        SELECT player_id::bigint AS player_id,
               COUNT(*)::int AS resolved_games,
               SUM(CASE WHEN COALESCE(hits,0) >= 1 THEN 1 ELSE 0 END)::int AS games_with_hit,
               MAX(game_date)::text AS latest_prior_game_date
        FROM mlb.player_stats
        WHERE player_id = ANY(%s)
          AND game_date::date < %s::date
          AND COALESCE(plate_appearances,0) > 0
        GROUP BY player_id
        """,
        (sorted(set(player_ids)), slate_date),
    )
    return {
        int(row["player_id"]): {
            "resolved_games": int(row.get("resolved_games") or 0),
            "games_with_hit": int(row.get("games_with_hit") or 0),
            "latest_prior_game_date": clean(row.get("latest_prior_game_date")),
        }
        for row in rows
    }


def _input_artifacts(artifacts: dict[str, Path], row: dict[str, Any]) -> list[dict[str, Any]]:
    output = [
        {"role": role, "path": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size}
        for role, path in sorted(artifacts.items())
    ]
    raw_path = ROOT / row["raw_response_path"] if row["raw_response_path"] else None
    if raw_path and raw_path.exists():
        actual_hash = sha256_file(raw_path)
        if row["raw_response_sha256"] and actual_hash != row["raw_response_sha256"]:
            raise RuntimeError("LINEUP_RAW_RESPONSE_HASH_MISMATCH")
        output.append({"role": "lineup_boxscore_raw", "path": rel(raw_path), "sha256": actual_hash, "bytes": raw_path.stat().st_size})
    output.extend([
        {"role": "frozen_model", "path": rel(MODEL_PATH), "sha256": sha256_file(MODEL_PATH), "bytes": MODEL_PATH.stat().st_size},
        {"role": "semantic_manifest", "path": rel(SEMANTIC_MANIFEST), "sha256": sha256_file(SEMANTIC_MANIFEST), "bytes": SEMANTIC_MANIFEST.stat().st_size},
        {"role": "feature_preparation_code", "path": rel(Path(prop_workflow.__file__)), "sha256": sha256_file(Path(prop_workflow.__file__)), "bytes": Path(prop_workflow.__file__).stat().st_size},
        {"role": "prediction_runtime_code", "path": rel(Path(prediction_runtime.__file__)), "sha256": sha256_file(Path(prediction_runtime.__file__)), "bytes": Path(prediction_runtime.__file__).stat().st_size},
        {"role": "scoring_code", "path": rel(Path(__file__).resolve()), "sha256": sha256_file(Path(__file__).resolve()), "bytes": Path(__file__).stat().st_size},
    ])
    return output


def _replay_references(row: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    query_contracts = {
        "derived_features": "mlb.player_derived_stats current pregame feature row or latest date <= slate date; capture must be pre-start",
        "bvp_features": "mlb.prop_features_precomputed prop_type=hits feature_set_tag=v1 current game pregame or latest date <= slate date",
        "baseline_history": "mlb.player_stats player_id exact and game_date < slate_date and plate_appearances > 0",
    }
    return {
        "experiment_id": ledger.EXPERIMENT_ID,
        "eligibility_contract": ELIGIBILITY_CONTRACT,
        "feature_construction_contract": FEATURE_CONSTRUCTION_CONTRACT,
        "score_contract": SCORE_CONTRACT,
        "game_identity": {"slate_date": row["slate_date"], "game_id": row["game_id"], "game_number": row["game_number"], "doubleheader": row["doubleheader"]},
        "player_identity": row["player_id"],
        "lineup_source_timestamp_utc": row["lineup_source_timestamp_utc"],
        "scheduled_start_utc": row["scheduled_start_utc"],
        "opposing_starter_id": row["opposing_starter_id"],
        "strict_prior_baseline_state": history,
        "database_query_contracts": query_contracts,
        "database_query_contracts_sha256": ledger.payload_hash(query_contracts),
        "market_inputs_in_model": False,
        "outcomes_accessed_during_scoring": 0,
    }


def _feature_payload(row: dict[str, Any], prepared: dict[str, Any], scored: dict[str, Any]) -> dict[str, Any]:
    support_keys = [
        "rolling_result_avg_7", "d7_hits", "d15_hits", "d30_hits", "line_diff",
        "bvp_at_bats", "bvp_hits", "bvp_home_runs", "bvp_strikeouts", "bvp_walks",
        "bvp_plate_appearances", "bvp_total_bases", "starting_pitcher_id",
    ]
    return {
        "feature_construction_contract": FEATURE_CONSTRUCTION_CONTRACT,
        "model_input_vector": scored["model_input_vector"],
        "model_input_columns": scored["model_input_columns"],
        "line_sensitivity_support": {key: prepared.get(key) for key in support_keys},
        "baseball_context": {
            "slate_date": row["slate_date"], "game_id": row["game_id"], "player_id": row["player_id"],
            "team": row["team"], "opponent": row["opponent"], "is_home": row["is_home"],
            "lineup_slot": row["lineup_slot"], "lineup_status": row["lineup_status"],
            "opposing_starter_id": row["opposing_starter_id"], "scheduled_start_utc": row["scheduled_start_utc"],
        },
        "component_probabilities": {"lr": scored["component_probability_lr"], "rf": scored["component_probability_rf"]},
        "blend_weights": {"lr": scored["weight_lr"], "rf": scored["weight_rf"]},
        "raw_auc_weighted_probability": scored["raw_auc_weighted_probability"],
        "line_sensitivity_alpha": scored["line_sensitivity_alpha"],
        "target_line_fixed_not_sportsbook": ledger.TARGET_LINE,
        "market_inputs_in_model": False,
        "outcomes_accessed_during_scoring": 0,
    }


def _rank_map(probabilities: dict[str, float]) -> dict[str, tuple[int, float]]:
    ordered = sorted(probabilities, key=lambda identity: (-probabilities[identity], identity))
    count = len(ordered)
    return {
        identity: (rank, 1.0 if count == 1 else 1.0 - ((rank - 1) / (count - 1)))
        for rank, identity in enumerate(ordered, start=1)
    }


def score_board(
    *,
    slate_date: str,
    run_tag: str,
    parent_dir: Path,
    capture_time: datetime,
    ledger_path: Path,
    evidence_mode: str = "PROSPECTIVE",
    history_provider: HistoryProvider = strict_prior_hitter_history,
) -> dict[str, Any]:
    if evidence_mode == "PROSPECTIVE" and slate_date < ledger.EXPERIMENT_START_DATE:
        return {
            "status": "FULL_BOARD_SHADOW_NOT_STARTED",
            "slate_date": slate_date,
            "experiment_start_date": ledger.EXPERIMENT_START_DATE,
            "new_prediction_rows": 0,
            "outcomes_accessed": 0,
        }
    verified_model_bundle()
    eligible, observations, artifacts = classify_lineup_rows(parent_dir, slate_date, capture_time)
    connection = ledger.connect_ledger(ledger_path)
    for observation in observations:
        observation["run_tag"] = run_tag
        ledger.append_eligibility_observation(connection, observation)
    existing = ledger.prediction_identities(connection, slate_date)
    candidates = [
        row for row in eligible
        if ledger.canonical_identity(slate_date, row["game_id"], row["player_id"]) not in existing
    ]
    history = history_provider([int(row["player_id"]) for row in candidates], slate_date)
    staged: list[dict[str, Any]] = []
    scoring_failures: list[dict[str, Any]] = []
    for row in candidates:
        identity = ledger.canonical_identity(slate_date, row["game_id"], row["player_id"])
        try:
            prepared = prepare_baseball_features(row)
            scored = score_prepared_features(prepared)
            player_history = history.get(int(row["player_id"]), {"resolved_games": 0, "games_with_hit": 0, "latest_prior_game_date": ""})
            n = int(player_history.get("resolved_games") or 0)
            hits = int(player_history.get("games_with_hit") or 0)
            baseline_hitter = (hits + BASELINE_PSEUDO_GAMES * BASELINE_POPULATION) / (n + BASELINE_PSEUDO_GAMES)
            inputs = _input_artifacts(artifacts, row)
            replay = _replay_references(row, player_history)
            feature_payload = _feature_payload(row, prepared, scored)
            staged.append({
                "identity": identity,
                "row": row,
                "probability": scored["probability_over"],
                "baseline_hitter": baseline_hitter,
                "history": player_history,
                "feature_payload": feature_payload,
                "replay": replay,
                "inputs": inputs,
            })
        except Exception as exc:
            scoring_failures.append({"identity": identity, "reason": f"{type(exc).__name__}:{exc}"})
            failure_observation = {
                "slate_date": slate_date, "game_id": row["game_id"], "player_id": row["player_id"],
                "player_name": row["player_name"], "team": row["team"], "run_tag": run_tag,
                "eligibility_state": "EXCLUDED_FAIL_CLOSED", "exclusion_reason": "MODEL_INPUT_OR_SCORE_INTEGRITY_FAILED",
                "capture_timestamp_utc": iso(capture_time), "scheduled_start_utc": row["scheduled_start_utc"],
                "eligibility_contract": ELIGIBILITY_CONTRACT,
            }
            ledger.append_eligibility_observation(connection, failure_observation)
    existing_payloads = ledger.predictions_for_date(connection, slate_date)
    all_probabilities = {item["canonical_identity"]: float(item["probability_over"]) for item in existing_payloads}
    all_probabilities.update({item["identity"]: float(item["probability"]) for item in staged})
    ranks = _rank_map(all_probabilities)
    new_rows = 0
    conflicts = 0
    for item in staged:
        row = item["row"]
        rank, percentile = ranks[item["identity"]]
        prediction = {
            "experiment_id": ledger.EXPERIMENT_ID,
            "slate_date": slate_date,
            "game_id": int(row["game_id"]),
            "player_id": int(row["player_id"]),
            "player_name": row["player_name"],
            "team": row["team"],
            "opponent": row["opponent"],
            "is_home": bool(row["is_home"]),
            "lineup_slot": int(row["lineup_slot"]),
            "lineup_status": row["lineup_status"],
            "opposing_starter_id": int(row["opposing_starter_id"]),
            "opposing_starter_name": row["opposing_starter_name"],
            "game_number": row["game_number"],
            "doubleheader": row["doubleheader"],
            "scheduled_start_utc": row["scheduled_start_utc"],
            "prediction_timestamp_utc": iso(capture_time),
            "run_tag": run_tag,
            "eligibility_state": "TECHNICALLY_ELIGIBLE_AND_SCORED",
            "exclusion_reason": "",
            "eligibility_contract": ELIGIBILITY_CONTRACT,
            "feature_construction_contract": FEATURE_CONSTRUCTION_CONTRACT,
            "score_contract": SCORE_CONTRACT,
            "model_semantic_id": ledger.MODEL_ID,
            "model_artifact_path": rel(MODEL_PATH),
            "model_artifact_sha256": ledger.MODEL_HASH,
            "semantic_manifest_path": rel(SEMANTIC_MANIFEST),
            "semantic_manifest_sha256": sha256_file(SEMANTIC_MANIFEST),
            "probability_semantics": "P(official_hits >= 1) for fixed target line 0.5",
            "probability_over": float(item["probability"]),
            "score_board_rank": rank,
            "score_board_percentile": percentile,
            "rank_semantics": "DESCENDING_PROBABILITY_AMONG_ALL_IDENTITIES_FROZEN_AS_OF_SCORE_TIMESTAMP",
            "baseline_population_probability": BASELINE_POPULATION,
            "baseline_hitter_shrunk_probability": float(item["baseline_hitter"]),
            "baseline_hitter_prior_games": int(item["history"].get("resolved_games") or 0),
            "baseline_hitter_prior_hits": int(item["history"].get("games_with_hit") or 0),
            "feature_state_sha256": ledger.payload_hash(item["feature_payload"]),
            "replay_references_sha256": ledger.payload_hash(item["replay"]),
            "input_artifacts_sha256": ledger.payload_hash(item["inputs"]),
            "prestart_integrity_result": "PASS_STRICTLY_BEFORE_SCHEDULED_START",
            "market_observation_required_for_admission": False,
            "market_inputs_in_model": False,
            "outcomes_accessed_during_scoring": 0,
            "grading_status": "UNGRADED_OUTCOME_SEPARATE_LEDGER",
            "evidence_mode": evidence_mode,
        }
        action = ledger.append_prediction_with_context(
            connection, prediction, item["feature_payload"], item["replay"], item["inputs"]
        )
        if action == "APPENDED_NEW":
            new_rows += 1
        elif action == "EXISTING_IDENTITY_DIFFERENT_CAPTURE_PRESERVED":
            conflicts += 1
    # Append an immutable rank observation for the complete board known after
    # this run.  Predictions themselves remain unchanged as later lineups post.
    for identity, (rank, percentile) in ranks.items():
        ledger.append_rank_snapshot(connection, identity, {
            "slate_date": slate_date,
            "snapshot_timestamp_utc": iso(capture_time),
            "board_rows": len(ranks),
            "board_rank": rank,
            "board_percentile": percentile,
            "snapshot_class": "RUN_AS_OF_FULL_BOARD_RANK",
        })
    retryable_pending = sum(row["eligibility_state"] == "TEAM_BOARD_PENDING_RETRY" for row in observations)
    if scoring_failures or conflicts:
        run_status = "PASS_WITH_FAIL_CLOSED_EXCLUSIONS"
    elif retryable_pending:
        run_status = "PASS_WITH_RETRYABLE_PENDING_LINEUPS"
    else:
        run_status = "PASS"
    run_payload = {
        "run_tag": run_tag,
        "slate_date": slate_date,
        "capture_timestamp_utc": iso(capture_time),
        "evidence_mode": evidence_mode,
        "run_status": run_status,
        "parent_dir": rel(parent_dir),
        "eligible_rows": len(eligible),
        "new_prediction_rows": new_rows,
        "existing_prediction_rows": len(eligible) - len(candidates),
        "excluded_rows": len([row for row in observations if row["eligibility_state"] != "TECHNICALLY_ELIGIBLE"]) + len(scoring_failures),
        "scoring_failures": scoring_failures,
        "retryable_pending_team_boards": retryable_pending,
        "identity_conflicts": conflicts,
        "board_rows_after_run": len(ranks),
        "model_semantic_id": ledger.MODEL_ID,
        "model_artifact_sha256": ledger.MODEL_HASH,
        "market_inputs_in_model": False,
        "market_required_for_population": False,
        "outcomes_accessed": 0,
    }
    ledger.append_run(connection, run_payload)
    return {**run_payload, "ledger_counts": ledger.counts(connection)}


def score_fixture(
    fixture_path: Path,
    *,
    capture_time: datetime,
    ledger_path: Path,
    run_tag: str,
) -> dict[str, Any]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    if not isinstance(fixture, dict) or not isinstance(fixture.get("rows"), list):
        raise RuntimeError("FIXTURE_SCHEMA_INVALID")
    verified_model_bundle()
    connection = ledger.connect_ledger(ledger_path)
    staged = []
    for source in fixture["rows"]:
        start = parse_dt(source.get("scheduled_start_utc"))
        if start is None or not capture_time < start:
            raise RuntimeError("FIXTURE_NOT_STRICT_PREGAME")
        scored = score_prepared_features(source["prepared_features"])
        identity = ledger.canonical_identity(source["slate_date"], source["game_id"], source["player_id"])
        staged.append((identity, source, scored))
    ranks = _rank_map({identity: scored["probability_over"] for identity, _, scored in staged})
    new_rows = 0
    for identity, source, scored in staged:
        rank, percentile = ranks[identity]
        feature_payload = {
            "feature_construction_contract": FEATURE_CONSTRUCTION_CONTRACT,
            "model_input_vector": scored["model_input_vector"],
            "model_input_columns": scored["model_input_columns"],
            "fixture_process_only": True,
            "market_inputs_in_model": False,
            "outcomes_accessed_during_scoring": 0,
        }
        replay = {"fixture": rel(fixture_path), "fixture_sha256": sha256_file(fixture_path), "evidence_counted": False}
        inputs = [
            {"role": "fixture", "path": rel(fixture_path), "sha256": sha256_file(fixture_path), "bytes": fixture_path.stat().st_size},
            {"role": "frozen_model", "path": rel(MODEL_PATH), "sha256": ledger.MODEL_HASH, "bytes": MODEL_PATH.stat().st_size},
        ]
        prediction = {
            "experiment_id": ledger.EXPERIMENT_ID,
            "slate_date": source["slate_date"], "game_id": int(source["game_id"]), "player_id": int(source["player_id"]),
            "player_name": source["player_name"], "team": source["team"], "opponent": source["opponent"],
            "scheduled_start_utc": source["scheduled_start_utc"], "prediction_timestamp_utc": iso(capture_time),
            "run_tag": run_tag, "model_semantic_id": ledger.MODEL_ID, "model_artifact_sha256": ledger.MODEL_HASH,
            "probability_over": scored["probability_over"], "score_board_rank": rank, "score_board_percentile": percentile,
            "baseline_population_probability": BASELINE_POPULATION, "baseline_hitter_shrunk_probability": BASELINE_POPULATION,
            "feature_state_sha256": ledger.payload_hash(feature_payload), "replay_references_sha256": ledger.payload_hash(replay),
            "input_artifacts_sha256": ledger.payload_hash(inputs), "prestart_integrity_result": "PASS_FIXTURE_STRICTLY_BEFORE_START",
            "market_inputs_in_model": False, "market_observation_required_for_admission": False,
            "outcomes_accessed_during_scoring": 0, "grading_status": "NOT_GRADED_PROCESS_ONLY_REPLAY",
            "evidence_mode": "PROCESS_ONLY_REPLAY",
        }
        if ledger.append_prediction_with_context(connection, prediction, feature_payload, replay, inputs) == "APPENDED_NEW":
            new_rows += 1
    run_payload = {
        "run_tag": run_tag, "slate_date": fixture["rows"][0]["slate_date"], "capture_timestamp_utc": iso(capture_time),
        "evidence_mode": "PROCESS_ONLY_REPLAY", "run_status": "PASS_PROCESS_ONLY_NOT_PROSPECTIVE_EVIDENCE",
        "eligible_rows": len(staged), "new_prediction_rows": new_rows, "existing_prediction_rows": len(staged)-new_rows,
        "excluded_rows": 0, "outcomes_accessed": 0,
    }
    ledger.append_run(connection, run_payload)
    return {**run_payload, "ledger_counts": ledger.counts(connection)}


def write_summary(result: dict[str, Any], summary_path: Path) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slate-date", default="")
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--parent-dir", type=Path)
    parser.add_argument("--capture-timestamp", default="")
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--summary-json", type=Path)
    parser.add_argument("--evidence-mode", choices=["PROSPECTIVE", "PROCESS_ONLY_REPLAY"], default="PROSPECTIVE")
    parser.add_argument("--fixture", type=Path)
    args = parser.parse_args()
    capture_time = parse_dt(args.capture_timestamp) or now_utc()
    if args.fixture:
        result = score_fixture(args.fixture, capture_time=capture_time, ledger_path=args.ledger, run_tag=args.run_tag)
        slate_date = result["slate_date"]
    else:
        if not args.slate_date:
            parser.error("--slate-date is required without --fixture")
        slate_date = args.slate_date
        parent_dir = args.parent_dir or latest_parent_dir(slate_date)
        if parent_dir is None:
            raise RuntimeError(f"GOVERNED_PARENT_DIR_NOT_FOUND slate_date={slate_date}")
        result = score_board(
            slate_date=slate_date,
            run_tag=args.run_tag,
            parent_dir=parent_dir,
            capture_time=capture_time,
            ledger_path=args.ledger,
            evidence_mode=args.evidence_mode,
        )
    summary = args.summary_json or DEFAULT_SUMMARY_ROOT / slate_date / f"hits05_full_board_score_{args.run_tag}.json"
    write_summary(result, summary)
    print(json.dumps({"summary_json": rel(summary), **result}, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

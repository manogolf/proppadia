"""Pure validation, outcome construction, grading, and append-only publication."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from backend.nhl.cross_market_shadow.core import (
    FEATURES as CROSS_MARKET_FEATURES,
    PARAMETER_PATH as MONEYLINE_PARAMETER_PATH,
    PUCK_PARAMETER_PATH,
    digest_value,
)
from backend.nhl.sog_cold_start.core import (
    CONTRACT_PATH as SOG_CONTRACT_PATH,
    digest as sog_digest,
    grade_predictions,
)


CONTRACT = "NHL_POSTGAME_RECONCILIATION_V1"
FINAL_STATES = {"FINAL", "OFF"}
SUPPORTED_GAME_TYPES = {1, 2, 3}
PROSPECTIVE_PROP_NOT_BEFORE = "2026-09-21"


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


def _verify_cross_market_identities(moneyline: pd.DataFrame, puck: pd.DataFrame) -> None:
    for row in moneyline.itertuples(index=False):
        raw = {name: getattr(row, name) for name in CROSS_MARKET_FEATURES}
        substantive = {
            "canonical_season": int(row.canonical_season), "game_id": int(row.game_id),
            "scheduled_start_time_utc": pd.Timestamp(row.scheduled_start_time_utc).isoformat(),
            "home_team": row.home_team, "away_team": row.away_team,
            "features": raw, "parameter_sha256": _sha(MONEYLINE_PARAMETER_PATH),
        }
        if digest_value(substantive) != row.substantive_prediction_sha256:
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
            raise RuntimeError("IMMUTABLE_PUCK_LINE_SUBSTANTIVE_IDENTITY_MISMATCH")


def resolve_operational_sources(*, slate_date: str, operational_root: Path) -> dict[str, Any]:
    """Resolve and fully validate immutable local inputs without I/O outside disk."""
    cross = _one_run(
        operational_root / "cross_market_shadow" / "season=2026" / f"slate_date={slate_date}"
        / "run_type=FINAL_PREGAME" / "state=*", "CROSS_MARKET_FINAL_PREGAME",
    )
    cross_manifest = _verify_manifest(cross)
    status = json.loads((cross / "daily_execution_status.json").read_text())
    if (status.get("slate_date") != slate_date or status.get("run_type") != "FINAL_PREGAME"
            or cross.name != f"state={status.get('substantive_state_sha256')}"):
        raise RuntimeError("IMMUTABLE_CROSS_MARKET_RUN_IDENTITY_MISMATCH")
    schedule = pd.read_csv(cross / "schedule_event_identity.csv").rename(
        columns={"game_type_code": "game_type_code"})
    schedule = _local_spine(schedule, slate_date)
    moneyline = pd.read_csv(cross / "v2_immutable_predictions.csv")
    puck = pd.read_csv(cross / "puck_line_v1_immutable_predictions.csv")
    common = {"canonical_season", "slate_date", "game_id", "scheduled_start_time_utc",
              "home_team_id", "away_team_id", "home_team", "away_team",
              "prediction_creation_time_utc", "substantive_prediction_sha256"} | set(CROSS_MARKET_FEATURES)
    _require_columns(moneyline, common, "MONEYLINE")
    _require_columns(puck, common, "PUCK_LINE")
    ids = set(schedule.game_id.astype(int))
    for label, frame in (("MONEYLINE", moneyline), ("PUCK_LINE", puck)):
        if frame.game_id.duplicated().any() or set(frame.game_id.astype(int)) != ids:
            raise RuntimeError(f"IMMUTABLE_{label}_GAME_IDENTITY_CONFLICT")
        if not frame.slate_date.astype(str).eq(slate_date).all():
            raise RuntimeError(f"IMMUTABLE_{label}_SLATE_DATE_MISMATCH")
        _pregame(frame, "prediction_creation_time_utc", schedule)
    _verify_cross_market_identities(moneyline, puck)

    sog = _one_run(
        operational_root / "sog_prediction_only" / "season=2026" / f"slate_date={slate_date}"
        / "phase=FINAL_PREGAME" / "run_id=*", "SOG_FINAL_PREGAME",
    )
    sog_manifest = _verify_manifest(sog)
    sog_meta = json.loads((sog / "run_metadata.json").read_text())
    if (sog_meta.get("slate_date") != slate_date or sog_meta.get("phase") != "FINAL_PREGAME"
            or sog.name != f"run_id={sog_meta.get('run_id')}"):
        raise RuntimeError("IMMUTABLE_SOG_RUN_IDENTITY_MISMATCH")
    sog_spine = pd.read_csv(sog / "canonical_game_spine.csv").rename(columns={"game_type": "game_type_code"})
    sog_spine = _local_spine(sog_spine, slate_date)
    spine_fields = ["game_id", "scheduled_start_time_utc", "home_team_id", "away_team_id"]
    left = schedule[spine_fields].copy(); right = sog_spine[spine_fields].copy()
    for frame in (left, right):
        frame["scheduled_start_time_utc"] = pd.to_datetime(frame.scheduled_start_time_utc, utc=True)
    if not left.sort_values("game_id").reset_index(drop=True).equals(right.sort_values("game_id").reset_index(drop=True)):
        raise RuntimeError("IMMUTABLE_SOURCE_CANONICAL_GAME_SET_CONFLICT")
    predictions = pd.read_csv(sog / "immutable_predictions.csv")
    exclusions = pd.read_csv(sog / "excluded_players.csv")
    required_sog = {"canonical_season", "slate_date", "game_id", "player_id", "line", "phase",
                    "prediction_timestamp_utc", "prediction_identity", "contract_arm", "contract_sha256"}
    _require_columns(predictions, required_sog, "SOG")
    if predictions.prediction_identity.duplicated().any():
        raise RuntimeError("IMMUTABLE_SOG_PREDICTION_IDENTITY_DUPLICATE")
    if (set(predictions.phase.astype(str)) != {"FINAL_PREGAME"}
            or set(predictions.contract_sha256.astype(str)) != {_sha(SOG_CONTRACT_PATH)}):
        raise RuntimeError("IMMUTABLE_SOG_CONTRACT_IDENTITY_MISMATCH")
    if set(predictions.game_id.astype(int)) - ids or not predictions.slate_date.astype(str).eq(slate_date).all():
        raise RuntimeError("IMMUTABLE_SOG_GAME_IDENTITY_CONFLICT")
    if (exclusions.duplicated(["game_id", "player_id"]).any()
            or set(exclusions.game_id.astype(int)) - ids
            or not exclusions.slate_date.astype(str).eq(slate_date).all()):
        raise RuntimeError("IMMUTABLE_SOG_EXCLUSION_IDENTITY_CONFLICT")
    _pregame(predictions, "prediction_timestamp_utc", schedule)
    for row in predictions.itertuples(index=False):
        identity = sog_digest({
            "contract": row.contract_sha256, "arm": row.contract_arm, "phase": row.phase,
            "slate_date": row.slate_date, "game_id": int(row.game_id),
            "player_id": int(row.player_id), "line": float(row.line),
        })
        if identity != row.prediction_identity:
            raise RuntimeError("IMMUTABLE_SOG_PREDICTION_IDENTITY_MISMATCH")
    if (len(predictions) != int(sog_meta.get("prediction_rows", -1))
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
        if len(points_runs) != 1:
            raise RuntimeError(f"IMMUTABLE_POINTS_FINAL_PREGAME_RUN_CARDINALITY:{len(points_runs)}")
        if len(saves_runs) != 1:
            raise RuntimeError(f"IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:{len(saves_runs)}")
        _verify_manifest(points_runs[0]); _verify_manifest(saves_runs[0])
        points = {"status": "PROSPECTIVE_SOURCE_BOUND", "run": str(points_runs[0])}
        saves = {"status": "PROSPECTIVE_SOURCE_BOUND", "run": str(saves_runs[0])}

    observed = pd.to_datetime(sog_meta["prediction_timestamp_utc"], utc=True)
    first_puck = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True).min()
    cross_observed = pd.to_datetime(status["run_timestamp_utc"], utc=True)
    if observed >= first_puck or cross_observed >= first_puck:
        raise RuntimeError("IMMUTABLE_SOURCE_OBSERVED_AFTER_FIRST_PUCK")
    return {
        "contract_version": "NHL_POSTGAME_LOCAL_SOURCE_BINDING_V1", "slate_date": slate_date,
        "canonical_games": len(schedule), "game_ids": sorted(ids),
        "canonical_game_set_hash": _hash_bytes(json.dumps(sorted(ids), separators=(",", ":")).encode()),
        "first_puck_utc": first_puck.isoformat(),
        "cross_market": {"run": str(cross), "manifest_sha256": _sha(cross / "SHA256SUMS"),
                         "files": cross_manifest, "observed_at_utc": cross_observed.isoformat(),
                         "moneyline_rows": len(moneyline), "puck_line_rows": len(puck)},
        "sog": {"run": str(sog), "manifest_sha256": _sha(sog / "SHA256SUMS"),
                "files": sog_manifest, "observed_at_utc": observed.isoformat(),
                "prediction_rows": len(predictions), "players": predictions.player_id.nunique(),
                "exclusions": len(exclusions), "arms": sorted(predictions.contract_arm.unique())},
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
            if team_goalies:
                starter = max(range(len(team_goalies)), key=lambda index: team_goalies[index]["_toi_seconds"])
                team_goalies[starter]["goalie_participation_state"] = "STARTED"
                team_goalies[starter]["actual_start_flag"] = True
                for item in team_goalies:
                    item["starter_identity_method"] = "MAX_OFFICIAL_TOI_POSTGAME"
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
    if "game_type_code" in joined and not joined.game_type_code.astype(int).eq(joined.canonical_game_type_code.astype(int)).all():
        raise RuntimeError("PREDICTION_GAME_TYPE_IDENTITY_MISMATCH")
    if "game_type_code" not in joined:
        joined["game_type_code"] = joined.canonical_game_type_code
    stamps = pd.to_datetime(joined[timestamp_col], utc=True, errors="coerce")
    starts = pd.to_datetime(joined.canonical_scheduled_start_time_utc, utc=True, errors="coerce")
    if stamps.isna().any() or starts.isna().any() or (stamps >= starts).any():
        raise RuntimeError("POST_START_PREDICTION_PRESENTED_AS_PREGAME")
    return joined


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
    moneyline["grading_status"] = "PRESEASON_NON_EVALUATION"
    moneyline["regular_season_evaluation_target"] = pd.NA
    puck["grading_status"] = "PRESEASON_NON_EVALUATION"
    puck["regular_season_evaluation_target"] = pd.NA

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


def grade_operational_sources(source_binding: dict[str, Any], schedule: pd.DataFrame,
                              games: pd.DataFrame, skaters: pd.DataFrame,
                              goalies: pd.DataFrame, observed_at: str) -> dict[str, pd.DataFrame]:
    cross = Path(source_binding["cross_market"]["run"])
    moneyline = _pregame(pd.read_csv(cross / "v2_immutable_predictions.csv"),
                         "prediction_creation_time_utc", schedule)
    puck = _pregame(pd.read_csv(cross / "puck_line_v1_immutable_predictions.csv"),
                    "prediction_creation_time_utc", schedule)
    outcome_columns = ["game_id", "official_full_game_winner", "official_final_home_goals",
                       "official_final_away_goals"]
    moneyline = moneyline.merge(games[outcome_columns], on="game_id", validate="one_to_one")
    puck = puck.merge(games[outcome_columns], on="game_id", validate="one_to_one")
    for frame in (moneyline, puck):
        frame["grading_status"] = "PRESEASON_NON_EVALUATION"
        frame["regular_season_evaluation_target"] = pd.NA

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

    points_status = source_binding["points"]
    saves_status = source_binding["saves"]
    if points_status["status"] != "NO_PROSPECTIVE_POINTS_PREDICTIONS":
        raise RuntimeError("PROSPECTIVE_POINTS_GRADING_NOT_IMPLEMENTED_FOR_OPERATIONAL_BINDING")
    if saves_status["status"] != "NO_PROSPECTIVE_SAVES_PREDICTIONS":
        raise RuntimeError("PROSPECTIVE_SAVES_GRADING_NOT_IMPLEMENTED_FOR_OPERATIONAL_BINDING")
    points = _zero_evidence_surface(points_status["status"], points_status["reason"])
    saves = _zero_evidence_surface(saves_status["status"], saves_status["reason"])
    # Empty surfaces carry their status in the package summary/source lineage;
    # these columns remain schema-valid without inventing a prediction row.
    return {"moneyline": moneyline, "puck_line": puck, "points": points, "saves": saves,
            "sog": graded_sog, "sog_missing_predictions": missing}


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
    if collector is not None:
        collector()
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
        summary = {
            "contract_version": CONTRACT, "slate_date": slate_date,
            "status": "COMPLETE", "substantive_identity": identity,
            "games": len(games), "skater_outcomes": len(skaters), "goalie_outcomes": len(goalies),
            "moneyline_status": "PRESEASON_NON_EVALUATION",
            "puck_line_status": "PRESEASON_NON_EVALUATION",
            "points_timestamp_qualification": (source_binding["points"]["status"] if source_binding else "RUN_SUMMARY_OBSERVATION_TIMESTAMP_PRESTART"),
            "saves_contract": (source_binding["saves"]["status"] if source_binding else "FROZEN_CONDITIONAL_STARTER_PARTICIPATION"),
            "sog_status": ("PROSPECTIVE_FINAL_PREGAME_GRADED_BY_CONTRACT_ARM" if source_binding else "NO_SEPTEMBER_19_PREDICTION_GRADE"),
            "strict_prior_update_status": "COMPLETE_AFTER_ALL_FINAL_AND_COLLECTOR_SUCCESS",
            "odds_api_requests": 0, "bookmaker_requests": 0, "paid_credits": 0,
        }
        if request_accounting is not None:
            summary["official_request_accounting"] = request_accounting
            packaged_journal = staging / "official_request_journal.jsonl"
            shutil.copyfile(request_journal, packaged_journal)
            os.chmod(packaged_journal, 0o600)
            (staging / "official_request_accounting.json").write_text(
                json.dumps(request_accounting, indent=2, sort_keys=True) + "\n"
            )
        if source_binding is not None:
            (staging / "source_bindings.json").write_text(
                json.dumps(source_binding, indent=2, sort_keys=True) + "\n"
            )
            summary["points_reason"] = source_binding["points"].get("reason")
            summary["saves_reason"] = source_binding["saves"].get("reason")
            summary["points_status"] = source_binding["points"]["status"]
            summary["saves_status"] = source_binding["saves"]["status"]
            summary["sog_evaluation_populations"] = {
                str(key): int(value) for key, value in grades["sog"].evaluation_lane.value_counts().items()
            }
            summary["sog_missing_prospective_participants"] = len(grades["sog_missing_predictions"])
        if request_lineage is not None:
            lineage = json.loads(json.dumps(request_lineage))
            if lineage.get("contract_version") in {
                    "NHL_POSTGAME_REQUEST_LINEAGE_V3", "NHL_POSTGAME_REQUEST_LINEAGE_V4"}:
                response_sources = lineage.get("response_sources") or []
                failed_ancestors = lineage.get("failed_ancestors") or []
                source_ids = [row.get("source_run_id") for row in response_sources]
                failed_ids = [row.get("run_id") for row in failed_ancestors]
                roles = [row.get("role") for row in response_sources]
                if (len(source_ids) != len(set(source_ids))
                        or len(failed_ids) != len(set(failed_ids))
                        or sorted(roles) != (["AUTHORITY_RESPONSE_SOURCE",
                                              "ROSTER_RESPONSE_SOURCE"]
                                             if lineage["contract_version"].endswith("V3")
                                             else ["AUTHORITY_RESPONSE_SOURCE",
                                                   "PLAYER_IDENTITY_RESPONSE_SOURCE",
                                                   "ROSTER_RESPONSE_SOURCE"])
                        or any(row.get("role") != "FAILED_EXECUTION_ANCESTOR"
                               for row in failed_ancestors)):
                    raise RuntimeError("REQUEST_LINEAGE_INVALID")
            else:
                ancestors = lineage.get("ancestors") or []
                ancestor_ids = [row.get("source_run_id") or row.get("run_id")
                                for row in ancestors]
                if (len(ancestor_ids) != len(set(ancestor_ids))
                        or sum(row.get("role") == "AUTHORITY_RESPONSE_SOURCE"
                               for row in ancestors) != 1):
                    raise RuntimeError("REQUEST_LINEAGE_INVALID")
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
            + (("Prospective SOG is graded by separate contract arm; absent pre-activation Points/Saves remain zero-row evidence. "
              if source_binding else "September 19 SOG is outcome-only and has no prediction grade. ")
             + "All preseason lanes remain non-evaluation.\n")
            + ("\nOfficial NHL request accounting is reconciled end to end in "
               "official_request_accounting.json.\n" if request_accounting is not None else "")
        )
        manifest_files = sorted(path for path in staging.iterdir() if path.is_file())
        (staging / "SHA256SUMS").write_text("".join(f"{_sha(path)}  {path.name}\n" for path in manifest_files))
        (staging / "RUN_COMPLETE.json").write_text(json.dumps({
            "status": "COMPLETE", "substantive_identity": identity,
            "completed_at_utc": observed_at,
        }, indent=2, sort_keys=True) + "\n")
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return destination, "COMPLETE_NEW_APPEND_ONLY"

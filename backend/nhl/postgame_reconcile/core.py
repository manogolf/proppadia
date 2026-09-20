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
from typing import Callable

import pandas as pd


CONTRACT = "NHL_POSTGAME_RECONCILIATION_V1"
FINAL_STATES = {"FINAL", "OFF"}
SUPPORTED_GAME_TYPES = {1, 2, 3}


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
                           observed_at: str | None = None) -> tuple[Path, str]:
    observed_at = observed_at or datetime.now(timezone.utc).isoformat()
    validated = validate_final_slate(canonical, official, slate_date)
    games, skaters, goalies = build_outcomes(validated, boxscores, observed_at)
    grades = grade_catchup(prediction_root, canonical, games, skaters, goalies)
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
            "points_timestamp_qualification": "RUN_SUMMARY_OBSERVATION_TIMESTAMP_PRESTART",
            "saves_contract": "FROZEN_CONDITIONAL_STARTER_PARTICIPATION",
            "sog_status": "NO_SEPTEMBER_19_PREDICTION_GRADE",
            "strict_prior_update_status": "COMPLETE_AFTER_ALL_FINAL_AND_COLLECTOR_SUCCESS",
            "odds_api_requests": 0, "bookmaker_requests": 0, "paid_credits": 0,
        }
        (staging / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        (staging / "report.md").write_text(
            f"# NHL postgame reconciliation — {slate_date}\n\n"
            f"Status: COMPLETE\n\nGames: {len(games)}; skaters: {len(skaters)}; goalies: {len(goalies)}.\n\n"
            "September 19 SOG is outcome-only and has no prediction grade. All preseason lanes remain non-evaluation.\n"
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

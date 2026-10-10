"""Immutable evidence binding for the production Poisson SOG feature input."""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, verify_package


CONTRACT = "NHL_SOG_PRODUCTION_FEATURE_INPUT_V1"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc(value: str | datetime) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("SOG_FEATURE_CUTOFF_OR_START_MUST_BE_TIMEZONED")
    return parsed.astimezone(timezone.utc)


def begin_capture(*, source_path: Path, root: Path, season: int, slate_date: str,
                  run_id: str, canonical_game_ids: list[int], canonical_game_starts_utc: dict[int, str],
                  cutoff_utc: str | datetime,
                  eligible_game_ids: list[int] | None = None) -> dict[str, Any]:
    """Validate, eligibility-filter, and stage scorer input for an unpublished run."""
    source_path = Path(source_path).resolve()
    source_hash = sha256_file(source_path)
    cutoff = _utc(cutoff_utc)
    ids = sorted(map(int, canonical_game_ids))
    if not ids or set(ids) != set(map(int, canonical_game_starts_utc)):
        raise ValueError("SOG_FEATURE_CANONICAL_GAME_STARTS_MISMATCH")
    starts = {gid: _utc(canonical_game_starts_utc[gid]) for gid in ids}
    eligible = sorted(gid for gid in ids if starts[gid] > cutoff)
    excluded = sorted(set(ids) - set(eligible))
    if eligible_game_ids is not None and sorted(map(int, eligible_game_ids)) != eligible:
        raise ValueError("SOG_FEATURE_ELIGIBILITY_CUTOFF_MISMATCH")
    if not eligible:
        raise ValueError("SOG_FEATURE_NO_PREGAME_GAMES_REMAIN")
    frame = pd.read_csv(source_path)
    required = {"player_id", "game_id", "game_date", "shots_on_goal"}
    if not required.issubset(frame.columns):
        raise ValueError("SOG_FEATURE_REQUIRED_COLUMNS_MISSING")
    if frame.empty or frame.duplicated(["game_id", "player_id"]).any():
        raise ValueError("SOG_FEATURE_EMPTY_OR_DUPLICATE_PLAYER_GAME")
    source_game_ids = sorted(pd.to_numeric(frame.game_id, errors="raise").astype(int).unique())
    if source_game_ids not in (ids, eligible):
        raise ValueError("SOG_FEATURE_CANONICAL_OR_ELIGIBLE_GAME_SET_MISMATCH")
    if not frame.game_date.astype(str).eq(slate_date).all():
        raise ValueError("SOG_FEATURE_SLATE_DATE_MISMATCH")
    frame["game_id"] = pd.to_numeric(frame.game_id, errors="raise").astype(int)
    frame = frame[frame.game_id.isin(eligible)].copy()
    if frame.empty or sorted(frame.game_id.unique()) != eligible:
        raise ValueError("SOG_FEATURE_ELIGIBLE_GAME_SET_MISMATCH")
    # The export carries the target column for legacy compatibility; it must be null pregame.
    if frame.shots_on_goal.notna().any():
        raise ValueError("SOG_FEATURE_TARGET_GAME_SOG_PRESENT")
    final = Path(root) / f"season={season}" / f"slate_date={slate_date}" / f"run_id={run_id}"
    if final.exists():
        raise FileExistsError(f"SOG_FEATURE_PACKAGE_ALREADY_EXISTS:{final}")
    final.parent.mkdir(parents=True, exist_ok=True)
    staging = final.with_name(f".{final.name}.incomplete")
    staging.mkdir(exist_ok=False)
    payload = staging / "sog_features.csv"
    if source_game_ids == eligible:
        shutil.copyfile(source_path, payload)
    else:
        frame.to_csv(payload, index=False)
    retained_hash = sha256_file(payload)
    columns = list(frame.columns)
    schema = {"ordered_columns": columns, "dtypes": [str(frame[c].dtype) for c in columns]}
    schema_hash = hashlib.sha256(json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"staging_dir": staging, "final_dir": final, "source_path": source_path,
            "source_sha256": source_hash, "retained_path": final / "sog_features.csv",
            "retained_sha256": retained_hash, "row_count": len(frame),
            "unique_player_game_count": int(frame[["game_id", "player_id"]].drop_duplicates().shape[0]),
            "ordered_columns": columns, "schema_sha256": schema_hash,
            "cutoff_utc": cutoff.isoformat().replace("+00:00", "Z"),
            "slate_date": slate_date, "run_id": run_id, "season": int(season),
            "canonical_game_ids": ids, "canonical_game_set_hash": canonical_game_set_hash(ids),
            "eligible_pregame_game_ids": eligible, "started_excluded_game_ids": excluded}


def finalize_capture(capture: dict[str, Any], *, scorer_path: Path,
                     model_family: str, model_version: str,
                     fitted_model_identity_sha256: str, prediction_path: Path,
                     python_executable: str, names_path: Path | None = None) -> dict[str, Any]:
    """Bind scoring evidence and atomically publish the immutable package."""
    staging, final = capture["staging_dir"], capture["final_dir"]
    if sha256_file(capture["source_path"]) != capture["source_sha256"]:
        raise ValueError("SOG_FEATURE_SOURCE_CHANGED_AFTER_CAPTURE")
    prediction_sha = sha256_file(prediction_path)
    scorer_sha = sha256_file(scorer_path)
    replay_path = staging / ".replay_predictions.csv"
    replay_unscored = staging / ".replay_unscored.csv"
    replay_command = [python_executable, str(scorer_path), "--in",
                      str(staging / "sog_features.csv"), "--out", str(replay_path),
                      "--unscored-out", str(replay_unscored)]
    if names_path is not None and Path(names_path).is_file():
        replay_command.extend(["--names", str(names_path)])
    subprocess.run(replay_command, check=True, capture_output=True, text=True)
    replay_sha = sha256_file(replay_path)
    replay_path.unlink()
    replay_unscored.unlink(missing_ok=True)
    if replay_sha != prediction_sha:
        raise ValueError("SOG_PRODUCTION_FEATURE_REPLAY_HASH_MISMATCH")
    manifest = {
        "contract": CONTRACT, "contract_version": 1,
        "slate_date": capture["slate_date"], "parent_daily_run_id": capture["run_id"],
        "canonical_game_ids": capture["canonical_game_ids"],
        "canonical_game_set_hash": capture["canonical_game_set_hash"],
        "eligible_pregame_game_ids": capture["eligible_pregame_game_ids"],
        "started_excluded_game_ids": capture["started_excluded_game_ids"],
        "feature_cutoff_utc": capture["cutoff_utc"],
        "source_feature_path": str(capture["source_path"]),
        "source_feature_sha256": capture["source_sha256"],
        "feature_input_path": str(capture["retained_path"]),
        "feature_input_sha256": capture["retained_sha256"],
        "feature_input_bytes": (staging / "sog_features.csv").stat().st_size,
        "row_count": capture["row_count"],
        "unique_player_game_count": capture["unique_player_game_count"],
        "ordered_columns": capture["ordered_columns"],
        "feature_schema_sha256": capture["schema_sha256"],
        "scorer_path": str(scorer_path), "scorer_sha256": scorer_sha,
        "model_family": model_family, "model_version": model_version,
        "fitted_model_identity_sha256": fitted_model_identity_sha256,
        "prediction_artifact_path": str(Path(prediction_path).resolve()),
        "prediction_artifact_sha256": prediction_sha,
        "replay_prediction_sha256": replay_sha,
        "replay_classification": "SOG_PRODUCTION_FEATURE_REPLAY_PASS",
        "lineage": "FEATURE_INPUT_SHA -> SCORER/MODEL -> PREDICTION_SHA",
        "classification": "PROSPECTIVELY_BOUND_FEATURE_INPUT",
    }
    manifest_path = staging / "feature_input_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    manifest_sha = sha256_file(manifest_path)
    marker = {"contract": CONTRACT, "parent_daily_run_id": capture["run_id"],
              "feature_input_manifest_sha256": manifest_sha, "status": "COMPLETE"}
    (staging / "RUN_COMPLETE.json").write_text(json.dumps(marker, indent=2, sort_keys=True) + "\n")
    names = ["sog_features.csv", "feature_input_manifest.json", "RUN_COMPLETE.json"]
    (staging / "SHA256SUMS").write_text("".join(f"{sha256_file(staging / name)}  {name}\n" for name in names))
    staging.replace(final)
    verify_package(final)
    return {"feature_input_path": str(final / "sog_features.csv"),
            "feature_input_sha256": capture["retained_sha256"],
            "source_feature_sha256": capture["source_sha256"],
            "feature_input_manifest_path": str(final / "feature_input_manifest.json"),
            "feature_input_manifest_sha256": manifest_sha,
            "feature_contract": CONTRACT, "feature_contract_version": 1,
            "feature_cutoff_utc": capture["cutoff_utc"],
            "scorer_sha256": scorer_sha,
            "prediction_artifact_sha256": prediction_sha,
            "replay_prediction_sha256": replay_sha,
            "replay_classification": "SOG_PRODUCTION_FEATURE_REPLAY_PASS",
            "fitted_model_identity_sha256": fitted_model_identity_sha256,
            "parent_daily_run_id": capture["run_id"],
            "canonical_game_set_hash": capture["canonical_game_set_hash"],
            "eligible_pregame_game_ids": capture["eligible_pregame_game_ids"],
            "started_excluded_game_ids": capture["started_excluded_game_ids"],
            "classification": "PROSPECTIVELY_BOUND_FEATURE_INPUT"}

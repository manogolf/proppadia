"""Create-only, run-bound retention for Points and Saves market attachments."""
from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.attachment_integrity import (
    canonical_attachment_keys, prediction_rows, validate_attachment_frame,
)
from backend.nhl.daily_capture import sha256_file, verify_package


_FILES = {
    "points": ("points_attachment_integrity.json", "points_with_market.csv",
               "unmatched_points.csv", None),
    "saves": ("saves_attachment_integrity.json", "saves_with_market.csv",
              "unmatched_saves.csv", "ambiguous_saves_alias_matches.csv"),
}


def _key_identity(path: Path, lane: str) -> tuple[list[tuple[str, str, str, str]], str]:
    frame = pd.read_csv(path)
    if {"game_date", "game_id", "player_id", "line", "p_over"}.issubset(frame.columns):
        rows = frame[["game_date", "game_id", "player_id", "line"]].copy()
        rows = rows[pd.to_numeric(frame["p_over"], errors="coerce").notna()]
    else:
        rows = prediction_rows(path, lane=lane)
    keys = canonical_attachment_keys(rows)
    prop = "player_points" if lane == "points" else "goalie_saves"
    exact = sorted((game_id, player_id, prop, line)
                   for _, game_id, player_id, line in keys)
    import hashlib
    digest = hashlib.sha256(json.dumps(exact, separators=(",", ":")).encode()).hexdigest()
    return exact, digest


def _timing(prediction_path: Path, lane: str, observation_timestamp: str | None,
            game_starts_utc: dict[int, str] | None) -> dict[str, Any] | None:
    if observation_timestamp is None or game_starts_utc is None:
        return None
    observed = datetime.fromisoformat(observation_timestamp.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("ATTACHMENT_OBSERVATION_TIMESTAMP_NOT_UTC")
    rows = prediction_rows(prediction_path, lane=lane)
    by_game: dict[str, Any] = {}
    for game_id, group in rows.groupby("game_id"):
        gid = int(game_id)
        if gid not in game_starts_utc:
            raise ValueError(f"ATTACHMENT_CANONICAL_GAME_START_MISSING:{gid}")
        start = datetime.fromisoformat(str(game_starts_utc[gid]).replace("Z", "+00:00"))
        if start.tzinfo is None:
            raise ValueError("ATTACHMENT_GAME_START_NOT_UTC")
        eligible = observed.astimezone(timezone.utc) < start.astimezone(timezone.utc)
        by_game[str(gid)] = {
            "canonical_game_start_utc": start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "prediction_key_count": int(len(group)),
            "timing_classification": "PRESTART_ELIGIBLE" if eligible else "POSTSTART_INELIGIBLE",
        }
    return {"contract": "NHL_GAME_SPECIFIC_MARKET_PRESTART_V1",
            "odds_observed_at_utc": observed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "games": by_game}


def retain_market_attachment_package(
    *, lane: str, package_path: Path, integrity: dict[str, Any], prediction_path: Path,
    attachment_path: Path, unmatched_path: Path, ambiguous_path: Path | None = None,
    canonical_game_set_sha256: str | None = None,
    canonical_game_starts_utc: dict[int, str] | None = None,
) -> tuple[Path, str]:
    """Retain one successful daily Points/Saves attachment without overwriting."""
    lane = lane.lower()
    if lane not in _FILES:
        raise ValueError(f"MARKET_ATTACHMENT_RETENTION_LANE_UNSUPPORTED:{lane}")
    if integrity.get("status") != "PASS":
        raise ValueError("MARKET_ATTACHMENT_RETENTION_REQUIRES_PASS")
    report_name, attached_name, unmatched_name, ambiguous_name = _FILES[lane]
    if (lane == "saves") != (ambiguous_path is not None):
        raise ValueError("MARKET_ATTACHMENT_RETENTION_AMBIGUOUS_FILE_MISMATCH")
    final = Path(package_path).resolve()
    if final.exists():
        raise FileExistsError(f"MARKET_ATTACHMENT_PACKAGE_ALREADY_EXISTS:{final}")
    staging = final.with_name(f".{final.name}.{uuid.uuid4().hex}.incomplete")
    staging.mkdir(parents=True, exist_ok=False)
    published = False
    try:
        attached_dest = staging / attached_name
        unmatched_dest = staging / unmatched_name
        shutil.copy2(attachment_path, attached_dest)
        shutil.copy2(unmatched_path, unmatched_dest)
        retained = dict(integrity)
        retained.update({
            "retention_schema_version": f"NHL_{lane.upper()}_MARKET_ATTACHMENT_RETENTION_V1",
            "integrity_status": integrity.get("status"),
            "slate_date": integrity.get("slate_date"),
            "canonical_game_set_hash": canonical_game_set_sha256,
            "prediction_artifact_path": str(Path(prediction_path).resolve()),
            "prediction_artifact_sha256": sha256_file(Path(prediction_path)),
            "prediction_row_count": int((integrity.get("counts") or {}).get("prediction_row_count", 0)),
            "natural_identity_count": int(prediction_rows(
                Path(prediction_path), lane=lane)[["game_id", "player_id"]]
                .drop_duplicates().shape[0]),
            "exact_proposition_key_count": int((integrity.get("counts") or {}).get(
                "unique_prediction_key_count", 0)),
            "exact_proposition_key_columns": ["game_id", "player_id", "prop", "line"],
            "proposition": "player_points" if lane == "points" else "goalie_saves",
            "attachment_path": str(final / attached_name),
            "attachment_sha256": sha256_file(attached_dest),
            "unmatched_path": str(final / unmatched_name),
            "unmatched_sha256": sha256_file(unmatched_dest),
        })
        prediction_source = pd.read_csv(prediction_path)
        cutoff_column = next((name for name in (
            "feature_input_cutoff_utc", "feature_cutoff_timestamp_utc",
            "input_cutoff_timestamp_utc", "prediction_timestamp_utc",
        ) if name in prediction_source.columns), None)
        if cutoff_column:
            retained["prediction_snapshot_timestamp_utc"] = sorted(
                prediction_source[cutoff_column].dropna().astype(str).unique().tolist())
        _, prediction_key_sha = _key_identity(Path(prediction_path), lane)
        _, attachment_key_sha = _key_identity(attached_dest, lane)
        if prediction_key_sha != attachment_key_sha:
            raise ValueError("MARKET_ATTACHMENT_RETAINED_KEYSET_MISMATCH")
        retained["prediction_proposition_key_set_sha256"] = prediction_key_sha
        retained["attachment_proposition_key_set_sha256"] = attachment_key_sha
        if ambiguous_path is not None and ambiguous_name is not None:
            ambiguous_dest = staging / ambiguous_name
            shutil.copy2(ambiguous_path, ambiguous_dest)
            retained["ambiguous_inventory_path"] = str(final / ambiguous_name)
            retained["ambiguous_inventory_sha256"] = sha256_file(ambiguous_dest)
        obs_path = retained.get("odds_observation_path")
        observation_timestamp = None
        if obs_path and (Path(obs_path) / "observation_summary.json").is_file():
            observation = json.loads((Path(obs_path) / "observation_summary.json").read_text())
            observation_timestamp = observation.get("observation_timestamp_utc")
            retained["odds_observation_identity"] = observation.get("invocation_id") or Path(obs_path).name
        retained["odds_observation_timestamp_utc"] = observation_timestamp
        timing = _timing(Path(prediction_path), lane, observation_timestamp,
                         canonical_game_starts_utc)
        if timing is not None:
            retained["game_specific_timing"] = timing
        report_path = staging / report_name
        report_path.write_text(json.dumps(retained, indent=2, sort_keys=True) + "\n")
        marker_path = staging / "RUN_COMPLETE.json"
        marker_path.write_text(json.dumps({
            "schema_version": retained["retention_schema_version"],
            "parent_daily_run_id": retained.get("parent_daily_run_id"),
            "slate_date": retained.get("slate_date"), "lane": lane,
            "integrity_status": retained.get("status"), "status": "COMPLETE",
        }, indent=2, sort_keys=True) + "\n")
        files = sorted(path for path in staging.iterdir() if path.is_file())
        (staging / "SHA256SUMS").write_text("".join(
            f"{sha256_file(path)}  {path.name}\n" for path in files))
        final.parent.mkdir(parents=True, exist_ok=True)
        staging.replace(final)
        published = True
        report_path = final / report_name
        _, manifest_sha = verify_market_attachment_package(
            report_path, lane=lane, slate_date=str(integrity.get("slate_date")))
        return report_path, manifest_sha
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        if published:
            shutil.rmtree(final, ignore_errors=True)
        raise


def verify_market_attachment_package(report_path: Path, *, lane: str,
                                     slate_date: str) -> tuple[dict[str, Any], str]:
    lane = lane.lower()
    if lane not in _FILES:
        raise ValueError(f"MARKET_ATTACHMENT_RETENTION_LANE_UNSUPPORTED:{lane}")
    report_name, attached_name, unmatched_name, ambiguous_name = _FILES[lane]
    report_path = Path(report_path).resolve()
    package = report_path.parent
    manifest_sha = verify_package(package)
    report = json.loads(report_path.read_text())
    marker = json.loads((package / "RUN_COMPLETE.json").read_text())
    required = {report_name, attached_name, unmatched_name, "RUN_COMPLETE.json"}
    if ambiguous_name:
        required.add(ambiguous_name)
    if not required.issubset({path.name for path in package.iterdir() if path.is_file()}):
        raise ValueError("MARKET_ATTACHMENT_PACKAGE_REQUIRED_FILE_MISSING")
    if (report_path.name != report_name or report.get("lane") != lane
            or report.get("retention_schema_version") != f"NHL_{lane.upper()}_MARKET_ATTACHMENT_RETENTION_V1"
            or report.get("status") != "PASS" or report.get("integrity_status") != "PASS"
            or report.get("slate_date") != slate_date or not report.get("parent_daily_run_id")
            or marker.get("status") != "COMPLETE" or marker.get("lane") != lane
            or marker.get("slate_date") != slate_date
            or marker.get("parent_daily_run_id") != report.get("parent_daily_run_id")):
        raise ValueError("MARKET_ATTACHMENT_PACKAGE_LINEAGE_OR_COMPLETION_INVALID")
    counts = report.get("counts") or {}
    checks = report.get("checks") or {}
    if (any(value is not True for value in checks.values())
            or counts.get("duplicate_prediction_key_count") != 0
            or counts.get("duplicate_attachment_key_count") != 0
            or counts.get("missing_prediction_key_count") != 0
            or counts.get("extra_attachment_key_count") != 0):
        raise ValueError("MARKET_ATTACHMENT_PACKAGE_INTEGRITY_CHECKS_FAILED")
    prediction_path = Path(report.get("prediction_artifact_path", "")).resolve()
    if (not prediction_path.is_file()
            or sha256_file(prediction_path) != report.get("prediction_artifact_sha256")):
        raise ValueError("MARKET_ATTACHMENT_PREDICTION_HASH_MISMATCH")
    expected_paths = {"attachment_path": package / attached_name,
                      "unmatched_path": package / unmatched_name}
    if ambiguous_name:
        expected_paths["ambiguous_inventory_path"] = package / ambiguous_name
    for field, expected in expected_paths.items():
        path = Path(report.get(field, "")).resolve()
        if path != expected or sha256_file(path) != report.get(
                "attachment_sha256" if field == "attachment_path" else
                "unmatched_sha256" if field == "unmatched_path" else
                "ambiguous_inventory_sha256"):
            raise ValueError("MARKET_ATTACHMENT_PACKAGE_CONTENT_HASH_MISMATCH")
    _, prediction_key_sha = _key_identity(prediction_path, lane)
    attached_frame = pd.read_csv(package / attached_name)
    _, attachment_key_sha = _key_identity(package / attached_name, lane)
    if (prediction_key_sha != attachment_key_sha
            or prediction_key_sha != report.get("prediction_proposition_key_set_sha256")
            or attachment_key_sha != report.get("attachment_proposition_key_set_sha256")):
        raise ValueError("MARKET_ATTACHMENT_PACKAGE_KEYSET_MISMATCH")
    validation = validate_attachment_frame(
        prediction_frame=prediction_rows(prediction_path, lane=lane),
        attachment_frame=attached_frame,
        expected_parent_daily_run_id=report.get("parent_daily_run_id"),
        expected_prediction_sha256=report.get("prediction_artifact_sha256"),
        expected_odds_manifest_sha256=report.get("odds_observation_manifest_sha256"),
    )
    if validation["counts"] != report.get("counts"):
        raise ValueError("MARKET_ATTACHMENT_PACKAGE_COUNTS_MISMATCH")
    attached_keys = canonical_attachment_keys(attached_frame)
    if "attachment_status" in attached_frame:
        nonmatched = attached_frame["attachment_status"].fillna("").astype(str).ne("MATCHED")
    else:
        nonmatched = pd.to_numeric(attached_frame.get(
            "price_over", pd.Series([pd.NA] * len(attached_frame))), errors="coerce").isna()
    expected_unmatched = {key for key, selected in zip(attached_keys, nonmatched.tolist()) if selected}
    unmatched_keys = set(canonical_attachment_keys(pd.read_csv(package / unmatched_name)))
    if unmatched_keys != expected_unmatched:
        raise ValueError("MARKET_ATTACHMENT_PACKAGE_UNMATCHED_INVENTORY_MISMATCH")
    odds_manifest = report.get("odds_observation_manifest_sha256")
    if odds_manifest:
        observation_path = Path(report.get("odds_observation_path", "")).resolve()
        if verify_package(observation_path) != odds_manifest:
            raise ValueError("MARKET_ATTACHMENT_ODDS_MANIFEST_MISMATCH")
        observation = json.loads((observation_path / "observation_summary.json").read_text())
        if (observation.get("slate_date") != slate_date
                or observation.get("parent_daily_run_id") != report.get("parent_daily_run_id")
                or observation.get("canonical_game_set_hash") != report.get(
                    "canonical_game_set_hash")
                or observation.get("observation_timestamp_utc") != report.get(
                    "odds_observation_timestamp_utc")
                or (report.get("odds_observation_identity") not in {
                    observation.get("invocation_id"), observation_path.name})):
            raise ValueError("MARKET_ATTACHMENT_ODDS_LINEAGE_MISMATCH")
    return report, manifest_sha

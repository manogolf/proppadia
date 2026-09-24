"""Fail-closed grain and lineage checks for NHL prediction-market attachments."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from backend.nhl.daily_capture import sha256_file, verify_package


ATTACHMENT_KEY_COLUMNS = ("game_date", "game_id", "player_id", "line")


class AttachmentIntegrityError(RuntimeError):
    """An attachment cannot be admitted as a complete current-run artifact."""


def _line_key(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if pd.isna(number):
        return ""
    return str(int(number)) if abs(number - round(number)) < 1e-9 else f"{number:.6f}".rstrip("0").rstrip(".")


def _id_key(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if pd.isna(number) or abs(number - round(number)) >= 1e-9:
        return ""
    return str(int(number))


def canonical_attachment_keys(frame: pd.DataFrame) -> list[tuple[str, str, str, str]]:
    missing = [column for column in ATTACHMENT_KEY_COLUMNS if column not in frame.columns]
    if missing:
        raise AttachmentIntegrityError(f"ATTACHMENT_KEY_COLUMNS_MISSING:{','.join(missing)}")
    keys: list[tuple[str, str, str, str]] = []
    for row in frame.loc[:, ATTACHMENT_KEY_COLUMNS].itertuples(index=False, name=None):
        game_date, game_id, player_id, line = row
        key = (str(game_date).strip(), _id_key(game_id), _id_key(player_id), _line_key(line))
        if not all(key):
            raise AttachmentIntegrityError(f"ATTACHMENT_KEY_VALUE_INVALID:{key}")
        keys.append(key)
    return keys


def prediction_rows(path: Path, *, lane: str) -> pd.DataFrame:
    """Read a governed prediction artifact at attachment grain."""
    frame = pd.read_csv(path)
    lane = str(lane).lower()
    if lane == "saves":
        pattern = re.compile(r"^p_over_(\d+(?:[._]\d+)?)$")
        probability_columns = [column for column in frame.columns if pattern.match(str(column))]
        if not probability_columns:
            raise AttachmentIntegrityError("SAVES_PREDICTION_PROBABILITY_COLUMNS_MISSING")
        required = ["game_date", "game_id", "player_id"]
        missing = [column for column in required if column not in frame.columns]
        if missing:
            raise AttachmentIntegrityError(f"PREDICTION_COLUMNS_MISSING:{','.join(missing)}")
        rows = frame.melt(
            id_vars=required,
            value_vars=probability_columns,
            var_name="probability_column",
            value_name="prediction_probability",
        )
        rows["line"] = rows["probability_column"].str.replace("p_over_", "", regex=False).str.replace("_", ".", regex=False)
        rows = rows[pd.to_numeric(rows["prediction_probability"], errors="coerce").notna()].copy()
        return rows[["game_date", "game_id", "player_id", "line"]]
    if lane == "points":
        probability = "p_over" if "p_over" in frame.columns else "prob_over"
        if "line" in frame.columns and probability in frame.columns:
            required = ["game_date", "game_id", "player_id", "line"]
            missing = [column for column in required if column not in frame.columns]
            if missing:
                raise AttachmentIntegrityError(f"PREDICTION_COLUMNS_MISSING:{','.join(missing)}")
            rows = frame[pd.to_numeric(frame[probability], errors="coerce").notna()].copy()
            return rows[required]
        pattern = re.compile(r"^p_over_(\d+(?:[._]\d+)?)$")
        probability_columns = [column for column in frame.columns if pattern.match(str(column))]
        if not probability_columns:
            raise AttachmentIntegrityError("POINTS_PREDICTION_PROBABILITY_COLUMNS_MISSING")
        required = ["game_date", "game_id", "player_id"]
        rows = frame.melt(id_vars=required, value_vars=probability_columns,
                          var_name="probability_column", value_name="prediction_probability")
        rows["line"] = rows["probability_column"].str.replace("p_over_", "", regex=False).str.replace("_", ".", regex=False)
        rows = rows[pd.to_numeric(rows["prediction_probability"], errors="coerce").notna()].copy()
        return rows[["game_date", "game_id", "player_id", "line"]]
    raise AttachmentIntegrityError(f"ATTACHMENT_LANE_UNSUPPORTED:{lane}")


def validate_odds_observation(
    *, observation_dir: Path, odds_json: Path, expected_manifest_sha256: str,
    expected_parent_daily_run_id: str, expected_slate_date: str,
) -> dict[str, Any]:
    observation_dir = Path(observation_dir).resolve()
    odds_json = Path(odds_json).resolve()
    if odds_json != observation_dir / "raw_response.json":
        raise AttachmentIntegrityError("ODDS_INPUT_NOT_IMMUTABLE_OBSERVATION_RAW_RESPONSE")
    try:
        manifest_sha256 = verify_package(observation_dir)
    except Exception as error:
        raise AttachmentIntegrityError(
            f"ODDS_OBSERVATION_PACKAGE_INVALID:{type(error).__name__}:{error}") from error
    if manifest_sha256 != expected_manifest_sha256:
        raise AttachmentIntegrityError("ODDS_OBSERVATION_MANIFEST_HASH_MISMATCH")
    summary_path = observation_dir / "observation_summary.json"
    marker_path = observation_dir / "RUN_COMPLETE.json"
    if not summary_path.is_file() or not marker_path.is_file():
        raise AttachmentIntegrityError("ODDS_OBSERVATION_NOT_COMPLETE")
    summary = json.loads(summary_path.read_text())
    if summary.get("parent_daily_run_id") != expected_parent_daily_run_id:
        raise AttachmentIntegrityError("ODDS_OBSERVATION_PARENT_RUN_MISMATCH")
    if summary.get("slate_date") != expected_slate_date:
        raise AttachmentIntegrityError("ODDS_OBSERVATION_SLATE_MISMATCH")
    if not str(summary.get("classification", "")).startswith("CAPTURED_"):
        raise AttachmentIntegrityError("ODDS_OBSERVATION_NOT_CAPTURED")
    return {
        "odds_observation_path": str(observation_dir),
        "odds_observation_manifest_sha256": manifest_sha256,
        "odds_raw_response_sha256": sha256_file(odds_json),
    }


def validate_attachment_frame(
    *, prediction_frame: pd.DataFrame, attachment_frame: pd.DataFrame,
    expected_parent_daily_run_id: str | None = None,
    expected_prediction_sha256: str | None = None,
    expected_odds_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    prediction_keys = canonical_attachment_keys(prediction_frame)
    attachment_keys = canonical_attachment_keys(attachment_frame)
    prediction_set = set(prediction_keys)
    attachment_set = set(attachment_keys)
    duplicate_prediction_count = len(prediction_keys) - len(prediction_set)
    duplicate_attachment_count = len(attachment_keys) - len(attachment_set)
    missing = prediction_set - attachment_set
    extra = attachment_set - prediction_set

    status = (
        attachment_frame["attachment_status"].fillna("").astype(str)
        if "attachment_status" in attachment_frame.columns
        else pd.Series(
            ["MATCHED" if pd.notna(value) else "UNMATCHED"
             for value in attachment_frame.get("price_over", pd.Series([pd.NA] * len(attachment_frame)))],
            index=attachment_frame.index,
        )
    )
    prices = attachment_frame.get("price_over", pd.Series([pd.NA] * len(attachment_frame), index=attachment_frame.index))
    matched_count = int(status.eq("MATCHED").sum())
    unmatched_count = int(status.eq("UNMATCHED").sum())
    ambiguous_count = int(status.eq("AMBIGUOUS_ALIAS_MATCH").sum())
    unknown_status_count = int((~status.isin({"MATCHED", "UNMATCHED", "AMBIGUOUS_ALIAS_MATCH"})).sum())
    ambiguous_with_price_count = int((status.eq("AMBIGUOUS_ALIAS_MATCH") & prices.notna()).sum())
    unmatched_with_price_count = int((status.eq("UNMATCHED") & prices.notna()).sum())
    matched_without_price_count = int((status.eq("MATCHED") & prices.isna()).sum())

    lineage_failures: list[str] = []
    lineage_expectations = (
        ("parent_daily_run_id", expected_parent_daily_run_id),
        ("prediction_artifact_sha256", expected_prediction_sha256),
        ("odds_observation_manifest_sha256", expected_odds_manifest_sha256),
    )
    for column, expected in lineage_expectations:
        if expected is None:
            continue
        if column not in attachment_frame.columns:
            lineage_failures.append(f"{column}:missing")
            continue
        actual = set(attachment_frame[column].fillna("").astype(str))
        if actual != {str(expected)}:
            lineage_failures.append(f"{column}:mismatch")

    checks = {
        "prediction_keys_unique": duplicate_prediction_count == 0,
        "output_count_equals_prediction_count": len(attachment_keys) == len(prediction_keys),
        "attachment_keys_unique": duplicate_attachment_count == 0,
        "prediction_attachment_key_set_equal": not missing and not extra,
        "statuses_exhaustive": matched_count + unmatched_count + ambiguous_count == len(attachment_frame),
        "statuses_known": unknown_status_count == 0,
        "matched_prices_present": matched_without_price_count == 0,
        "unmatched_prices_absent": unmatched_with_price_count == 0,
        "ambiguous_prices_absent": ambiguous_with_price_count == 0,
        "lineage_matches": not lineage_failures,
    }
    counts = {
        "prediction_row_count": len(prediction_keys),
        "attachment_row_count": len(attachment_keys),
        "unique_prediction_key_count": len(prediction_set),
        "unique_attachment_key_count": len(attachment_set),
        "duplicate_prediction_key_count": duplicate_prediction_count,
        "duplicate_attachment_key_count": duplicate_attachment_count,
        "missing_prediction_key_count": len(missing),
        "extra_attachment_key_count": len(extra),
        "matched_count": matched_count,
        "unmatched_count": unmatched_count,
        "ambiguous_count": ambiguous_count,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        detail = {
            "failed_checks": failed,
            "counts": counts,
            "lineage_failures": lineage_failures,
        }
        raise AttachmentIntegrityError(
            "ATTACHMENT_INTEGRITY_FAILED:" + json.dumps(detail, sort_keys=True, separators=(",", ":")))
    return {"checks": checks, "counts": counts}


def audit_attachment_files(
    *, lane: str, prediction_path: Path, attachment_path: Path,
    expected_prediction_sha256: str, expected_parent_daily_run_id: str,
    expected_odds_manifest_sha256: str | None,
) -> dict[str, Any]:
    prediction_path = Path(prediction_path)
    attachment_path = Path(attachment_path)
    actual_prediction_sha256 = sha256_file(prediction_path)
    if actual_prediction_sha256 != expected_prediction_sha256:
        raise AttachmentIntegrityError("PREDICTION_ARTIFACT_HASH_MISMATCH")
    source = pd.read_csv(prediction_path)
    if "parent_daily_run_id" not in source.columns:
        raise AttachmentIntegrityError("PREDICTION_PARENT_RUN_ID_MISSING")
    parent_ids = set(source["parent_daily_run_id"].fillna("").astype(str))
    if parent_ids != {expected_parent_daily_run_id}:
        raise AttachmentIntegrityError("PREDICTION_PARENT_RUN_ID_MISMATCH")
    attachment = pd.read_csv(attachment_path)
    validation = validate_attachment_frame(
        prediction_frame=prediction_rows(prediction_path, lane=lane),
        attachment_frame=attachment,
        expected_parent_daily_run_id=(
            expected_parent_daily_run_id
            if "parent_daily_run_id" in attachment.columns else None),
        expected_prediction_sha256=(
            expected_prediction_sha256
            if "prediction_artifact_sha256" in attachment.columns else None),
        expected_odds_manifest_sha256=(
            expected_odds_manifest_sha256
            if "odds_observation_manifest_sha256" in attachment.columns else None),
    )
    return {
        "schema_version": "NHL_ATTACHMENT_INTEGRITY_V1",
        "lane": lane,
        "status": "PASS",
        "canonical_key_columns": list(ATTACHMENT_KEY_COLUMNS),
        "parent_daily_run_id": expected_parent_daily_run_id,
        "prediction_artifact_path": str(prediction_path.resolve()),
        "prediction_artifact_sha256": actual_prediction_sha256,
        "attachment_path": str(attachment_path.resolve()),
        "attachment_sha256": sha256_file(attachment_path),
        "odds_observation_manifest_sha256": expected_odds_manifest_sha256,
        **validation,
    }


def stable_candidate_identity(values: Iterable[Any]) -> str:
    payload = json.dumps(list(values), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()

"""Exact-grain integrity evidence for NHL SOG market attachments."""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, sha256_file, verify_package
from backend.nhl.attachment_integrity import validate_odds_observation


SCHEMA_VERSION = "NHL_SOG_ATTACHMENT_INTEGRITY_V1"
KEY_COLUMNS = ("game_date", "game_id", "player_id", "prop", "line")


def _line(value: Any) -> str:
    number = float(value)
    return str(int(number)) if number.is_integer() else f"{number:.6f}".rstrip("0").rstrip(".")


def _keys(frame: pd.DataFrame) -> list[tuple[str, int, int, str, str]]:
    required = ("game_date", "game_id", "player_id", "line")
    missing = [name for name in required if name not in frame]
    if missing:
        raise ValueError("SOG_ATTACHMENT_KEY_COLUMNS_MISSING:" + ",".join(missing))
    values = []
    for date, game_id, player_id, line in frame.loc[:, required].itertuples(index=False, name=None):
        if pd.isna(date) or pd.isna(game_id) or pd.isna(player_id) or pd.isna(line):
            raise ValueError("SOG_ATTACHMENT_KEY_VALUE_MISSING")
        date_str = pd.to_datetime(date, errors="raise").date().isoformat()
        values.append((date_str, int(game_id), int(player_id), "shots_on_goal", _line(line)))
    return values


def prediction_key_frame(path: Path) -> tuple[pd.DataFrame, list[str]]:
    """Expand SOG wide or tall scoring output using the builder's exact line set."""
    frame = pd.read_csv(path)
    if "game_date" not in frame:
        raise ValueError("SOG_PREDICTION_GAME_DATE_MISSING")
    wide = []
    for column in frame.columns:
        match = re.fullmatch(r"p_over_(\d+(?:[._]\d+)?)", str(column))
        if match:
            wide.append((column, float(match.group(1).replace("_", "."))))
        else:
            match = re.fullmatch(r"p_over_(\d+)_5", str(column))
            if match:
                wide.append((column, float(match.group(1) + ".5")))
    if wide:
        required = {"game_id", "player_id"}
        if not required.issubset(frame.columns):
            raise ValueError("SOG_PREDICTION_ID_COLUMNS_MISSING")
        rows = []
        for _, source in frame.iterrows():
            for column, line in wide:
                rows.append({"game_date": source["game_date"], "game_id": source["game_id"],
                             "player_id": source["player_id"], "line": line})
        keys = pd.DataFrame(rows)
    elif {"game_id", "player_id", "line"}.issubset(frame.columns):
        keys = frame[["game_date", "game_id", "player_id", "line"]].copy()
    else:
        raise ValueError("SOG_PREDICTION_LINE_COLUMNS_MISSING")
    identity_columns = [column for column in ("model_family", "model_version", "poisson_source")
                        if column in frame]
    return keys, identity_columns


def audit_sog_attachment(
    *, prediction_path: Path, attachment_path: Path, unmatched_path: Path,
    slate_date: str, parent_daily_run_id: str, odds_observation_path: Path | None,
    odds_observation_manifest_sha256: str | None,
    names_path: Path | None = None, reconstructed: bool = False,
    source_daily_receipt_manifest_sha256: str | None = None,
    canonical_game_starts_utc: dict[int, str] | None = None,
) -> dict[str, Any]:
    predictions, identity_columns = prediction_key_frame(prediction_path)
    source_frame = pd.read_csv(prediction_path)
    attachment = pd.read_csv(attachment_path)
    unmatched = pd.read_csv(unmatched_path)
    prediction_keys = _keys(predictions)
    attachment_keys = _keys(attachment)
    unmatched_keys = _keys(unmatched) if len(unmatched) else []
    pred_set, attach_set = set(prediction_keys), set(attachment_keys)
    unmatched_set = set(unmatched_keys)
    duplicate_prediction = len(prediction_keys) - len(pred_set)
    duplicate_attachment = len(attachment_keys) - len(attach_set)
    missing = pred_set - attach_set
    extra = attach_set - pred_set
    duplicate_unmatched = len(unmatched_keys) - len(unmatched_set)
    if attachment.duplicated(["game_date", "game_id", "player_id", "line"]).any():
        duplicate_attachment = max(duplicate_attachment, 1)
    matched_mask = pd.to_numeric(attachment.get("p_over_mkt"), errors="coerce").notna()
    matched_keys = {key for key, matched in zip(attachment_keys, matched_mask.tolist()) if matched}
    unmatched_by_market = attach_set - matched_keys
    model_identity_rows = (
        source_frame[identity_columns].fillna("").astype(str).to_dict(orient="records")
        if identity_columns else []
    )
    model_identity_values = {
        column: sorted(set(source_frame[column].dropna().astype(str)))
        for column in identity_columns
    }
    # SOG's established builder has no ambiguous state; every key is matched or unmatched.
    ambiguous = 0
    checks = {
        "same_slate": all(key[0] == slate_date for key in prediction_keys + attachment_keys),
        "prediction_keys_unique": duplicate_prediction == 0,
        "attachment_keys_unique": duplicate_attachment == 0,
        "unmatched_keys_unique": duplicate_unmatched == 0,
        "key_sets_equal": not missing and not extra,
        "unmatched_inventory_exact": unmatched_set == unmatched_by_market,
        "statuses_exhaustive": len(matched_keys) + len(unmatched_by_market) == len(prediction_keys),
        "exact_line_identity": all(key[4] for key in prediction_keys + attachment_keys),
        "odds_manifest_bound": bool(odds_observation_manifest_sha256),
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    counts = {
        "prediction_row_count": len(prediction_keys),
        "prediction_key_count": len(prediction_keys),
        "attachment_row_count": len(attachment_keys),
        "unique_prediction_key_count": len(pred_set),
        "unique_attachment_key_count": len(attach_set),
        "duplicate_prediction_key_count": duplicate_prediction,
        "duplicate_attachment_key_count": duplicate_attachment,
        "duplicate_unmatched_key_count": duplicate_unmatched,
        "missing_prediction_key_count": len(missing),
        "extra_attachment_key_count": len(extra),
        "missing_prediction_key_count": len(missing),
        "extra_attachment_key_count": len(extra),
        "matched_count": len(matched_keys),
        "unmatched_count": len(unmatched_by_market),
        "ambiguous_count": ambiguous,
    }
    timing: dict[str, Any] | None = None
    if canonical_game_starts_utc is not None:
        odds_dir = Path(odds_observation_path) if odds_observation_path else None
        observed_at_text = None
        if odds_dir and (odds_dir / "observation_summary.json").is_file():
            observed_at_text = json.loads((odds_dir / "observation_summary.json").read_text()).get(
                "observation_timestamp_utc")
        if not observed_at_text:
            raise ValueError("SOG_ATTACHMENT_OBSERVATION_TIMESTAMP_MISSING")
        observed_at = datetime.fromisoformat(str(observed_at_text).replace("Z", "+00:00"))
        if observed_at.tzinfo is None:
            raise ValueError("SOG_ATTACHMENT_OBSERVATION_TIMESTAMP_NOT_UTC")
        by_game: dict[str, Any] = {}
        for game_id in sorted({key[1] for key in prediction_keys}):
            start_text = canonical_game_starts_utc.get(game_id)
            if start_text is None:
                raise ValueError(f"SOG_ATTACHMENT_CANONICAL_GAME_START_MISSING:{game_id}")
            start_at = datetime.fromisoformat(str(start_text).replace("Z", "+00:00"))
            eligible = observed_at.astimezone(timezone.utc) < start_at.astimezone(timezone.utc)
            game_keys = {key for key in pred_set if key[1] == game_id}
            game_matched = game_keys & matched_keys
            game_unmatched = game_keys & unmatched_by_market
            by_game[str(game_id)] = {
                "canonical_start_time_utc": start_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "eligible": eligible,
                "prediction_keys": len(game_keys),
                "matched": len(game_matched),
                "unmatched": len(game_unmatched),
                "ambiguous": 0,
            }
        timing = {
            "contract": "NHL_SOG_GAME_SPECIFIC_PRESTART_V1",
            "observation_timestamp_utc": observed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "games": by_game,
            "prestart_eligible_prediction_keys": sum(x["prediction_keys"] for x in by_game.values() if x["eligible"]),
            "poststart_ineligible_prediction_keys": sum(x["prediction_keys"] for x in by_game.values() if not x["eligible"]),
            "prestart_matched": sum(x["matched"] for x in by_game.values() if x["eligible"]),
            "prestart_unmatched": sum(x["unmatched"] for x in by_game.values() if x["eligible"]),
            "eligible_game_count": sum(bool(x["eligible"]) for x in by_game.values()),
            "ineligible_game_count": sum(not bool(x["eligible"]) for x in by_game.values()),
        }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "lane": "sog",
        "status": status,
        "integrity_status": status,
        "slate_date": slate_date,
        "parent_daily_run_id": parent_daily_run_id,
        "prop": "shots_on_goal",
        "canonical_key_columns": list(KEY_COLUMNS),
        "prediction_identity_columns": identity_columns,
        "prediction_model_identity": model_identity_values,
        "prediction_model_identity_sha256": hashlib.sha256(json.dumps(
            model_identity_rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "canonical_game_set_hash": canonical_game_set_hash(
            {key[1] for key in prediction_keys}),
        "prediction_artifact_path": str(Path(prediction_path).resolve()),
        "prediction_artifact_sha256": sha256_file(Path(prediction_path)),
        "prediction_key_population_sha256": hashlib.sha256(
            json.dumps(sorted(pred_set), separators=(",", ":")).encode()).hexdigest(),
        "odds_observation_path": str(Path(odds_observation_path).resolve()) if odds_observation_path else None,
        "odds_observation_manifest_sha256": odds_observation_manifest_sha256,
        "attachment_path": str(Path(attachment_path).resolve()),
        "attachment_sha256": sha256_file(Path(attachment_path)),
        "unmatched_path": str(Path(unmatched_path).resolve()),
        "unmatched_sha256": sha256_file(Path(unmatched_path)),
        "names_path": str(Path(names_path).resolve()) if names_path else None,
        "names_sha256": sha256_file(Path(names_path)) if names_path else None,
        "counts": counts,
        "checks": checks,
        "reconstructed_from_retained_evidence": bool(reconstructed),
    }
    if source_daily_receipt_manifest_sha256:
        payload["source_daily_receipt_manifest_sha256"] = source_daily_receipt_manifest_sha256
    if timing is not None:
        payload["game_specific_prestart"] = timing
    return payload


def retain_sog_attachment_package(
    *, package_path: Path, integrity: dict[str, Any], attachment_path: Path,
    unmatched_path: Path, names_path: Path | None = None,
    feature_input_binding: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    """Create a run-scoped create-only package of SOG attachment evidence."""
    final = Path(package_path).resolve()
    if final.exists():
        raise FileExistsError(f"SOG_ATTACHMENT_PACKAGE_ALREADY_EXISTS:{final}")
    staging = final.with_name(f".{final.name}.{uuid.uuid4().hex}.incomplete")
    staging.mkdir(parents=True, exist_ok=False)
    try:
        attached = staging / "sog_with_market.csv"
        unmatched = staging / "unmatched_sog.csv"
        shutil.copy2(attachment_path, attached)
        shutil.copy2(unmatched_path, unmatched)
        retained = dict(integrity)
        if feature_input_binding:
            retained["upstream_production_feature_input"] = dict(feature_input_binding)
        retained["attachment_path"] = str((final / attached.name))
        retained["attachment_sha256"] = sha256_file(attached)
        retained["unmatched_path"] = str((final / unmatched.name))
        retained["unmatched_sha256"] = sha256_file(unmatched)
        if names_path is not None:
            names = staging / "names_identity.csv"
            shutil.copy2(names_path, names)
            retained["names_path"] = str(final / names.name)
            retained["names_sha256"] = sha256_file(names)
        report = staging / "sog_attachment_integrity.json"
        report.write_text(json.dumps(retained, indent=2, sort_keys=True) + "\n")
        marker = staging / "RUN_COMPLETE.json"
        marker.write_text(json.dumps({
            "schema_version": SCHEMA_VERSION,
            "parent_daily_run_id": retained["parent_daily_run_id"],
            "slate_date": retained["slate_date"], "status": retained["status"],
        }, indent=2, sort_keys=True) + "\n")
        files = sorted(path for path in staging.iterdir() if path.is_file())
        (staging / "SHA256SUMS").write_text("".join(
            f"{sha256_file(path)}  {path.name}\n" for path in files))
        final.parent.mkdir(parents=True, exist_ok=True)
        staging.replace(final)
        return final / report.name, final / "SHA256SUMS"
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def verify_sog_integrity_package(report_path: Path, slate_date: str) -> tuple[dict[str, Any], str]:
    """Verify a retained native or reconstructed SOG attachment package."""
    report_path = Path(report_path).resolve()
    package = report_path.parent
    manifest_sha = verify_package(package)
    report = json.loads(report_path.read_text())
    marker = json.loads((package / "RUN_COMPLETE.json").read_text())
    if (report.get("schema_version") != SCHEMA_VERSION or report.get("status") != "PASS"
            or report.get("integrity_status") != "PASS"
            or report.get("slate_date") != slate_date
            or not report.get("canonical_game_set_hash")
            or not report.get("odds_observation_phase")
            or report.get("odds_observation_season") is None
            or marker.get("slate_date") != slate_date
            or marker.get("parent_daily_run_id") != report.get("parent_daily_run_id")):
        raise ValueError("SOG_ATTACHMENT_PACKAGE_STATUS_OR_SLATE_INVALID")
    prediction_path = Path(report["prediction_artifact_path"]).resolve()
    odds_dir = Path(report["odds_observation_path"]).resolve()
    attachment_path = Path(report["attachment_path"]).resolve()
    unmatched_path = Path(report["unmatched_path"]).resolve()
    names_path = Path(report["names_path"]).resolve()
    expected_paths = {package / "sog_with_market.csv", package / "unmatched_sog.csv",
                      package / "names_identity.csv"}
    if {attachment_path, unmatched_path, names_path} != expected_paths:
        raise ValueError("SOG_ATTACHMENT_PACKAGE_PATH_MISMATCH")
    if (_sha_path(prediction_path) != report.get("prediction_artifact_sha256")
            or _sha_path(attachment_path) != report.get("attachment_sha256")
            or _sha_path(unmatched_path) != report.get("unmatched_sha256")
            or _sha_path(names_path) != report.get("names_sha256")):
        raise ValueError("SOG_ATTACHMENT_SOURCE_HASH_MISMATCH")
    odds_manifest_sha = verify_package(odds_dir)
    if odds_manifest_sha != report.get("odds_observation_manifest_sha256"):
        raise ValueError("SOG_ATTACHMENT_ODDS_MANIFEST_MISMATCH")
    odds_summary = json.loads((odds_dir / "observation_summary.json").read_text())
    if (odds_summary.get("slate_date") != slate_date
            or odds_summary.get("parent_daily_run_id") != report.get("parent_daily_run_id")):
        raise ValueError("SOG_ATTACHMENT_ODDS_LINEAGE_MISMATCH")
    from backend.nhl.daily_capture import load_canonical_slate
    slate_root = Path(__file__).resolve().parents[1] / ".." / "artifacts" / "operational" / "nhl" / "slates" / slate_date
    slate_root = slate_root.resolve()
    if (report.get("game_specific_prestart")
            and (slate_root / "raw_schedule_response.json").is_file()
            and (slate_root / "slate_health.json").is_file()):
        games = load_canonical_slate(
            slate_date=slate_date, raw_schedule_path=slate_root / "raw_schedule_response.json",
            slate_health_path=slate_root / "slate_health.json")
        starts = {int(game.game_id): game.start_time_utc for game in games}
    else:
        starts = {}
    pred_frame, _ = prediction_key_frame(prediction_path)
    pred_game_ids = {int(value) for value in pred_frame.game_id.unique()}
    if starts and pred_game_ids != set(starts):
        raise ValueError("SOG_ATTACHMENT_CANONICAL_GAME_IDENTITY_MISMATCH")
    observed_at = datetime.fromisoformat(
        str(odds_summary.get("observation_timestamp_utc", "")).replace("Z", "+00:00"))
    if observed_at.tzinfo is None:
        raise ValueError("SOG_ATTACHMENT_OBSERVATION_TIMESTAMP_NOT_UTC")
    if not starts:
        first_puck = datetime.fromisoformat(
            str(odds_summary.get("first_puck_utc", "")).replace("Z", "+00:00"))
        if odds_summary.get("strictly_prestart") is not True or observed_at >= first_puck:
            raise ValueError("SOG_ATTACHMENT_ODDS_NOT_STRICTLY_PRESTART")
    odds_lineage = validate_odds_observation(
        observation_dir=odds_dir, odds_json=odds_dir / "raw_response.json",
        expected_manifest_sha256=odds_manifest_sha,
        expected_parent_daily_run_id=str(report["parent_daily_run_id"]),
        expected_slate_date=slate_date,
        expected_season=int(report.get("odds_observation_season", 2026)),
        expected_phase=report.get("odds_observation_phase"),
        expected_game_set_hash=report.get("canonical_game_set_hash"),
        replayed=bool(report.get("odds_observation_replayed", False)),
    )
    if odds_lineage.get("odds_observation_manifest_sha256") != report.get(
            "odds_observation_manifest_sha256"):
        raise ValueError("SOG_ATTACHMENT_ODDS_LINEAGE_REAUDIT_MISMATCH")
    audited = audit_sog_attachment(
        prediction_path=prediction_path, attachment_path=attachment_path,
        unmatched_path=unmatched_path, slate_date=slate_date,
        parent_daily_run_id=str(report["parent_daily_run_id"]),
        odds_observation_path=odds_dir,
        odds_observation_manifest_sha256=odds_manifest_sha,
        names_path=names_path,
        reconstructed=bool(report.get("reconstructed_from_retained_evidence")),
        canonical_game_starts_utc=starts or None,
    )
    if audited.get("status") != "PASS" or audited.get("counts") != report.get("counts"):
        raise ValueError("SOG_ATTACHMENT_REAUDIT_MISMATCH")
    return report, manifest_sha


def _sha_path(path: Path) -> str:
    return sha256_file(Path(path))

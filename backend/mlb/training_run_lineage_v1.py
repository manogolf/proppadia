"""Immutable manifest binding for MLB model-training inputs and results."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


INPUT_MANIFEST_CONTRACT = "MLB_IMMUTABLE_TRAINING_INPUT_MANIFEST_V1"
RESULT_MANIFEST_CONTRACT = "MLB_IMMUTABLE_TRAINING_RESULT_MANIFEST_V1"
_SHA256 = re.compile(r"[0-9a-f]{64}")


class TrainingLineageError(RuntimeError):
    """Fail-closed lineage construction or verification error."""


def _normal(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return {"__float__": "NaN"}
        if math.isinf(value):
            return {"__float__": "Infinity" if value > 0 else "-Infinity"}
        return {"__float__": format(value, ".17g")}
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return {str(key): _normal(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_normal(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _normal(value.item())
        except (TypeError, ValueError):
            pass
    if hasattr(value, "isoformat"):
        try:
            return {"__temporal__": value.isoformat()}
        except (TypeError, ValueError):
            pass
    return {"__type__": type(value).__name__, "value": str(value)}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _normal(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ordered_projection_sha256(
    rows: Iterable[Mapping[str, Any]],
    fields: Sequence[str],
) -> str:
    digest = hashlib.sha256()
    for row in rows:
        payload = canonical_bytes([row.get(field) for field in fields])
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def frame_projection_sha256(frame: Any, fields: Sequence[str]) -> str:
    missing = [field for field in fields if field not in frame.columns]
    if missing:
        raise TrainingLineageError(
            "TRAINING_LINEAGE_FIELDS_MISSING:" + ",".join(sorted(missing))
        )
    return ordered_projection_sha256(
        frame.loc[:, list(fields)].to_dict(orient="records"),
        fields,
    )


def _require_sha256(value: Any, label: str) -> str:
    rendered = str(value or "")
    if not _SHA256.fullmatch(rendered):
        raise TrainingLineageError(f"TRAINING_LINEAGE_SHA256_INVALID:{label}")
    return rendered


def _manifest_with_hash(body: Mapping[str, Any]) -> dict[str, Any]:
    manifest = dict(body)
    manifest["manifest_sha256"] = canonical_sha256(body)
    return manifest


def verify_manifest(manifest: Mapping[str, Any], contract: str) -> str:
    if manifest.get("contract") != contract:
        raise TrainingLineageError("TRAINING_LINEAGE_CONTRACT_MISMATCH")
    claimed = _require_sha256(manifest.get("manifest_sha256"), "manifest")
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    actual = canonical_sha256(body)
    if claimed != actual:
        raise TrainingLineageError("TRAINING_LINEAGE_MANIFEST_HASH_MISMATCH")
    return actual


def build_input_manifest(
    *,
    run_identity: str,
    code_commit: str,
    trainer_config: Mapping[str, Any],
    admitted_game_pks: Sequence[int],
    admitted_row_count: int,
    gate_report: Mapping[str, Any],
    source_snapshot: Mapping[str, Any],
    canonical_row_order_sha256: str,
    retained_feature_sha256: str,
    retained_target_sha256: str,
    feature_contract: Mapping[str, Any],
    target_contract: Mapping[str, Any],
    split_definition: Mapping[str, Any],
) -> dict[str, Any]:
    if not str(run_identity).strip():
        raise TrainingLineageError("TRAINING_LINEAGE_RUN_IDENTITY_MISSING")
    if not re.fullmatch(r"[0-9a-f]{40}", str(code_commit)):
        raise TrainingLineageError("TRAINING_LINEAGE_CODE_COMMIT_INVALID")
    exact_game_pks = sorted({int(value) for value in admitted_game_pks})
    if any(value <= 0 for value in exact_game_pks):
        raise TrainingLineageError("TRAINING_LINEAGE_GAME_PK_INVALID")
    if int(gate_report.get("blocked_row_count", -1)) != 0:
        raise TrainingLineageError("TRAINING_LINEAGE_GATE_NOT_CERTIFIABLE")
    if str(gate_report.get("gate_status")) != "PASS":
        raise TrainingLineageError("TRAINING_LINEAGE_GATE_NOT_PASSED")
    gate_admitted = {
        int(value)
        for value in (gate_report.get("decision_game_pks") or {}).get(
            "ADMITTED_REGULAR_SEASON", []
        )
    }
    if not exact_game_pks or not set(exact_game_pks).issubset(gate_admitted):
        raise TrainingLineageError("TRAINING_LINEAGE_ADMITTED_GAME_PK_MISMATCH")
    if int(admitted_row_count) <= 0:
        raise TrainingLineageError("TRAINING_LINEAGE_ADMITTED_ROW_COUNT_INVALID")
    authority = gate_report.get("authority")
    if not isinstance(authority, Mapping):
        raise TrainingLineageError("TRAINING_LINEAGE_AUTHORITY_MISSING")
    for key in ("proposal_sha256", "source_manifest_sha256", "phase_contract_sha256"):
        _require_sha256(authority.get(key), f"authority.{key}")
    source_hashes = source_snapshot.get("hashes")
    if not isinstance(source_hashes, Mapping) or not source_hashes:
        raise TrainingLineageError("TRAINING_LINEAGE_SOURCE_SNAPSHOT_MISSING")
    for key, value in source_hashes.items():
        _require_sha256(value, f"source_snapshot.{key}")
    for label, value in (
        ("canonical_row_order", canonical_row_order_sha256),
        ("retained_feature", retained_feature_sha256),
        ("retained_target", retained_target_sha256),
    ):
        _require_sha256(value, label)

    decisions = gate_report.get("decision_row_counts") or {}
    decision_game_pks = gate_report.get("decision_game_pks") or {}
    body = {
        "contract": INPUT_MANIFEST_CONTRACT,
        "status": "INPUT_FROZEN_BEFORE_FIT",
        "run_identity": str(run_identity),
        "code_commit": str(code_commit),
        "trainer_config": dict(trainer_config),
        "input_population": {
            "admitted_row_count": int(admitted_row_count),
            "admitted_game_pk_count": len(exact_game_pks),
            "exact_admitted_game_pks": exact_game_pks,
            "admitted_game_pks_sha256": canonical_sha256(exact_game_pks),
            "canonical_row_order_sha256": canonical_row_order_sha256,
            "retained_feature_sha256": retained_feature_sha256,
            "retained_target_sha256": retained_target_sha256,
        },
        "phase_eligibility": {
            "contract_name": gate_report.get("contract_name"),
            "authority_interface": authority.get("authority_interface"),
            "authority_backend": authority.get("backend"),
            "proposal_sha256": authority.get("proposal_sha256"),
            "source_manifest_sha256": authority.get("source_manifest_sha256"),
            "phase_contract_sha256": authority.get("phase_contract_sha256"),
            "authority_records_sha256": authority.get("authority_records_sha256"),
            "supported_window": {
                "from": authority.get("supported_from_date"),
                "through": authority.get("supported_through_date"),
            },
            "decision_row_counts": dict(decisions),
            "decision_game_pk_counts": {
                key: len(values) for key, values in sorted(decision_game_pks.items())
            },
            "explicit_excluded_game_pks": {
                key: list(values)
                for key, values in sorted(decision_game_pks.items())
                if key.startswith("EXCLUDED_") and values
            },
            "blocked_row_count": 0,
            "calendar_inference": False,
            "missing_type_default": False,
        },
        "source_snapshot": dict(source_snapshot),
        "feature_contract": dict(feature_contract),
        "target_contract": dict(target_contract),
        "split_definition": dict(split_definition),
    }
    manifest = _manifest_with_hash(body)
    verify_manifest(manifest, INPUT_MANIFEST_CONTRACT)
    return manifest


def build_result_manifest(
    *,
    input_manifest: Mapping[str, Any],
    model_artifacts: Sequence[Mapping[str, Any]],
    result_artifacts: Sequence[Mapping[str, Any]],
    completed_at_utc: str | None = None,
) -> dict[str, Any]:
    input_sha256 = verify_manifest(input_manifest, INPUT_MANIFEST_CONTRACT)
    if input_manifest.get("status") != "INPUT_FROZEN_BEFORE_FIT":
        raise TrainingLineageError("TRAINING_LINEAGE_INPUT_STATUS_INVALID")
    models = [dict(item) for item in model_artifacts]
    results = [dict(item) for item in result_artifacts]
    if not models:
        raise TrainingLineageError("TRAINING_LINEAGE_MODEL_ARTIFACT_MISSING")
    if not results:
        raise TrainingLineageError("TRAINING_LINEAGE_RESULT_ARTIFACT_MISSING")
    for group_name, artifacts in (("model", models), ("result", results)):
        for item in artifacts:
            if not str(item.get("path") or ""):
                raise TrainingLineageError(
                    f"TRAINING_LINEAGE_{group_name.upper()}_PATH_MISSING"
                )
            _require_sha256(item.get("sha256"), f"{group_name}.{item.get('path')}")
            if int(item.get("bytes", -1)) < 0:
                raise TrainingLineageError(
                    f"TRAINING_LINEAGE_{group_name.upper()}_BYTES_INVALID"
                )
    timestamp = completed_at_utc or datetime.now(UTC).isoformat()
    body = {
        "contract": RESULT_MANIFEST_CONTRACT,
        "status": "COMPLETED_BOUND",
        "run_identity": input_manifest["run_identity"],
        "input_manifest_sha256": input_sha256,
        "model_artifacts": models,
        "evaluation_result_artifacts": results,
        "completed_at_utc": timestamp,
    }
    manifest = _manifest_with_hash(body)
    verify_manifest(manifest, RESULT_MANIFEST_CONTRACT)
    return manifest


def _encoded_manifest(manifest: Mapping[str, Any], contract: str) -> bytes:
    verify_manifest(manifest, contract)
    return (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")


def write_manifest_immutable(
    path: Path,
    manifest: Mapping[str, Any],
    *,
    contract: str,
    before_publish_hook: Any = None,
) -> str:
    payload = _encoded_manifest(manifest, contract)
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise TrainingLineageError("TRAINING_LINEAGE_IMMUTABLE_PATH_CONFLICT")
        return verify_manifest(manifest, contract)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if before_publish_hook is not None:
            before_publish_hook(temporary, path)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise TrainingLineageError("TRAINING_LINEAGE_IMMUTABLE_PATH_CONFLICT")
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return verify_manifest(manifest, contract)


def load_and_verify_manifest(path: Path, contract: str) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TrainingLineageError("TRAINING_LINEAGE_MANIFEST_UNREADABLE") from exc
    if not isinstance(manifest, dict):
        raise TrainingLineageError("TRAINING_LINEAGE_MANIFEST_NOT_OBJECT")
    verify_manifest(manifest, contract)
    return manifest


def require_completed_result_binding(
    input_manifest_path: Path,
    result_manifest_path: Path,
) -> dict[str, Any]:
    input_manifest = load_and_verify_manifest(
        input_manifest_path, INPUT_MANIFEST_CONTRACT
    )
    result_manifest = load_and_verify_manifest(
        result_manifest_path, RESULT_MANIFEST_CONTRACT
    )
    input_sha256 = verify_manifest(input_manifest, INPUT_MANIFEST_CONTRACT)
    if result_manifest.get("status") != "COMPLETED_BOUND":
        raise TrainingLineageError("TRAINING_LINEAGE_RESULT_INCOMPLETE")
    if result_manifest.get("input_manifest_sha256") != input_sha256:
        raise TrainingLineageError("TRAINING_LINEAGE_RESULT_INPUT_MISMATCH")
    if result_manifest.get("run_identity") != input_manifest.get("run_identity"):
        raise TrainingLineageError("TRAINING_LINEAGE_RESULT_RUN_MISMATCH")
    for section in ("model_artifacts", "evaluation_result_artifacts"):
        artifacts = result_manifest.get(section)
        if not isinstance(artifacts, list) or not artifacts:
            raise TrainingLineageError(
                f"TRAINING_LINEAGE_RESULT_SECTION_INCOMPLETE:{section}"
            )
        for artifact in artifacts:
            artifact_path = Path(str(artifact.get("path") or ""))
            try:
                actual_bytes = artifact_path.stat().st_size
            except OSError as exc:
                raise TrainingLineageError(
                    f"TRAINING_LINEAGE_BOUND_ARTIFACT_MISSING:{section}"
                ) from exc
            if actual_bytes != int(artifact.get("bytes", -1)):
                raise TrainingLineageError(
                    f"TRAINING_LINEAGE_BOUND_ARTIFACT_SIZE_MISMATCH:{section}"
                )
            if file_sha256(artifact_path) != artifact.get("sha256"):
                raise TrainingLineageError(
                    f"TRAINING_LINEAGE_BOUND_ARTIFACT_HASH_MISMATCH:{section}"
                )
    return result_manifest

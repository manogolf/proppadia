"""Explicit, input-bound publication for the 2026 MLB regular-season close.

Readiness remains a separate read-only operation.  Publication is impossible
without a separately reviewed authorization document bound to the exact pinned
V2 inventory, reconciliation manifest, and phase-authority manifest.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from backend.mlb.season_transition import regular_season_close_inventory_v1 as v1
from backend.mlb.season_transition import regular_season_close_inventory_v2 as v2

SCHEMA = "MLB_2026_REGULAR_SEASON_CLOSE_ARTIFACT_V1"
AUTH_SCHEMA = "MLB_2026_REGULAR_SEASON_CLOSE_AUTHORIZATION_V1"
DEFAULT_OUTPUT = v1.REPO_ROOT / "artifacts/operational/mlb/season_close/2026/regular_season_close.json"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _valid_digest(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def validate_close_rows(rows: list[dict[str, Any]]) -> None:
    if len(rows) != 2430 or len({int(row["game_pk"]) for row in rows}) != 2430:
        raise v1.CloseInventoryError("CLOSE_POPULATION_CHANGED")
    if any(row.get("authoritative_raw_game_type") != "R" for row in rows):
        raise v1.CloseInventoryError("CLOSE_POSTSEASON_ROW_PRESENT")
    if any(row.get("close_disposition") not in v1.CLOSE_COMPLETE_DISPOSITIONS for row in rows):
        raise v1.CloseInventoryError("CLOSE_UNRESOLVED_GAME_PRESENT")
    cancelled = [row for row in rows if int(row["game_pk"]) == v2.RAIN_CANCELLATION_GAME_PK]
    if (len(cancelled) != 1 or cancelled[0].get("close_disposition") != "AUTHORITATIVELY_CANCELLED"
            or "played_official_date" in cancelled[0] or "final_outcome" in cancelled[0]):
        raise v1.CloseInventoryError("CLOSE_CANCELLATION_SEMANTICS_INVALID")


def close_inputs() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Validate current pinned V2 bytes and return exact close input identities."""
    report = v2.validate_close_readiness_package()
    if not report.get("integrity_passed") or not report.get("close_ready"):
        raise v1.CloseInventoryError("CLOSE_READINESS_NOT_PASSED")
    if report.get("population_counts", {}).get("regular_season_game_pks") != 2430:
        raise v1.CloseInventoryError("CLOSE_POPULATION_NOT_2430")
    inventory_path = v2.PACKAGE / v2.INVENTORY
    manifest_path = v2.PACKAGE / v2.MANIFEST
    inventory_bytes = inventory_path.read_bytes()
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    inventory_hash = _digest(inventory_bytes)
    if not _valid_digest(inventory_hash) or manifest.get("inventory_sha256") != inventory_hash:
        raise v1.CloseInventoryError("CLOSE_INVENTORY_HASH_MISMATCH")
    if not _valid_digest(manifest.get("manifest_sha256")):
        raise v1.CloseInventoryError("CLOSE_RECONCILIATION_MANIFEST_HASH_INVALID")
    if manifest.get("manifest_sha256") != v2.EXPECTED_RECONCILIATION_MANIFEST_SHA256:
        raise v1.CloseInventoryError("CLOSE_RECONCILIATION_MANIFEST_NOT_PINNED")
    authority_hash = manifest.get("authority_manifest_sha256")
    if not _valid_digest(authority_hash):
        raise v1.CloseInventoryError("CLOSE_AUTHORITY_HASH_INVALID")
    rows = [json.loads(line) for line in inventory_bytes.decode("utf-8").splitlines() if line.strip()]
    validate_close_rows(rows)
    inputs = {
        "contract_name": v2.CONTRACT,
        "authority_manifest_sha256": authority_hash,
        "inventory_path": f"{v2.PACKAGE_RELATIVE_PATH}/{v2.INVENTORY}" if hasattr(v2, "PACKAGE_RELATIVE_PATH") else str(inventory_path.relative_to(v1.REPO_ROOT)),
        "inventory_sha256": inventory_hash,
        "reconciliation_manifest_path": str(manifest_path.relative_to(v1.REPO_ROOT)),
        "reconciliation_manifest_sha256": _digest(manifest_bytes),
        "reconciliation_manifest_content_sha256": manifest["manifest_sha256"],
        "population_size": len(rows),
    }
    return inputs, rows


def expected_authorization_binding(inputs: dict[str, Any]) -> dict[str, Any]:
    return {key: inputs[key] for key in (
        "authority_manifest_sha256", "inventory_sha256",
        "reconciliation_manifest_sha256", "reconciliation_manifest_content_sha256",
        "population_size")}


def validate_authorization(path: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    try:
        authorization_bytes = Path(path).read_bytes()
        authorization = json.loads(authorization_bytes.decode("utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise v1.CloseInventoryError("CLOSE_AUTHORIZATION_UNREADABLE") from exc
    if authorization.get("schema") != AUTH_SCHEMA or authorization.get("authorizes_close") is not True:
        raise v1.CloseInventoryError("CLOSE_AUTHORIZATION_NOT_EXPLICIT")
    if authorization.get("input_binding") != expected_authorization_binding(inputs):
        raise v1.CloseInventoryError("CLOSE_AUTHORIZATION_INPUT_MISMATCH")
    if not str(authorization.get("authorization_id", "")).strip() or not str(authorization.get("authorized_by", "")).strip():
        raise v1.CloseInventoryError("CLOSE_AUTHORIZATION_IDENTITY_MISSING")
    authorization["_sha256"] = _digest(authorization_bytes)
    return authorization


def close_document(inputs: dict[str, Any], rows: list[dict[str, Any]], authorization: dict[str, Any]) -> dict[str, Any]:
    body = {
        "schema": SCHEMA,
        "close_status": "REGULAR_SEASON_CLOSED",
        "inputs": inputs,
        "game_count": len(rows),
        "disposition_counts": dict(sorted({
            disposition: sum(row.get("close_disposition") == disposition for row in rows)
            for disposition in sorted({row["close_disposition"] for row in rows})
        }.items())),
        "authorization": {
            "schema": AUTH_SCHEMA,
            "authorization_id": authorization["authorization_id"],
            "authorized_by": authorization["authorized_by"],
            "authorized_at_utc": authorization.get("authorized_at_utc"),
            "authorization_sha256": authorization["_sha256"],
        },
        "games": rows,
    }
    body["close_artifact_sha256"] = hashlib.sha256(v1.canonical_json_bytes(body)).hexdigest()
    return body


def publish_immutable(path: Path, document: dict[str, Any]) -> None:
    """Atomically publish once; hard-link makes existing targets non-overwritable."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise v1.CloseInventoryError("CLOSE_ARTIFACT_ALREADY_EXISTS")
    data = json.dumps(document, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temp, path)
        except FileExistsError as exc:
            raise v1.CloseInventoryError("CLOSE_ARTIFACT_ALREADY_EXISTS") from exc
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temp.unlink(missing_ok=True)


def execute_close(authorization_path: Path, output_path: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    inputs, rows = close_inputs()
    authorization = validate_authorization(authorization_path, inputs)
    document = close_document(inputs, rows, authorization)
    publish_immutable(output_path, document)
    return document


def validate_close_artifact(path: Path, *, expected_inputs: dict[str, Any] | None = None,
                            expected_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Verify artifact self-hash and, when omitted, bind to the current V2 package."""
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise v1.CloseInventoryError("CLOSE_ARTIFACT_UNREADABLE") from exc
    claimed = document.pop("close_artifact_sha256", None)
    if not _valid_digest(claimed) or _digest(v1.canonical_json_bytes(document)) != claimed:
        raise v1.CloseInventoryError("CLOSE_ARTIFACT_HASH_MISMATCH")
    document["close_artifact_sha256"] = claimed
    if document.get("schema") != SCHEMA or document.get("close_status") != "REGULAR_SEASON_CLOSED":
        raise v1.CloseInventoryError("CLOSE_ARTIFACT_SCHEMA_INVALID")
    if expected_inputs is None or expected_rows is None:
        current_inputs, current_rows = close_inputs()
        expected_inputs = current_inputs if expected_inputs is None else expected_inputs
        expected_rows = current_rows if expected_rows is None else expected_rows
    validate_close_rows(document.get("games", []))
    if document.get("inputs") != expected_inputs or document.get("games") != expected_rows:
        raise v1.CloseInventoryError("CLOSE_ARTIFACT_INPUT_MISMATCH")
    if document.get("game_count") != 2430:
        raise v1.CloseInventoryError("CLOSE_ARTIFACT_POPULATION_INVALID")
    return document

"""Immutable descriptor verification for versioned MLB phase authority files.

This module is pure and offline.  It verifies descriptor, proposal, source
manifest, and parent-chain bytes.  It deliberately knows nothing about model,
market, database, or acquisition behavior.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping


SNAPSHOT_CONTRACT_NAME = "MLB_2026_VERSIONED_FILE_PHASE_AUTHORITY_V1"
SNAPSHOT_SCHEMA_VERSION = 1
ACTIVE_SELECTION_CONTRACT_NAME = "MLB_2026_ACTIVE_FILE_PHASE_AUTHORITY_SELECTION_V1"
ACTIVE_SELECTION_SCHEMA_VERSION = 1
BUILDER_CONTRACT_NAME = "MLB_2026_VERSIONED_FILE_PHASE_AUTHORITY_BUILDER_V1"
BUILDER_VERSION = "1"
REPO_ROOT = Path(__file__).resolve().parents[3]
SNAPSHOT_ROOT = Path("backend/mlb/season_transition/authority_snapshots")
V1_DESCRIPTOR_PATH = REPO_ROOT / SNAPSHOT_ROOT / "v1/descriptor.json"
ACTIVE_SELECTION_PATH = REPO_ROOT / SNAPSHOT_ROOT / "active_selection.json"


class SnapshotDescriptorError(RuntimeError):
    """A descriptor or selection failed before authority data could be used."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}:{detail}" if detail else code)


@dataclass(frozen=True)
class VerifiedSnapshotDescriptor:
    path: Path
    sha256: str
    data: Mapping[str, Any]
    proposal_path: Path
    source_manifest_path: Path
    parent: "VerifiedSnapshotDescriptor | None"


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise SnapshotDescriptorError("SNAPSHOT_EVIDENCE_MISSING", str(path)) from exc
    return digest.hexdigest()


def _require_sha256(value: Any, code: str) -> str:
    text = str(value or "")
    if re.fullmatch(r"[0-9a-f]{64}", text) is None:
        raise SnapshotDescriptorError(code, text)
    return text


def _load_object(path: Path, code: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotDescriptorError(code, str(path)) from exc
    if not isinstance(value, dict):
        raise SnapshotDescriptorError(code, str(path))
    return value


def _within_root(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError:
        raise SnapshotDescriptorError("SNAPSHOT_PATH_OUTSIDE_REPOSITORY", str(path)) from None
    return resolved


def resolve_descriptor_reference(
    descriptor_path: Path, value: Any, *, root: Path = REPO_ROOT
) -> Path:
    text = str(value or "")
    if not text or Path(text).is_absolute():
        raise SnapshotDescriptorError("SNAPSHOT_REFERENCE_PATH_INVALID", text)
    return _within_root(descriptor_path.parent / text, root)


def _read_jsonl(path: Path, code: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SnapshotDescriptorError("SNAPSHOT_EVIDENCE_MISSING", str(path)) from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SnapshotDescriptorError(code, f"{path}:{line_number}:{exc.msg}") from None
        if not isinstance(row, dict):
            raise SnapshotDescriptorError(code, f"{path}:{line_number}:not_object")
        rows.append(row)
    return rows


def _validate_descriptor_fields(data: Mapping[str, Any]) -> None:
    if data.get("contract_name") != SNAPSHOT_CONTRACT_NAME:
        raise SnapshotDescriptorError("SNAPSHOT_CONTRACT_UNSUPPORTED")
    if data.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise SnapshotDescriptorError("SNAPSHOT_SCHEMA_UNSUPPORTED")
    if str(data.get("snapshot_status") or "") not in {"CANDIDATE", "GOVERNED"}:
        raise SnapshotDescriptorError("SNAPSHOT_STATUS_INVALID")
    if not str(data.get("snapshot_id") or ""):
        raise SnapshotDescriptorError("SNAPSHOT_ID_MISSING")
    if data.get("supported_season") != 2026:
        raise SnapshotDescriptorError("SNAPSHOT_SEASON_UNSUPPORTED")
    try:
        start = date.fromisoformat(str(data.get("scheduled_date_from")))
        end = date.fromisoformat(str(data.get("scheduled_date_through")))
    except ValueError:
        raise SnapshotDescriptorError("SNAPSHOT_DATE_RANGE_INVALID") from None
    if start > end or start.year != 2026 or end.year != 2026:
        raise SnapshotDescriptorError("SNAPSHOT_DATE_RANGE_INVALID")
    if data.get("builder_contract") != BUILDER_CONTRACT_NAME or str(
        data.get("builder_version")
    ) != BUILDER_VERSION:
        raise SnapshotDescriptorError("SNAPSHOT_BUILDER_UNSUPPORTED")
    for field in (
        "proposal_sha256",
        "source_manifest_sha256",
        "deterministic_source_set_sha256",
        "game_pk_population_sha256",
        "classification_sha256",
        "v1_proposal_sha256",
        "v1_source_manifest_sha256",
        "v1_authority_records_sha256",
    ):
        _require_sha256(data.get(field), f"SNAPSHOT_{field.upper()}_INVALID")
    for field in (
        "row_count",
        "distinct_game_pk_count",
        "source_file_count",
        "source_observation_count",
    ):
        value = data.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise SnapshotDescriptorError("SNAPSHOT_COUNT_INVALID", field)
    for field in ("raw_game_type_counts", "normalized_phase_counts"):
        counts = data.get(field)
        if not isinstance(counts, dict) or any(
            not isinstance(key, str)
            or isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            for key, value in counts.items()
        ):
            raise SnapshotDescriptorError("SNAPSHOT_COUNTS_MAP_INVALID", field)


def verify_descriptor_chain(
    descriptor_path: Path,
    *,
    expected_sha256: str,
    root: Path = REPO_ROOT,
    allow_candidate: bool = False,
    _seen: frozenset[Path] = frozenset(),
) -> VerifiedSnapshotDescriptor:
    """Verify one descriptor and every parent without fallback."""

    path = _within_root(descriptor_path, root)
    if path in _seen:
        raise SnapshotDescriptorError("SNAPSHOT_PARENT_CHAIN_CYCLE", str(path))
    expected = _require_sha256(expected_sha256, "SNAPSHOT_DESCRIPTOR_EXPECTED_HASH_INVALID")
    actual = sha256_file(path)
    if actual != expected:
        raise SnapshotDescriptorError(
            "SNAPSHOT_DESCRIPTOR_HASH_MISMATCH", f"expected={expected};actual={actual}"
        )
    data = _load_object(path, "SNAPSHOT_DESCRIPTOR_MALFORMED")
    _validate_descriptor_fields(data)
    if data["snapshot_status"] == "CANDIDATE" and not allow_candidate:
        raise SnapshotDescriptorError("SNAPSHOT_CANDIDATE_NOT_ACTIVE")

    proposal_path = resolve_descriptor_reference(path, data.get("proposal_path"), root=root)
    source_manifest_path = resolve_descriptor_reference(
        path, data.get("source_manifest_path"), root=root
    )
    if sha256_file(proposal_path) != data["proposal_sha256"]:
        raise SnapshotDescriptorError("SNAPSHOT_PROPOSAL_HASH_MISMATCH")
    if sha256_file(source_manifest_path) != data["source_manifest_sha256"]:
        raise SnapshotDescriptorError("SNAPSHOT_SOURCE_MANIFEST_HASH_MISMATCH")

    proposal_rows = _read_jsonl(proposal_path, "SNAPSHOT_PROPOSAL_MALFORMED")
    source_rows = _read_jsonl(
        source_manifest_path, "SNAPSHOT_SOURCE_MANIFEST_MALFORMED"
    )
    game_pks: list[int] = []
    for row in proposal_rows:
        value = row.get("game_pk")
        if isinstance(value, bool):
            raise SnapshotDescriptorError("SNAPSHOT_GAME_PK_INVALID")
        try:
            game_pks.append(int(value))
        except (TypeError, ValueError):
            raise SnapshotDescriptorError("SNAPSHOT_GAME_PK_INVALID") from None
    if len(proposal_rows) != data["row_count"]:
        raise SnapshotDescriptorError("SNAPSHOT_PROPOSAL_ROW_COUNT_MISMATCH")
    if len(set(game_pks)) != data["distinct_game_pk_count"]:
        raise SnapshotDescriptorError("SNAPSHOT_DISTINCT_GAME_PK_COUNT_MISMATCH")
    if len(source_rows) != data["source_file_count"]:
        raise SnapshotDescriptorError("SNAPSHOT_SOURCE_FILE_COUNT_MISMATCH")
    observations = 0
    for row in source_rows:
        try:
            observations += int(row.get("schedule_game_rows"))
        except (TypeError, ValueError):
            raise SnapshotDescriptorError("SNAPSHOT_SOURCE_OBSERVATION_INVALID") from None
    if observations != data["source_observation_count"]:
        raise SnapshotDescriptorError("SNAPSHOT_SOURCE_OBSERVATION_COUNT_MISMATCH")
    source_commitment = hashlib.sha256(canonical_json_bytes(source_rows)).hexdigest()
    if source_commitment != data["deterministic_source_set_sha256"]:
        raise SnapshotDescriptorError("SNAPSHOT_SOURCE_SET_COMMITMENT_MISMATCH")
    game_pk_commitment = hashlib.sha256(canonical_json_bytes(game_pks)).hexdigest()
    if game_pk_commitment != data["game_pk_population_sha256"]:
        raise SnapshotDescriptorError("SNAPSHOT_GAME_PK_POPULATION_HASH_MISMATCH")

    parent_path_value = data.get("parent_descriptor_path")
    parent_hash_value = data.get("parent_descriptor_sha256")
    parent: VerifiedSnapshotDescriptor | None = None
    if parent_path_value is None and parent_hash_value is None:
        if data.get("snapshot_id") != data.get("root_snapshot_id"):
            raise SnapshotDescriptorError("SNAPSHOT_ROOT_IDENTITY_MISMATCH")
    elif parent_path_value is None or parent_hash_value is None:
        raise SnapshotDescriptorError("SNAPSHOT_PARENT_REFERENCE_INCOMPLETE")
    else:
        parent_path = resolve_descriptor_reference(path, parent_path_value, root=root)
        parent = verify_descriptor_chain(
            parent_path,
            expected_sha256=_require_sha256(
                parent_hash_value, "SNAPSHOT_PARENT_DESCRIPTOR_HASH_INVALID"
            ),
            root=root,
            allow_candidate=True,
            _seen=_seen | {path},
        )
        if data.get("parent_snapshot_id") != parent.data.get("snapshot_id"):
            raise SnapshotDescriptorError("SNAPSHOT_PARENT_IDENTITY_MISMATCH")
        if data.get("root_snapshot_id") != parent.data.get("root_snapshot_id"):
            raise SnapshotDescriptorError("SNAPSHOT_ROOT_IDENTITY_MISMATCH")
        if not proposal_path.read_bytes().startswith(parent.proposal_path.read_bytes()):
            raise SnapshotDescriptorError("SNAPSHOT_PARENT_PROPOSAL_PREFIX_MISMATCH")
        if not source_manifest_path.read_bytes().startswith(
            parent.source_manifest_path.read_bytes()
        ):
            raise SnapshotDescriptorError("SNAPSHOT_PARENT_SOURCE_PREFIX_MISMATCH")
        if data["row_count"] <= parent.data["row_count"]:
            raise SnapshotDescriptorError("SNAPSHOT_CHILD_HAS_NO_NEW_GAME_PK")
        for field in (
            "v1_proposal_sha256",
            "v1_source_manifest_sha256",
            "v1_authority_records_sha256",
        ):
            if data.get(field) != parent.data.get(field):
                raise SnapshotDescriptorError("SNAPSHOT_V1_INVARIANT_MISMATCH", field)

    return VerifiedSnapshotDescriptor(
        path=path,
        sha256=actual,
        data=data,
        proposal_path=proposal_path,
        source_manifest_path=source_manifest_path,
        parent=parent,
    )


def load_active_descriptor(
    selection_path: Path = ACTIVE_SELECTION_PATH,
    *,
    root: Path = REPO_ROOT,
) -> VerifiedSnapshotDescriptor:
    """Resolve the one configured descriptor.  Invalid selection never falls back."""

    path = _within_root(selection_path, root)
    data = _load_object(path, "ACTIVE_AUTHORITY_SELECTION_MALFORMED")
    if data.get("contract_name") != ACTIVE_SELECTION_CONTRACT_NAME:
        raise SnapshotDescriptorError("ACTIVE_AUTHORITY_SELECTION_CONTRACT_INVALID")
    if data.get("schema_version") != ACTIVE_SELECTION_SCHEMA_VERSION:
        raise SnapshotDescriptorError("ACTIVE_AUTHORITY_SELECTION_SCHEMA_UNSUPPORTED")
    if data.get("selection_status") not in {
        "PINNED_V1_NOT_ACTIVATED_CHILD",
        "GOVERNED_ACTIVE",
    }:
        raise SnapshotDescriptorError("ACTIVE_AUTHORITY_SELECTION_STATUS_INVALID")
    descriptor_value = data.get("descriptor_path")
    if not isinstance(descriptor_value, str) or not descriptor_value or Path(
        descriptor_value
    ).is_absolute():
        raise SnapshotDescriptorError("ACTIVE_AUTHORITY_DESCRIPTOR_PATH_INVALID")
    descriptor_path = _within_root(root / descriptor_value, root)
    return verify_descriptor_chain(
        descriptor_path,
        expected_sha256=_require_sha256(
            data.get("descriptor_sha256"), "ACTIVE_AUTHORITY_DESCRIPTOR_HASH_INVALID"
        ),
        root=root,
        allow_candidate=False,
    )

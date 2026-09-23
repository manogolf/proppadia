"""Backend-neutral, fail-closed MLB game-phase authority interface.

The V1 production backend is deliberately file-only.  It validates the exact
source-hashed 2026 proposal selected by the sidecar architecture decision and
does not call a database or network service.  A future database backend must
implement the same ``lookup_exact`` and ``require_membership`` semantics.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime
from numbers import Integral, Real
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.mlb.season_transition.contract_v1 import (
    CONTRACT_NAME as PHASE_CONTRACT_NAME,
    GAME_TYPE_CONTRACT,
    PHASES,
    PhaseContractError,
    normalize_source_game_type,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_PATH,
    V1_DESCRIPTOR_PATH,
    SnapshotDescriptorError,
    load_active_descriptor,
    verify_descriptor_chain,
)


AUTHORITY_INTERFACE_NAME = "MLB_CANONICAL_GAME_PHASE_AUTHORITY_V1"
FILE_BACKEND_NAME = "MLB_2026_HASHED_PROPOSAL_AUTHORITY_V1"
PHASE_CONTRACT_VERSION = "contract_v1"
EXPECTED_PHASE_CONTRACT_NAME = (
    "MLB_2026_REGULAR_SEASON_CLOSE_AND_POSTSEASON_DATA_PLAN_V1"
)
EXPECTED_PHASE_CONTRACT_SHA256 = (
    "eed52e24d123c24e0fbbff21249705d653d77787eab98eb756d5c9752a481c19"
)
EXPECTED_PROPOSAL_SHA256 = (
    "b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879"
)
EXPECTED_SOURCE_MANIFEST_SHA256 = (
    "766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100"
)
EXPECTED_V1_DESCRIPTOR_SHA256 = (
    "543fda06d3c066bb6f0608ee8c05987829216fa441459a8fc08b4f87ee8c245a"
)
EXPECTED_V1_AUTHORITY_RECORDS_SHA256 = (
    "5a7cdc460cc42ca2b4ed328c74e978b9da6967f95d7a7c4ba888b3b8d3401a84"
)
EXPECTED_PROPOSAL_COUNT = 2919
EXPECTED_SOURCE_FILE_COUNT = 464
EXPECTED_SOURCE_OBSERVATION_COUNT = 9092
EXPECTED_TYPE_COUNTS = {"E": 38, "R": 2430, "S": 451}
EXPECTED_PHASE_COUNTS = {"PRESEASON": 489, "REGULAR_SEASON": 2430}
SUPPORTED_SEASON = 2026
SUPPORTED_FROM_DATE = date(2026, 2, 20)
SUPPORTED_THROUGH_DATE = date(2026, 9, 27)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROPOSAL_PATH = REPO_ROOT / (
    "docs/contracts/mlb_2026_canonical_phase_source_completion_v1/"
    "canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl"
)
DEFAULT_SOURCE_MANIFEST_PATH = REPO_ROOT / (
    "docs/contracts/mlb_2026_canonical_phase_source_completion_v1/"
    "canonical_backfill_proposal/retained_source_manifest.jsonl"
)
PHASE_CONTRACT_PATH = REPO_ROOT / "backend/mlb/season_transition/contract_v1.py"


class GamePhaseAuthorityError(RuntimeError):
    """Typed fail-closed error returned by every authority backend."""

    def __init__(
        self,
        code: str,
        *,
        game_pk: int | None = None,
        detail: str = "",
    ) -> None:
        self.code = code
        self.game_pk = game_pk
        self.detail = detail
        components = [code]
        if game_pk is not None:
            components.append(str(game_pk))
        if detail:
            components.append(detail)
        super().__init__(":".join(components))


@dataclass(frozen=True)
class GamePhaseAuthorityRecord:
    game_pk: int
    source_season: int
    source_game_type: str
    season_phase: str | None
    postseason_round: str | None
    season_name: str | None
    source_round: str | None
    schedule_relationships: Mapping[str, Any]
    primary_source_path: str
    primary_source_sha256: str
    source_paths: tuple[str, ...]
    source_hashes: tuple[str, ...]
    phase_decision: str
    authority_status: str
    scheduled_start_utc: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        if value["scheduled_start_utc"] is None:
            value.pop("scheduled_start_utc")
        value["schedule_relationships"] = dict(self.schedule_relationships)
        value["source_paths"] = list(self.source_paths)
        value["source_hashes"] = list(self.source_hashes)
        return value


@dataclass(frozen=True)
class GamePhaseAuthorityMetadata:
    authority_interface: str
    backend: str
    supported_season: int
    supported_from_date: str
    supported_through_date: str
    proposal_path: str
    proposal_sha256: str
    proposal_count: int
    source_manifest_path: str
    source_manifest_sha256: str
    source_file_count: int
    source_observation_count: int
    phase_contract_name: str
    phase_contract_version: str
    phase_contract_sha256: str
    source_type_counts: Mapping[str, int]
    phase_counts: Mapping[str, int]
    authority_records_sha256: str
    missing_count: int = 0
    unknown_count: int = 0
    conflicting_count: int = 0
    duplicate_identity_count: int = 0
    snapshot_id: str = ""
    snapshot_status: str = ""
    snapshot_descriptor_path: str = ""
    snapshot_descriptor_sha256: str = ""
    parent_descriptor_sha256: str = ""

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["source_type_counts"] = dict(self.source_type_counts)
        value["phase_counts"] = dict(self.phase_counts)
        return value


class CanonicalGamePhaseAuthority(ABC):
    """Stable application interface for file and future database backends."""

    @property
    @abstractmethod
    def metadata(self) -> GamePhaseAuthorityMetadata:
        raise NotImplementedError

    @abstractmethod
    def lookup_exact(self, game_pk: Any) -> GamePhaseAuthorityRecord:
        raise NotImplementedError

    def require_membership(
        self,
        game_pk: Any,
        allowed_phases: Iterable[str],
    ) -> GamePhaseAuthorityRecord:
        allowed = frozenset(str(value) for value in allowed_phases)
        if not allowed or not allowed.issubset(PHASES):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_ALLOWED_MEMBERSHIP_INVALID",
                detail=",".join(sorted(allowed)),
            )
        record = self.lookup_exact(game_pk)
        if record.season_phase not in allowed:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_NOT_IN_ALLOWED_MEMBERSHIP",
                game_pk=record.game_pk,
                detail=str(record.season_phase),
            )
        return record

    def require_supported_window(self, from_date: Any, to_date: Any) -> None:
        start = _parse_iso_date(from_date)
        end = _parse_iso_date(to_date)
        if (
            start > end
            or start.year != self.metadata.supported_season
            or end.year != self.metadata.supported_season
            or start < date.fromisoformat(self.metadata.supported_from_date)
            or end > date.fromisoformat(self.metadata.supported_through_date)
        ):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_AUTHORITY_STALE",
                detail=(
                    f"requested={start.isoformat()}..{end.isoformat()};"
                    f"supported={self.metadata.supported_from_date}.."
                    f"{self.metadata.supported_through_date}"
                ),
            )


def _parse_iso_date(value: Any) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        raise GamePhaseAuthorityError(
            "GAME_PHASE_AUTHORITY_STALE",
            detail=f"invalid_date={value!r}",
        ) from None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise GamePhaseAuthorityError(
            "GAME_PHASE_EVIDENCE_MISSING",
            detail=str(path),
        ) from exc
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise GamePhaseAuthorityError(
            "GAME_PHASE_EVIDENCE_MISSING",
            detail=str(path),
        ) from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_EVIDENCE_MALFORMED",
                detail=f"{path}:{line_number}:{exc.msg}",
            ) from None
        if not isinstance(value, dict):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_EVIDENCE_MALFORMED",
                detail=f"{path}:{line_number}:not_object",
            )
        rows.append(value)
    return rows


def _coerce_game_pk(value: Any) -> int:
    if value is None or isinstance(value, bool):
        raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
    if isinstance(value, Integral):
        game_pk = int(value)
    elif isinstance(value, Real):
        if not math.isfinite(float(value)) or not float(value).is_integer():
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
        game_pk = int(value)
    elif isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
        game_pk = int(value)
    else:
        raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
    if game_pk <= 0:
        raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
    return game_pk


def _safe_source_path(value: Any, *, root: Path) -> tuple[str, Path]:
    source_path = str(value or "")
    if not source_path or Path(source_path).is_absolute():
        raise GamePhaseAuthorityError(
            "GAME_PHASE_SOURCE_PATH_INVALID",
            detail=source_path,
        )
    resolved = (root / source_path).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError:
        raise GamePhaseAuthorityError(
            "GAME_PHASE_SOURCE_PATH_INVALID",
            detail=source_path,
        ) from None
    return source_path, resolved


def _load_and_verify_source_manifest(
    manifest_path: Path,
    *,
    root: Path,
    expected_sha256: str,
    expected_count: int,
) -> tuple[dict[str, str], int]:
    actual_sha256 = _sha256(manifest_path)
    if actual_sha256 != expected_sha256:
        raise GamePhaseAuthorityError(
            "GAME_PHASE_SOURCE_MANIFEST_HASH_MISMATCH",
            detail=f"expected={expected_sha256};actual={actual_sha256}",
        )
    rows = _read_jsonl(manifest_path)
    if len(rows) != expected_count:
        raise GamePhaseAuthorityError(
            "GAME_PHASE_SOURCE_MANIFEST_COUNT_MISMATCH",
            detail=f"expected={expected_count};actual={len(rows)}",
        )
    by_path: dict[str, str] = {}
    observation_count = 0
    for row in rows:
        source_path, resolved = _safe_source_path(row.get("source_path"), root=root)
        source_sha256 = str(row.get("source_sha256") or "")
        if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_SOURCE_HASH_INVALID",
                detail=source_path,
            )
        if source_path in by_path:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_SOURCE_MANIFEST_DUPLICATE_PATH",
                detail=source_path,
            )
        try:
            expected_bytes = int(row.get("source_bytes"))
            schedule_rows = int(row.get("schedule_game_rows"))
        except (TypeError, ValueError):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_SOURCE_MANIFEST_ROW_INVALID",
                detail=source_path,
            ) from None
        if expected_bytes < 0 or schedule_rows < 0:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_SOURCE_MANIFEST_ROW_INVALID",
                detail=source_path,
            )
        try:
            actual_bytes = resolved.stat().st_size
        except OSError as exc:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_EVIDENCE_MISSING",
                detail=source_path,
            ) from exc
        if actual_bytes != expected_bytes or _sha256(resolved) != source_sha256:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_EVIDENCE_HASH_MISMATCH",
                detail=source_path,
            )
        by_path[source_path] = source_sha256
        observation_count += schedule_rows
    return by_path, observation_count


def validate_proposal_records(
    rows: Sequence[Mapping[str, Any]],
    *,
    source_hash_by_path: Mapping[str, str],
    expected_count: int | None = None,
    expected_type_counts: Mapping[str, int] | None = None,
    expected_phase_counts: Mapping[str, int] | None = None,
    required_season: int = SUPPORTED_SEASON,
) -> tuple[dict[int, GamePhaseAuthorityRecord], dict[str, Any]]:
    """Validate proposal rows without introducing another phase mapping."""

    records: dict[int, GamePhaseAuthorityRecord] = {}
    source_type_counts: Counter[str] = Counter()
    phase_counts: Counter[str] = Counter()
    for row in rows:
        try:
            game_pk = _coerce_game_pk(row.get("game_pk"))
        except GamePhaseAuthorityError as exc:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_PROPOSAL_GAME_PK_INVALID",
                detail=str(exc),
            ) from None
        if game_pk in records:
            prior = records[game_pk]
            incoming_core = (
                row.get("source_season"),
                row.get("source_game_type"),
                row.get("season_phase"),
                row.get("postseason_round"),
            )
            prior_core = (
                prior.source_season,
                prior.source_game_type,
                prior.season_phase,
                prior.postseason_round,
            )
            code = (
                "GAME_PHASE_PROPOSAL_DUPLICATE_IDENTITY"
                if incoming_core == prior_core
                else "GAME_PHASE_PROPOSAL_CONFLICTING_IDENTITY"
            )
            raise GamePhaseAuthorityError(code, game_pk=game_pk)
        try:
            source_season = int(row.get("source_season"))
        except (TypeError, ValueError):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_PROPOSAL_SEASON_MISSING",
                game_pk=game_pk,
            ) from None
        if source_season != required_season:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_PROPOSAL_SEASON_CONFLICT",
                game_pk=game_pk,
                detail=f"expected={required_season};actual={source_season}",
            )
        source_game_type = row.get("source_game_type")
        try:
            classification = normalize_source_game_type(
                source_game_type,
                season=source_season,
                source_round=row.get("source_round"),
            )
        except PhaseContractError as exc:
            code = (
                "GAME_PHASE_PROPOSAL_TYPE_MISSING"
                if "MISSING" in str(exc)
                else "GAME_PHASE_PROPOSAL_TYPE_UNKNOWN"
            )
            raise GamePhaseAuthorityError(
                code,
                game_pk=game_pk,
                detail=str(exc),
            ) from None
        for field_name, expected_value in (
            ("season_phase", classification.phase),
            ("postseason_round", classification.postseason_round),
            ("season_name", classification.season_name),
            ("phase_decision", classification.decision),
        ):
            if row.get(field_name) != expected_value:
                raise GamePhaseAuthorityError(
                    "GAME_PHASE_PROPOSAL_CLASSIFICATION_CONFLICT",
                    game_pk=game_pk,
                    detail=field_name,
                )
        relationships = row.get("schedule_relationships")
        if not isinstance(relationships, dict):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_PROPOSAL_RELATIONSHIPS_INVALID",
                game_pk=game_pk,
            )
        primary_path = str(row.get("game_type_source_path") or "")
        primary_sha256 = str(row.get("game_type_source_sha256") or "")
        source_paths = tuple(sorted(str(value) for value in (row.get("source_paths") or [])))
        source_hashes = tuple(sorted(str(value) for value in (row.get("source_hashes") or [])))
        if (
            not primary_path
            or not primary_sha256
            or not source_paths
            or not source_hashes
            or primary_path not in source_paths
            or primary_sha256 not in source_hashes
            or source_hash_by_path.get(primary_path) != primary_sha256
        ):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_PROPOSAL_PRIMARY_EVIDENCE_INVALID",
                game_pk=game_pk,
            )
        evidence_hashes: set[str] = set()
        for source_path in source_paths:
            source_hash = source_hash_by_path.get(source_path)
            if source_hash is None:
                raise GamePhaseAuthorityError(
                    "GAME_PHASE_PROPOSAL_SOURCE_UNVERIFIED",
                    game_pk=game_pk,
                    detail=source_path,
                )
            evidence_hashes.add(source_hash)
        if evidence_hashes != set(source_hashes):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_PROPOSAL_SOURCE_HASH_CONFLICT",
                game_pk=game_pk,
            )
        authority_status = (
            "AUTHORITATIVE_UNAMBIGUOUS"
            if classification.phase in PHASES
            else "SPECIAL_EXCLUDED"
        )
        scheduled_start_value = row.get("scheduled_start_utc")
        scheduled_start_utc: str | None = None
        if scheduled_start_value not in (None, ""):
            scheduled_start_utc = str(scheduled_start_value)
            try:
                parsed_start = datetime.fromisoformat(
                    scheduled_start_utc.replace("Z", "+00:00")
                )
            except ValueError:
                raise GamePhaseAuthorityError(
                    "GAME_PHASE_PROPOSAL_SCHEDULED_START_INVALID",
                    game_pk=game_pk,
                ) from None
            if parsed_start.tzinfo is None or parsed_start.year != required_season:
                raise GamePhaseAuthorityError(
                    "GAME_PHASE_PROPOSAL_SCHEDULED_START_INVALID",
                    game_pk=game_pk,
                )
        record = GamePhaseAuthorityRecord(
            game_pk=game_pk,
            source_season=source_season,
            source_game_type=str(source_game_type),
            season_phase=classification.phase,
            postseason_round=classification.postseason_round,
            season_name=classification.season_name,
            source_round=classification.source_round,
            schedule_relationships=dict(relationships),
            primary_source_path=primary_path,
            primary_source_sha256=primary_sha256,
            source_paths=source_paths,
            source_hashes=source_hashes,
            phase_decision=classification.decision,
            authority_status=authority_status,
            scheduled_start_utc=scheduled_start_utc,
        )
        records[game_pk] = record
        source_type_counts[record.source_game_type] += 1
        if record.season_phase is not None:
            phase_counts[record.season_phase] += 1

    if expected_count is not None and len(records) != expected_count:
        raise GamePhaseAuthorityError(
            "GAME_PHASE_PROPOSAL_COUNT_MISMATCH",
            detail=f"expected={expected_count};actual={len(records)}",
        )
    if expected_type_counts is not None and dict(sorted(source_type_counts.items())) != dict(
        sorted(expected_type_counts.items())
    ):
        raise GamePhaseAuthorityError(
            "GAME_PHASE_PROPOSAL_TYPE_COUNTS_MISMATCH",
            detail=json.dumps(dict(sorted(source_type_counts.items())), sort_keys=True),
        )
    if expected_phase_counts is not None and dict(sorted(phase_counts.items())) != dict(
        sorted(expected_phase_counts.items())
    ):
        raise GamePhaseAuthorityError(
            "GAME_PHASE_PROPOSAL_PHASE_COUNTS_MISMATCH",
            detail=json.dumps(dict(sorted(phase_counts.items())), sort_keys=True),
        )
    canonical_records = [records[game_pk].to_dict() for game_pk in sorted(records)]
    return records, {
        "source_type_counts": dict(sorted(source_type_counts.items())),
        "phase_counts": dict(sorted(phase_counts.items())),
        "authority_records_sha256": hashlib.sha256(
            _canonical_json(canonical_records)
        ).hexdigest(),
    }


class HashedProposalAuthority(CanonicalGamePhaseAuthority):
    """Validated file-backed authority selected by an immutable descriptor.

    The no-argument operational path resolves exactly one active selection and
    never falls back.  Explicit legacy proposal/manifest arguments remain only
    for existing tamper tests and frozen V1 reproduction.
    """

    def __init__(
        self,
        *,
        proposal_path: Path = DEFAULT_PROPOSAL_PATH,
        source_manifest_path: Path = DEFAULT_SOURCE_MANIFEST_PATH,
        expected_proposal_sha256: str = EXPECTED_PROPOSAL_SHA256,
        expected_source_manifest_sha256: str = EXPECTED_SOURCE_MANIFEST_SHA256,
        root: Path = REPO_ROOT,
        descriptor_path: Path | None = None,
        expected_descriptor_sha256: str | None = None,
        selection_path: Path = ACTIVE_SELECTION_PATH,
        allow_candidate: bool = False,
    ) -> None:
        contract_sha256 = _sha256(PHASE_CONTRACT_PATH)
        if (
            PHASE_CONTRACT_NAME != EXPECTED_PHASE_CONTRACT_NAME
            or contract_sha256 != EXPECTED_PHASE_CONTRACT_SHA256
        ):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_CONTRACT_IDENTITY_MISMATCH",
                detail=(
                    f"name={PHASE_CONTRACT_NAME};version={PHASE_CONTRACT_VERSION};"
                    f"sha256={contract_sha256}"
                ),
            )
        root = root.resolve()
        legacy_override = (
            proposal_path.resolve() != DEFAULT_PROPOSAL_PATH.resolve()
            or source_manifest_path.resolve() != DEFAULT_SOURCE_MANIFEST_PATH.resolve()
            or expected_proposal_sha256 != EXPECTED_PROPOSAL_SHA256
            or expected_source_manifest_sha256 != EXPECTED_SOURCE_MANIFEST_SHA256
        )
        if descriptor_path is not None and legacy_override:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_DESCRIPTOR_AND_LEGACY_INPUT_CONFLICT"
            )

        verified_descriptor = None
        if descriptor_path is not None:
            if expected_descriptor_sha256 is None:
                raise GamePhaseAuthorityError(
                    "GAME_PHASE_DESCRIPTOR_EXPECTED_HASH_REQUIRED"
                )
            try:
                verified_descriptor = verify_descriptor_chain(
                    descriptor_path,
                    expected_sha256=expected_descriptor_sha256,
                    root=root,
                    allow_candidate=allow_candidate,
                )
            except SnapshotDescriptorError as exc:
                raise GamePhaseAuthorityError(exc.code, detail=exc.detail) from exc
        elif not legacy_override:
            try:
                verified_descriptor = load_active_descriptor(
                    selection_path=selection_path,
                    root=root,
                )
            except SnapshotDescriptorError as exc:
                raise GamePhaseAuthorityError(exc.code, detail=exc.detail) from exc

        descriptor_data: Mapping[str, Any] = {}
        if verified_descriptor is not None:
            root_descriptor = verified_descriptor
            while root_descriptor.parent is not None:
                root_descriptor = root_descriptor.parent
            if (
                root_descriptor.sha256 != EXPECTED_V1_DESCRIPTOR_SHA256
                or root_descriptor.data.get("snapshot_id")
                != "MLB_2026_GAME_PHASE_AUTHORITY_V1"
                or root_descriptor.data.get("proposal_sha256")
                != EXPECTED_PROPOSAL_SHA256
                or root_descriptor.data.get("source_manifest_sha256")
                != EXPECTED_SOURCE_MANIFEST_SHA256
            ):
                raise GamePhaseAuthorityError(
                    "GAME_PHASE_ROOT_DESCRIPTOR_NOT_GOVERNED_V1"
                )
            descriptor_data = verified_descriptor.data
            proposal_path = verified_descriptor.proposal_path
            source_manifest_path = verified_descriptor.source_manifest_path
            expected_proposal_sha256 = str(descriptor_data["proposal_sha256"])
            expected_source_manifest_sha256 = str(
                descriptor_data["source_manifest_sha256"]
            )
            expected_proposal_count = int(descriptor_data["row_count"])
            expected_source_file_count = int(descriptor_data["source_file_count"])
            expected_source_observation_count = int(
                descriptor_data["source_observation_count"]
            )
            expected_type_counts = dict(descriptor_data["raw_game_type_counts"])
            expected_phase_counts = dict(descriptor_data["normalized_phase_counts"])
            supported_from_date = date.fromisoformat(
                str(descriptor_data["scheduled_date_from"])
            )
            supported_through_date = date.fromisoformat(
                str(descriptor_data["scheduled_date_through"])
            )
        else:
            expected_proposal_count = EXPECTED_PROPOSAL_COUNT
            expected_source_file_count = EXPECTED_SOURCE_FILE_COUNT
            expected_source_observation_count = EXPECTED_SOURCE_OBSERVATION_COUNT
            expected_type_counts = EXPECTED_TYPE_COUNTS
            expected_phase_counts = EXPECTED_PHASE_COUNTS
            supported_from_date = SUPPORTED_FROM_DATE
            supported_through_date = SUPPORTED_THROUGH_DATE

        proposal_path = proposal_path.resolve()
        source_manifest_path = source_manifest_path.resolve()
        proposal_sha256 = _sha256(proposal_path)
        if proposal_sha256 != expected_proposal_sha256:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_PROPOSAL_HASH_MISMATCH",
                detail=(
                    f"expected={expected_proposal_sha256};actual={proposal_sha256}"
                ),
            )
        source_hash_by_path, observation_count = _load_and_verify_source_manifest(
            source_manifest_path,
            root=root,
            expected_sha256=expected_source_manifest_sha256,
            expected_count=expected_source_file_count,
        )
        if observation_count != expected_source_observation_count:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_SOURCE_OBSERVATION_COUNT_MISMATCH",
                detail=(
                    f"expected={expected_source_observation_count};"
                    f"actual={observation_count}"
                ),
            )
        rows = _read_jsonl(proposal_path)
        records, validation = validate_proposal_records(
            rows,
            source_hash_by_path=source_hash_by_path,
            expected_count=expected_proposal_count,
            expected_type_counts=expected_type_counts,
            expected_phase_counts=expected_phase_counts,
        )
        classification_rows = [
            {
                key: row.get(key)
                for key in (
                    "game_pk",
                    "source_season",
                    "source_game_type",
                    "season_phase",
                    "postseason_round",
                    "season_name",
                    "source_round",
                    "schedule_relationships",
                )
            }
            for row in rows
        ]
        if verified_descriptor is not None:
            classification_sha256 = hashlib.sha256(
                _canonical_json(classification_rows)
            ).hexdigest()
            if classification_sha256 != descriptor_data["classification_sha256"]:
                raise GamePhaseAuthorityError(
                    "GAME_PHASE_DESCRIPTOR_CLASSIFICATION_HASH_MISMATCH"
                )
            if verified_descriptor.parent is None:
                if (
                    proposal_sha256 != EXPECTED_PROPOSAL_SHA256
                    or expected_source_manifest_sha256
                    != EXPECTED_SOURCE_MANIFEST_SHA256
                    or validation["authority_records_sha256"]
                    != EXPECTED_V1_AUTHORITY_RECORDS_SHA256
                    or descriptor_data["v1_proposal_sha256"]
                    != EXPECTED_PROPOSAL_SHA256
                    or descriptor_data["v1_source_manifest_sha256"]
                    != EXPECTED_SOURCE_MANIFEST_SHA256
                    or descriptor_data["v1_authority_records_sha256"]
                    != EXPECTED_V1_AUTHORITY_RECORDS_SHA256
                ):
                    raise GamePhaseAuthorityError(
                        "GAME_PHASE_V1_DESCRIPTOR_INVARIANT_MISMATCH"
                    )
        self._records = records
        self._metadata = GamePhaseAuthorityMetadata(
            authority_interface=AUTHORITY_INTERFACE_NAME,
            backend=FILE_BACKEND_NAME,
            supported_season=SUPPORTED_SEASON,
            supported_from_date=supported_from_date.isoformat(),
            supported_through_date=supported_through_date.isoformat(),
            proposal_path=str(proposal_path.relative_to(root)),
            proposal_sha256=proposal_sha256,
            proposal_count=len(records),
            source_manifest_path=str(source_manifest_path.relative_to(root)),
            source_manifest_sha256=expected_source_manifest_sha256,
            source_file_count=len(source_hash_by_path),
            source_observation_count=observation_count,
            phase_contract_name=PHASE_CONTRACT_NAME,
            phase_contract_version=PHASE_CONTRACT_VERSION,
            phase_contract_sha256=contract_sha256,
            source_type_counts=validation["source_type_counts"],
            phase_counts=validation["phase_counts"],
            authority_records_sha256=validation["authority_records_sha256"],
            snapshot_id=str(descriptor_data.get("snapshot_id") or ""),
            snapshot_status=str(descriptor_data.get("snapshot_status") or ""),
            snapshot_descriptor_path=(
                str(verified_descriptor.path.relative_to(root))
                if verified_descriptor is not None
                else ""
            ),
            snapshot_descriptor_sha256=(
                verified_descriptor.sha256 if verified_descriptor is not None else ""
            ),
            parent_descriptor_sha256=str(
                descriptor_data.get("parent_descriptor_sha256") or ""
            ),
        )

    @property
    def metadata(self) -> GamePhaseAuthorityMetadata:
        return self._metadata

    @property
    def records(self) -> tuple[GamePhaseAuthorityRecord, ...]:
        """Return the verified authority population in exact gamePk order."""

        return tuple(self._records[game_pk] for game_pk in sorted(self._records))

    def lookup_exact(self, game_pk: Any) -> GamePhaseAuthorityRecord:
        exact_game_pk = _coerce_game_pk(game_pk)
        record = self._records.get(exact_game_pk)
        if record is None:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_ABSENT",
                game_pk=exact_game_pk,
            )
        if record.authority_status == "SPECIAL_EXCLUDED" or record.season_phase is None:
            raise GamePhaseAuthorityError(
                "GAME_PHASE_SPECIAL_EXCLUDED",
                game_pk=exact_game_pk,
            )
        if record.authority_status == "CONFLICT_BLOCKED":
            raise GamePhaseAuthorityError(
                "GAME_PHASE_CONFLICT_BLOCKED",
                game_pk=exact_game_pk,
            )
        if record.authority_status == "CORRECTION_REVIEW_REQUIRED":
            raise GamePhaseAuthorityError(
                "GAME_PHASE_CORRECTION_REVIEW_REQUIRED",
                game_pk=exact_game_pk,
            )
        if record.authority_status != "AUTHORITATIVE_UNAMBIGUOUS":
            raise GamePhaseAuthorityError(
                "GAME_PHASE_AUTHORITY_STATUS_UNKNOWN",
                game_pk=exact_game_pk,
                detail=record.authority_status,
            )
        return record


def source_type_is_recognized(raw_type: Any) -> bool:
    """Exact StatsAPI wire-value check for consumer conflict auditing."""

    return isinstance(raw_type, str) and raw_type in GAME_TYPE_CONTRACT


class VersionedFileAuthority(HashedProposalAuthority):
    """Explicit descriptor loader used for candidate validation and rollback tests."""

    def __init__(
        self,
        *,
        descriptor_path: Path,
        expected_descriptor_sha256: str,
        root: Path = REPO_ROOT,
        allow_candidate: bool = False,
    ) -> None:
        super().__init__(
            root=root,
            descriptor_path=descriptor_path,
            expected_descriptor_sha256=expected_descriptor_sha256,
            allow_candidate=allow_candidate,
        )


def load_v1_authority(*, root: Path = REPO_ROOT) -> VersionedFileAuthority:
    """Load the exact immutable V1 descriptor, bypassing active selection."""

    return VersionedFileAuthority(
        descriptor_path=root / V1_DESCRIPTOR_PATH.relative_to(REPO_ROOT),
        expected_descriptor_sha256=EXPECTED_V1_DESCRIPTOR_SHA256,
        root=root,
    )

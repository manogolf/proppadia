"""Pure exact-player/game strict-prior feature-state contract V1.

No function in this module connects to a database or network service.  The
legacy ``mlb.player_derived_stats`` relation is deliberately outside this
contract and remains historical daily-grain evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping

from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
)


CONTRACT_VERSION = "MLB_PLAYER_GAME_FEATURE_STATE_V1"
LEGACY_PLAYER_DERIVED_GRAIN = "PLAYER_DATE_POSTGAME_AGGREGATE_WITH_MIXED_GAME_ID_SEMANTICS"
HISTORICAL_EXACT_GAME_FACT_RECONSTRUCTABLE = "HISTORICAL_EXACT_GAME_FACT_RECONSTRUCTABLE"
HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE = (
    "HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE"
)
EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE = (
    "EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ExactGameContractError(ValueError):
    """Typed fail-closed contract error."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(code if not detail else f"{code}:{detail}")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _positive_int(value: Any, code: str) -> int:
    if isinstance(value, bool):
        raise ExactGameContractError(code)
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ExactGameContractError(code) from None
    if result <= 0 or str(value).strip() != str(result):
        raise ExactGameContractError(code)
    return result


def _utc(value: str | datetime | None, code: str) -> datetime:
    if value in (None, ""):
        raise ExactGameContractError(code)
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise ExactGameContractError(code) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExactGameContractError(code)
    return parsed.astimezone(timezone.utc)


def _date(value: str | date, code: str) -> date:
    try:
        return value if isinstance(value, date) and not isinstance(value, datetime) else date.fromisoformat(str(value))
    except ValueError:
        raise ExactGameContractError(code) from None


def _sha(value: Any, code: str) -> str:
    text = str(value or "")
    if not _SHA256.fullmatch(text):
        raise ExactGameContractError(code)
    return text


@dataclass(frozen=True)
class SourceObservationV1:
    source_path: str
    source_sha256: str
    observed_at_utc: datetime
    source_kind: str

    def __post_init__(self) -> None:
        if not self.source_path or self.source_path.startswith("/") or ".." in self.source_path.split("/"):
            raise ExactGameContractError("SOURCE_PATH_INVALID", self.source_path)
        _sha(self.source_sha256, "SOURCE_SHA256_INVALID")
        _utc(self.observed_at_utc, "SOURCE_OBSERVED_AT_MISSING")
        if not self.source_kind:
            raise ExactGameContractError("SOURCE_KIND_MISSING")

    @classmethod
    def create(
        cls,
        *,
        source_path: str,
        source_sha256: str,
        observed_at_utc: str | datetime,
        source_kind: str,
    ) -> "SourceObservationV1":
        path = str(source_path or "").strip()
        kind = str(source_kind or "").strip()
        if not path or path.startswith("/") or ".." in path.split("/"):
            raise ExactGameContractError("SOURCE_PATH_INVALID", path)
        if not kind:
            raise ExactGameContractError("SOURCE_KIND_MISSING")
        return cls(
            source_path=path,
            source_sha256=_sha(source_sha256, "SOURCE_SHA256_INVALID"),
            observed_at_utc=_utc(observed_at_utc, "SOURCE_OBSERVED_AT_MISSING"),
            source_kind=kind,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
            "observed_at_utc": self.observed_at_utc.isoformat().replace("+00:00", "Z"),
            "source_kind": self.source_kind,
        }


@dataclass(frozen=True)
class ExactGameAuthorityV1:
    game_pk: int
    official_date: date
    scheduled_start_utc: datetime
    phase_authority_interface: str
    phase_authority_snapshot_id: str
    phase_authority_descriptor_sha256: str
    source_season: int
    source_game_type: str
    season_phase: str
    schedule_relationships: Mapping[str, Any]
    source_paths: tuple[str, ...]
    source_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        _positive_int(self.game_pk, "EXACT_GAME_PK_MISSING")
        _date(self.official_date, "OFFICIAL_DATE_MISSING")
        _utc(self.scheduled_start_utc, "SCHEDULED_START_MISSING")
        if not self.phase_authority_interface or not self.phase_authority_snapshot_id:
            raise ExactGameContractError("PHASE_AUTHORITY_IDENTITY_MISSING")
        _sha(
            self.phase_authority_descriptor_sha256,
            "PHASE_AUTHORITY_DESCRIPTOR_SHA256_INVALID",
        )
        if self.source_season <= 0 or not self.source_game_type or not self.season_phase:
            raise ExactGameContractError("CANONICAL_PHASE_AUTHORITY_CONFLICT", str(self.game_pk))
        if not self.source_paths or len(self.source_paths) != len(self.source_hashes):
            raise ExactGameContractError("AUTHORITY_SOURCE_PROVENANCE_INVALID")
        for value in self.source_hashes:
            _sha(value, "AUTHORITY_SOURCE_SHA256_INVALID")

    @classmethod
    def from_canonical_interface(
        cls,
        authority: CanonicalGamePhaseAuthority,
        *,
        game_pk: Any,
        official_date: str | date,
        scheduled_start_utc: str | datetime,
        schedule_relationships: Mapping[str, Any] | None = None,
    ) -> "ExactGameAuthorityV1":
        exact_game_pk = _positive_int(game_pk, "EXACT_GAME_PK_MISSING")
        try:
            record = authority.lookup_exact(exact_game_pk)
        except GamePhaseAuthorityError as exc:
            raise ExactGameContractError("CANONICAL_PHASE_AUTHORITY_REJECTED", str(exc)) from exc
        if record.game_pk != exact_game_pk or not record.season_phase:
            raise ExactGameContractError("CANONICAL_PHASE_AUTHORITY_CONFLICT", str(exact_game_pk))
        metadata = authority.metadata
        descriptor_hash = metadata.snapshot_descriptor_sha256 or metadata.authority_records_sha256
        descriptor_id = metadata.snapshot_id or metadata.proposal_sha256
        return cls(
            game_pk=exact_game_pk,
            official_date=_date(official_date, "OFFICIAL_DATE_MISSING"),
            scheduled_start_utc=_utc(scheduled_start_utc, "SCHEDULED_START_MISSING"),
            phase_authority_interface=metadata.authority_interface,
            phase_authority_snapshot_id=str(descriptor_id or ""),
            phase_authority_descriptor_sha256=_sha(
                descriptor_hash, "PHASE_AUTHORITY_DESCRIPTOR_SHA256_INVALID"
            ),
            source_season=int(record.source_season),
            source_game_type=str(record.source_game_type),
            season_phase=str(record.season_phase),
            schedule_relationships=dict(schedule_relationships or record.schedule_relationships),
            source_paths=tuple(str(value) for value in record.source_paths),
            source_hashes=tuple(_sha(value, "AUTHORITY_SOURCE_SHA256_INVALID") for value in record.source_hashes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "official_date": self.official_date.isoformat(),
            "scheduled_start_utc": self.scheduled_start_utc.isoformat().replace("+00:00", "Z"),
            "phase_authority_interface": self.phase_authority_interface,
            "phase_authority_snapshot_id": self.phase_authority_snapshot_id,
            "phase_authority_descriptor_sha256": self.phase_authority_descriptor_sha256,
            "source_season": self.source_season,
            "source_game_type": self.source_game_type,
            "season_phase": self.season_phase,
            "schedule_relationships": dict(self.schedule_relationships),
            "source_paths": list(self.source_paths),
            "source_hashes": list(self.source_hashes),
        }


@dataclass(frozen=True)
class ExactGameFeatureStateV1:
    player_id: int
    authority: ExactGameAuthorityV1
    feature_input_cutoff_utc: datetime
    source_observations: tuple[SourceObservationV1, ...]
    feature_payload: Mapping[str, Any]
    feature_payload_sha256: str
    row_sha256: str
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        _positive_int(self.player_id, "PLAYER_ID_MISSING")
        if self.contract_version != CONTRACT_VERSION:
            raise ExactGameContractError("CONTRACT_VERSION_INVALID", self.contract_version)
        cutoff = _utc(self.feature_input_cutoff_utc, "FEATURE_INPUT_CUTOFF_MISSING")
        if cutoff > self.authority.scheduled_start_utc:
            raise ExactGameContractError("FEATURE_INPUT_CUTOFF_POST_START")
        if not self.source_observations:
            raise ExactGameContractError("SOURCE_OBSERVATIONS_MISSING")
        if any(item.observed_at_utc > cutoff for item in self.source_observations):
            raise ExactGameContractError("POST_CUTOFF_SOURCE_OBSERVATION")
        if content_sha256(dict(self.feature_payload)) != self.feature_payload_sha256:
            raise ExactGameContractError("FEATURE_PAYLOAD_HASH_MISMATCH")
        _sha(self.row_sha256, "ROW_SHA256_INVALID")
        expected_row_sha = content_sha256(
            {
                "player_id": self.player_id,
                "authority": self.authority.to_dict(),
                "feature_input_cutoff_utc": cutoff.isoformat().replace("+00:00", "Z"),
                "source_observations": [item.to_dict() for item in self.source_observations],
                "feature_payload": dict(self.feature_payload),
                "feature_payload_sha256": self.feature_payload_sha256,
                "contract_version": self.contract_version,
            }
        )
        if expected_row_sha != self.row_sha256:
            raise ExactGameContractError("ROW_HASH_MISMATCH")

    @property
    def identity(self) -> tuple[int, int, str]:
        return self.player_id, self.authority.game_pk, self.contract_version

    @classmethod
    def create(
        cls,
        *,
        player_id: Any,
        authority: ExactGameAuthorityV1,
        feature_input_cutoff_utc: str | datetime,
        source_observations: Iterable[SourceObservationV1],
        feature_payload: Mapping[str, Any],
        contract_version: str = CONTRACT_VERSION,
    ) -> "ExactGameFeatureStateV1":
        exact_player_id = _positive_int(player_id, "PLAYER_ID_MISSING")
        cutoff = _utc(feature_input_cutoff_utc, "FEATURE_INPUT_CUTOFF_MISSING")
        if cutoff > authority.scheduled_start_utc:
            raise ExactGameContractError("FEATURE_INPUT_CUTOFF_POST_START")
        observations = tuple(source_observations)
        if not observations:
            raise ExactGameContractError("SOURCE_OBSERVATIONS_MISSING")
        if any(item.observed_at_utc > cutoff for item in observations):
            raise ExactGameContractError("POST_CUTOFF_SOURCE_OBSERVATION")
        if not isinstance(feature_payload, Mapping):
            raise ExactGameContractError("FEATURE_PAYLOAD_INVALID")
        payload = dict(feature_payload)
        payload_sha = content_sha256(payload)
        base = {
            "player_id": exact_player_id,
            "authority": authority.to_dict(),
            "feature_input_cutoff_utc": cutoff.isoformat().replace("+00:00", "Z"),
            "source_observations": [item.to_dict() for item in observations],
            "feature_payload": payload,
            "feature_payload_sha256": payload_sha,
            "contract_version": contract_version,
        }
        return cls(
            player_id=exact_player_id,
            authority=authority,
            feature_input_cutoff_utc=cutoff,
            source_observations=observations,
            feature_payload=payload,
            feature_payload_sha256=payload_sha,
            row_sha256=content_sha256(base),
            contract_version=contract_version,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "player_id": self.player_id,
            "authority": self.authority.to_dict(),
            "feature_input_cutoff_utc": self.feature_input_cutoff_utc.isoformat().replace("+00:00", "Z"),
            "source_observations": [item.to_dict() for item in self.source_observations],
            "feature_payload": dict(self.feature_payload),
            "feature_payload_sha256": self.feature_payload_sha256,
            "row_sha256": self.row_sha256,
            "contract_version": self.contract_version,
        }


def coalesce_exact_states(
    rows: Iterable[ExactGameFeatureStateV1],
) -> tuple[ExactGameFeatureStateV1, ...]:
    """Idempotently coalesce matches and reject payload/provenance conflicts."""
    indexed: dict[tuple[int, int, str], ExactGameFeatureStateV1] = {}
    for row in rows:
        prior = indexed.get(row.identity)
        if prior is None:
            indexed[row.identity] = row
        elif prior.row_sha256 != row.row_sha256:
            raise ExactGameContractError(
                "DUPLICATE_EXACT_IDENTITY_CONFLICTING_PAYLOAD",
                "/".join(str(value) for value in row.identity),
            )
    return tuple(indexed[key] for key in sorted(indexed))

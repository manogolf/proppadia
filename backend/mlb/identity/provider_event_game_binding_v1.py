"""Fail-closed provider-event to official MLB game identity binding.

This module is deliberately phase-neutral.  It proves an exact ``gamePk``
against a reproducible official schedule candidate set and the existing
canonical authority interface, then emits immutable provenance suitable for
market and prediction-time lineage.  It performs no network access.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


CONTRACT_NAME = "MLB_PROVIDER_EVENT_GAME_IDENTITY_BINDING_V1"
CONTRACT_VERSION = "provider_event_game_binding_v1"
ACCEPTED_STATUS = "EXACT_GAME_PK_AUTHORITY_CERTIFIED"
START_TOLERANCE_SECONDS = 600
REPO_ROOT = Path(__file__).resolve().parents[3]


class ProviderEventGameIdentityError(RuntimeError):
    """Typed failure that callers must treat as an excluded event."""

    def __init__(self, code: str, *, provider_event_id: str = "", detail: str = "") -> None:
        self.code = code
        self.provider_event_id = provider_event_id
        self.detail = detail
        message = ":".join(value for value in (code, provider_event_id, detail) if value)
        super().__init__(message)


@dataclass(frozen=True)
class EvidenceSource:
    path: str
    sha256: str


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    provider_event_id: str
    home_team: str
    away_team: str
    commence_time_raw: str | None
    game_number: int | None = None


@dataclass(frozen=True)
class OfficialGameCandidate:
    game_pk: int
    home_team: str
    away_team: str
    scheduled_start_utc: str
    game_number: int | None
    doubleheader_indicator: str | None
    source_game_type: str | None
    schedule_source: EvidenceSource
    schedule_observation_timestamp_utc: str | None = None


@dataclass(frozen=True)
class BindingReceipt:
    provider: str
    provider_event_id: str
    game_pk: int
    normalized_home_team: str
    normalized_away_team: str
    provider_commence_time_raw: str | None
    provider_commence_time_utc: str | None
    official_scheduled_start_utc: str
    official_game_number: int | None
    official_doubleheader_indicator: str | None
    official_schedule_source_path: str
    official_schedule_source_sha256: str
    official_schedule_observation_timestamp_utc: str | None
    provider_snapshot_path: str
    provider_snapshot_sha256: str
    resolver_contract_name: str
    resolver_contract_version: str
    binding_status: str
    observation_timestamp_utc: str
    authority_backend: str
    authority_manifest_sha256: str
    binding_identity_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _resolved_path(value: str, *, root: Path) -> Path:
    raw = Path(value)
    return raw.resolve() if raw.is_absolute() else (root / raw).resolve()


def verify_evidence_source(source: EvidenceSource, *, root: Path = REPO_ROOT) -> Path:
    if not source.path.strip():
        raise ProviderEventGameIdentityError("EVIDENCE_PATH_MISSING")
    if len(source.sha256) != 64 or any(ch not in "0123456789abcdef" for ch in source.sha256):
        raise ProviderEventGameIdentityError("EVIDENCE_SHA256_INVALID", detail=source.path)
    path = _resolved_path(source.path, root=root)
    if not path.is_file():
        raise ProviderEventGameIdentityError("EVIDENCE_FILE_MISSING", detail=source.path)
    actual = _sha256(path)
    if actual != source.sha256:
        raise ProviderEventGameIdentityError(
            "EVIDENCE_HASH_MISMATCH", detail=f"{source.path};expected={source.sha256};actual={actual}"
        )
    return path


def _parse_aware_utc(value: str | None) -> datetime | None:
    if value is None or not str(value).strip():
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value is not None else None


def _normalize_required(value: Any, code: str, event_id: str = "") -> str:
    normalized = " ".join(str(value or "").strip().split())
    if not normalized:
        raise ProviderEventGameIdentityError(code, provider_event_id=event_id)
    return normalized


def _candidate_identity(candidate: OfficialGameCandidate) -> tuple[Any, ...]:
    return (
        int(candidate.game_pk), candidate.home_team, candidate.away_team,
        candidate.scheduled_start_utc, candidate.game_number,
        candidate.doubleheader_indicator, candidate.source_game_type,
        candidate.schedule_source.path, candidate.schedule_source.sha256,
        candidate.schedule_observation_timestamp_utc,
    )


class BindingRegistry:
    """Detect contradictory reuse while allowing identical duplicate ingestion."""

    def __init__(self, receipts: Iterable[BindingReceipt | Mapping[str, Any]] = ()) -> None:
        self._by_event: dict[tuple[str, str], int] = {}
        for receipt in receipts:
            row = receipt.to_dict() if isinstance(receipt, BindingReceipt) else dict(receipt)
            self.register_values(
                str(row.get("provider") or ""),
                str(row.get("provider_event_id") or ""),
                int(row["game_pk"]),
            )

    def register_values(self, provider: str, provider_event_id: str, game_pk: int) -> None:
        key = (provider, provider_event_id)
        prior = self._by_event.get(key)
        if prior is not None and prior != game_pk:
            raise ProviderEventGameIdentityError(
                "PROVIDER_EVENT_REUSE_CONFLICT",
                provider_event_id=provider_event_id,
                detail=f"prior={prior};incoming={game_pk}",
            )
        self._by_event[key] = game_pk

    def register(self, receipt: BindingReceipt) -> None:
        self.register_values(receipt.provider, receipt.provider_event_id, receipt.game_pk)


def load_receipt_registry(root: Path) -> BindingRegistry:
    rows: list[dict[str, Any]] = []
    if root.is_dir():
        for path in sorted(root.rglob("*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    rows.append(json.loads(line))
    return BindingRegistry(rows)


def resolve_binding(
    *,
    event: ProviderEvent,
    candidates: Iterable[OfficialGameCandidate],
    provider_snapshot: EvidenceSource,
    authority: Any,
    observation_timestamp_utc: str,
    registry: BindingRegistry | None = None,
    root: Path = REPO_ROOT,
) -> BindingReceipt:
    """Resolve exactly one official candidate or raise a typed fail-closed error."""

    provider = _normalize_required(event.provider, "PROVIDER_MISSING").upper()
    event_id = _normalize_required(event.provider_event_id, "PROVIDER_EVENT_ID_MISSING")
    home = _normalize_required(event.home_team, "HOME_TEAM_UNRESOLVED", event_id)
    away = _normalize_required(event.away_team, "AWAY_TEAM_UNRESOLVED", event_id)
    if home == away:
        raise ProviderEventGameIdentityError("TEAM_IDENTITY_CONFLICT", provider_event_id=event_id)
    observation = _parse_aware_utc(observation_timestamp_utc)
    if observation is None:
        raise ProviderEventGameIdentityError("OBSERVATION_TIMESTAMP_INVALID", provider_event_id=event_id)
    verify_evidence_source(provider_snapshot, root=root)

    unique: dict[int, OfficialGameCandidate] = {}
    for candidate in candidates:
        if candidate.home_team != home or candidate.away_team != away:
            continue
        prior = unique.get(int(candidate.game_pk))
        if prior is not None and _candidate_identity(prior) != _candidate_identity(candidate):
            raise ProviderEventGameIdentityError(
                "OFFICIAL_CANDIDATE_IDENTITY_CONFLICT", provider_event_id=event_id,
                detail=str(candidate.game_pk),
            )
        unique[int(candidate.game_pk)] = candidate
    remaining = list(unique.values())
    if not remaining:
        raise ProviderEventGameIdentityError("OFFICIAL_CANDIDATE_ZERO", provider_event_id=event_id)

    commence = _parse_aware_utc(event.commence_time_raw)
    if event.commence_time_raw and commence is None and len(remaining) > 1:
        raise ProviderEventGameIdentityError("COMMENCE_TIME_INVALID_AMBIGUOUS", provider_event_id=event_id)
    if commence is not None:
        timed: list[OfficialGameCandidate] = []
        for candidate in remaining:
            official_start = _parse_aware_utc(candidate.scheduled_start_utc)
            if official_start is None:
                raise ProviderEventGameIdentityError(
                    "OFFICIAL_START_INVALID", provider_event_id=event_id, detail=str(candidate.game_pk)
                )
            if abs((official_start - commence).total_seconds()) <= START_TOLERANCE_SECONDS:
                timed.append(candidate)
        remaining = timed

    if event.game_number is not None:
        try:
            provider_number = int(event.game_number)
        except (TypeError, ValueError):
            raise ProviderEventGameIdentityError("PROVIDER_GAME_NUMBER_INVALID", provider_event_id=event_id) from None
        remaining = [candidate for candidate in remaining if candidate.game_number == provider_number]

    if not remaining:
        raise ProviderEventGameIdentityError("OFFICIAL_CANDIDATE_ZERO_AFTER_EVIDENCE", provider_event_id=event_id)
    if len(remaining) != 1:
        raise ProviderEventGameIdentityError(
            "OFFICIAL_CANDIDATE_AMBIGUOUS", provider_event_id=event_id,
            detail="gamePks=" + ",".join(str(item.game_pk) for item in sorted(remaining, key=lambda row: row.game_pk)),
        )
    selected = remaining[0]
    verify_evidence_source(selected.schedule_source, root=root)

    try:
        authority_record = authority.lookup_exact(selected.game_pk)
    except Exception as exc:
        raise ProviderEventGameIdentityError(
            "CANONICAL_AUTHORITY_REJECTED", provider_event_id=event_id,
            detail=f"gamePk={selected.game_pk};{type(exc).__name__}",
        ) from exc
    authority_type = str(getattr(authority_record, "source_game_type", "") or "")
    authority_game_pk = int(getattr(authority_record, "game_pk", selected.game_pk))
    if authority_game_pk != int(selected.game_pk):
        raise ProviderEventGameIdentityError(
            "CANONICAL_AUTHORITY_IDENTITY_CONFLICT", provider_event_id=event_id,
            detail=f"schedule={selected.game_pk};authority={authority_game_pk}",
        )
    if selected.source_game_type and authority_type != selected.source_game_type:
        raise ProviderEventGameIdentityError(
            "OFFICIAL_GAME_TYPE_CONFLICT", provider_event_id=event_id,
            detail=f"schedule={selected.source_game_type};authority={authority_type}",
        )
    metadata = getattr(authority, "metadata", None)
    metadata_dict = metadata.to_dict() if metadata is not None and hasattr(metadata, "to_dict") else {}
    schedule_observation = _parse_aware_utc(selected.schedule_observation_timestamp_utc)
    if selected.schedule_observation_timestamp_utc and schedule_observation is None:
        raise ProviderEventGameIdentityError(
            "OFFICIAL_SCHEDULE_OBSERVATION_TIMESTAMP_INVALID", provider_event_id=event_id
        )
    semantic = {
        "provider": provider,
        "provider_event_id": event_id,
        "game_pk": int(selected.game_pk),
        "normalized_home_team": home,
        "normalized_away_team": away,
        "provider_commence_time_raw": event.commence_time_raw,
        "provider_commence_time_utc": _iso_utc(commence),
        "official_scheduled_start_utc": _iso_utc(_parse_aware_utc(selected.scheduled_start_utc)),
        "official_game_number": selected.game_number,
        "official_doubleheader_indicator": selected.doubleheader_indicator,
        "official_schedule_source_path": selected.schedule_source.path,
        "official_schedule_source_sha256": selected.schedule_source.sha256,
        "official_schedule_observation_timestamp_utc": _iso_utc(schedule_observation),
        "provider_snapshot_path": provider_snapshot.path,
        "provider_snapshot_sha256": provider_snapshot.sha256,
        "resolver_contract_name": CONTRACT_NAME,
        "resolver_contract_version": CONTRACT_VERSION,
        "binding_status": ACCEPTED_STATUS,
        "observation_timestamp_utc": _iso_utc(observation),
        "authority_backend": str(metadata_dict.get("backend") or type(authority).__name__),
        "authority_manifest_sha256": str(
            metadata_dict.get("authority_records_sha256")
            or metadata_dict.get("proposal_sha256")
            or "UNAVAILABLE"
        ),
    }
    receipt = BindingReceipt(
        **semantic,
        binding_identity_sha256=hashlib.sha256(_canonical_json(semantic).encode("utf-8")).hexdigest(),
    )
    (registry or BindingRegistry()).register(receipt)
    return receipt


def write_receipts_immutable(path: Path, receipts: Iterable[BindingReceipt]) -> str:
    """Atomically create deterministic JSONL; identical replays are idempotent."""

    rows = sorted(
        (receipt.to_dict() for receipt in receipts),
        key=lambda row: (row["provider"], row["provider_event_id"], row["game_pk"]),
    )
    identities = [row["binding_identity_sha256"] for row in rows]
    if len(identities) != len(set(identities)):
        rows = [dict(row) for row in {row["binding_identity_sha256"]: row for row in rows}.values()]
        rows.sort(key=lambda row: (row["provider"], row["provider_event_id"], row["game_pk"]))
    body = "".join(_canonical_json(row) + "\n" for row in rows).encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != body:
            raise ProviderEventGameIdentityError("IMMUTABLE_RECEIPT_CONFLICT", detail=str(path))
        return digest
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, path)
        except FileExistsError:
            if path.read_bytes() != body:
                raise ProviderEventGameIdentityError("IMMUTABLE_RECEIPT_CONFLICT", detail=str(path))
    finally:
        temp_path.unlink(missing_ok=True)
    return digest

"""Fail-closed exact-gamePk phase gating for the MLB agreement study.

The study's immutable prediction, risk, price, outcome, claim, and provenance
ledgers are observation history.  Phase is joined transiently from the shared
canonical authority and is never reconstructed from a date or persisted as an
independently maintained ledger column.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from functools import lru_cache
from numbers import Integral, Real
from typing import Any, Iterable, Mapping, Sequence

from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
)


REGULAR_SEASON = "REGULAR_SEASON"
POSTSEASON = "POSTSEASON"
PRESEASON = "PRESEASON"
EVALUATION_PHASES = frozenset({REGULAR_SEASON, POSTSEASON})


class AgreementPhaseGateError(RuntimeError):
    """A study row cannot safely enter a governed phase cohort."""

    def __init__(self, code: str, *, game_pk: int | None = None, detail: str = "") -> None:
        self.code = code
        self.game_pk = game_pk
        self.detail = detail
        parts = [code]
        if game_pk is not None:
            parts.append(str(game_pk))
        if detail:
            parts.append(detail)
        super().__init__(":".join(parts))


@dataclass(frozen=True)
class AgreementPhaseDecision:
    game_pk: int
    source_game_type: str | None
    normalized_phase: str | None
    postseason_round: str | None
    evaluation_partition: str
    decision_code: str
    authority_proposal_sha256: str
    authority_records_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AgreementPhasePartitions:
    regular_season: tuple[Mapping[str, Any], ...]
    postseason: tuple[Mapping[str, Any], ...]
    excluded_preseason: tuple[Mapping[str, Any], ...]
    excluded_special: tuple[Mapping[str, Any], ...]
    decisions: tuple[AgreementPhaseDecision, ...]

    def selected(self, evaluation_phase: str) -> tuple[Mapping[str, Any], ...]:
        require_evaluation_phase(evaluation_phase)
        return self.regular_season if evaluation_phase == REGULAR_SEASON else self.postseason

    def counts(self) -> dict[str, int]:
        return {
            REGULAR_SEASON: len(self.regular_season),
            POSTSEASON: len(self.postseason),
            "EXCLUDED_PRESEASON": len(self.excluded_preseason),
            "EXCLUDED_SPECIAL": len(self.excluded_special),
        }


@lru_cache(maxsize=1)
def verified_agreement_phase_authority() -> HashedProposalAuthority:
    return HashedProposalAuthority()


def require_evaluation_phase(value: str) -> str:
    if value not in EVALUATION_PHASES:
        raise AgreementPhaseGateError("AGREEMENT_EVALUATION_PHASE_INVALID", detail=str(value))
    return value


def exact_game_pk(row: Mapping[str, Any]) -> int:
    values: list[int] = []
    for field in ("game_id", "game_pk", "gamePk"):
        value = row.get(field)
        if value in (None, ""):
            continue
        if isinstance(value, bool):
            raise AgreementPhaseGateError("AGREEMENT_GAME_PK_MISSING")
        if isinstance(value, Integral):
            exact = int(value)
        elif isinstance(value, Real) and math.isfinite(float(value)) and float(value).is_integer():
            exact = int(value)
        elif isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
            exact = int(value)
        else:
            raise AgreementPhaseGateError("AGREEMENT_GAME_PK_MISSING")
        values.append(exact)
    if not values or values[0] <= 0:
        raise AgreementPhaseGateError("AGREEMENT_GAME_PK_MISSING")
    if len(set(values)) != 1:
        raise AgreementPhaseGateError(
            "AGREEMENT_EXACT_GAME_PK_MISMATCH", detail=",".join(str(value) for value in values)
        )
    return values[0]


def _game_date(row: Mapping[str, Any], game_pk: int) -> str:
    values = {
        str(row[field]) for field in ("game_date", "slate_date")
        if row.get(field) not in (None, "")
    }
    if len(values) != 1:
        raise AgreementPhaseGateError(
            "AGREEMENT_AUTHORITY_FRESHNESS_UNPROVABLE", game_pk=game_pk
        )
    return next(iter(values))


def _source_type(row: Mapping[str, Any]) -> str | None:
    present = {
        str(row[field]) for field in ("source_game_type", "game_type", "gameType")
        if row.get(field) not in (None, "")
    }
    if len(present) > 1:
        raise AgreementPhaseGateError(
            "AGREEMENT_CONFLICTING_SOURCE_TYPES", detail=",".join(sorted(present))
        )
    return next(iter(present), None)


def _assert_authority_health(authority: CanonicalGamePhaseAuthority) -> None:
    metadata = authority.metadata
    violations = {
        "missing": metadata.missing_count,
        "unknown": metadata.unknown_count,
        "conflicting": metadata.conflicting_count,
        "duplicate": metadata.duplicate_identity_count,
    }
    if any(violations.values()):
        raise AgreementPhaseGateError(
            "AGREEMENT_PHASE_AUTHORITY_POPULATION_INVALID",
            detail=json.dumps(violations, sort_keys=True, separators=(",", ":")),
        )


def _decision(
    row: Mapping[str, Any],
    record: GamePhaseAuthorityRecord,
    authority: CanonicalGamePhaseAuthority,
) -> AgreementPhaseDecision:
    source_type = _source_type(row)
    if source_type is not None and source_type != record.source_game_type:
        raise AgreementPhaseGateError(
            "AGREEMENT_SOURCE_TYPE_CONFLICT", game_pk=record.game_pk,
            detail=f"row={source_type};authority={record.source_game_type}",
        )
    if record.season_phase == REGULAR_SEASON:
        partition, code = REGULAR_SEASON, "ADMITTED_REGULAR_SEASON"
    elif record.season_phase == POSTSEASON:
        partition, code = POSTSEASON, "ADMITTED_POSTSEASON_SHADOW"
    elif record.season_phase == PRESEASON:
        partition, code = "EXCLUDED_PRESEASON", "EXCLUDED_PRESEASON"
    elif record.season_phase is None:
        partition, code = "EXCLUDED_SPECIAL", "EXCLUDED_SPECIAL_AUTHORITATIVE_TYPE"
    else:
        raise AgreementPhaseGateError(
            "AGREEMENT_NORMALIZED_PHASE_UNKNOWN", game_pk=record.game_pk,
            detail=str(record.season_phase),
        )
    metadata = authority.metadata
    return AgreementPhaseDecision(
        game_pk=record.game_pk,
        source_game_type=record.source_game_type,
        normalized_phase=record.season_phase,
        postseason_round=record.postseason_round,
        evaluation_partition=partition,
        decision_code=code,
        authority_proposal_sha256=metadata.proposal_sha256,
        authority_records_sha256=metadata.authority_records_sha256,
    )


def classify_agreement_row(
    row: Mapping[str, Any], *, authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
) -> AgreementPhaseDecision:
    authority = authority or verified_agreement_phase_authority()
    _assert_authority_health(authority)
    game_pk = exact_game_pk(row)
    if require_freshness:
        game_date = _game_date(row, game_pk)
        try:
            authority.require_supported_window(game_date, game_date)
        except GamePhaseAuthorityError as exc:
            raise AgreementPhaseGateError(
                "AGREEMENT_PHASE_AUTHORITY_STALE", game_pk=game_pk, detail=exc.code
            ) from exc
    try:
        record = authority.lookup_exact(game_pk)
    except GamePhaseAuthorityError as exc:
        if exc.code == "GAME_PHASE_SPECIAL_EXCLUDED":
            metadata = authority.metadata
            return AgreementPhaseDecision(
                game_pk=game_pk, source_game_type=None, normalized_phase=None,
                postseason_round=None, evaluation_partition="EXCLUDED_SPECIAL",
                decision_code="EXCLUDED_SPECIAL_AUTHORITATIVE_TYPE",
                authority_proposal_sha256=metadata.proposal_sha256,
                authority_records_sha256=metadata.authority_records_sha256,
            )
        raise AgreementPhaseGateError(
            "AGREEMENT_PHASE_AUTHORITY_INVALID", game_pk=game_pk, detail=exc.code
        ) from exc
    return _decision(row, record, authority)


def partition_agreement_rows(
    rows: Iterable[Mapping[str, Any]], *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
    unique_identity_fields: Sequence[str] | None = None,
) -> AgreementPhasePartitions:
    authority = authority or verified_agreement_phase_authority()
    buckets: dict[str, list[Mapping[str, Any]]] = {
        REGULAR_SEASON: [], POSTSEASON: [],
        "EXCLUDED_PRESEASON": [], "EXCLUDED_SPECIAL": [],
    }
    decisions: list[AgreementPhaseDecision] = []
    seen: set[tuple[Any, ...]] = set()
    for row in tuple(rows):
        if unique_identity_fields:
            identity = tuple(row.get(field) for field in unique_identity_fields)
            if any(value in (None, "") for value in identity):
                raise AgreementPhaseGateError("AGREEMENT_EVALUATION_IDENTITY_INCOMPLETE")
            if identity in seen:
                raise AgreementPhaseGateError(
                    "AGREEMENT_DUPLICATE_EVALUATION_IDENTITY", game_pk=exact_game_pk(row)
                )
            seen.add(identity)
        decision = classify_agreement_row(
            row, authority=authority, require_freshness=require_freshness
        )
        decisions.append(decision)
        buckets[decision.evaluation_partition].append(row)
    return AgreementPhasePartitions(
        regular_season=tuple(buckets[REGULAR_SEASON]),
        postseason=tuple(buckets[POSTSEASON]),
        excluded_preseason=tuple(buckets["EXCLUDED_PRESEASON"]),
        excluded_special=tuple(buckets["EXCLUDED_SPECIAL"]),
        decisions=tuple(decisions),
    )


def require_snapshot_membership(
    rows: Iterable[Mapping[str, Any]], evaluation_phase: str, *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
) -> tuple[AgreementPhaseDecision, ...]:
    """Require every prospective row to belong to one explicit study phase."""

    phase = require_evaluation_phase(evaluation_phase)
    materialized = tuple(rows)
    partitions = partition_agreement_rows(
        materialized, authority=authority, require_freshness=require_freshness,
        unique_identity_fields=("game_id",),
    )
    selected = partitions.selected(phase)
    if len(selected) != len(materialized):
        rejected = sorted(
            (decision.game_pk, decision.evaluation_partition)
            for decision in partitions.decisions if decision.evaluation_partition != phase
        )
        raise AgreementPhaseGateError(
            "AGREEMENT_CAPTURE_PHASE_MEMBERSHIP_INVALID",
            detail=json.dumps(rejected, separators=(",", ":")),
        )
    return partitions.decisions

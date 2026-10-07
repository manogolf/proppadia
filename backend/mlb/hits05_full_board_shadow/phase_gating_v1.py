"""Exact-gamePk phase gating for the MLB Full-board Hits shadow lane.

Phase is joined from the stable canonical authority and is never persisted on
the append-only Full-board ledgers.  Dates are used only to prove that the
authority covers the requested observation window; they never classify a game.
"""

from __future__ import annotations

import hashlib
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
from backend.mlb.season_transition.runtime_schedule_authority_v1 import RuntimeScheduleAuthority


REGULAR_SEASON = "REGULAR_SEASON"
POSTSEASON = "POSTSEASON"
PRESEASON = "PRESEASON"
EVALUATION_PHASES = frozenset({REGULAR_SEASON, POSTSEASON})


class FullBoardHitsPhaseGateError(RuntimeError):
    """A Full-board Hits row cannot safely enter a governed phase cohort."""

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
class FullBoardHitsPhaseDecision:
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
class FullBoardHitsPhasePartitions:
    regular_season: tuple[Mapping[str, Any], ...]
    postseason: tuple[Mapping[str, Any], ...]
    excluded_preseason: tuple[Mapping[str, Any], ...]
    excluded_special: tuple[Mapping[str, Any], ...]
    decisions: tuple[FullBoardHitsPhaseDecision, ...]

    @property
    def admitted(self) -> tuple[Mapping[str, Any], ...]:
        return self.regular_season + self.postseason

    def counts(self) -> dict[str, int]:
        return {
            REGULAR_SEASON: len(self.regular_season),
            POSTSEASON: len(self.postseason),
            "EXCLUDED_PRESEASON": len(self.excluded_preseason),
            "EXCLUDED_SPECIAL": len(self.excluded_special),
        }


@lru_cache(maxsize=1)
def verified_full_board_hits_phase_authority() -> HashedProposalAuthority:
    """Load and verify the source-hashed authority once per process."""

    return HashedProposalAuthority()


def _game_pk(row: Mapping[str, Any]) -> int:
    values: list[int] = []
    for field in ("game_id", "game_pk", "gamePk"):
        value = row.get(field)
        if value not in (None, ""):
            if value is None or isinstance(value, bool):
                raise FullBoardHitsPhaseGateError("FULL_BOARD_HITS_GAME_PK_MISSING") from None
            if isinstance(value, Integral):
                exact = int(value)
            elif isinstance(value, Real):
                if not math.isfinite(float(value)) or not float(value).is_integer():
                    raise FullBoardHitsPhaseGateError("FULL_BOARD_HITS_GAME_PK_MISSING")
                exact = int(value)
            elif isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
                exact = int(value)
            else:
                raise FullBoardHitsPhaseGateError("FULL_BOARD_HITS_GAME_PK_MISSING")
            values.append(exact)
    if not values or values[0] <= 0:
        raise FullBoardHitsPhaseGateError("FULL_BOARD_HITS_GAME_PK_MISSING")
    if len(set(values)) != 1:
        raise FullBoardHitsPhaseGateError(
            "FULL_BOARD_HITS_EXACT_GAME_PK_MISMATCH",
            detail=",".join(str(value) for value in values),
        )
    return values[0]


def _game_date(row: Mapping[str, Any], game_pk: int) -> str:
    values = {
        str(row[field])
        for field in ("game_date", "slate_date")
        if row.get(field) not in (None, "")
    }
    if len(values) != 1:
        raise FullBoardHitsPhaseGateError(
            "FULL_BOARD_HITS_AUTHORITY_FRESHNESS_UNPROVABLE", game_pk=game_pk
        )
    return next(iter(values))


def _consumer_source_type(row: Mapping[str, Any]) -> str | None:
    present = {
        str(row[field])
        for field in ("source_game_type", "game_type", "gameType")
        if row.get(field) not in (None, "")
    }
    if len(present) > 1:
        raise FullBoardHitsPhaseGateError(
            "FULL_BOARD_HITS_CONFLICTING_SOURCE_TYPES",
            detail=",".join(sorted(present)),
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
        raise FullBoardHitsPhaseGateError(
            "FULL_BOARD_HITS_PHASE_AUTHORITY_POPULATION_INVALID",
            detail=json.dumps(violations, sort_keys=True, separators=(",", ":")),
        )


def _decision_from_record(
    row: Mapping[str, Any],
    record: GamePhaseAuthorityRecord,
    authority: CanonicalGamePhaseAuthority,
) -> FullBoardHitsPhaseDecision:
    source_type = _consumer_source_type(row)
    if source_type is not None and source_type != record.source_game_type:
        raise FullBoardHitsPhaseGateError(
            "FULL_BOARD_HITS_SOURCE_TYPE_CONFLICT",
            game_pk=record.game_pk,
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
        raise FullBoardHitsPhaseGateError(
            "FULL_BOARD_HITS_NORMALIZED_PHASE_UNKNOWN",
            game_pk=record.game_pk,
            detail=str(record.season_phase),
        )
    metadata = authority.metadata
    return FullBoardHitsPhaseDecision(
        game_pk=record.game_pk,
        source_game_type=record.source_game_type,
        normalized_phase=record.season_phase,
        postseason_round=record.postseason_round,
        evaluation_partition=partition,
        decision_code=code,
        authority_proposal_sha256=metadata.proposal_sha256,
        authority_records_sha256=metadata.authority_records_sha256,
    )


def classify_full_board_hits_row(
    row: Mapping[str, Any],
    *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
) -> FullBoardHitsPhaseDecision:
    """Classify one immutable row without deriving phase from its date."""

    authority = authority or verified_full_board_hits_phase_authority()
    _assert_authority_health(authority)
    game_pk = _game_pk(row)
    if require_freshness:
        game_date = _game_date(row, game_pk)
        try:
            if isinstance(authority, RuntimeScheduleAuthority):
                authority.require_exact_game_date(game_pk, game_date)
            else:
                authority.require_supported_window(game_date, game_date)
        except GamePhaseAuthorityError as exc:
            raise FullBoardHitsPhaseGateError(
                "FULL_BOARD_HITS_PHASE_AUTHORITY_STALE",
                game_pk=game_pk,
                detail=exc.code,
            ) from exc
    try:
        record = authority.lookup_exact(game_pk)
    except GamePhaseAuthorityError as exc:
        if exc.code == "GAME_PHASE_SPECIAL_EXCLUDED":
            metadata = authority.metadata
            return FullBoardHitsPhaseDecision(
                game_pk=game_pk,
                source_game_type=None,
                normalized_phase=None,
                postseason_round=None,
                evaluation_partition="EXCLUDED_SPECIAL",
                decision_code="EXCLUDED_SPECIAL_AUTHORITATIVE_TYPE",
                authority_proposal_sha256=metadata.proposal_sha256,
                authority_records_sha256=metadata.authority_records_sha256,
            )
        raise FullBoardHitsPhaseGateError(
            "FULL_BOARD_HITS_PHASE_AUTHORITY_INVALID",
            game_pk=game_pk,
            detail=exc.code,
        ) from exc
    return _decision_from_record(row, record, authority)


def partition_full_board_hits_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
    unique_identity_fields: Sequence[str] | None = None,
) -> FullBoardHitsPhasePartitions:
    """Return disjoint phase cohorts, or fail before returning any rows."""

    authority = authority or verified_full_board_hits_phase_authority()
    materialized = tuple(rows)
    buckets: dict[str, list[Mapping[str, Any]]] = {
        REGULAR_SEASON: [],
        POSTSEASON: [],
        "EXCLUDED_PRESEASON": [],
        "EXCLUDED_SPECIAL": [],
    }
    decisions: list[FullBoardHitsPhaseDecision] = []
    seen: set[tuple[Any, ...]] = set()
    for row in materialized:
        if unique_identity_fields:
            identity = tuple(row.get(field) for field in unique_identity_fields)
            if any(value in (None, "") for value in identity):
                raise FullBoardHitsPhaseGateError(
                    "FULL_BOARD_HITS_EVALUATION_IDENTITY_INCOMPLETE"
                )
            if identity in seen:
                raise FullBoardHitsPhaseGateError(
                    "FULL_BOARD_HITS_DUPLICATE_EVALUATION_IDENTITY",
                    game_pk=_game_pk(row),
                )
            seen.add(identity)
        decision = classify_full_board_hits_row(
            row, authority=authority, require_freshness=require_freshness
        )
        decisions.append(decision)
        buckets[decision.evaluation_partition].append(row)
    return FullBoardHitsPhasePartitions(
        regular_season=tuple(buckets[REGULAR_SEASON]),
        postseason=tuple(buckets[POSTSEASON]),
        excluded_preseason=tuple(buckets["EXCLUDED_PRESEASON"]),
        excluded_special=tuple(buckets["EXCLUDED_SPECIAL"]),
        decisions=tuple(decisions),
    )


def canonical_rows_sha256(rows: Iterable[Mapping[str, Any]]) -> str:
    """Hash row content without mutating or normalizing any retained value."""

    encoded = json.dumps(
        list(rows), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

"""Fail-closed exact-gamePk phase gating shared by RAW Totals and Totals C.

Phase labels are transient.  Immutable Totals ledgers remain unchanged and are
joined to the source-hashed canonical authority only when scoring, attaching,
grading, or reporting.  Dates prove authority coverage; they never classify a
game.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from functools import lru_cache
from numbers import Integral, Real
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT
from backend.mlb.season_transition.runtime_schedule_authority_v1 import RuntimeScheduleAuthority


REGULAR_SEASON = "REGULAR_SEASON"
POSTSEASON = "POSTSEASON"
PRESEASON = "PRESEASON"
EVALUATION_PHASES = frozenset({REGULAR_SEASON, POSTSEASON})


class TotalsPhaseGateError(RuntimeError):
    """A Totals row cannot safely enter a governed phase cohort."""

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
class TotalsPhaseDecision:
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
class TotalsPhasePartitions:
    regular_season: tuple[Mapping[str, Any], ...]
    postseason: tuple[Mapping[str, Any], ...]
    excluded_preseason: tuple[Mapping[str, Any], ...]
    excluded_special: tuple[Mapping[str, Any], ...]
    decisions: tuple[TotalsPhaseDecision, ...]

    @property
    def admitted(self) -> tuple[Mapping[str, Any], ...]:
        return self.regular_season + self.postseason

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
def verified_totals_phase_authority() -> HashedProposalAuthority:
    return HashedProposalAuthority()


def authority_with_retained_schedule_rows(
    rows: Iterable[Mapping[str, Any]], *,
    base: CanonicalGamePhaseAuthority | None = None,
) -> CanonicalGamePhaseAuthority:
    """Rehydrate each prediction's exact, hash-bound schedule overlay for grading."""
    authority = base or verified_totals_phase_authority()
    sources: dict[str, str] = {}
    for row in rows:
        path, digest = row.get("schedule_source_path"), row.get("schedule_source_sha256")
        if path in (None, "") and digest in (None, ""):
            continue
        if not path and digest:
            # Legacy rows can rely on the pinned authority only when it already
            # contains their exact game identity; stale identities still fail
            # later at classification rather than gaining date-only coverage.
            try:
                authority.lookup_exact(exact_game_pk(row))
            except GamePhaseAuthorityError as exc:
                raise TotalsPhaseGateError(
                    "TOTALS_RETAINED_SCHEDULE_BINDING_INCOMPLETE",
                    game_pk=exact_game_pk(row), detail=exc.code,
                ) from exc
            continue
        if not path or not digest:
            raise TotalsPhaseGateError(
                "TOTALS_RETAINED_SCHEDULE_BINDING_INCOMPLETE", game_pk=exact_game_pk(row)
            )
        prior = sources.get(str(path))
        if prior is not None and prior != str(digest):
            raise TotalsPhaseGateError(
                "TOTALS_RETAINED_SCHEDULE_HASH_CONFLICT", game_pk=exact_game_pk(row)
            )
        sources[str(path)] = str(digest)
    for source_path, digest in sorted(sources.items()):
        path = Path(source_path)
        if not path.is_absolute():
            path = REPO_ROOT / path
        try:
            authority = RuntimeScheduleAuthority(
                source_path=path, expected_source_sha256=digest, base=authority
            )
        except GamePhaseAuthorityError as exc:
            raise TotalsPhaseGateError(
                "TOTALS_RETAINED_SCHEDULE_INVALID", detail=exc.code
            ) from exc
    return authority


def require_evaluation_phase(value: str) -> str:
    if value not in EVALUATION_PHASES:
        raise TotalsPhaseGateError("TOTALS_EVALUATION_PHASE_INVALID", detail=str(value))
    return value


def exact_game_pk(row: Mapping[str, Any]) -> int:
    values: list[int] = []
    for field in ("game_id", "game_pk", "gamePk"):
        value = row.get(field)
        if value in (None, ""):
            continue
        if isinstance(value, bool):
            raise TotalsPhaseGateError("TOTALS_GAME_PK_MISSING")
        if isinstance(value, Integral):
            exact = int(value)
        elif isinstance(value, Real) and math.isfinite(float(value)) and float(value).is_integer():
            exact = int(value)
        elif isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
            exact = int(value)
        else:
            raise TotalsPhaseGateError("TOTALS_GAME_PK_MISSING")
        values.append(exact)
    if not values or values[0] <= 0:
        raise TotalsPhaseGateError("TOTALS_GAME_PK_MISSING")
    if len(set(values)) != 1:
        raise TotalsPhaseGateError(
            "TOTALS_EXACT_GAME_PK_MISMATCH", detail=",".join(str(value) for value in values)
        )
    return values[0]


def _game_date(row: Mapping[str, Any], game_pk: int) -> str:
    values = {
        str(row[field]) for field in ("game_date", "slate_date")
        if row.get(field) not in (None, "")
    }
    if len(values) != 1:
        raise TotalsPhaseGateError("TOTALS_AUTHORITY_FRESHNESS_UNPROVABLE", game_pk=game_pk)
    return next(iter(values))


def _source_type(row: Mapping[str, Any]) -> str | None:
    present = {
        str(row[field]) for field in ("source_game_type", "game_type", "gameType")
        if row.get(field) not in (None, "")
    }
    if len(present) > 1:
        raise TotalsPhaseGateError("TOTALS_CONFLICTING_SOURCE_TYPES", detail=",".join(sorted(present)))
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
        raise TotalsPhaseGateError(
            "TOTALS_PHASE_AUTHORITY_POPULATION_INVALID",
            detail=json.dumps(violations, sort_keys=True, separators=(",", ":")),
        )


def _decision(
    row: Mapping[str, Any],
    record: GamePhaseAuthorityRecord,
    authority: CanonicalGamePhaseAuthority,
) -> TotalsPhaseDecision:
    source_type = _source_type(row)
    if source_type is not None and source_type != record.source_game_type:
        raise TotalsPhaseGateError(
            "TOTALS_SOURCE_TYPE_CONFLICT", game_pk=record.game_pk,
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
        raise TotalsPhaseGateError(
            "TOTALS_NORMALIZED_PHASE_UNKNOWN", game_pk=record.game_pk,
            detail=str(record.season_phase),
        )
    metadata = authority.metadata
    return TotalsPhaseDecision(
        game_pk=record.game_pk,
        source_game_type=record.source_game_type,
        normalized_phase=record.season_phase,
        postseason_round=record.postseason_round,
        evaluation_partition=partition,
        decision_code=code,
        authority_proposal_sha256=metadata.proposal_sha256,
        authority_records_sha256=metadata.authority_records_sha256,
    )


def classify_totals_row(
    row: Mapping[str, Any], *, authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
) -> TotalsPhaseDecision:
    authority = authority or verified_totals_phase_authority()
    _assert_authority_health(authority)
    game_pk = exact_game_pk(row)
    if require_freshness:
        game_date = _game_date(row, game_pk)
        try:
            authority.require_supported_window(game_date, game_date)
        except GamePhaseAuthorityError as exc:
            exact_date_check = getattr(authority, "require_exact_game_date", None)
            if exact_date_check is not None:
                try:
                    exact_date_check(game_pk, game_date)
                except GamePhaseAuthorityError:
                    raise TotalsPhaseGateError(
                        "TOTALS_PHASE_AUTHORITY_STALE", game_pk=game_pk,
                        detail=exc.code,
                    ) from exc
            else:
                raise TotalsPhaseGateError(
                    "TOTALS_PHASE_AUTHORITY_STALE", game_pk=game_pk, detail=exc.code
                ) from exc
    try:
        record = authority.lookup_exact(game_pk)
    except GamePhaseAuthorityError as exc:
        if exc.code == "GAME_PHASE_SPECIAL_EXCLUDED":
            metadata = authority.metadata
            return TotalsPhaseDecision(
                game_pk=game_pk, source_game_type=None, normalized_phase=None,
                postseason_round=None, evaluation_partition="EXCLUDED_SPECIAL",
                decision_code="EXCLUDED_SPECIAL_AUTHORITATIVE_TYPE",
                authority_proposal_sha256=metadata.proposal_sha256,
                authority_records_sha256=metadata.authority_records_sha256,
            )
        raise TotalsPhaseGateError(
            "TOTALS_PHASE_AUTHORITY_INVALID", game_pk=game_pk, detail=exc.code
        ) from exc
    return _decision(row, record, authority)


def partition_totals_rows(
    rows: Iterable[Mapping[str, Any]], *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
    unique_identity_fields: Sequence[str] | None = None,
) -> TotalsPhasePartitions:
    authority = authority or verified_totals_phase_authority()
    buckets: dict[str, list[Mapping[str, Any]]] = {
        REGULAR_SEASON: [], POSTSEASON: [],
        "EXCLUDED_PRESEASON": [], "EXCLUDED_SPECIAL": [],
    }
    decisions: list[TotalsPhaseDecision] = []
    seen: set[tuple[Any, ...]] = set()
    for row in tuple(rows):
        if unique_identity_fields:
            identity = tuple(row.get(field) for field in unique_identity_fields)
            if any(value in (None, "") for value in identity):
                raise TotalsPhaseGateError("TOTALS_EVALUATION_IDENTITY_INCOMPLETE")
            if identity in seen:
                raise TotalsPhaseGateError(
                    "TOTALS_DUPLICATE_EVALUATION_IDENTITY", game_pk=exact_game_pk(row)
                )
            seen.add(identity)
        decision = classify_totals_row(
            row, authority=authority, require_freshness=require_freshness
        )
        decisions.append(decision)
        buckets[decision.evaluation_partition].append(row)
    return TotalsPhasePartitions(
        regular_season=tuple(buckets[REGULAR_SEASON]),
        postseason=tuple(buckets[POSTSEASON]),
        excluded_preseason=tuple(buckets["EXCLUDED_PRESEASON"]),
        excluded_special=tuple(buckets["EXCLUDED_SPECIAL"]),
        decisions=tuple(decisions),
    )


def require_raw_c_consistency(
    raw_rows: Iterable[Mapping[str, Any]], c_rows: Iterable[Mapping[str, Any]], *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
) -> dict[int, TotalsPhaseDecision]:
    """Require every C identity to have one RAW parent with the same phase."""

    authority = authority or verified_totals_phase_authority()
    raw_by_game: dict[int, TotalsPhaseDecision] = {}
    for row in raw_rows:
        game_pk = exact_game_pk(row)
        if game_pk in raw_by_game:
            raise TotalsPhaseGateError("TOTALS_RAW_DUPLICATE_GAME_PK", game_pk=game_pk)
        raw_by_game[game_pk] = classify_totals_row(
            row, authority=authority, require_freshness=require_freshness
        )
    c_by_game: set[int] = set()
    for row in c_rows:
        game_pk = exact_game_pk(row)
        if game_pk in c_by_game:
            raise TotalsPhaseGateError("TOTALS_C_DUPLICATE_GAME_PK", game_pk=game_pk)
        c_by_game.add(game_pk)
        c_decision = classify_totals_row(
            row, authority=authority, require_freshness=require_freshness
        )
        raw_decision = raw_by_game.get(game_pk)
        if raw_decision is None:
            raise TotalsPhaseGateError("TOTALS_C_RAW_PARENT_MISSING", game_pk=game_pk)
        if (
            raw_decision.source_game_type != c_decision.source_game_type
            or raw_decision.normalized_phase != c_decision.normalized_phase
            or raw_decision.postseason_round != c_decision.postseason_round
        ):
            raise TotalsPhaseGateError("TOTALS_CROSS_LANE_PHASE_CONFLICT", game_pk=game_pk)
    return {game_pk: raw_by_game[game_pk] for game_pk in sorted(c_by_game)}


def canonical_rows_sha256(rows: Iterable[Mapping[str, Any]]) -> str:
    encoded = json.dumps(
        list(rows), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

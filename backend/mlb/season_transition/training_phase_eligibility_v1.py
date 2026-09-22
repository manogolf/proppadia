"""Shared exact-gamePk regular-season dataset-membership gate.

This module deliberately knows nothing about model features, targets, fitting,
or artifacts.  It consumes the stable ``CanonicalGamePhaseAuthority``
interface and preserves admitted row objects and their input order exactly.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence, TypeVar

from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    source_type_is_recognized,
)


CONTRACT_NAME = "MLB_REGULAR_SEASON_TRAINING_ELIGIBILITY_V1"

ADMITTED_REGULAR_SEASON = "ADMITTED_REGULAR_SEASON"
EXCLUDED_PRESEASON = "EXCLUDED_PRESEASON"
EXCLUDED_POSTSEASON = "EXCLUDED_POSTSEASON"
BLOCKED_MISSING_GAME_PK = "BLOCKED_MISSING_GAME_PK"
BLOCKED_ABSENT_AUTHORITY = "BLOCKED_ABSENT_AUTHORITY"
BLOCKED_SPECIAL_TYPE = "BLOCKED_SPECIAL_TYPE"
BLOCKED_UNKNOWN_TYPE = "BLOCKED_UNKNOWN_TYPE"
BLOCKED_CONFLICTING_TYPE = "BLOCKED_CONFLICTING_TYPE"
BLOCKED_DUPLICATE_AUTHORITY = "BLOCKED_DUPLICATE_AUTHORITY"
BLOCKED_AUTHORITY_HASH_MISMATCH = "BLOCKED_AUTHORITY_HASH_MISMATCH"
BLOCKED_STALE_AUTHORITY = "BLOCKED_STALE_AUTHORITY"

DECISION_CODES = (
    ADMITTED_REGULAR_SEASON,
    EXCLUDED_PRESEASON,
    EXCLUDED_POSTSEASON,
    BLOCKED_MISSING_GAME_PK,
    BLOCKED_ABSENT_AUTHORITY,
    BLOCKED_SPECIAL_TYPE,
    BLOCKED_UNKNOWN_TYPE,
    BLOCKED_CONFLICTING_TYPE,
    BLOCKED_DUPLICATE_AUTHORITY,
    BLOCKED_AUTHORITY_HASH_MISMATCH,
    BLOCKED_STALE_AUTHORITY,
)

_ERROR_DECISIONS = {
    "GAME_PHASE_GAME_PK_MISSING": BLOCKED_MISSING_GAME_PK,
    "GAME_PHASE_ABSENT": BLOCKED_ABSENT_AUTHORITY,
    "GAME_PHASE_SPECIAL_EXCLUDED": BLOCKED_SPECIAL_TYPE,
    "GAME_PHASE_CONFLICT_BLOCKED": BLOCKED_CONFLICTING_TYPE,
    "GAME_PHASE_CORRECTION_REVIEW_REQUIRED": BLOCKED_CONFLICTING_TYPE,
    "GAME_PHASE_PROPOSAL_CONFLICTING_IDENTITY": BLOCKED_CONFLICTING_TYPE,
    "GAME_PHASE_PROPOSAL_DUPLICATE_IDENTITY": BLOCKED_DUPLICATE_AUTHORITY,
    "GAME_PHASE_AUTHORITY_STALE": BLOCKED_STALE_AUTHORITY,
    "GAME_PHASE_PROPOSAL_HASH_MISMATCH": BLOCKED_AUTHORITY_HASH_MISMATCH,
    "GAME_PHASE_SOURCE_MANIFEST_HASH_MISMATCH": BLOCKED_AUTHORITY_HASH_MISMATCH,
    "GAME_PHASE_EVIDENCE_HASH_MISMATCH": BLOCKED_AUTHORITY_HASH_MISMATCH,
}

RowT = TypeVar("RowT", bound=Mapping[str, Any])


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


class _StreamHash:
    """Length-delimited deterministic hash for an ordered value stream."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self.count = 0

    def add(self, value: Any) -> None:
        payload = _canonical_bytes(value)
        self._digest.update(len(payload).to_bytes(8, "big"))
        self._digest.update(payload)
        self.count += 1

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


@dataclass(frozen=True)
class EligibilityGateResult:
    """Rows admitted by positive authority plus deterministic gate evidence."""

    admitted_rows: tuple[Mapping[str, Any], ...]
    report: Mapping[str, Any]


class EligibilityGateBlocked(RuntimeError):
    """Raised after enumeration when any fail-closed identity is observed."""

    def __init__(self, report: Mapping[str, Any]) -> None:
        self.report = report
        blocked = int(report.get("blocked_row_count", 0))
        super().__init__(f"{CONTRACT_NAME}:BLOCKED:{blocked}")


def _row_projection(row: Mapping[str, Any], fields: Sequence[str]) -> list[Any]:
    return [row.get(field) for field in fields]


def _metadata_block(metadata: Any) -> str | None:
    if int(getattr(metadata, "duplicate_identity_count", 0)):
        return BLOCKED_DUPLICATE_AUTHORITY
    if int(getattr(metadata, "conflicting_count", 0)):
        return BLOCKED_CONFLICTING_TYPE
    if int(getattr(metadata, "missing_count", 0)):
        return BLOCKED_ABSENT_AUTHORITY
    if int(getattr(metadata, "unknown_count", 0)):
        return BLOCKED_UNKNOWN_TYPE
    return None


def _optional_source_type_is_missing(value: Any) -> bool:
    if value is None or value == "":
        return True
    return isinstance(value, float) and math.isnan(value)


def _decision_for_row(
    row: Mapping[str, Any],
    *,
    game_pk_field: str,
    source_type_field: str | None,
    authority: CanonicalGamePhaseAuthority,
) -> tuple[str, int | None, str | None, str | None]:
    value = row.get(game_pk_field)
    try:
        record = authority.lookup_exact(value)
    except GamePhaseAuthorityError as exc:
        return (
            _ERROR_DECISIONS.get(exc.code, BLOCKED_UNKNOWN_TYPE),
            exc.game_pk,
            None,
            None,
        )

    if source_type_field:
        retained_type = row.get(source_type_field)
        retained_type_missing = _optional_source_type_is_missing(retained_type)
        if not retained_type_missing:
            if not source_type_is_recognized(retained_type):
                return (
                    BLOCKED_UNKNOWN_TYPE,
                    record.game_pk,
                    record.season_phase,
                    record.source_game_type,
                )
            if retained_type != record.source_game_type:
                return (
                    BLOCKED_CONFLICTING_TYPE,
                    record.game_pk,
                    record.season_phase,
                    record.source_game_type,
                )

    if record.season_phase == "REGULAR_SEASON":
        return (
            ADMITTED_REGULAR_SEASON,
            record.game_pk,
            record.season_phase,
            record.source_game_type,
        )
    if record.season_phase == "PRESEASON":
        return (
            EXCLUDED_PRESEASON,
            record.game_pk,
            record.season_phase,
            record.source_game_type,
        )
    if record.season_phase == "POSTSEASON":
        return (
            EXCLUDED_POSTSEASON,
            record.game_pk,
            record.season_phase,
            record.source_game_type,
        )
    return (
        BLOCKED_UNKNOWN_TYPE,
        record.game_pk,
        record.season_phase,
        record.source_game_type,
    )


def filter_regular_season_membership(
    rows: Iterable[RowT],
    *,
    game_pk_field: str,
    authority: CanonicalGamePhaseAuthority,
    consumer_identity: str,
    input_identity: str,
    source_type_field: str | None = None,
    row_identity_fields: Sequence[str] = (),
    invariant_field_groups: Mapping[str, Sequence[str]] | None = None,
    collect_admitted_rows: bool = True,
) -> EligibilityGateResult:
    """Admit only exact authoritative regular-season memberships.

    Expected preseason and postseason memberships are exclusions.  Every
    unresolved condition is enumerated and then raises ``EligibilityGateBlocked``;
    partial admitted output is never returned to a caller after a blocked row.
    """

    if not game_pk_field or not consumer_identity or not input_identity:
        raise ValueError("ELIGIBILITY_GATE_REQUIRED_IDENTITY_MISSING")

    metadata = authority.metadata
    preexisting_block = _metadata_block(metadata)
    if preexisting_block is not None:
        report = {
            "contract_name": CONTRACT_NAME,
            "consumer_identity": consumer_identity,
            "input_identity": input_identity,
            "authority": metadata.to_dict(),
            "decision_row_counts": {
                code: (1 if code == preexisting_block else 0)
                for code in DECISION_CODES
            },
            "decision_game_pks": {code: [] for code in DECISION_CODES},
            "input_row_count": 0,
            "input_distinct_game_pk_count": 0,
            "admitted_row_count": 0,
            "admitted_distinct_game_pk_count": 0,
            "excluded_row_count": 0,
            "blocked_row_count": 1,
            "gate_status": "BLOCKED",
            "calendar_inference": False,
            "missing_type_default": False,
        }
        raise EligibilityGateBlocked(report)

    decisions: Counter[str] = Counter()
    game_pks_by_decision: defaultdict[str, set[int]] = defaultdict(set)
    game_pk_rows_by_decision: defaultdict[str, Counter[int]] = defaultdict(Counter)
    phases_by_game_pk: dict[int, str] = {}
    authority_types_by_game_pk: dict[int, str] = {}
    input_game_pks: set[int] = set()
    admitted_rows: list[Mapping[str, Any]] = []
    input_hash = _StreamHash()
    admitted_hash = _StreamHash()
    input_identity_hash = _StreamHash()
    admitted_identity_before_hash = _StreamHash()
    admitted_identity_after_hash = _StreamHash()
    invariant_groups = dict(invariant_field_groups or {})
    invariants_before = {name: _StreamHash() for name in invariant_groups}
    invariants_after = {name: _StreamHash() for name in invariant_groups}

    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError("ELIGIBILITY_GATE_ROW_NOT_MAPPING")
        input_hash.add(row)
        identity = (
            _row_projection(row, row_identity_fields)
            if row_identity_fields
            else row
        )
        input_identity_hash.add(identity)
        decision, game_pk, phase, authority_type = _decision_for_row(
            row,
            game_pk_field=game_pk_field,
            source_type_field=source_type_field,
            authority=authority,
        )
        decisions[decision] += 1
        if game_pk is not None:
            exact = int(game_pk)
            input_game_pks.add(exact)
            game_pks_by_decision[decision].add(exact)
            game_pk_rows_by_decision[decision][exact] += 1
            if phase is not None:
                phases_by_game_pk[exact] = phase
            if authority_type is not None:
                authority_types_by_game_pk[exact] = authority_type
        if decision == ADMITTED_REGULAR_SEASON:
            admitted_identity_before_hash.add(identity)
            for name, fields in invariant_groups.items():
                invariants_before[name].add(_row_projection(row, fields))
            if collect_admitted_rows:
                admitted_rows.append(row)
                admitted = admitted_rows[-1]
            else:
                admitted = row
            admitted_hash.add(admitted)
            admitted_identity_after_hash.add(
                _row_projection(admitted, row_identity_fields)
                if row_identity_fields
                else admitted
            )
            for name, fields in invariant_groups.items():
                invariants_after[name].add(_row_projection(admitted, fields))

    decision_row_counts = {
        code: int(decisions.get(code, 0)) for code in DECISION_CODES
    }
    decision_game_pks = {
        code: sorted(game_pks_by_decision.get(code, set())) for code in DECISION_CODES
    }
    decision_game_pk_row_counts = {
        code: [
            {"game_pk": game_pk, "row_count": int(row_count)}
            for game_pk, row_count in sorted(
                game_pk_rows_by_decision.get(code, Counter()).items()
            )
        ]
        for code in DECISION_CODES
    }
    blocked_row_count = sum(
        count
        for code, count in decision_row_counts.items()
        if code.startswith("BLOCKED_")
    )
    excluded_row_count = sum(
        count
        for code, count in decision_row_counts.items()
        if code.startswith("EXCLUDED_")
    )
    identity_before = admitted_identity_before_hash.hexdigest()
    identity_after = admitted_identity_after_hash.hexdigest()
    invariant_hashes = {
        name: {
            "before_sha256": invariants_before[name].hexdigest(),
            "after_sha256": invariants_after[name].hexdigest(),
            "identical": (
                invariants_before[name].hexdigest()
                == invariants_after[name].hexdigest()
            ),
        }
        for name in sorted(invariant_groups)
    }
    report = {
        "contract_name": CONTRACT_NAME,
        "consumer_identity": consumer_identity,
        "input_identity": input_identity,
        "authority": metadata.to_dict(),
        "game_pk_field": game_pk_field,
        "source_type_field": source_type_field,
        "decision_row_counts": decision_row_counts,
        "decision_game_pks": decision_game_pks,
        "decision_game_pk_row_counts": decision_game_pk_row_counts,
        "input_row_count": input_hash.count,
        "input_distinct_game_pk_count": len(input_game_pks),
        "input_game_pks_sha256": hashlib.sha256(
            _canonical_bytes(sorted(input_game_pks))
        ).hexdigest(),
        "admitted_row_count": int(decisions.get(ADMITTED_REGULAR_SEASON, 0)),
        "admitted_distinct_game_pk_count": len(
            game_pks_by_decision.get(ADMITTED_REGULAR_SEASON, set())
        ),
        "excluded_row_count": excluded_row_count,
        "blocked_row_count": blocked_row_count,
        "input_row_content_sha256": input_hash.hexdigest(),
        "admitted_row_content_sha256": admitted_hash.hexdigest(),
        "input_identity_sha256": input_identity_hash.hexdigest(),
        "admitted_identity_before_sha256": identity_before,
        "admitted_identity_after_sha256": identity_after,
        "admitted_identity_identical": identity_before == identity_after,
        "invariant_hashes": invariant_hashes,
        "phase_counts_by_distinct_game_pk": dict(
            sorted(Counter(phases_by_game_pk.values()).items())
        ),
        "source_type_counts_by_distinct_game_pk": dict(
            sorted(Counter(authority_types_by_game_pk.values()).items())
        ),
        "stable_input_order_preserved": True,
        "calendar_inference": False,
        "missing_type_default": False,
        "gate_status": "BLOCKED" if blocked_row_count else "PASS",
    }
    if blocked_row_count:
        raise EligibilityGateBlocked(report)
    return EligibilityGateResult(tuple(admitted_rows), report)

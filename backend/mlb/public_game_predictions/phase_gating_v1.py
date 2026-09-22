"""Fail-closed phase partitions for MLB Moneyline evaluation.

This module joins immutable Moneyline rows to the stable canonical game-phase
authority by exact gamePk.  It does not persist phase on a Moneyline row and it
does not infer phase from dates or any model/market field.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from dataclasses import asdict, dataclass
from functools import lru_cache
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


class MoneylinePhaseGateError(RuntimeError):
    """A Moneyline row cannot safely enter an evaluation partition."""

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
class MoneylinePhaseDecision:
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
class MoneylinePhasePartitions:
    regular_season: tuple[Mapping[str, Any], ...]
    postseason: tuple[Mapping[str, Any], ...]
    excluded_preseason: tuple[Mapping[str, Any], ...]
    excluded_special: tuple[Mapping[str, Any], ...]
    decisions: tuple[MoneylinePhaseDecision, ...]

    @property
    def admitted(self) -> tuple[Mapping[str, Any], ...]:
        return self.regular_season + self.postseason

    def counts(self) -> dict[str, int]:
        return {
            "REGULAR_SEASON": len(self.regular_season),
            "POSTSEASON": len(self.postseason),
            "EXCLUDED_PRESEASON": len(self.excluded_preseason),
            "EXCLUDED_SPECIAL": len(self.excluded_special),
        }


@lru_cache(maxsize=1)
def verified_moneyline_phase_authority() -> HashedProposalAuthority:
    """Load the source-hashed authority once per process."""

    return HashedProposalAuthority()


def _game_pk(row: Mapping[str, Any]) -> int:
    values = []
    for field in ("game_id", "game_pk", "gamePk"):
        value = row.get(field)
        if value not in (None, ""):
            try:
                values.append(int(value))
            except (TypeError, ValueError):
                raise MoneylinePhaseGateError("MONEYLINE_GAME_PK_MISSING") from None
    if not values or values[0] <= 0:
        raise MoneylinePhaseGateError("MONEYLINE_GAME_PK_MISSING")
    if len(set(values)) != 1:
        raise MoneylinePhaseGateError(
            "MONEYLINE_EXACT_GAME_PK_MISMATCH",
            detail=",".join(str(value) for value in values),
        )
    return values[0]


def _assert_authority_health(authority: CanonicalGamePhaseAuthority) -> None:
    metadata = authority.metadata
    violations = {
        "missing": metadata.missing_count,
        "unknown": metadata.unknown_count,
        "conflicting": metadata.conflicting_count,
        "duplicate": metadata.duplicate_identity_count,
    }
    if any(violations.values()):
        raise MoneylinePhaseGateError(
            "MONEYLINE_PHASE_AUTHORITY_POPULATION_INVALID",
            detail=json.dumps(violations, sort_keys=True, separators=(",", ":")),
        )


def _consumer_source_type(row: Mapping[str, Any]) -> str | None:
    present = {
        str(row[field])
        for field in ("source_game_type", "game_type", "gameType")
        if row.get(field) not in (None, "")
    }
    if len(present) > 1:
        raise MoneylinePhaseGateError(
            "MONEYLINE_CONFLICTING_SOURCE_TYPES",
            detail=",".join(sorted(present)),
        )
    return next(iter(present), None)


def classify_moneyline_row(
    row: Mapping[str, Any],
    *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
) -> MoneylinePhaseDecision:
    """Classify one row without modifying it or deriving phase from its date."""

    authority = authority or verified_moneyline_phase_authority()
    _assert_authority_health(authority)
    game_pk = _game_pk(row)
    game_date = row.get("game_date")
    if require_freshness:
        if game_date in (None, ""):
            raise MoneylinePhaseGateError(
                "MONEYLINE_AUTHORITY_FRESHNESS_UNPROVABLE", game_pk=game_pk
            )
        try:
            authority.require_supported_window(game_date, game_date)
        except GamePhaseAuthorityError as exc:
            raise MoneylinePhaseGateError(
                "MONEYLINE_PHASE_AUTHORITY_STALE", game_pk=game_pk, detail=exc.code
            ) from exc
    try:
        record = authority.lookup_exact(game_pk)
    except GamePhaseAuthorityError as exc:
        if exc.code == "GAME_PHASE_SPECIAL_EXCLUDED":
            metadata = authority.metadata
            return MoneylinePhaseDecision(
                game_pk=game_pk,
                source_game_type=None,
                normalized_phase=None,
                postseason_round=None,
                evaluation_partition="EXCLUDED_SPECIAL",
                decision_code="EXCLUDED_SPECIAL_AUTHORITATIVE_TYPE",
                authority_proposal_sha256=metadata.proposal_sha256,
                authority_records_sha256=metadata.authority_records_sha256,
            )
        raise MoneylinePhaseGateError(
            "MONEYLINE_PHASE_AUTHORITY_INVALID", game_pk=game_pk, detail=exc.code
        ) from exc
    return _decision_from_record(row, record, authority)


def _decision_from_record(
    row: Mapping[str, Any],
    record: GamePhaseAuthorityRecord,
    authority: CanonicalGamePhaseAuthority,
) -> MoneylinePhaseDecision:
    source_type = _consumer_source_type(row)
    if source_type is not None and source_type != record.source_game_type:
        raise MoneylinePhaseGateError(
            "MONEYLINE_SOURCE_TYPE_CONFLICT",
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
        raise MoneylinePhaseGateError(
            "MONEYLINE_NORMALIZED_PHASE_UNKNOWN",
            game_pk=record.game_pk,
            detail=str(record.season_phase),
        )
    metadata = authority.metadata
    return MoneylinePhaseDecision(
        game_pk=record.game_pk,
        source_game_type=record.source_game_type,
        normalized_phase=record.season_phase,
        postseason_round=record.postseason_round,
        evaluation_partition=partition,
        decision_code=code,
        authority_proposal_sha256=metadata.proposal_sha256,
        authority_records_sha256=metadata.authority_records_sha256,
    )


def partition_moneyline_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    authority: CanonicalGamePhaseAuthority | None = None,
    require_freshness: bool = True,
    unique_identity_fields: Sequence[str] | None = None,
) -> MoneylinePhasePartitions:
    """Return disjoint evaluation partitions or fail before returning any rows."""

    authority = authority or verified_moneyline_phase_authority()
    materialized = tuple(rows)
    decisions: list[MoneylinePhaseDecision] = []
    buckets: dict[str, list[Mapping[str, Any]]] = {
        REGULAR_SEASON: [], POSTSEASON: [], "EXCLUDED_PRESEASON": [], "EXCLUDED_SPECIAL": []
    }
    seen: set[tuple[Any, ...]] = set()
    for row in materialized:
        if unique_identity_fields:
            identity = tuple(row.get(field) for field in unique_identity_fields)
            if any(value in (None, "") for value in identity):
                raise MoneylinePhaseGateError("MONEYLINE_EVALUATION_IDENTITY_INCOMPLETE")
            if identity in seen:
                raise MoneylinePhaseGateError(
                    "MONEYLINE_DUPLICATE_EVALUATION_IDENTITY",
                    game_pk=_game_pk(row),
                )
            seen.add(identity)
        decision = classify_moneyline_row(
            row, authority=authority, require_freshness=require_freshness
        )
        decisions.append(decision)
        buckets[decision.evaluation_partition].append(row)
    return MoneylinePhasePartitions(
        regular_season=tuple(buckets[REGULAR_SEASON]),
        postseason=tuple(buckets[POSTSEASON]),
        excluded_preseason=tuple(buckets["EXCLUDED_PRESEASON"]),
        excluded_special=tuple(buckets["EXCLUDED_SPECIAL"]),
        decisions=tuple(decisions),
    )


def require_evaluation_row(
    row: Mapping[str, Any],
    *,
    authority: CanonicalGamePhaseAuthority | None = None,
) -> MoneylinePhaseDecision:
    """Require a row to belong to one and only one evaluation cohort."""

    decision = classify_moneyline_row(row, authority=authority)
    if decision.evaluation_partition not in EVALUATION_PHASES:
        raise MoneylinePhaseGateError(
            "MONEYLINE_ROW_NOT_EVALUATION_ELIGIBLE",
            game_pk=decision.game_pk,
            detail=decision.evaluation_partition,
        )
    return decision


def canonical_rows_sha256(rows: Iterable[Mapping[str, Any]]) -> str:
    """Hash row content without mutating or normalizing values."""

    encoded = json.dumps(
        list(rows), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def prediction_quality_metrics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Compute prediction metrics only; prices and returns are intentionally absent."""

    materialized = list(rows)
    if not materialized:
        return {"rows": 0, "accuracy": None, "brier": None, "log_loss": None,
                "calibration_residual": None}
    outcomes: list[int] = []
    probabilities: list[float] = []
    correct: list[int] = []
    for row in materialized:
        probability = float(row["home_win_probability"])
        if not 0.0 < probability < 1.0:
            raise MoneylinePhaseGateError("MONEYLINE_PROBABILITY_INVALID", game_pk=_game_pk(row))
        if "official_home_runs" in row and "official_away_runs" in row:
            home_win = int(int(row["official_home_runs"]) > int(row["official_away_runs"]))
        elif "home_win" in row:
            home_win = int(bool(row["home_win"]))
        else:
            raise MoneylinePhaseGateError("MONEYLINE_OUTCOME_MISSING", game_pk=_game_pk(row))
        selected_home = str(row["predicted_winner"]) == str(row["home_team"])
        outcomes.append(home_win)
        probabilities.append(probability)
        correct.append(int(selected_home == bool(home_win)))
    clipped = [min(1.0 - 1e-15, max(1e-15, value)) for value in probabilities]
    n = len(materialized)
    return {
        "rows": n,
        "accuracy": sum(correct) / n,
        "brier": sum((p - y) ** 2 for p, y in zip(probabilities, outcomes)) / n,
        "log_loss": sum(-(y * math.log(p) + (1 - y) * math.log(1 - p))
                        for p, y in zip(clipped, outcomes)) / n,
        "calibration_residual": sum(outcomes) / n - sum(probabilities) / n,
    }


def market_roi_metrics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Compute hypothetical flat-stake economics, separate from prediction quality."""

    materialized = list(rows)
    if not materialized:
        return {"rows": 0, "wins": 0, "flat_stake_return": None, "roi": None,
                "execution_status": "HYPOTHETICAL_NO_FILL_EVIDENCE"}
    returns: list[float] = []
    for row in materialized:
        won = int(bool(row["selected_win"]))
        decimal_price = float(row["selected_decimal_price"])
        returns.append(decimal_price - 1.0 if won else -1.0)
    return {
        "rows": len(materialized),
        "wins": sum(int(bool(row["selected_win"])) for row in materialized),
        "flat_stake_return": sum(returns),
        "roi": sum(returns) / len(returns),
        "execution_status": "HYPOTHETICAL_NO_FILL_EVIDENCE",
    }


def decision_counts(decisions: Iterable[MoneylinePhaseDecision]) -> dict[str, int]:
    return dict(sorted(Counter(item.decision_code for item in decisions).items()))

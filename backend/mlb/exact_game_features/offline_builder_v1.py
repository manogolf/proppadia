"""Retained-file-only proposal builder for exact-game strict-prior states.

The builder emits candidate classifications and hashes, never operational
feature rows.  It has no database or network dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from backend.mlb.exact_game_features.contract_v1 import (
    EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE,
    HISTORICAL_EXACT_GAME_FACT_RECONSTRUCTABLE,
    HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE,
    ExactGameAuthorityV1,
    ExactGameContractError,
    SourceObservationV1,
    content_sha256,
)


BUILDER_VERSION = "MLB_EXACT_GAME_OFFLINE_CANDIDATE_BUILDER_V1"


def _utc(value: str | datetime | None) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise ExactGameContractError("CHRONOLOGY_TIMESTAMP_INVALID", str(value)) from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ExactGameContractError("CHRONOLOGY_TIMESTAMP_INVALID", str(value))
    return result.astimezone(timezone.utc)


@dataclass(frozen=True)
class HistoricalExactGameFactV1:
    player_id: int
    authority: ExactGameAuthorityV1
    terminal_observed_at_utc: datetime | None
    source_observations: tuple[SourceObservationV1, ...]
    fact_payload: Mapping[str, Any]
    fact_payload_sha256: str

    @classmethod
    def create(
        cls,
        *,
        player_id: int,
        authority: ExactGameAuthorityV1,
        terminal_observed_at_utc: str | datetime | None,
        source_observations: Iterable[SourceObservationV1],
        fact_payload: Mapping[str, Any],
    ) -> "HistoricalExactGameFactV1":
        if int(player_id) <= 0:
            raise ExactGameContractError("PLAYER_ID_MISSING")
        observations = tuple(source_observations)
        if not observations:
            raise ExactGameContractError("SOURCE_OBSERVATIONS_MISSING")
        payload = dict(fact_payload)
        return cls(
            player_id=int(player_id),
            authority=authority,
            terminal_observed_at_utc=_utc(terminal_observed_at_utc),
            source_observations=observations,
            fact_payload=payload,
            fact_payload_sha256=content_sha256(payload),
        )

    @property
    def identity(self) -> tuple[int, int]:
        return self.player_id, self.authority.game_pk


@dataclass(frozen=True)
class ExactGameTargetV1:
    player_id: int
    authority: ExactGameAuthorityV1
    feature_input_cutoff_utc: datetime | None

    @classmethod
    def create(
        cls,
        *,
        player_id: int,
        authority: ExactGameAuthorityV1,
        feature_input_cutoff_utc: str | datetime | None,
    ) -> "ExactGameTargetV1":
        if int(player_id) <= 0:
            raise ExactGameContractError("PLAYER_ID_MISSING")
        return cls(
            player_id=int(player_id),
            authority=authority,
            feature_input_cutoff_utc=_utc(feature_input_cutoff_utc),
        )

    @property
    def identity(self) -> tuple[int, int]:
        return self.player_id, self.authority.game_pk


@dataclass(frozen=True)
class CandidateProposalV1:
    player_id: int
    game_pk: int
    official_date: str
    scheduled_start_utc: str
    classification: str
    reason: str
    eligible_prior_game_pks: tuple[int, ...]
    excluded_prior_games: tuple[tuple[int, str], ...]
    exact_fact_hashes: tuple[str, ...]
    source_hashes: tuple[str, ...]
    deterministic_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "builder_version": BUILDER_VERSION,
            "player_id": self.player_id,
            "game_pk": self.game_pk,
            "official_date": self.official_date,
            "scheduled_start_utc": self.scheduled_start_utc,
            "classification": self.classification,
            "reason": self.reason,
            "eligible_prior_game_pks": list(self.eligible_prior_game_pks),
            "excluded_prior_games": [
                {"game_pk": game_pk, "reason": reason}
                for game_pk, reason in self.excluded_prior_games
            ],
            "exact_fact_hashes": list(self.exact_fact_hashes),
            "source_hashes": list(self.source_hashes),
            "deterministic_sha256": self.deterministic_sha256,
        }


class OfflineExactGameCandidateBuilderV1:
    """Build deterministic, non-operational proposals from typed retained facts."""

    @staticmethod
    def _coalesce_facts(
        facts: Iterable[HistoricalExactGameFactV1],
    ) -> tuple[HistoricalExactGameFactV1, ...]:
        result: dict[tuple[int, int], HistoricalExactGameFactV1] = {}
        for fact in facts:
            prior = result.get(fact.identity)
            if prior is None:
                result[fact.identity] = fact
                continue
            prior_hash = content_sha256(
                {
                    "authority": prior.authority.to_dict(),
                    "payload_sha256": prior.fact_payload_sha256,
                    "terminal_observed_at_utc": None
                    if prior.terminal_observed_at_utc is None
                    else prior.terminal_observed_at_utc.isoformat(),
                    "sources": [item.to_dict() for item in prior.source_observations],
                }
            )
            incoming_hash = content_sha256(
                {
                    "authority": fact.authority.to_dict(),
                    "payload_sha256": fact.fact_payload_sha256,
                    "terminal_observed_at_utc": None
                    if fact.terminal_observed_at_utc is None
                    else fact.terminal_observed_at_utc.isoformat(),
                    "sources": [item.to_dict() for item in fact.source_observations],
                }
            )
            if prior_hash != incoming_hash:
                raise ExactGameContractError(
                    "DUPLICATE_EXACT_IDENTITY_CONFLICTING_PAYLOAD",
                    f"{fact.player_id}/{fact.authority.game_pk}",
                )
        return tuple(result[key] for key in sorted(result))

    @staticmethod
    def _coalesce_targets(targets: Iterable[ExactGameTargetV1]) -> tuple[ExactGameTargetV1, ...]:
        result: dict[tuple[int, int], ExactGameTargetV1] = {}
        for target in targets:
            prior = result.get(target.identity)
            if prior is None:
                result[target.identity] = target
            elif prior != target:
                raise ExactGameContractError(
                    "DUPLICATE_EXACT_TARGET_CONFLICT",
                    f"{target.player_id}/{target.authority.game_pk}",
                )
        return tuple(result[key] for key in sorted(result))

    def build(
        self,
        *,
        facts: Iterable[HistoricalExactGameFactV1],
        targets: Iterable[ExactGameTargetV1],
    ) -> tuple[CandidateProposalV1, ...]:
        exact_facts = self._coalesce_facts(facts)
        exact_targets = self._coalesce_targets(targets)
        by_player: dict[int, list[HistoricalExactGameFactV1]] = {}
        for fact in exact_facts:
            by_player.setdefault(fact.player_id, []).append(fact)

        proposals: list[CandidateProposalV1] = []
        for target in exact_targets:
            cutoff = target.feature_input_cutoff_utc
            eligible: list[HistoricalExactGameFactV1] = []
            excluded: list[tuple[int, str]] = []
            player_facts = by_player.get(target.player_id, [])
            for fact in sorted(
                player_facts,
                key=lambda item: (
                    item.authority.scheduled_start_utc,
                    item.authority.game_pk,
                ),
            ):
                if fact.authority.game_pk == target.authority.game_pk:
                    excluded.append((fact.authority.game_pk, "TARGET_GAME_OUTCOME_EXCLUDED"))
                elif cutoff is None:
                    excluded.append((fact.authority.game_pk, "TARGET_INPUT_CUTOFF_UNPROVEN"))
                elif fact.terminal_observed_at_utc is None:
                    excluded.append((fact.authority.game_pk, "TERMINAL_OBSERVATION_TIME_UNPROVEN"))
                elif fact.authority.scheduled_start_utc >= target.authority.scheduled_start_utc:
                    excluded.append((fact.authority.game_pk, "EVENT_CHRONOLOGY_NOT_STRICTLY_PRIOR"))
                elif fact.terminal_observed_at_utc >= cutoff:
                    excluded.append((fact.authority.game_pk, "TERMINAL_OBSERVED_AT_OR_AFTER_CUTOFF"))
                elif any(source.observed_at_utc > cutoff for source in fact.source_observations):
                    excluded.append((fact.authority.game_pk, "SOURCE_OBSERVED_POST_CUTOFF"))
                else:
                    eligible.append(fact)

            if cutoff is None:
                classification = HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE
                reason = "IMMUTABLE_FEATURE_INPUT_CUTOFF_NOT_RETAINED"
            elif cutoff > target.authority.scheduled_start_utc:
                classification = HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE
                reason = "FEATURE_INPUT_CUTOFF_POST_START"
            else:
                classification = EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE
                reason = "STRICT_PRIOR_CHRONOLOGY_AND_OBSERVATION_CUTOFF_PROVEN"

            eligible_ids = tuple(item.authority.game_pk for item in eligible)
            fact_hashes = tuple(item.fact_payload_sha256 for item in eligible)
            source_hashes = tuple(
                sorted(
                    {
                        source.source_sha256
                        for item in eligible
                        for source in item.source_observations
                    }
                    | set(target.authority.source_hashes)
                )
            )
            base = {
                "builder_version": BUILDER_VERSION,
                "player_id": target.player_id,
                "game_pk": target.authority.game_pk,
                "authority": target.authority.to_dict(),
                "feature_input_cutoff_utc": None if cutoff is None else cutoff.isoformat(),
                "classification": classification,
                "reason": reason,
                "eligible_prior_game_pks": eligible_ids,
                "excluded_prior_games": excluded,
                "exact_fact_hashes": fact_hashes,
                "source_hashes": source_hashes,
            }
            proposals.append(
                CandidateProposalV1(
                    player_id=target.player_id,
                    game_pk=target.authority.game_pk,
                    official_date=target.authority.official_date.isoformat(),
                    scheduled_start_utc=target.authority.scheduled_start_utc.isoformat().replace(
                        "+00:00", "Z"
                    ),
                    classification=classification,
                    reason=reason,
                    eligible_prior_game_pks=eligible_ids,
                    excluded_prior_games=tuple(excluded),
                    exact_fact_hashes=fact_hashes,
                    source_hashes=source_hashes,
                    deterministic_sha256=content_sha256(base),
                )
            )
        return tuple(proposals)


def classify_exact_fact(fact: HistoricalExactGameFactV1) -> dict[str, Any]:
    """Describe fact reconstructability without asserting a feature cutoff."""
    return {
        "builder_version": BUILDER_VERSION,
        "player_id": fact.player_id,
        "game_pk": fact.authority.game_pk,
        "classification": HISTORICAL_EXACT_GAME_FACT_RECONSTRUCTABLE,
        "fact_payload_sha256": fact.fact_payload_sha256,
        "source_hashes": sorted(item.source_sha256 for item in fact.source_observations),
    }

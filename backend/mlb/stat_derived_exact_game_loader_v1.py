"""Inactive exact-game MLB stat-derived loader contract V1.

This module contains only deterministic planning and backend-neutral transaction
orchestration.  It has no network or database adapter and is not imported by the
installed production wrapper.  The legacy loader remains contained until a
separate activation and reconciliation authorization is reviewed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Protocol, Sequence

from backend.mlb.identity.playable_terminal_v1 import (
    PLAYABLE_TERMINAL,
    ScheduleAppearance,
    classify_playable_terminal,
    reconcile_schedule_by_game_pk,
)


CONTRACT_VERSION = "MLB_2026_STAT_DERIVED_ROOT_LOADER_CORRECTION_V1"
PLANNER_VERSION = "MLB_EXACT_GAME_MUTATION_PLANNER_V1"
EXECUTOR_VERSION = "MLB_EXACT_GAME_ORDERED_TRANSACTION_EXECUTOR_V1_INACTIVE"
AUTHORIZATION_VERSION = "MLB_EXACT_GAME_MUTATION_AUTHORIZATION_V1"
FEATURE_CONTRACT_VERSION = "MLB_PLAYER_GAME_FEATURE_STATE_V1"
PHASE_AUTHORITY_INTERFACE = "MLB_CANONICAL_GAME_PHASE_AUTHORITY_V1"

INSERT_NEW_EXACT_FACT = "INSERT_NEW_EXACT_FACT"
MATCH_EXISTING_EXACT_FACT = "MATCH_EXISTING_EXACT_FACT"
RELOCATE_MATCHING_MISDATED_FACT = "RELOCATE_MATCHING_MISDATED_FACT"
QUARANTINE_CONFLICTING_LEGACY_IDENTITY = "QUARANTINE_CONFLICTING_LEGACY_IDENTITY"
BLOCK_CONFLICTING_PAYLOAD = "BLOCK_CONFLICTING_PAYLOAD"
NO_ACTION = "NO_ACTION"
UNPROVABLE_FAIL_CLOSED = "UNPROVABLE_FAIL_CLOSED"

OPERATIONS = {
    INSERT_NEW_EXACT_FACT,
    MATCH_EXISTING_EXACT_FACT,
    RELOCATE_MATCHING_MISDATED_FACT,
    QUARANTINE_CONFLICTING_LEGACY_IDENTITY,
    BLOCK_CONFLICTING_PAYLOAD,
    NO_ACTION,
    UNPROVABLE_FAIL_CLOSED,
}

MUTABLE_EXACT_RELATIONS = (
    "game_info",
    "player_stats",
    "model_training_props",
    "player_game_feature_state_v1",
)
LEGACY_DERIVED_RELATION = "player_derived_stats"
RELATION_ORDER = {
    "game_info": 10,
    "player_stats": 20,
    "model_training_props": 30,
    "player_derived_stats": 40,
    "player_game_feature_state_v1": 50,
}
EXECUTION_OPERATION_ORDER = {
    QUARANTINE_CONFLICTING_LEGACY_IDENTITY: 10,
    RELOCATE_MATCHING_MISDATED_FACT: 20,
    INSERT_NEW_EXACT_FACT: 30,
    MATCH_EXISTING_EXACT_FACT: 40,
    NO_ACTION: 40,
}


class LoaderContractError(RuntimeError):
    """Fail-closed exact-game loader contract error."""

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


def _utc(value: str | datetime, code: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise LoaderContractError(code, str(value)) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LoaderContractError(code, str(value))
    return parsed.astimezone(timezone.utc)


def _require_sha(value: str, code: str) -> str:
    text = str(value or "")
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        raise LoaderContractError(code, text)
    return text


@dataclass(frozen=True)
class SourceEvidenceV1:
    path: str
    sha256: str
    observed_at_utc: str
    kind: str

    def __post_init__(self) -> None:
        if not self.path or self.path.startswith("/") or ".." in self.path.split("/"):
            raise LoaderContractError("SOURCE_PATH_INVALID", self.path)
        _require_sha(self.sha256, "SOURCE_SHA256_INVALID")
        _utc(self.observed_at_utc, "SOURCE_OBSERVATION_TIME_INVALID")
        if not self.kind:
            raise LoaderContractError("SOURCE_KIND_MISSING")

    def to_dict(self) -> dict[str, str]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "observed_at_utc": self.observed_at_utc,
            "kind": self.kind,
        }


@dataclass(frozen=True)
class AcceptedGameV1:
    game_pk: int
    operational_date: str
    scheduled_start_utc: str
    game_number: int
    double_header: str
    team_ids: tuple[int, ...]
    relationship_evidence: Mapping[str, str]
    status_fields: Mapping[str, str]
    source_evidence: tuple[SourceEvidenceV1, ...]
    phase_authority_interface: str
    phase_authority_descriptor_sha256: str
    phase: str
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.game_pk <= 0:
            raise LoaderContractError("EXACT_GAME_PK_MISSING")
        datetime.fromisoformat(self.operational_date)
        _utc(self.scheduled_start_utc, "SCHEDULED_START_INVALID")
        if self.phase_authority_interface != PHASE_AUTHORITY_INTERFACE:
            raise LoaderContractError("PHASE_AUTHORITY_INTERFACE_INVALID")
        _require_sha(self.phase_authority_descriptor_sha256, "PHASE_AUTHORITY_HASH_INVALID")
        if self.phase not in {"REGULAR_SEASON", "POSTSEASON"}:
            raise LoaderContractError("UNSUPPORTED_PHASE", self.phase)
        if not self.source_evidence:
            raise LoaderContractError("SOURCE_EVIDENCE_MISSING")

    @property
    def authority_hash(self) -> str:
        return content_sha256(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "game_pk": self.game_pk,
            "operational_date": self.operational_date,
            "scheduled_start_utc": self.scheduled_start_utc,
            "game_number": self.game_number,
            "double_header": self.double_header,
            "team_ids": list(self.team_ids),
            "relationship_evidence": dict(self.relationship_evidence),
            "status_fields": dict(self.status_fields),
            "source_evidence": [item.to_dict() for item in self.source_evidence],
            "phase_authority_interface": self.phase_authority_interface,
            "phase_authority_descriptor_sha256": self.phase_authority_descriptor_sha256,
            "phase": self.phase,
        }


def accepted_games_from_retained_schedule(
    payload: Mapping[str, Any],
    *,
    schedule_sources: Sequence[SourceEvidenceV1],
    phase_by_game_pk: Mapping[int, str],
    phase_authority_descriptor_sha256: str,
) -> tuple[AcceptedGameV1, ...]:
    """Select one playable appearance per exact gamePk using the shared contract."""
    results: list[AcceptedGameV1] = []
    for decision in reconcile_schedule_by_game_pk(payload):
        if decision.decision != "FETCH_PLAYABLE_FINAL":
            continue
        selected = decision.selected
        if selected is None or selected.status.classification != PLAYABLE_TERMINAL:
            raise LoaderContractError("PLAYABLE_SELECTION_INCONSISTENT", str(decision.game_pk))
        phase = phase_by_game_pk.get(decision.game_pk)
        if phase not in {"REGULAR_SEASON", "POSTSEASON"}:
            raise LoaderContractError("CANONICAL_PHASE_AUTHORITY_MISSING", str(decision.game_pk))
        if not selected.official_date:
            raise LoaderContractError("ACCEPTED_OPERATIONAL_DATE_MISSING", str(decision.game_pk))
        if not selected.game_date_utc:
            raise LoaderContractError("ACCEPTED_SCHEDULED_START_MISSING", str(decision.game_pk))
        results.append(
            AcceptedGameV1(
                game_pk=decision.game_pk,
                operational_date=selected.official_date,
                scheduled_start_utc=selected.game_date_utc,
                game_number=selected.game_number,
                double_header=selected.double_header,
                team_ids=selected.team_ids,
                relationship_evidence=dict(selected.relations),
                status_fields=dict(selected.status.fields),
                source_evidence=tuple(schedule_sources),
                phase_authority_interface=PHASE_AUTHORITY_INTERFACE,
                phase_authority_descriptor_sha256=phase_authority_descriptor_sha256,
                phase=phase,
            )
        )
    return tuple(sorted(results, key=lambda item: item.game_pk))


def validate_terminal_feed(
    game: AcceptedGameV1,
    feed: Mapping[str, Any],
    source: SourceEvidenceV1,
) -> None:
    try:
        feed_game_pk = int(feed["gamePk"])
    except (KeyError, TypeError, ValueError):
        raise LoaderContractError("TERMINAL_FEED_GAME_PK_MISSING") from None
    if feed_game_pk != game.game_pk:
        raise LoaderContractError("TERMINAL_FEED_GAME_PK_MISMATCH")
    decision = classify_playable_terminal(feed)
    if not decision.accepted:
        raise LoaderContractError("TERMINAL_FEED_NOT_PLAYABLE", decision.classification)
    game_data = feed.get("gameData") or {}
    feed_date = str((game_data.get("datetime") or {}).get("officialDate") or "")
    if feed_date != game.operational_date:
        raise LoaderContractError("TERMINAL_FEED_OPERATIONAL_DATE_MISMATCH")
    _require_sha(source.sha256, "TERMINAL_FEED_SOURCE_HASH_INVALID")


@dataclass(frozen=True)
class StrictPriorDecisionV1:
    admitted: bool
    reason: str


def strict_prior_admission(
    *,
    prior_game: AcceptedGameV1,
    target_game: AcceptedGameV1,
    prior_terminal_observed_at_utc: str | None,
    target_feature_cutoff_utc: str | None,
    prior_source_observed_at_utc: Iterable[str],
) -> StrictPriorDecisionV1:
    """Apply observation-time chronology; calendar date never supplies a cutoff."""
    if prior_game.game_pk == target_game.game_pk:
        return StrictPriorDecisionV1(False, "TARGET_GAME_OUTCOME_EXCLUDED")
    if target_feature_cutoff_utc in (None, ""):
        return StrictPriorDecisionV1(False, "IMMUTABLE_FEATURE_INPUT_CUTOFF_NOT_RETAINED")
    if prior_terminal_observed_at_utc in (None, ""):
        return StrictPriorDecisionV1(False, "TERMINAL_OBSERVATION_TIME_UNPROVEN")
    cutoff = _utc(str(target_feature_cutoff_utc), "TARGET_FEATURE_CUTOFF_INVALID")
    if cutoff > _utc(target_game.scheduled_start_utc, "TARGET_START_INVALID"):
        return StrictPriorDecisionV1(False, "FEATURE_INPUT_CUTOFF_POST_START")
    terminal = _utc(str(prior_terminal_observed_at_utc), "TERMINAL_OBSERVATION_TIME_INVALID")
    if terminal >= cutoff:
        return StrictPriorDecisionV1(False, "TERMINAL_OBSERVED_AT_OR_AFTER_CUTOFF")
    observations = tuple(
        _utc(value, "SOURCE_OBSERVATION_TIME_INVALID")
        for value in prior_source_observed_at_utc
    )
    if not observations:
        return StrictPriorDecisionV1(False, "SOURCE_OBSERVATIONS_MISSING")
    if any(value > cutoff for value in observations):
        return StrictPriorDecisionV1(False, "SOURCE_OBSERVED_POST_CUTOFF")
    return StrictPriorDecisionV1(True, "STRICT_PRIOR_TERMINAL_AND_SOURCE_OBSERVATIONS_PROVEN")


@dataclass(frozen=True)
class MutationIntentV1:
    relation: str
    exact_key: Mapping[str, Any]
    authoritative_game_pk: int
    accepted_operational_date: str
    source_evidence: tuple[SourceEvidenceV1, ...]
    relationship_evidence: Mapping[str, Any]
    reason: str
    rollback_identity: Mapping[str, Any]
    proposed_value: Mapping[str, Any] | None
    current_value: Mapping[str, Any] | None = None
    current_substantive_hash: str | None = None
    proposed_substantive_hash: str | None = None
    force_operation: str | None = None
    collision_status: str = "NO_COLLISION_OBSERVED"

    def __post_init__(self) -> None:
        if self.relation not in set(RELATION_ORDER):
            raise LoaderContractError("RELATION_UNSUPPORTED", self.relation)
        if self.authoritative_game_pk <= 0 or not self.exact_key:
            raise LoaderContractError("EXACT_IDENTITY_MISSING", self.relation)
        if self.force_operation is not None and self.force_operation not in OPERATIONS:
            raise LoaderContractError("OPERATION_UNSUPPORTED", self.force_operation)
        if not self.source_evidence:
            raise LoaderContractError("SOURCE_EVIDENCE_MISSING", self.relation)


@dataclass(frozen=True)
class MutationProposalV1:
    sequence: int
    relation: str
    operation: str
    exact_key: Mapping[str, Any]
    current_value_hash: str | None
    proposed_value_hash: str | None
    current_substantive_hash: str | None
    proposed_substantive_hash: str | None
    authoritative_game_pk: int
    accepted_operational_date: str
    source_evidence: tuple[SourceEvidenceV1, ...]
    relationship_evidence: Mapping[str, Any]
    reason: str
    collision_status: str
    rollback_identity: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "relation": self.relation,
            "operation": self.operation,
            "exact_key": dict(self.exact_key),
            "current_value_hash": self.current_value_hash,
            "proposed_value_hash": self.proposed_value_hash,
            "current_substantive_hash": self.current_substantive_hash,
            "proposed_substantive_hash": self.proposed_substantive_hash,
            "authoritative_game_pk": self.authoritative_game_pk,
            "accepted_operational_date": self.accepted_operational_date,
            "source_evidence": [item.to_dict() for item in self.source_evidence],
            "relationship_evidence": dict(self.relationship_evidence),
            "reason": self.reason,
            "collision_status": self.collision_status,
            "rollback_identity": dict(self.rollback_identity),
        }


@dataclass(frozen=True)
class MutationPlanV1:
    proposals: tuple[MutationProposalV1, ...]
    source_set_sha256: str
    expected_state_sha256: str
    plan_sha256: str
    contract_version: str = CONTRACT_VERSION
    planner_version: str = PLANNER_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "planner_version": self.planner_version,
            "source_set_sha256": self.source_set_sha256,
            "expected_state_sha256": self.expected_state_sha256,
            "plan_sha256": self.plan_sha256,
            "proposals": [proposal.to_dict() for proposal in self.proposals],
        }


class ExactGameMutationPlannerV1:
    """Pure deterministic classifier for exact-key mutation intents."""

    @staticmethod
    def _operation(intent: MutationIntentV1) -> str:
        if intent.force_operation:
            return intent.force_operation
        if intent.relation == LEGACY_DERIVED_RELATION:
            return QUARANTINE_CONFLICTING_LEGACY_IDENTITY if intent.current_value else NO_ACTION
        if intent.proposed_value is None:
            return UNPROVABLE_FAIL_CLOSED
        if intent.current_value is None:
            return INSERT_NEW_EXACT_FACT
        current_substantive = intent.current_substantive_hash or content_sha256(intent.current_value)
        proposed_substantive = intent.proposed_substantive_hash or content_sha256(intent.proposed_value)
        if current_substantive != proposed_substantive:
            return BLOCK_CONFLICTING_PAYLOAD
        if content_sha256(intent.current_value) == content_sha256(intent.proposed_value):
            return MATCH_EXISTING_EXACT_FACT
        return RELOCATE_MATCHING_MISDATED_FACT

    def build(self, intents: Iterable[MutationIntentV1]) -> MutationPlanV1:
        ordered = sorted(
            intents,
            key=lambda item: (
                item.authoritative_game_pk,
                RELATION_ORDER[item.relation],
                canonical_json_bytes(dict(item.exact_key)),
            ),
        )
        seen: dict[tuple[str, bytes], MutationIntentV1] = {}
        proposals: list[MutationProposalV1] = []
        for sequence, intent in enumerate(ordered, start=1):
            identity = (intent.relation, canonical_json_bytes(dict(intent.exact_key)))
            prior = seen.get(identity)
            if prior is not None:
                if prior != intent:
                    raise LoaderContractError("DUPLICATE_EXACT_IDENTITY_CONFLICT", str(identity))
                continue
            seen[identity] = intent
            operation = self._operation(intent)
            proposals.append(
                MutationProposalV1(
                    sequence=sequence,
                    relation=intent.relation,
                    operation=operation,
                    exact_key=dict(intent.exact_key),
                    current_value_hash=(
                        None if intent.current_value is None else content_sha256(intent.current_value)
                    ),
                    proposed_value_hash=(
                        None if intent.proposed_value is None else content_sha256(intent.proposed_value)
                    ),
                    current_substantive_hash=intent.current_substantive_hash,
                    proposed_substantive_hash=intent.proposed_substantive_hash,
                    authoritative_game_pk=intent.authoritative_game_pk,
                    accepted_operational_date=intent.accepted_operational_date,
                    source_evidence=intent.source_evidence,
                    relationship_evidence=dict(intent.relationship_evidence),
                    reason=intent.reason,
                    collision_status=intent.collision_status,
                    rollback_identity=dict(intent.rollback_identity),
                )
            )
        source_set = sorted(
            {
                (source.path, source.sha256, source.observed_at_utc, source.kind)
                for proposal in proposals
                for source in proposal.source_evidence
            }
        )
        source_set_sha256 = content_sha256(source_set)
        expected_state_sha256 = content_sha256(
            [
                {
                    "relation": proposal.relation,
                    "key": dict(proposal.exact_key),
                    "current": proposal.current_value_hash,
                }
                for proposal in proposals
            ]
        )
        body = {
            "contract_version": CONTRACT_VERSION,
            "planner_version": PLANNER_VERSION,
            "source_set_sha256": source_set_sha256,
            "expected_state_sha256": expected_state_sha256,
            "proposals": [proposal.to_dict() for proposal in proposals],
        }
        return MutationPlanV1(
            proposals=tuple(proposals),
            source_set_sha256=source_set_sha256,
            expected_state_sha256=expected_state_sha256,
            plan_sha256=content_sha256(body),
        )


@dataclass(frozen=True)
class MutationAuthorizationV1:
    authorization_id: str
    plan_sha256: str
    source_set_sha256: str
    expected_state_sha256: str
    issued_at_utc: str
    expires_at_utc: str
    authorized_game_pks: tuple[int, ...]
    artifact_sha256: str
    version: str = AUTHORIZATION_VERSION

    @classmethod
    def create(
        cls,
        *,
        authorization_id: str,
        plan: MutationPlanV1,
        issued_at_utc: str,
        expires_at_utc: str,
        authorized_game_pks: Iterable[int],
    ) -> "MutationAuthorizationV1":
        body = {
            "version": AUTHORIZATION_VERSION,
            "authorization_id": authorization_id,
            "plan_sha256": plan.plan_sha256,
            "source_set_sha256": plan.source_set_sha256,
            "expected_state_sha256": plan.expected_state_sha256,
            "issued_at_utc": issued_at_utc,
            "expires_at_utc": expires_at_utc,
            "authorized_game_pks": sorted({int(value) for value in authorized_game_pks}),
        }
        return cls(**body, artifact_sha256=content_sha256(body))

    def verify(self, plan: MutationPlanV1, *, now_utc: str) -> None:
        body = {
            "version": self.version,
            "authorization_id": self.authorization_id,
            "plan_sha256": self.plan_sha256,
            "source_set_sha256": self.source_set_sha256,
            "expected_state_sha256": self.expected_state_sha256,
            "issued_at_utc": self.issued_at_utc,
            "expires_at_utc": self.expires_at_utc,
            "authorized_game_pks": list(self.authorized_game_pks),
        }
        if content_sha256(body) != self.artifact_sha256:
            raise LoaderContractError("AUTHORIZATION_ARTIFACT_HASH_MISMATCH")
        if (
            self.plan_sha256 != plan.plan_sha256
            or self.source_set_sha256 != plan.source_set_sha256
            or self.expected_state_sha256 != plan.expected_state_sha256
        ):
            raise LoaderContractError("AUTHORIZATION_PLAN_MISMATCH")
        now = _utc(now_utc, "AUTHORIZATION_NOW_INVALID")
        if now < _utc(self.issued_at_utc, "AUTHORIZATION_ISSUED_AT_INVALID"):
            raise LoaderContractError("AUTHORIZATION_NOT_YET_VALID")
        if now >= _utc(self.expires_at_utc, "AUTHORIZATION_EXPIRES_AT_INVALID"):
            raise LoaderContractError("AUTHORIZATION_STALE")
        games = {proposal.authoritative_game_pk for proposal in plan.proposals}
        if games != set(self.authorized_game_pks):
            raise LoaderContractError("AUTHORIZATION_GAME_SCOPE_MISMATCH")


class OrderedMutationBackendV1(Protocol):
    """Narrow adapter required by the inactive ordered executor."""

    def begin_serializable(self) -> None: ...
    def acquire_protection(self, game_pks: Sequence[int]) -> None: ...
    def triggers_and_constraints_enabled(self) -> bool: ...
    def current_value_hash(self, relation: str, exact_key: Mapping[str, Any]) -> str | None: ...
    def completed_plan_hash(self, authorization_id: str) -> str | None: ...
    def write_attempt_claim(self, authorization: MutationAuthorizationV1, plan: MutationPlanV1) -> None: ...
    def write_before_state_receipt(self, authorization: MutationAuthorizationV1, plan: MutationPlanV1) -> None: ...
    def insert_exact_fact(self, proposal: MutationProposalV1) -> None: ...
    def relocate_exact_fact(self, proposal: MutationProposalV1) -> None: ...
    def record_legacy_quarantine(self, proposal: MutationProposalV1) -> None: ...
    def validate_expected_state(self, plan: MutationPlanV1) -> bool: ...
    def write_completion_receipt(self, authorization: MutationAuthorizationV1, plan: MutationPlanV1) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


@dataclass(frozen=True)
class ExecutionResultV1:
    status: str
    mutation_count: int
    verification_count: int
    plan_sha256: str


class OrderedMutationExecutorV1:
    """Deterministically ordered executor; inactive without explicit authorization."""

    def execute(
        self,
        plan: MutationPlanV1,
        backend: OrderedMutationBackendV1,
        *,
        authorization: MutationAuthorizationV1 | None = None,
        execute: bool = False,
        now_utc: str | None = None,
    ) -> ExecutionResultV1:
        if not execute:
            return ExecutionResultV1("DRY_RUN_NO_MUTATION", 0, 0, plan.plan_sha256)
        if authorization is None:
            raise LoaderContractError("AUTHORIZATION_ARTIFACT_REQUIRED")
        authorization.verify(plan, now_utc=now_utc or datetime.now(timezone.utc).isoformat())
        blocking = [
            item for item in plan.proposals
            if item.operation in {BLOCK_CONFLICTING_PAYLOAD, UNPROVABLE_FAIL_CLOSED}
        ]
        if blocking:
            raise LoaderContractError("PLAN_CONTAINS_BLOCKING_OPERATIONS", str(len(blocking)))
        mutation_count = 0
        verification_count = 0
        backend.begin_serializable()
        try:
            games = sorted({item.authoritative_game_pk for item in plan.proposals})
            backend.acquire_protection(games)
            if not backend.triggers_and_constraints_enabled():
                raise LoaderContractError("TRIGGERS_OR_CONSTRAINTS_DISABLED")
            prior = backend.completed_plan_hash(authorization.authorization_id)
            if prior is not None:
                if prior != plan.plan_sha256 or not backend.validate_expected_state(plan):
                    raise LoaderContractError("COMPLETED_RECEIPT_STATE_MISMATCH")
                backend.rollback()
                return ExecutionResultV1("IDEMPOTENT_COMPLETED_PLAN_MATCH", 0, 1, plan.plan_sha256)
            for proposal in plan.proposals:
                current = backend.current_value_hash(proposal.relation, proposal.exact_key)
                if current != proposal.current_value_hash:
                    raise LoaderContractError(
                        "CONCURRENT_STATE_MISMATCH",
                        f"{proposal.relation}:{dict(proposal.exact_key)}",
                    )
            backend.write_attempt_claim(authorization, plan)
            backend.write_before_state_receipt(authorization, plan)
            execution_order = sorted(
                plan.proposals,
                key=lambda item: (
                    EXECUTION_OPERATION_ORDER[item.operation],
                    item.authoritative_game_pk,
                    RELATION_ORDER[item.relation],
                    canonical_json_bytes(dict(item.exact_key)),
                ),
            )
            for proposal in execution_order:
                if proposal.operation == INSERT_NEW_EXACT_FACT:
                    backend.insert_exact_fact(proposal)
                    mutation_count += 1
                elif proposal.operation == RELOCATE_MATCHING_MISDATED_FACT:
                    backend.relocate_exact_fact(proposal)
                    mutation_count += 1
                elif proposal.operation == QUARANTINE_CONFLICTING_LEGACY_IDENTITY:
                    backend.record_legacy_quarantine(proposal)
                    mutation_count += 1
                elif proposal.operation in {MATCH_EXISTING_EXACT_FACT, NO_ACTION}:
                    verification_count += 1
                else:
                    raise LoaderContractError("EXECUTOR_OPERATION_NOT_ALLOWED", proposal.operation)
            if not backend.validate_expected_state(plan):
                raise LoaderContractError("EXPECTED_COUNT_OR_HASH_MISMATCH")
            backend.write_completion_receipt(authorization, plan)
            backend.commit()
            return ExecutionResultV1("COMMITTED", mutation_count, verification_count, plan.plan_sha256)
        except Exception:
            backend.rollback()
            raise


def assert_no_legacy_derived_write(plan: MutationPlanV1) -> None:
    for proposal in plan.proposals:
        if proposal.relation == LEGACY_DERIVED_RELATION and proposal.operation not in {
            QUARANTINE_CONFLICTING_LEGACY_IDENTITY,
            NO_ACTION,
        }:
            raise LoaderContractError("LEGACY_DERIVED_WRITE_FORBIDDEN")

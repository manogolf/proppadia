"""Offline contract and fail-closed orchestration for MLB schema activation.

Importing this module has no database or network side effects. Database access
is isolated in the CLI and is only reachable with ``--mode apply`` and a valid
single-use authorization artifact.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol

CONTRACT = "MLB_PROJECT_OWNED_APPEND_ONLY_LEDGER_V1"
RUNNER_VERSION = "MLB_MIGRATION_LEDGER_GUARDED_RUNNER_V1"
LEDGER = "mlb.schema_migration_ledger_v1"
BOOTSTRAP_ID = "MLB_20260924_SCHEMA_MIGRATION_LEDGER_V1"
EXACT_ID = "MLB_20260924_PLAYER_GAME_FEATURE_STATE_V1"
ROLLBACK_ID = "MLB_20260924_PLAYER_GAME_FEATURE_STATE_V1_COMPENSATING_ROLLBACK"
EXACT_SHA256 = "15893ea94b02c13ce0d5d7797a4bc546df250fee06b36e66ef8531525d1f897d"
ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SQL = ROOT / "backend/mlb/sql/migrations/20260924_bootstrap_schema_migration_ledger_v1.sql"
EXACT_SQL = ROOT / "backend/mlb/sql/migrations/20260924_prepare_player_game_feature_state_v1.sql"
MAX_LOCK_TIMEOUT_MS = 30_000
MAX_STATEMENT_TIMEOUT_MS = 120_000
TASK_ADVISORY_LOCK = (20260924, 15893)
EXPECTED_LEGACY_RELATIONS = (
    "mlb.game_info", "mlb.player_stats", "mlb.model_training_props", "mlb.player_derived_stats",
)
EXACT_GAMEPK_PROVEN_RELATIONS = frozenset(("mlb.game_info", "mlb.player_stats", "mlb.model_training_props"))


class GovernanceError(RuntimeError):
    """A fail-closed governance/precondition error with a stable code."""


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def valid_sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


@dataclass(frozen=True)
class LedgerRecordV1:
    migration_id: str
    migration_sha256: str
    migration_kind: str
    description: str
    parent_migration_id: str | None = None

    def __post_init__(self) -> None:
        if not self.migration_id or not valid_sha(self.migration_sha256):
            raise GovernanceError("LEDGER_RECORD_INVALID")


@dataclass(frozen=True)
class AuthorizationV1:
    operation_id: str
    target_database_identity: str
    bootstrap: LedgerRecordV1
    exact: LedgerRecordV1
    expected_pre_state: Mapping[str, Any]
    expected_created_objects: tuple[str, ...]
    expected_zero_row_relations: tuple[str, ...]
    issued_at_utc: str
    expires_at_utc: str
    allowed_runner_version: str
    nonce: str
    authorization_statement: str
    artifact_sha256: str

    @classmethod
    def load(cls, path: Path) -> "AuthorizationV1":
        if not path.is_file():
            raise GovernanceError("AUTHORIZATION_ARTIFACT_REQUIRED")
        try:
            raw = json.loads(path.read_text())
            def record(v: Mapping[str, Any]) -> LedgerRecordV1:
                return LedgerRecordV1(**v)
            raw["bootstrap"] = record(raw["bootstrap"])
            raw["exact"] = record(raw["exact"])
            raw["expected_created_objects"] = tuple(raw["expected_created_objects"])
            raw["expected_zero_row_relations"] = tuple(raw["expected_zero_row_relations"])
            return cls(**raw)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise GovernanceError("AUTHORIZATION_ARTIFACT_INVALID") from exc

    def payload(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "target_database_identity": self.target_database_identity,
            "bootstrap": self.bootstrap.__dict__, "exact": self.exact.__dict__,
            "expected_pre_state": self.expected_pre_state,
            "expected_created_objects": self.expected_created_objects,
            "expected_zero_row_relations": self.expected_zero_row_relations,
            "issued_at_utc": self.issued_at_utc, "expires_at_utc": self.expires_at_utc,
            "allowed_runner_version": self.allowed_runner_version, "nonce": self.nonce,
            "authorization_statement": self.authorization_statement,
        }

    def verify(self, *, target_identity: str, now_utc: str, used_nonces: set[str]) -> None:
        if not valid_sha(self.artifact_sha256) or sha256_bytes(canonical(self.payload())) != self.artifact_sha256:
            raise GovernanceError("AUTHORIZATION_HASH_MISMATCH")
        if self.target_database_identity != target_identity:
            raise GovernanceError("AUTHORIZATION_TARGET_MISMATCH")
        if self.allowed_runner_version != RUNNER_VERSION:
            raise GovernanceError("AUTHORIZATION_RUNNER_VERSION_MISMATCH")
        if self.nonce in used_nonces:
            raise GovernanceError("AUTHORIZATION_NONCE_REUSED")
        try:
            now = datetime.fromisoformat(now_utc.replace("Z", "+00:00"))
            issued = datetime.fromisoformat(self.issued_at_utc.replace("Z", "+00:00"))
            expires = datetime.fromisoformat(self.expires_at_utc.replace("Z", "+00:00"))
        except ValueError as exc:
            raise GovernanceError("AUTHORIZATION_TIME_INVALID") from exc
        if any(t.tzinfo is None for t in (now, issued, expires)) or now < issued or now >= expires:
            raise GovernanceError("AUTHORIZATION_EXPIRED_OR_NOT_YET_VALID")
        if (self.operation_id != "MLB_2026_PROJECT_MIGRATION_LEDGER_BOOTSTRAP_AND_EXACT_GAME_SCHEMA_ACTIVATION_V1"
                or self.bootstrap.migration_id != BOOTSTRAP_ID or self.exact.migration_id != EXACT_ID):
            raise GovernanceError("AUTHORIZATION_SCOPE_TOO_BROAD_OR_WRONG")
        if (not valid_sha(self.target_database_identity) or len(self.nonce) < 24
                or not self.authorization_statement.strip() or len(self.authorization_statement) > 2000):
            raise GovernanceError("AUTHORIZATION_ARTIFACT_FIELDS_INVALID")
        if set(self.expected_pre_state) != {"postgres_server_version_num", "absent_objects", "present_objects", "legacy_relation_definition_hashes", "legacy_game_pk_evidence", "migration_ids_absent", "migration_sha256s_absent"}:
            raise GovernanceError("AUTHORIZATION_SCOPE_TOO_BROAD_OR_PRESTATE_INVALID")
        if not isinstance(self.expected_pre_state["postgres_server_version_num"], int) or self.expected_pre_state["postgres_server_version_num"] < 10000:
            raise GovernanceError("AUTHORIZATION_POSTGRES_VERSION_INVALID")
        plan = build_plan()
        if (self.bootstrap != plan.bootstrap or self.exact != plan.exact
                or set(self.expected_created_objects) != set(plan.objects)
                or set(self.expected_pre_state.get("absent_objects", ())) != {LEDGER, "mlb.player_game_feature_state_v1"}
                or set(self.expected_pre_state.get("present_objects", ())) - set(self.expected_pre_state.get("legacy_relation_definition_hashes", {}))):
            raise GovernanceError("AUTHORIZATION_SCOPE_TOO_BROAD_OR_INPUT_HASH_MISMATCH")
        if set(self.expected_pre_state.get("migration_ids_absent", ())) != {BOOTSTRAP_ID, EXACT_ID}:
            raise GovernanceError("AUTHORIZATION_MIGRATION_ID_PRESTATE_MISMATCH")
        if set(self.expected_pre_state.get("migration_sha256s_absent", ())) != {plan.bootstrap.migration_sha256, plan.exact.migration_sha256}:
            raise GovernanceError("AUTHORIZATION_MIGRATION_CHECKSUM_PRESTATE_MISMATCH")
        definition_hashes = self.expected_pre_state.get("legacy_relation_definition_hashes", {})
        game_evidence = self.expected_pre_state.get("legacy_game_pk_evidence", {})
        if (set(definition_hashes) != set(game_evidence)
                or set(definition_hashes) != set(EXPECTED_LEGACY_RELATIONS)
                or any(set(v) != {"824785", "824784"} for v in game_evidence.values())):
            raise GovernanceError("AUTHORIZATION_LEGACY_SCOPE_INVALID")
        for relation, by_game in game_evidence.items():
            label = ("EXACT_GAMEPK_PROVEN" if relation in EXACT_GAMEPK_PROVEN_RELATIONS
                     else "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY")
            for evidence in by_game.values():
                if (not isinstance(evidence, Mapping) or not isinstance(evidence.get("count"), int)
                        or evidence["count"] < 0 or not valid_sha(evidence.get("sha256"))
                        or not evidence.get("index") or evidence.get("explain_used_index") is not True
                        or evidence.get("exact_game_identity_proven") is not (relation in EXACT_GAMEPK_PROVEN_RELATIONS)
                        or evidence.get("identity_evidence_label") != label):
                    raise GovernanceError("AUTHORIZATION_LEGACY_EVIDENCE_INVALID")
        if self.exact.migration_sha256 != EXACT_SHA256:
            raise GovernanceError("EXACT_MIGRATION_SHA_MISMATCH")
        if self.expected_zero_row_relations != ("mlb.player_game_feature_state_v1",):
            raise GovernanceError("AUTHORIZATION_ZERO_ROW_SCOPE_MISMATCH")


@dataclass(frozen=True)
class PreflightV1:
    mode: str
    project_ledger_exists: bool
    ledger_owner: str | None
    ledger_definition_sha256: str | None
    ledger_contract_valid: bool
    can_select: bool
    can_insert: bool
    internal_ledger_present: bool
    absent_objects: tuple[str, ...]
    existing_migration_ids: tuple[str, ...]
    existing_migration_hashes: Mapping[str, str]
    target_database_identity: str
    legacy_relation_definition_hashes: Mapping[str, str]
    legacy_game_pk_evidence: Mapping[str, Mapping[str, Any]]

    def readiness(self, authorization: AuthorizationV1 | None = None) -> str:
        if self.internal_ledger_present:
            return "BLOCKED_SUPABASE_INTERNAL_LEDGER_REJECTED"
        if self.mode not in ("ordinary", "bootstrap"):
            return "BLOCKED_UNKNOWN_MIGRATION_MODE"
        if self.mode == "ordinary" and not self.project_ledger_exists:
            return "BLOCKED_MIGRATION_GOVERNANCE_UNPROVEN"
        if self.mode == "bootstrap" and self.project_ledger_exists:
            return "BLOCKED_BOOTSTRAP_LEDGER_ALREADY_EXISTS"
        if self.project_ledger_exists and (
            self.ledger_owner != "postgres" or self.ledger_definition_sha256 != file_sha256(BOOTSTRAP_SQL)
            or not self.ledger_contract_valid
            or not self.can_select or not self.can_insert
        ):
            return "BLOCKED_PROJECT_LEDGER_CONTRACT_MISMATCH"
        if authorization:
            if authorization.bootstrap.migration_id in self.existing_migration_ids or authorization.exact.migration_id in self.existing_migration_ids:
                return "BLOCKED_MIGRATION_ID_ALREADY_APPLIED"
            for record in (authorization.bootstrap, authorization.exact):
                prior = self.existing_migration_hashes.get(record.migration_id)
                if prior is not None and prior != record.migration_sha256:
                    return "BLOCKED_MIGRATION_ID_CHECKSUM_CONFLICT"
        return "READY_FOR_GUARDED_TRANSACTION"


@dataclass(frozen=True)
class PlanV1:
    bootstrap: LedgerRecordV1
    exact: LedgerRecordV1
    objects: tuple[str, ...]

    def steps(self) -> tuple[str, ...]:
        return (
            "VERIFY_COMMITTED_INPUT_BYTES", "VERIFY_TARGET_AND_PRESTATE", "CHECK_CONFLICTING_LOCKS",
            "ACQUIRE_TASK_ADVISORY_LOCK", "BEGIN_SERIALIZABLE", "CREATE_LEDGER",
            "VALIDATE_LEDGER_AND_GRANTS", "INSERT_BOOTSTRAP_RECORD", "APPLY_EXACT_GAME_MIGRATION",
            "VALIDATE_EXACT_GAME_SCHEMA_EMPTY", "INSERT_EXACT_GAME_RECORD",
            "VALIDATE_BOTH_RECORDS_AND_LEGACY_BOUNDARY", "COMMIT",
        )


def build_plan(bootstrap_sha256: str | None = None) -> PlanV1:
    actual_bootstrap = file_sha256(BOOTSTRAP_SQL)
    supplied = bootstrap_sha256 or actual_bootstrap
    if not valid_sha(supplied) or supplied != actual_bootstrap:
        raise GovernanceError("BOOTSTRAP_MIGRATION_SHA_MISMATCH")
    if file_sha256(EXACT_SQL) != EXACT_SHA256:
        raise GovernanceError("EXACT_MIGRATION_INPUT_BYTE_IDENTITY_MISMATCH")
    return PlanV1(
        LedgerRecordV1(BOOTSTRAP_ID, actual_bootstrap, "BOOTSTRAP", "Bootstrap project-owned append-only migration ledger"),
        LedgerRecordV1(EXACT_ID, EXACT_SHA256, "SCHEMA", "Create empty exact-player/game feature-state relation", BOOTSTRAP_ID),
        (LEDGER, "mlb.reject_schema_migration_ledger_v1_mutation", "mlb.schema_migration_ledger_v1_append_only",
         "mlb.player_game_feature_state_v1", "mlb.reject_player_game_feature_state_v1_mutation",
         "mlb.player_game_feature_state_v1_append_only", "mlb.idx_player_game_feature_state_v1_game",
         "mlb.idx_player_game_feature_state_v1_cutoff"),
    )


class BackendV1(Protocol):
    def acquire_lock(self) -> None: ...
    def begin_serializable(self) -> None: ...
    def apply_bootstrap(self) -> None: ...
    def validate_ledger(self) -> bool: ...
    def insert_record(self, record: LedgerRecordV1) -> None: ...
    def apply_exact_schema(self) -> None: ...
    def validate_exact_empty(self) -> bool: ...
    def validate_ledger_records(self, records: tuple[LedgerRecordV1, LedgerRecordV1]) -> bool: ...
    def validate_legacy(self, definition_hashes: Mapping[str, str], game_evidence: Mapping[str, Any]) -> bool: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


class GuardedRunnerV1:
    def execute(self, preflight: PreflightV1, backend: BackendV1,
                authorization: AuthorizationV1 | None = None, *, mode: str = "inspect",
                now_utc: str | None = None, used_nonces: set[str] | None = None) -> str:
        if mode not in ("inspect", "plan", "verify", "apply"):
            raise GovernanceError("RUNNER_MODE_INVALID")
        if mode != "apply":
            return "INSPECT_ONLY" if mode == "inspect" else ("PLAN_ONLY_NO_DATABASE_MUTATION" if mode == "plan" else "VERIFY_ONLY")
        if authorization is None:
            raise GovernanceError("AUTHORIZATION_ARTIFACT_REQUIRED")
        readiness = preflight.readiness(authorization)
        if readiness != "READY_FOR_GUARDED_TRANSACTION":
            raise GovernanceError(readiness)
        used = used_nonces if used_nonces is not None else set()
        authorization.verify(target_identity=preflight.target_database_identity,
                             now_utc=now_utc or datetime.now(timezone.utc).isoformat(), used_nonces=used)
        try:
            backend.acquire_lock()
            backend.begin_serializable()
            backend.apply_bootstrap()
            if not backend.validate_ledger(): raise GovernanceError("LEDGER_DEFINITION_OR_ENFORCEMENT_MISMATCH")
            backend.insert_record(authorization.bootstrap)
            backend.apply_exact_schema()
            if not backend.validate_exact_empty(): raise GovernanceError("EXACT_SCHEMA_ASSERTION_FAILED")
            backend.insert_record(authorization.exact)
            if not backend.validate_ledger_records((authorization.bootstrap, authorization.exact)):
                raise GovernanceError("TWO_RECORD_LEDGER_ASSERTION_FAILED")
            if not backend.validate_legacy(preflight.legacy_relation_definition_hashes, preflight.legacy_game_pk_evidence):
                raise GovernanceError("LEGACY_BOUNDARY_MISMATCH")
            backend.commit()
            used.add(authorization.nonce)
            return "COMMITTED_TWO_RECORD_EMPTY_SCHEMA"
        except Exception:
            backend.rollback()
            raise


def compensating_rollback_spec(exact_record: LedgerRecordV1, rollback_sha256: str) -> LedgerRecordV1:
    if exact_record.migration_id != EXACT_ID or exact_record.migration_sha256 != EXACT_SHA256 or not valid_sha(rollback_sha256):
        raise GovernanceError("COMPENSATING_ROLLBACK_INVALID")
    return LedgerRecordV1(ROLLBACK_ID, rollback_sha256, "COMPENSATING_ROLLBACK",
                          "Compensating rollback of exact-game schema; retain ledger history", EXACT_ID)


def compensating_rollback_steps() -> tuple[str, ...]:
    """Prepared runner sequence; requires its own authorization and is never part of apply."""
    return (
        "VERIFY_SEPARATE_ROLLBACK_AUTHORIZATION", "VERIFY_ORIGINAL_EXACT_RECORD",
        "VERIFY_EXACT_GAME_OBJECTS_AND_LEGACY_BOUNDARY", "ACQUIRE_TASK_ADVISORY_LOCK",
        "BEGIN_SERIALIZABLE", "DROP_ONLY_ALLOWLISTED_EXACT_GAME_V1_OBJECTS",
        "VERIFY_LEDGER_REMAINS_AND_LEGACY_BOUNDARY", "INSERT_CHILD_COMPENSATING_RECORD",
        "COMMIT_OR_ROLLBACK_ALL",
    )

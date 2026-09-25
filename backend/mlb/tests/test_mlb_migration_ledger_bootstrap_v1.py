from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.mlb.migration_ledger_bootstrap_v1 import (
    AuthorizationV1, BOOTSTRAP_ID, BOOTSTRAP_SQL, EXACT_ID, EXACT_SHA256,
    EXACT_SQL, LEDGER, RUNNER_VERSION, GovernanceError, GuardedRunnerV1,
    LedgerRecordV1, PreflightV1, build_plan, canonical, compensating_rollback_spec,
    compensating_rollback_steps, file_sha256,
)
from backend.mlb.scripts.validate_mlb_schema_migration_preflight_v1 import (
    false_ready_regression_fixture, validate,
)

ROOT = Path(__file__).resolve().parents[3]


def plan():
    return build_plan()


def authorization(**overrides):
    p = plan()
    values = dict(
        operation_id="MLB_2026_PROJECT_MIGRATION_LEDGER_BOOTSTRAP_AND_EXACT_GAME_SCHEMA_ACTIVATION_V1",
        target_database_identity="a" * 64,
        bootstrap=p.bootstrap, exact=p.exact,
        expected_pre_state={"absent_objects": [LEDGER, "mlb.player_game_feature_state_v1"],
                            "present_objects": [], "legacy_state_hashes": {"mlb.player_stats": "b" * 64},
                            "legacy_counts": {"mlb.player_stats": 100},
                            "migration_ids_absent": [BOOTSTRAP_ID, EXACT_ID],
                            "migration_sha256s_absent": [p.bootstrap.migration_sha256, p.exact.migration_sha256]},
        expected_created_objects=p.objects,
        expected_zero_row_relations=("mlb.player_game_feature_state_v1",),
        issued_at_utc="2026-09-24T00:00:00Z", expires_at_utc="2026-09-25T00:00:00Z",
        allowed_runner_version=RUNNER_VERSION, nonce="one-time-test-nonce-00001",
        authorization_statement="Operator explicitly authorizes exact empty schema activation.",
    )
    values.update(overrides)
    values.pop("artifact_sha256", None)
    auth = AuthorizationV1(**values, artifact_sha256="pending")
    return AuthorizationV1(**values, artifact_sha256=hashlib.sha256(canonical(auth.payload())).hexdigest())


def preflight(**overrides):
    values = dict(mode="bootstrap", project_ledger_exists=False, ledger_owner=None,
                  ledger_definition_sha256=None, ledger_contract_valid=False, can_select=True,
                  can_insert=True, internal_ledger_present=False,
                  absent_objects=(LEDGER, "mlb.player_game_feature_state_v1"),
                  existing_migration_ids=(), existing_migration_hashes={},
                  target_database_identity="a" * 64,
                  legacy_state_hashes={"mlb.player_stats": "b" * 64},
                  legacy_counts={"mlb.player_stats": 100})
    values.update(overrides)
    return PreflightV1(**values)


class FakeBackend:
    def __init__(self, fail=None):
        self.events, self.fail = [], fail
        self.records, self.committed, self.legacy_mutations = [], False, 0

    def hit(self, name):
        self.events.append(name)
        if self.fail == name:
            raise RuntimeError(name)

    def acquire_lock(self): self.hit("lock")
    def begin_serializable(self): self.hit("serializable")
    def apply_bootstrap(self): self.hit("bootstrap")
    def validate_ledger(self): self.hit("ledger_validate"); return self.fail != "ledger_invalid"
    def insert_record(self, record): self.hit("record_" + record.migration_id); self.records.append(record)
    def apply_exact_schema(self): self.hit("exact_schema")
    def validate_exact_empty(self): self.hit("exact_empty_validate"); return self.fail != "exact_invalid"
    def validate_ledger_records(self, records): self.hit("both_records_validate"); return self.fail != "records_invalid"
    def validate_legacy(self, hashes, counts): self.hit("legacy_validate"); return self.fail != "legacy_invalid" and hashes == preflight().legacy_state_hashes and counts == preflight().legacy_counts
    def commit(self): self.hit("commit"); self.committed = True
    def rollback(self): self.events.append("rollback"); self.records.clear(); self.committed = False


def execute(fake=None, pf=None, auth=None, **kwargs):
    return GuardedRunnerV1().execute(pf or preflight(), fake or FakeBackend(), auth or authorization(),
                                     mode="apply", now_utc="2026-09-24T12:00:00Z", **kwargs)


def test_bootstrap_sql_contract_is_project_owned_bounded_and_transaction_neutral():
    sql = BOOTSTRAP_SQL.read_text()
    assert "CREATE TABLE mlb.schema_migration_ledger_v1" in sql
    for name in ("migration_id", "migration_sha256", "migration_kind", "description", "applied_at_utc", "applied_by", "tool_identity", "parent_migration_id", "target_database_identity_sha256", "metadata"):
        assert name in sql
    assert "PRIMARY KEY" in sql and "timestamptz" in sql and "octet_length(metadata::text) <= 8192" in sql
    assert "BEFORE UPDATE OR DELETE OR TRUNCATE" in sql and "REVOKE ALL" in sql and "GRANT SELECT, INSERT" in sql
    assert "OWNER TO postgres" in sql and "auth." not in sql and "realtime." not in sql and "storage." not in sql
    assert EXACT_SHA256 not in sql


def test_plan_is_two_records_ordered_and_exact_migration_bytes_are_pinned():
    p = plan()
    assert p.bootstrap.migration_id == BOOTSTRAP_ID
    assert p.bootstrap.migration_sha256 == file_sha256(BOOTSTRAP_SQL)
    assert p.exact.migration_id == EXACT_ID and p.exact.migration_sha256 == EXACT_SHA256
    assert file_sha256(EXACT_SQL) == EXACT_SHA256
    assert p.exact.parent_migration_id == BOOTSTRAP_ID
    assert p.steps().index("INSERT_BOOTSTRAP_RECORD") < p.steps().index("INSERT_EXACT_GAME_RECORD")


def test_false_ready_regression_and_explicit_bootstrap_mode():
    fixture = false_ready_regression_fixture()
    assert fixture["mode"] == "ordinary" and not fixture["project_ledger_exists"]
    assert validate(fixture)["classification"] == "BLOCKED_MIGRATION_GOVERNANCE_UNPROVEN"
    assert preflight(mode="ordinary").readiness() == "BLOCKED_MIGRATION_GOVERNANCE_UNPROVEN"
    assert preflight().readiness(authorization()) == "READY_FOR_GUARDED_TRANSACTION"


def test_internal_supabase_ledger_wrong_owner_and_wrong_grants_block():
    assert preflight(internal_ledger_present=True).readiness() == "BLOCKED_SUPABASE_INTERNAL_LEDGER_REJECTED"
    assert preflight(mode="ordinary", project_ledger_exists=True, ledger_owner="wrong", ledger_contract_valid=True,
                     can_select=True, can_insert=True).readiness() == "BLOCKED_PROJECT_LEDGER_CONTRACT_MISMATCH"
    assert preflight(mode="ordinary", project_ledger_exists=True, ledger_owner="postgres", ledger_contract_valid=True,
                     can_select=True, can_insert=False).readiness() == "BLOCKED_PROJECT_LEDGER_CONTRACT_MISMATCH"


def test_authorization_artifact_required_hash_scope_target_expiry_and_nonce():
    with pytest.raises(GovernanceError, match="AUTHORIZATION_ARTIFACT_REQUIRED"):
        GuardedRunnerV1().execute(preflight(), FakeBackend(), None, mode="apply")
    bad_hash = authorization()
    bad_hash = AuthorizationV1(**{**bad_hash.__dict__, "artifact_sha256": "0" * 64})
    with pytest.raises(GovernanceError, match="AUTHORIZATION_HASH_MISMATCH"):
        bad_hash.verify(target_identity="a" * 64, now_utc="2026-09-24T12:00:00Z", used_nonces=set())
    with pytest.raises(GovernanceError, match="AUTHORIZATION_TARGET_MISMATCH"):
        authorization().verify(target_identity="c" * 64, now_utc="2026-09-24T12:00:00Z", used_nonces=set())
    with pytest.raises(GovernanceError, match="EXPIRED"):
        authorization(expires_at_utc="2026-09-24T00:01:00Z").verify(target_identity="a" * 64, now_utc="2026-09-24T12:00:00Z", used_nonces=set())
    with pytest.raises(GovernanceError, match="AUTHORIZATION_NONCE_REUSED"):
        authorization().verify(target_identity="a" * 64, now_utc="2026-09-24T12:00:00Z", used_nonces={"one-time-test-nonce-00001"})
    with pytest.raises(GovernanceError, match="SCOPE"):
        GuardedRunnerV1().execute(preflight(), FakeBackend(), authorization(expected_created_objects=("*",)),
                                  mode="apply", now_utc="2026-09-24T12:00:00Z")


@pytest.mark.parametrize("failure", ["lock", "serializable", "bootstrap", "ledger_validate",
    "record_" + BOOTSTRAP_ID, "exact_schema", "exact_invalid", "record_" + EXACT_ID,
    "both_records_validate", "legacy_validate", "commit"])
def test_any_failure_before_commit_rolls_back_both_records(failure):
    fake = FakeBackend(failure)
    with pytest.raises((RuntimeError, GovernanceError)):
        execute(fake=fake)
    assert fake.events[-1] == "rollback"
    assert fake.records == [] and not fake.committed and fake.legacy_mutations == 0


def test_successful_serializable_commit_is_exactly_two_records_and_zero_target_rows():
    fake, used = FakeBackend(), set()
    result = GuardedRunnerV1().execute(preflight(), fake, authorization(), mode="apply",
                                       now_utc="2026-09-24T12:00:00Z", used_nonces=used)
    assert result == "COMMITTED_TWO_RECORD_EMPTY_SCHEMA"
    assert [r.migration_id for r in fake.records] == [BOOTSTRAP_ID, EXACT_ID]
    assert fake.committed and fake.legacy_mutations == 0
    assert fake.events.index("record_" + BOOTSTRAP_ID) < fake.events.index("record_" + EXACT_ID)
    assert authorization().expected_zero_row_relations == ("mlb.player_game_feature_state_v1",)
    assert authorization().nonce in used


def test_repeat_apply_is_already_applied_and_checksum_id_reuse_conflict():
    auth = authorization()
    assert preflight(existing_migration_ids=(EXACT_ID,)).readiness(auth) == "BLOCKED_MIGRATION_ID_ALREADY_APPLIED"
    assert preflight(existing_migration_hashes={BOOTSTRAP_ID: "f" * 64}).readiness(auth) == "BLOCKED_MIGRATION_ID_CHECKSUM_CONFLICT"
    assert preflight(existing_migration_hashes={EXACT_ID: "f" * 64}).readiness(auth) == "BLOCKED_MIGRATION_ID_CHECKSUM_CONFLICT"


def test_compensation_is_separate_child_record_and_ledger_history_is_retained():
    rollback = compensating_rollback_spec(plan().exact, "b" * 64)
    assert rollback.migration_id.endswith("COMPENSATING_ROLLBACK")
    assert rollback.migration_kind == "COMPENSATING_ROLLBACK"
    assert rollback.parent_migration_id == EXACT_ID
    assert "retain ledger history" in rollback.description
    steps = compensating_rollback_steps()
    assert steps.index("VERIFY_ORIGINAL_EXACT_RECORD") < steps.index("DROP_ONLY_ALLOWLISTED_EXACT_GAME_V1_OBJECTS")
    assert steps.index("DROP_ONLY_ALLOWLISTED_EXACT_GAME_V1_OBJECTS") < steps.index("INSERT_CHILD_COMPENSATING_RECORD")
    rollback_sql = (ROOT / "backend/mlb/sql/migrations/20260924_rollback_player_game_feature_state_v1.sql").read_text()
    assert "DROP TABLE IF EXISTS mlb.player_game_feature_state_v1" in rollback_sql
    assert "DROP TABLE IF EXISTS mlb.player_stats" not in rollback_sql
    assert "schema_migration_ledger_v1" not in rollback_sql


def test_default_inspect_and_plan_modes_do_not_touch_backend():
    backend = FakeBackend()
    for mode in ("inspect", "plan", "verify"):
        assert GuardedRunnerV1().execute(preflight(), backend, mode=mode) in ("INSPECT_ONLY", "PLAN_ONLY_NO_DATABASE_MUTATION", "VERIFY_ONLY")
    assert backend.events == []


def test_preflight_rejects_internal_ledger_and_definition_grant_drift():
    base = {"mode": "ordinary", "supabase_internal_ledger_present": False,
            "project_ledger_exists": True, "ledger_relation": LEDGER,
            "ledger_owner": "postgres", "ledger_definition_sha256": file_sha256(BOOTSTRAP_SQL),
            "can_select": True, "can_insert": True, "expected_absent_objects_verified": True,
            "target_identity": "a" * 64, "migration_ids": []}
    assert validate(base)["readiness"] == "READY"
    for mutate in ({"supabase_internal_ledger_present": True}, {"ledger_owner": "service_role"},
                   {"can_insert": False}, {"ledger_definition_sha256": "0" * 64}):
        assert validate({**base, **mutate})["readiness"] == "BLOCKED"
    assert validate({**base, "migration_ids": [EXACT_ID]})["readiness"] == "BLOCKED"
    assert validate({**base, "migration_sha256s": [plan().exact.migration_sha256]})["readiness"] == "BLOCKED"


def test_migration_id_and_hash_are_absent_before_initial_apply_in_bootstrap_snapshot():
    f = false_ready_regression_fixture()
    f.update(mode="bootstrap", expected_absent_objects_verified=True, project_ledger_exists=False,
             migration_ids=[EXACT_ID])
    assert validate(f)["readiness"] == "BLOCKED"

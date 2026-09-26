from __future__ import annotations

import hashlib
import json
import re
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
from backend.mlb.scripts.run_mlb_migration_ledger_guarded_v1 import (
    LEDGER_SHA256_PATTERN, LEGACY_GAME_IDENTITY_SPECS, _bounded_game_pk_evidence,
    _record_game_pk_diagnostics,
    _eligible_game_identity_index, checksum_constraint_pattern, decode_explain_result,
    game_identity_evidence_label, hash_scoped_rows, plan_uses_index,
    resolve_game_identity_column, validate_exact_migration_allowlist,
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
        expected_pre_state={"postgres_server_version_num": 150008,
                            "absent_objects": [LEDGER, "mlb.player_game_feature_state_v1"],
                            "present_objects": [],
                            "legacy_relation_definition_hashes": {r: "b" * 64 for r in ("mlb.game_info", "mlb.player_stats", "mlb.model_training_props", "mlb.player_derived_stats")},
                            "legacy_game_pk_evidence": {r: {"824785": {"count": 0, "sha256": "c" * 64, "index": "game_id_idx", "explain_used_index": True, "exact_game_identity_proven": r != "mlb.player_derived_stats", "identity_evidence_label": ("EXACT_GAMEPK_PROVEN" if r != "mlb.player_derived_stats" else "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY")}, "824784": {"count": 0, "sha256": "d" * 64, "index": "game_id_idx", "explain_used_index": True, "exact_game_identity_proven": r != "mlb.player_derived_stats", "identity_evidence_label": ("EXACT_GAMEPK_PROVEN" if r != "mlb.player_derived_stats" else "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY")}} for r in ("mlb.game_info", "mlb.player_stats", "mlb.model_training_props", "mlb.player_derived_stats")},
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
                  legacy_relation_definition_hashes={r: "b" * 64 for r in ("mlb.game_info", "mlb.player_stats", "mlb.model_training_props", "mlb.player_derived_stats")},
                  legacy_game_pk_evidence={r: {"824785": {"count": 0, "sha256": "c" * 64, "index": "game_id_idx", "explain_used_index": True, "exact_game_identity_proven": r != "mlb.player_derived_stats", "identity_evidence_label": ("EXACT_GAMEPK_PROVEN" if r != "mlb.player_derived_stats" else "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY")}, "824784": {"count": 0, "sha256": "d" * 64, "index": "game_id_idx", "explain_used_index": True, "exact_game_identity_proven": r != "mlb.player_derived_stats", "identity_evidence_label": ("EXACT_GAMEPK_PROVEN" if r != "mlb.player_derived_stats" else "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY")}} for r in ("mlb.game_info", "mlb.player_stats", "mlb.model_training_props", "mlb.player_derived_stats")})
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
    def validate_legacy(self, definitions, games): self.hit("legacy_validate"); return self.fail != "legacy_invalid" and definitions == preflight().legacy_relation_definition_hashes and games == preflight().legacy_game_pk_evidence
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


def test_postgres_checksum_constraint_query_contract_accepts_only_lowercase_sha256():
    # Fixture mirrors pg_get_constraintdef output. No PostgreSQL server is used here.
    definition = "CHECK ((migration_sha256 ~ '^[0-9a-f]{64}$'::text))"
    pattern = checksum_constraint_pattern(definition)
    assert pattern == LEDGER_SHA256_PATTERN
    matcher = re.compile(pattern).fullmatch
    predicate = lambda value: isinstance(value, str) and matcher(value) is not None
    assert predicate("a" * 64)
    assert predicate("0123456789abcdef" * 4)
    for invalid in ("A" * 64, "g" * 64, "a" * 63, "a" * 65, "", None):
        assert not predicate(invalid)

    assert checksum_constraint_pattern("CHECK (migration_sha256 LIKE '%[0-9a-f]%')") is None
    assert checksum_constraint_pattern("CHECK (migration_sha256 ~ '^[A-F0-9]{64}$')") is None
    assert checksum_constraint_pattern("CHECK ((migration_sha256 ~ '^[0-9a-f]{64}$'::text) OR true)") is None
    runner_source = (ROOT / "backend/mlb/scripts/run_mlb_migration_ledger_guarded_v1.py").read_text()
    assert "pg_get_constraintdef(c.oid)" in runner_source
    assert "checksum_constraint_pattern(definition)" in runner_source
    assert "LIKE '%[0-9a-f]%'" not in runner_source
    assert "migration_sha256 ~ '^[0-9a-f]{64}$'" in BOOTSTRAP_SQL.read_text()
    bootstrap_sql = BOOTSTRAP_SQL.read_text()
    assert "migration_sha256 text NOT NULL CHECK (migration_sha256 ~ '^[0-9a-f]{64}$')" in bootstrap_sql


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


def test_previous_whole_table_fingerprint_timeout_is_replaced_by_catalog_and_key_scoped_checks():
    source = (ROOT / "backend/mlb/scripts/run_mlb_migration_ledger_guarded_v1.py").read_text()
    assert "SELECT to_jsonb(t)::text FROM {rel} AS t ORDER BY to_jsonb(t)::text" not in source
    assert "pg_get_indexdef(i.indexrelid)" in source
    assert "i.indkey[0]=%s" in source and "i.indisvalid AND i.indisready" in source
    assert "WHERE {identity_column}=%s::bigint ORDER BY to_jsonb(t)::text" in source
    assert "EXPLAIN (FORMAT JSON, COSTS) " in source
    assert "SET LOCAL statement_timeout = '5000ms'" in source

    # Prior EXPLAIN shape: full sequential scan feeding a sort for JSON row ordering.
    old_plan = [{"Plan": {"Node Type": "Sort", "Plans": [{"Node Type": "Seq Scan", "Relation Name": "game_info"}]}}]
    bounded_plan = [{"Plan": {"Node Type": "Index Scan", "Index Name": "game_id_idx", "Index Cond": "(game_id = 824785)"}}]
    assert not plan_uses_index(old_plan, "game_id_idx")
    assert plan_uses_index(bounded_plan, "game_id_idx")


def test_psycopg2_explain_cursor_row_decodes_actual_one_column_json_representation():
    # psycopg2 cursor.fetchone() returns a one-element tuple; its JSON value is a decoded list.
    driver_row = ([{"Plan": {"Node Type": "Index Scan", "Index Name": "game_id_idx", "Index Cond": "(game_id = 824785)"}}],)
    assert decode_explain_result(driver_row) == {"Node Type": "Index Scan", "Index Name": "game_id_idx",
                                                "Index Cond": "(game_id = 824785)"}
    assert plan_uses_index(driver_row, "game_id_idx")


def test_index_plan_requires_index_condition_on_game_id_and_scoped_query_uses_bigint_parameter():
    indexed = ([{"Plan": {"Node Type": "Index Scan", "Index Name": "player_stats_game_player_idx",
                           "Index Cond": "(game_id = 824785)"}}],)
    other_index = ([{"Plan": {"Node Type": "Index Scan", "Index Name": "player_stats_other_idx",
                               "Index Cond": "(player_id = 10)"}}],)
    seqscan = ([{"Plan": {"Node Type": "Seq Scan", "Relation Name": "player_stats",
                            "Filter": "(game_id = 824785)"}}],)
    assert plan_uses_index(indexed, "player_stats_game_player_idx", "game_id")
    assert not plan_uses_index(other_index, "player_stats_game_player_idx", "game_id")
    assert not plan_uses_index(seqscan, "player_stats_game_player_idx", "game_id")
    source = (ROOT / "backend/mlb/scripts/run_mlb_migration_ledger_guarded_v1.py").read_text()
    assert "WHERE {identity_column}=%s::bigint" in source
    assert "SET LOCAL enable_seqscan = off" in source
    assert '"default_explain"' in source
    assert '"seqscan_disabled_local_explain"] = _explain_diagnostic' in source
    assert '"definition": str(row[1])' in source
    assert '"index_definition": next(item["definition"]' in source


@pytest.mark.parametrize("malformed", [(), (None,), ("not-json",), ([{"Other": {}}],), ([{"Plan": None}],), ({"Plan": {}},)])
def test_malformed_explain_results_fail_closed(malformed):
    with pytest.raises(GovernanceError, match="EXPLAIN_RESULT"):
        decode_explain_result(malformed)


def test_bounded_game_lookup_blocks_before_row_query_without_leading_key_index():
    class NoIndexCursor:
        def __init__(self): self.calls = []
        def execute(self, query, params=None):
            self.calls.append(query)
        def fetchone(self):
            if "format_type" in self.calls[-1]: return (42, 3, "bigint")
            return None
        def fetchall(self): return []

    cur = NoIndexCursor()
    evidence = _bounded_game_pk_evidence(cur, "mlb.player_stats")
    assert all("SELECT to_jsonb(t)::text" not in sql for sql in cur.calls)
    assert evidence["824785"]["eligible_indexes"] == []
    assert evidence["824785"]["default_explain"] is None
    assert evidence["824785"]["acceptance_reason"] == "NO_ELIGIBLE_BTREE_INDEX"


def test_relation_specific_game_id_mappings_are_supported_by_authoritative_producer_contracts():
    expected = {
        "mlb.game_info": "game_id",
        "mlb.player_stats": "game_id",
        "mlb.model_training_props": "game_id",
        "mlb.player_derived_stats": "game_id",
    }
    assert {relation: spec["column"] for relation, spec in LEGACY_GAME_IDENTITY_SPECS.items()} == expected
    for relation in ("mlb.game_info", "mlb.player_stats", "mlb.model_training_props"):
        assert resolve_game_identity_column(relation) == "game_id"
        assert LEGACY_GAME_IDENTITY_SPECS[relation]["status"] == "AUTHORITATIVE_STATAPI_GAMEPK"
        assert game_identity_evidence_label(relation) == "EXACT_GAMEPK_PROVEN"
    assert game_identity_evidence_label("mlb.player_derived_stats") == "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY"

    producer = (ROOT / "backend/mlb/scripts/insert_mlb_stat_derived.py").read_text()
    assert 'game_id = _to_int(game.get("gamePk"))' in producer
    assert 'out.append((game_pk, game_type, g))' in producer
    assert '"game_id": int(game_id)' in producer
    assert '"game_id": game_id' in producer
    assert "array_agg(DISTINCT ps.game_id ORDER BY ps.game_id) AS exact_game_ids" in producer
    assert "HAVING COUNT(DISTINCT ps.game_id) = 1" in producer
    assert "GROUP BY ps.player_id, ps.game_date, sgd.exact_game_ids" in producer
    assert '"player_derived_stats_semantics": "LEGACY_DAILY_EVIDENCE_NOT_EXACT_GAME_FEATURE_STATE"' in producer


def test_ambiguous_and_unknown_game_identity_mapping_fails_closed():
    with pytest.raises(GovernanceError, match="GAME_PK_RELATION_MAPPING_AMBIGUOUS"):
        resolve_game_identity_column("mlb.player_derived_stats")
    with pytest.raises(GovernanceError, match="GAME_PK_RELATION_MAPPING_UNKNOWN"):
        resolve_game_identity_column("mlb.unreviewed_relation")


def test_player_derived_stats_inspection_is_physical_key_only_not_exact_game_evidence():
    class PhysicalRowsCursor:
        def __init__(self): self.last_query = ""
        def execute(self, query, params=None): self.last_query = query
        def fetchone(self):
            if "format_type" in self.last_query: return (42, 3, "bigint")
            if "EXPLAIN" in self.last_query: return ([{"Plan": {"Node Type": "Index Scan", "Index Name": "player_derived_stats_game_id_idx", "Index Cond": "(game_id = 824785)"}}],)
            raise AssertionError(self.last_query)
        def fetchall(self):
            if "pg_get_indexdef" in self.last_query:
                return [("player_derived_stats_game_id_idx", "CREATE INDEX player_derived_stats_game_id_idx ON mlb.player_derived_stats USING btree (game_id)", "btree", True, True, 3)]
            return []
        def __iter__(self): return iter(())

    evidence = _bounded_game_pk_evidence(PhysicalRowsCursor(), "mlb.player_derived_stats")
    assert evidence["824785"]["count"] == evidence["824784"]["count"] == 0
    assert evidence["824785"]["identity_evidence_label"] == "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY"
    assert evidence["824784"]["identity_evidence_label"] == "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY"
    assert evidence["824785"]["exact_game_identity_proven"] is False
    assert evidence["824784"]["exact_game_identity_proven"] is False
    assert "(game_id)" in evidence["824785"]["index_definition"]
    assert evidence["824785"]["default_explain"]["decoded_plan"]["Index Cond"] == "(game_id = 824785)"


def test_player_stats_rejected_plan_retains_default_and_local_probe_diagnostics():
    index_name = "player_stats_game_player_idx"
    index_definition = "CREATE INDEX player_stats_game_player_idx ON mlb.player_stats USING btree (game_id, player_id)"

    class RejectedPlayerStatsCursor:
        def __init__(self): self.query = ""; self.params = None; self.seqscan_disabled = False; self.explain_count = 0
        def execute(self, query, params=None):
            self.query, self.params = query, params
            if query == "SET LOCAL enable_seqscan = off": self.seqscan_disabled = True
        def fetchone(self):
            if "format_type" in self.query: return (71001, 4, "bigint")
            if "EXPLAIN" in self.query:
                self.explain_count += 1
                if self.explain_count % 2 == 0:
                    return ([{"Plan": {"Node Type": "Index Scan", "Index Name": index_name,
                                      "Index Cond": "(player_id = 4)"}}],)
                return ([{"Plan": {"Node Type": "Seq Scan", "Relation Name": "player_stats",
                                  "Filter": "(game_id = 824785)"}}],)
            raise AssertionError(f"unexpected fetchone after {self.query}")
        def fetchall(self):
            if "pg_get_indexdef" in self.query:
                return [(index_name, index_definition, "btree", True, True, 4)]
            return []

    cur = RejectedPlayerStatsCursor()
    evidence = _bounded_game_pk_evidence(cur, "mlb.player_stats")
    report = {"blockers": []}
    relation_item = {"identity_column": "game_id"}
    assert not _record_game_pk_diagnostics(report, relation_item, "mlb.player_stats", evidence)
    assert relation_item["game_pk_evidence"] is evidence
    assert report["blockers"][0]["diagnostic"]["default_explain"]["decoded_plan"]["Node Type"] == "Seq Scan"
    assert report["blockers"][0]["diagnostic"]["seqscan_disabled_local_explain"]["decoded_plan"]["Node Type"] == "Index Scan"
    assert report["blockers"][0]["classification"] == "NO_ELIGIBLE_INDEX_CONDITION_ON_GAME_ID"
    for game_pk in ("824785", "824784"):
        diagnostic = evidence[game_pk]
        assert diagnostic["relation"] == "mlb.player_stats"
        assert diagnostic["identity_column"] == "game_id"
        assert "WHERE game_id=%s::bigint" in diagnostic["scoped_sql"]
        assert diagnostic["parameter"] == {"value": int(game_pk), "postgres_type": "bigint", "sql_cast": "%s::bigint"}
        assert diagnostic["eligible_indexes"][0]["name"] == index_name
        assert diagnostic["eligible_indexes"][0]["definition"] == index_definition
        assert diagnostic["default_explain"]["decoded_plan"]["Node Type"] == "Seq Scan"
        assert diagnostic["default_explain"]["accepted"] is False
        assert diagnostic["seqscan_disabled_local_explain"]["decoded_plan"]["Node Type"] == "Index Scan"
        assert diagnostic["seqscan_disabled_local_explain"]["index_conditions"] == [
            {"index": index_name, "condition": "(player_id = 4)"}]
        assert diagnostic["accepted"] is False
        assert diagnostic["acceptance_reason"] == "NO_ELIGIBLE_INDEX_CONDITION_ON_GAME_ID"
        assert "count" not in diagnostic and "sha256" not in diagnostic


def test_player_stats_local_probe_acceptance_retains_both_plans_and_index_definition():
    index_name = "player_stats_game_player_idx"
    index_definition = "CREATE INDEX player_stats_game_player_idx ON mlb.player_stats USING btree (game_id, player_id)"

    class ProbeCursor:
        def __init__(self): self.query = ""; self.seqscan_disabled = False; self.explain_count = 0; self.rows = []
        def execute(self, query, params=None):
            self.query = query
            if query == "SET LOCAL enable_seqscan = off": self.seqscan_disabled = True
            if query.startswith("SELECT to_jsonb(t)::text"):
                self.rows = []
        def fetchone(self):
            if "format_type" in self.query: return (71001, 4, "bigint")
            if "EXPLAIN" in self.query:
                self.explain_count += 1
                if self.explain_count % 2 == 0:
                    return ([{"Plan": {"Node Type": "Index Scan", "Index Name": index_name,
                                      "Index Cond": "(game_id = 824785)"}}],)
                return ([{"Plan": {"Node Type": "Seq Scan", "Relation Name": "player_stats",
                                  "Filter": "(game_id = 824785)"}}],)
            raise AssertionError(self.query)
        def fetchall(self):
            if "pg_get_indexdef" in self.query:
                return [(index_name, index_definition, "btree", True, True, 4)]
            return []
        def __iter__(self): return iter(self.rows)

    evidence = _bounded_game_pk_evidence(ProbeCursor(), "mlb.player_stats")
    for diagnostic in evidence.values():
        assert diagnostic["accepted"] is True
        assert diagnostic["acceptance_reason"] == "ACCEPTED_ELIGIBLE_INDEX_CONDITION_ON_GAME_ID"
        assert diagnostic["plan_mode"] == "SEQSCAN_DISABLED_LOCAL_PROBE"
        assert diagnostic["default_explain"]["decoded_plan"]["Node Type"] == "Seq Scan"
        assert diagnostic["seqscan_disabled_local_explain"]["accepted"] is True
        assert diagnostic["index_definition"] == index_definition
        assert diagnostic["count"] == 0
        assert len(diagnostic["sha256"]) == 64


def test_eligible_game_identity_index_selection_requires_valid_unfiltered_btree_prefix():
    class IndexCursor:
        def __init__(self, result): self.result, self.query = result, ""
        def execute(self, query, params=None): self.query = query
        def fetchone(self): return self.result

    eligible = IndexCursor(("player_stats_game_identity_idx",))
    assert _eligible_game_identity_index(eligible, 123, 4) == "player_stats_game_identity_idx"
    assert "i.indisvalid AND i.indisready" in eligible.query
    assert "am.amname='btree'" in eligible.query
    assert "i.indpred IS NULL" in eligible.query and "i.indexprs IS NULL" in eligible.query
    assert "i.indkey[0]=%s" in eligible.query

    for unusable_rows in (None,):  # absent, invalid, not-ready, partial, expression, or non-btree indexes are filtered by the catalog predicate
        unusable = IndexCursor(unusable_rows)
        with pytest.raises(GovernanceError, match="BOUNDED_GAME_PK_INDEX_UNAVAILABLE"):
            _eligible_game_identity_index(unusable, 123, 4)


def test_scoped_gamepk_row_hashes_are_deterministic_and_separate():
    rows_785 = ['{"game_id":824785,"player_id":10}', '{"game_id":824785,"player_id":11}']
    rows_784 = ['{"game_id":824784,"player_id":10}']
    evidence_785 = hash_scoped_rows(sorted(rows_785))
    evidence_784 = hash_scoped_rows(sorted(rows_784))
    assert evidence_785 == (2, "51e9ae2d46114ba937cc408ecbb64c8ac0dc0bb97dc539ec3eb39400f13299b6")
    assert evidence_784 == (1, "b336c342fc4f667199af2503b98ba6b35ed00f4131dc750caf294c9b3e9cadd4")
    assert evidence_785 != evidence_784


def test_exact_migration_is_static_additive_allowlist_with_unchanged_bytes():
    assert validate_exact_migration_allowlist()
    assert file_sha256(EXACT_SQL) == "15893ea94b02c13ce0d5d7797a4bc546df250fee06b36e66ef8531525d1f897d"
    sql = EXACT_SQL.read_text().lower()
    assert "create table if not exists mlb.player_game_feature_state_v1" in sql
    assert "insert into mlb." not in sql and "update mlb." not in sql and "delete from mlb." not in sql
    assert "truncate mlb." not in sql and "alter table mlb.player_stats" not in sql


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

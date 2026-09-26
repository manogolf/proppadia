#!/usr/bin/env python3
"""Guarded MLB schema ledger runner. Default mode is offline plan; never prompts to continue."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from backend.mlb.migration_ledger_bootstrap_v1 import (
    AuthorizationV1, BOOTSTRAP_SQL, EXACT_SQL, EXACT_SHA256, LEDGER, RUNNER_VERSION,
    EXPECTED_LEGACY_RELATIONS, TASK_ADVISORY_LOCK, GovernanceError, build_plan, canonical, file_sha256,
)

LEDGER_SHA256_PATTERN = "^[0-9a-f]{64}$"
LEGACY_GAME_IDENTITY_SPECS = {
    "mlb.game_info": {
        "column": "game_id", "status": "AUTHORITATIVE_STATAPI_GAMEPK",
        "producer_evidence": "insert_mlb_stat_derived._upsert_game_info_min: game.get('gamePk') -> game_id",
    },
    "mlb.player_stats": {
        "column": "game_id", "status": "AUTHORITATIVE_STATAPI_GAMEPK",
        "producer_evidence": "insert_mlb_stat_derived._final_games: int(schedule_game['gamePk']); _extract_player_stats_row preserves game_id",
    },
    "mlb.player_derived_stats": {
        "column": "game_id", "status": "AMBIGUOUS_DAILY_AGGREGATE",
        "producer_evidence": "insert_mlb_stat_derived._refresh_player_derived_stats: player/date aggregate admits only single_game_days with one distinct game_id; still legacy daily evidence, not an exact-game relation",
    },
    "mlb.model_training_props": {
        "column": "game_id", "status": "AUTHORITATIVE_STATAPI_GAMEPK",
        "producer_evidence": "insert_mlb_stat_derived._final_games: int(schedule_game['gamePk']); training row copies loop game_id",
    },
}


def resolve_game_identity_column(relation: str) -> str:
    spec = LEGACY_GAME_IDENTITY_SPECS.get(relation)
    if spec is None:
        raise GovernanceError("GAME_PK_RELATION_MAPPING_UNKNOWN")
    if spec["status"] != "AUTHORITATIVE_STATAPI_GAMEPK":
        raise GovernanceError("GAME_PK_RELATION_MAPPING_AMBIGUOUS")
    return str(spec["column"])


def game_identity_evidence_label(relation: str) -> str:
    spec = LEGACY_GAME_IDENTITY_SPECS.get(relation)
    if spec is None:
        raise GovernanceError("GAME_PK_RELATION_MAPPING_UNKNOWN")
    return ("EXACT_GAMEPK_PROVEN" if spec["status"] == "AUTHORITATIVE_STATAPI_GAMEPK"
            else "PHYSICAL_GAME_ID_EQUALS_REQUESTED_VALUE_ONLY")


def hash_scoped_rows(rows: Any) -> tuple[int, str]:
    digest, count = hashlib.sha256(), 0
    for row_json in rows:
        if isinstance(row_json, (tuple, list)):
            row_json = row_json[0]
        encoded = str(row_json).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big")); digest.update(encoded); count += 1
    return count, digest.hexdigest()


def _catalog_identity_column(cur: Any, relation: str, identity_column: str) -> tuple[int, int, str]:
    schema, name = relation.split(".", 1)
    cur.execute("SELECT c.oid,a.attnum,format_type(a.atttypid,a.atttypmod) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=%s AND a.attnum>0 AND NOT a.attisdropped WHERE n.nspname=%s AND c.relname=%s AND c.relkind='r'", (identity_column, schema, name))
    column = cur.fetchone()
    if not column or column[2] not in ("bigint", "integer"):
        raise GovernanceError("GAME_PK_SCOPE_COLUMN_UNAVAILABLE")
    return column


def _eligible_game_identity_index(cur: Any, table_oid: int, attnum: int) -> str:
    cur.execute("SELECT ic.relname FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid JOIN pg_am am ON am.oid=ic.relam WHERE i.indrelid=%s AND i.indisvalid AND i.indisready AND am.amname='btree' AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts>0 AND i.indkey[0]=%s ORDER BY ic.relname LIMIT 1", (table_oid, attnum))
    row = cur.fetchone()
    if not row:
        raise GovernanceError("BOUNDED_GAME_PK_INDEX_UNAVAILABLE")
    return str(row[0])


def _eligible_game_identity_indexes(cur: Any, table_oid: int, attnum: int) -> list[dict[str, Any]]:
    cur.execute("SELECT ic.relname,pg_get_indexdef(i.indexrelid),am.amname,i.indisvalid,i.indisready,i.indkey[0] FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid JOIN pg_am am ON am.oid=ic.relam WHERE i.indrelid=%s AND i.indisvalid AND i.indisready AND am.amname='btree' AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts>0 AND i.indkey[0]=%s ORDER BY ic.relname", (table_oid, attnum))
    return [{"name": str(row[0]), "definition": str(row[1]), "access_method": str(row[2]),
             "valid": bool(row[3]), "ready": bool(row[4]), "leading_attribute_number": int(row[5])}
            for row in cur.fetchall()]


def _eligible_index_definition(cur: Any, table_oid: int, index_name: str) -> str:
    cur.execute("SELECT pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid WHERE i.indrelid=%s AND ic.relname=%s AND i.indisvalid AND i.indisready AND i.indpred IS NULL AND i.indexprs IS NULL", (table_oid, index_name))
    row = cur.fetchone()
    if not row:
        raise GovernanceError("BOUNDED_GAME_PK_INDEX_DEFINITION_UNAVAILABLE")
    return str(row[0])


def plan_uses_index(plan: Any, index_name: str, identity_column: str | None = None) -> bool:
    plan_tree = decode_explain_result(plan)
    def walk(node: Any) -> bool:
        if not isinstance(node, dict):
            return False
        index_condition = str(node.get("Index Cond", ""))
        index_matches = node.get("Index Name") == index_name
        column_matches = identity_column is None or re.search(r"\b" + re.escape(identity_column) + r"\b", index_condition) is not None
        if index_matches and index_condition and column_matches:
            return True
        return any(walk(child) for child in node.get("Plans", []))
    return walk(plan_tree)


def _index_conditions(plan_tree: Any) -> list[dict[str, str]]:
    conditions: list[dict[str, str]] = []
    def walk(node: Any) -> None:
        if not isinstance(node, dict):
            return
        if node.get("Index Cond") is not None:
            conditions.append({"index": str(node.get("Index Name", "")),
                               "condition": str(node["Index Cond"])})
        for child in node.get("Plans", []):
            walk(child)
    walk(plan_tree)
    return conditions


def _explain_diagnostic(cur: Any, query: str, parameter_value: int,
                        eligible_indexes: list[dict[str, Any]], identity_column: str) -> dict[str, Any]:
    cur.execute("EXPLAIN (FORMAT JSON, COSTS) " + query, (parameter_value,))
    raw = cur.fetchone()
    diagnostic: dict[str, Any] = {"raw_result_repr": repr(raw), "decoded_plan": None,
                                  "index_conditions": [], "accepted": False}
    try:
        plan_tree = decode_explain_result(raw)
    except GovernanceError as exc:
        diagnostic["rejection_reason"] = str(exc)
        return diagnostic
    diagnostic["decoded_plan"] = plan_tree
    diagnostic["index_conditions"] = _index_conditions(plan_tree)
    accepted_index = next((item["name"] for item in eligible_indexes
                           if plan_uses_index(raw, item["name"], identity_column)), None)
    diagnostic["accepted"] = accepted_index is not None
    diagnostic["accepted_index"] = accepted_index
    diagnostic["rejection_reason"] = (None if diagnostic["accepted"]
                                      else "NO_ELIGIBLE_INDEX_CONDITION_ON_GAME_ID")
    return diagnostic


def decode_explain_result(cursor_row: Any) -> dict[str, Any]:
    """Decode psycopg2's one-column EXPLAIN JSON row and fail closed on shape drift."""
    if isinstance(cursor_row, tuple):
        if len(cursor_row) != 1:
            raise GovernanceError("EXPLAIN_RESULT_COLUMN_COUNT_INVALID")
        value = cursor_row[0]
    elif isinstance(cursor_row, list):
        # Supports list-row cursor factories without confusing decoded EXPLAIN JSON.
        value = cursor_row[0] if len(cursor_row) == 1 and isinstance(cursor_row[0], list) else cursor_row
    else:
        raise GovernanceError(f"EXPLAIN_RESULT_ROW_TYPE_INVALID:{type(cursor_row).__name__}")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise GovernanceError("EXPLAIN_RESULT_JSON_INVALID") from exc
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise GovernanceError(f"EXPLAIN_RESULT_PLAN_SHAPE_INVALID:{type(value).__name__}")
    plan = value[0].get("Plan")
    if not isinstance(plan, dict):
        raise GovernanceError("EXPLAIN_RESULT_PLAN_NODE_INVALID")
    return plan


def validate_exact_migration_allowlist() -> bool:
    """Require the pinned additive migration to mention only its approved created objects."""
    if file_sha256(EXACT_SQL) != EXACT_SHA256:
        raise GovernanceError("EXACT_MIGRATION_INPUT_BYTE_IDENTITY_MISMATCH")
    sql = EXACT_SQL.read_text().lower()
    created = set(re.findall(r"\bcreate\s+table\s+if\s+not\s+exists\s+mlb\.([a-z0-9_]+)", sql))
    created.update(re.findall(r"\bcreate\s+index\s+if\s+not\s+exists\s+([a-z0-9_]+)", sql))
    created.update(re.findall(r"\bcreate\s+or\s+replace\s+function\s+mlb\.([a-z0-9_]+)", sql))
    created.update(re.findall(r"\bcreate\s+trigger\s+([a-z0-9_]+)", sql))
    expected = {"player_game_feature_state_v1", "idx_player_game_feature_state_v1_game",
                "idx_player_game_feature_state_v1_cutoff", "reject_player_game_feature_state_v1_mutation",
                "player_game_feature_state_v1_append_only"}
    if created != expected:
        raise GovernanceError("EXACT_MIGRATION_OBJECT_ALLOWLIST_MISMATCH")
    legacy = "|".join(re.escape(r.split(".", 1)[1]) for r in EXPECTED_LEGACY_RELATIONS)
    if re.search(r"\b(insert\s+into|update|delete\s+from|truncate|alter\s+table|drop\s+table)\s+(?:mlb\.)?(?:" + legacy + r")\b", sql):
        raise GovernanceError("EXACT_MIGRATION_LEGACY_MUTATION_FORBIDDEN")
    if "on mlb.player_game_feature_state_v1" not in sql or "before update or delete on mlb.player_game_feature_state_v1" not in sql:
        raise GovernanceError("EXACT_MIGRATION_TARGET_TRIGGER_ALLOWLIST_MISMATCH")
    return True


def checksum_constraint_pattern(definition: Any) -> str | None:
    """Extract the SHA check expression from PostgreSQL's rendered constraint definition."""
    if not isinstance(definition, str):
        return None
    normalized = re.sub(r"\s+", " ", definition.strip()).lower()
    expected = "check ((migration_sha256 ~ '^[0-9a-f]{64}$'::text))"
    if normalized != expected:
        return None
    return LEDGER_SHA256_PATTERN


def target_identity(connection: Any) -> str:
    info = connection.get_dsn_parameters()
    material = {k: info.get(k) for k in ("host", "port", "dbname")}
    return hashlib.sha256(canonical(material)).hexdigest()


def require_exact_artifact_scope(auth: AuthorizationV1) -> None:
    expected_objects = set(build_plan().objects)
    if set(auth.expected_created_objects) != expected_objects:
        raise GovernanceError("AUTHORIZATION_SCOPE_TOO_BROAD_OR_OBJECT_SET_MISMATCH")
    expected_absent = {LEDGER, "mlb.player_game_feature_state_v1"}
    if set(auth.expected_pre_state.get("absent_objects", ())) != expected_absent:
        raise GovernanceError("AUTHORIZATION_PRESTATE_OBJECT_SCOPE_MISMATCH")
    if set(auth.expected_pre_state.get("migration_ids_absent", ())) != {
        auth.bootstrap.migration_id, auth.exact.migration_id
    }:
        raise GovernanceError("AUTHORIZATION_MIGRATION_ID_PRESTATE_MISMATCH")


def consume_nonce(registry: Path, nonce: str) -> None:
    registry.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(registry, os.O_CREAT | os.O_RDWR | os.O_APPEND, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        content = os.read(fd, max(os.fstat(fd).st_size, 1)).decode("utf-8")
        used = set(content.splitlines())
        if nonce in used:
            raise GovernanceError("AUTHORIZATION_NONCE_REUSED")
        os.write(fd, (nonce + "\n").encode("utf-8"))
        os.fsync(fd)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def connect_readonly() -> Any:
    dsn = (os.environ.get("MLB_MIGRATION_DSN") or os.environ.get("SUPABASE_DB_URL")
           or os.environ.get("DATABASE_URL"))
    if not dsn:
        raise GovernanceError("MLB_MIGRATION_DSN_REQUIRED_FOR_DATABASE_MODE")
    try:
        import psycopg2
    except ImportError as exc:
        raise GovernanceError("PSYCOPG2_REQUIRED_FOR_DATABASE_MODE") from exc
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def _relation_state(cur: Any, rel: str) -> tuple[bool, str | None, int | None]:
    schema, name = rel.split(".", 1)
    cur.execute("SELECT to_regclass(%s)", (rel,))
    exists = cur.fetchone()[0] is not None
    if not exists:
        return False, None, None
    cur.execute("SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s", (schema, name))
    owner = cur.fetchone()[0]
    if rel == "mlb.player_game_feature_state_v1":
        cur.execute(f'SELECT EXISTS (SELECT 1 FROM "{schema}"."{name}" LIMIT 1)')
        has_rows = cur.fetchone()[0]
        return True, owner, None if has_rows else 0
    return True, owner, None


def _legacy_definition(cur: Any, rel: str) -> str:
    """Hash catalog definitions only; legacy table rows are never globally scanned."""
    schema, name = rel.split(".", 1)
    cur.execute("SELECT c.oid,c.relkind,pg_get_userbyid(c.relowner),c.relacl::text,c.relrowsecurity,c.relforcerowsecurity,c.relreplident,(SELECT jsonb_agg(jsonb_build_array(a.grantee,a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.privilege_type) FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a)::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s", (schema, name))
    relation = cur.fetchone()
    if not relation or relation[1] != "r":
        raise GovernanceError("LEGACY_RELATION_MISSING_OR_NOT_TABLE")
    oid = relation[0]
    cur.execute("SELECT a.attnum,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid),a.attidentity,a.attgenerated FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", (oid,))
    columns = cur.fetchall()
    cur.execute("SELECT conname,contype,condeferrable,condeferred,convalidated,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s ORDER BY conname", (oid,))
    constraints = cur.fetchall()
    cur.execute("SELECT ic.relname,am.amname,i.indisvalid,i.indisready,i.indisunique,i.indisprimary,i.indnkeyatts,i.indnatts,pg_get_expr(i.indpred,i.indrelid),pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid JOIN pg_am am ON am.oid=ic.relam WHERE i.indrelid=%s ORDER BY ic.relname", (oid,))
    indexes = cur.fetchall()
    cur.execute("SELECT t.tgname,t.tgtype,pg_get_triggerdef(t.oid),p.proname,pg_get_userbyid(p.proowner),p.prosecdef,pg_get_functiondef(p.oid) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=%s AND NOT t.tgisinternal ORDER BY t.tgname", (oid,))
    triggers = cur.fetchall()
    definition = {"relation": relation[1:], "columns": columns, "constraints": constraints, "indexes": indexes, "triggers": triggers}
    return hashlib.sha256(canonical(definition)).hexdigest()


def _bounded_game_pk_evidence(cur: Any, rel: str) -> dict[str, dict[str, Any]]:
    """Use a lineage-verified identity key only after proving an eligible btree index."""
    spec = LEGACY_GAME_IDENTITY_SPECS.get(rel)
    if spec is None:
        raise GovernanceError("GAME_PK_RELATION_MAPPING_UNKNOWN")
    identity_column = str(spec["column"])
    column = _catalog_identity_column(cur, rel, identity_column)
    eligible_indexes = _eligible_game_identity_indexes(cur, column[0], column[1])
    evidence: dict[str, dict[str, Any]] = {}
    for game_pk in (824785, 824784):
        query = f"SELECT to_jsonb(t)::text FROM {rel} AS t WHERE {identity_column}=%s::bigint ORDER BY to_jsonb(t)::text"
        diagnostic: dict[str, Any] = {
            "relation": rel, "identity_column": identity_column, "scoped_sql": query,
            "parameter": {"value": game_pk, "postgres_type": "bigint", "sql_cast": "%s::bigint"},
            "eligible_indexes": eligible_indexes,
        }
        default_explain = (_explain_diagnostic(cur, query, game_pk, eligible_indexes, identity_column)
                           if eligible_indexes else None)
        diagnostic["default_explain"] = default_explain
        chosen = default_explain
        if (default_explain is not None and not default_explain["accepted"]
                and default_explain["decoded_plan"] is not None):
            # The session is READ ONLY and this setting is transaction-local. This
            # tests index-backed feasibility without changing persistent planner state.
            cur.execute("SET LOCAL enable_seqscan = off")
            diagnostic["seqscan_disabled_local_explain"] = _explain_diagnostic(
                cur, query, game_pk, eligible_indexes, identity_column)
            chosen = diagnostic["seqscan_disabled_local_explain"]
        else:
            diagnostic["seqscan_disabled_local_explain"] = None
        diagnostic["accepted"] = bool(chosen and chosen["accepted"])
        diagnostic["acceptance_reason"] = (
            "ACCEPTED_ELIGIBLE_INDEX_CONDITION_ON_GAME_ID" if diagnostic["accepted"]
            else (chosen["rejection_reason"] if chosen is not None else "NO_ELIGIBLE_BTREE_INDEX"))
        index_name = chosen.get("accepted_index") if chosen else None
        evidence[str(game_pk)] = diagnostic
        if not diagnostic["accepted"]:
            continue
        cur.execute(query, (game_pk,))
        count, row_sha256 = hash_scoped_rows(cur)
        diagnostic.update({
            "count": count, "sha256": row_sha256, "index": index_name,
            "index_definition": next(item["definition"] for item in eligible_indexes if item["name"] == index_name),
            "plan_mode": "DEFAULT" if chosen is default_explain else "SEQSCAN_DISABLED_LOCAL_PROBE",
            "decoded_plan": chosen["decoded_plan"],
            "explain_used_index": True,
            "exact_game_identity_proven": spec["status"] == "AUTHORITATIVE_STATAPI_GAMEPK",
            "identity_evidence_label": game_identity_evidence_label(rel),
        })
    return evidence


def _record_game_pk_diagnostics(report: dict[str, Any], item: dict[str, Any],
                                relation: str, evidence: dict[str, dict[str, Any]]) -> bool:
    """Attach every attempted scoped plan before reporting a rejection to the caller."""
    item["game_pk_evidence"] = evidence
    item["eligible_indexes"] = evidence["824785"]["eligible_indexes"]
    item["eligible_index"] = item["eligible_indexes"][0]["name"] if item["eligible_indexes"] else None
    item["explain_index_assertions"] = {
        game_pk: {"index": evidence[game_pk].get("index"),
                  "accepted": evidence[game_pk]["accepted"],
                  "reason": evidence[game_pk]["acceptance_reason"]}
        for game_pk in ("824785", "824784")}
    rejected = next(((game_pk, evidence[game_pk]) for game_pk in ("824785", "824784")
                     if not evidence[game_pk]["accepted"]), None)
    if rejected is None:
        return True
    game_pk, diagnostic = rejected
    item["blocker"] = diagnostic["acceptance_reason"]
    report["blockers"].append({"relation": relation, "game_pk": game_pk,
                               "classification": item["blocker"], "diagnostic": diagnostic})
    return False


def inspect(conn: Any, auth: AuthorizationV1) -> dict[str, Any]:
    require_exact_artifact_scope(auth)
    cur = conn.cursor()
    try:
        identity = target_identity(conn)
        cur.execute("SELECT current_setting('server_version_num')::int")
        server_version_num = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='supabase_migrations'")
        internal = cur.fetchone()[0] > 0
        present: dict[str, Any] = {}
        absent: list[str] = []
        for rel in sorted(set(auth.expected_pre_state["absent_objects"]) | set(auth.expected_pre_state.get("present_objects", []))):
            exists, owner, count = _relation_state(cur, rel)
            (present if exists else absent)[rel] = {"owner": owner, "row_count": count} if exists else None
        migration_ids: list[str] = []
        migration_shas: list[str] = []
        if LEDGER in present:
            cur.execute(f"SELECT migration_id,migration_sha256 FROM {LEDGER}")
            ledger_rows = cur.fetchall()
            migration_ids = [r[0] for r in ledger_rows]
            migration_shas = [r[1] for r in ledger_rows]
        legacy_definitions: dict[str, str] = {}
        legacy_game_evidence: dict[str, Any] = {}
        expected_definitions = auth.expected_pre_state.get("legacy_relation_definition_hashes", {})
        expected_game_evidence = auth.expected_pre_state.get("legacy_game_pk_evidence", {})
        if set(expected_definitions) != set(expected_game_evidence):
            raise GovernanceError("AUTHORIZATION_LEGACY_SNAPSHOT_KEYS_MISMATCH")
        cur.execute("SET LOCAL statement_timeout = '5000ms'")
        for rel in sorted(expected_definitions):
            if rel not in EXPECTED_LEGACY_RELATIONS:
                raise GovernanceError("AUTHORIZATION_LEGACY_SCOPE_TOO_BROAD")
            legacy_definitions[rel] = _legacy_definition(cur, rel)
            legacy_game_evidence[rel] = _bounded_game_pk_evidence(cur, rel)
        return {"target_identity": identity, "supabase_internal_ledger_present": internal,
                "postgres_server_version_num": server_version_num,
                "present_objects": present, "absent_objects": absent, "migration_ids": migration_ids,
                "migration_sha256s": migration_shas,
                "legacy_relation_definition_hashes": legacy_definitions,
                "legacy_game_pk_evidence": legacy_game_evidence}
    finally:
        cur.close()


def inspect_database(conn: Any, expected_target_identity: str) -> dict[str, Any]:
    """Read-only operational preflight without an authorization artifact."""
    import psycopg2

    cur = conn.cursor()
    report: dict[str, Any] = {"mode": "inspect", "legacy_relations": {}, "blockers": []}
    try:
        cur.execute("SET LOCAL statement_timeout = '5000ms'")
        cur.execute("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')::boolean")
        isolation, is_read_only = cur.fetchone()
        report["transaction_isolation"] = isolation
        report["transaction_read_only"] = is_read_only
        if isolation != "repeatable read" or not is_read_only:
            report["blockers"].append({"classification": "READ_ONLY_SNAPSHOT_MODE_INVALID"})
        cur.execute("SELECT current_setting('server_version_num')::int")
        report["postgres_server_version_num"] = cur.fetchone()[0]
        report["target_database_identity"] = target_identity(conn)
        report["target_identity_matches"] = report["target_database_identity"] == expected_target_identity
        if not report["target_identity_matches"]:
            report["blockers"].append({"classification": "CONNECTED_TARGET_IDENTITY_MISMATCH"})
        cur.execute("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='supabase_migrations'")
        report["supabase_migration_relations"] = cur.fetchone()[0]
        cur.execute("SELECT to_regclass('mlb.schema_migration_ledger_v1') IS NOT NULL,to_regprocedure('mlb.reject_schema_migration_ledger_v1_mutation()') IS NOT NULL,to_regclass('mlb.player_game_feature_state_v1') IS NOT NULL,to_regprocedure('mlb.reject_player_game_feature_state_v1_mutation()') IS NOT NULL,to_regclass('mlb.idx_player_game_feature_state_v1_game') IS NOT NULL,to_regclass('mlb.idx_player_game_feature_state_v1_cutoff') IS NOT NULL")
        object_values = cur.fetchone()
        cur.execute("SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='mlb' AND t.tgname IN ('schema_migration_ledger_v1_append_only','player_game_feature_state_v1_append_only') AND NOT t.tgisinternal")
        trigger_names = {row[0] for row in cur.fetchall()}
        object_presence = {name: bool(value) for name, value in zip(
            ("mlb.schema_migration_ledger_v1", "mlb.reject_schema_migration_ledger_v1_mutation",
             "mlb.player_game_feature_state_v1", "mlb.reject_player_game_feature_state_v1_mutation",
             "mlb.idx_player_game_feature_state_v1_game", "mlb.idx_player_game_feature_state_v1_cutoff"), object_values)}
        object_presence["mlb.schema_migration_ledger_v1_append_only"] = "schema_migration_ledger_v1_append_only" in trigger_names
        object_presence["mlb.player_game_feature_state_v1_append_only"] = "player_game_feature_state_v1_append_only" in trigger_names
        report["planned_object_presence"] = object_presence
        ledger_present, exact_present = bool(object_values[0]), bool(object_values[2])
        report["ledger_present"] = ledger_present
        report["exact_game_relation_present"] = exact_present
        cur.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=%s::oid AND objid=%s::oid AND granted", TASK_ADVISORY_LOCK)
        report["task_advisory_lock_holders"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active'")
        report["other_active_sessions"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND (application_name ILIKE '%migration%' OR query ILIKE '%schema_migration_ledger_v1%' OR query ILIKE '%player_game_feature_state_v1%')")
        report["other_migration_sessions"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace WHERE l.pid<>pg_backend_pid() AND l.granted AND l.mode<>'AccessShareLock' AND n.nspname='mlb' AND c.relname=ANY(%s)", (list(r.split(".", 1)[1] for r in EXPECTED_LEGACY_RELATIONS),))
        report["conflicting_legacy_relation_locks"] = cur.fetchone()[0]

        for relation in EXPECTED_LEGACY_RELATIONS:
            spec = LEGACY_GAME_IDENTITY_SPECS[relation]
            item: dict[str, Any] = {"identity_column": spec["column"], "semantic_status": spec["status"],
                                    "producer_evidence": spec["producer_evidence"]}
            report["legacy_relations"][relation] = item
            try:
                item["definition_sha256"] = _legacy_definition(cur, relation)
                column = _catalog_identity_column(cur, relation, str(spec["column"]))
                item["column_type"] = column[2]
                evidence = _bounded_game_pk_evidence(cur, relation)
                if not _record_game_pk_diagnostics(report, item, relation, evidence):
                    break
            except GovernanceError as exc:
                item["blocker"] = str(exc)
                report["blockers"].append({"relation": relation, "classification": item["blocker"]})
                break
            except psycopg2.Error as exc:
                item["blocker"] = type(exc).__name__
                item["sqlstate"] = getattr(exc, "pgcode", None)
                report["blockers"].append({"relation": relation, "classification": item["blocker"],
                                           "sqlstate": item["sqlstate"]})
                break

        if any(object_presence.values()):
            report["blockers"].append({"classification": "EXPECTED_MIGRATION_OBJECT_ALREADY_PRESENT"})
        if report["supabase_migration_relations"]:
            report["blockers"].append({"classification": "BLOCKED_SUPABASE_INTERNAL_LEDGER_REJECTED"})
        if report["task_advisory_lock_holders"]:
            report["blockers"].append({"classification": "TASK_ADVISORY_LOCK_CONFLICT"})
        if report["other_migration_sessions"] or report["conflicting_legacy_relation_locks"]:
            report["blockers"].append({"classification": "CONFLICTING_MIGRATION_SESSION_OR_RELATION_LOCK"})
        report["readiness"] = "READY_FOR_GOVERNED_REVIEW" if not report["blockers"] and len(report["legacy_relations"]) == len(EXPECTED_LEGACY_RELATIONS) else "BLOCKED"
        return report
    finally:
        cur.close()


class PsycopgBackend:
    def __init__(self, conn: Any, auth: AuthorizationV1, timeout_lock_ms: int = 10000, timeout_statement_ms: int = 60000):
        if not (100 <= timeout_lock_ms <= 30000 and 1000 <= timeout_statement_ms <= 120000):
            raise GovernanceError("TIMEOUT_OUT_OF_BOUNDS")
        self.conn, self.auth = conn, auth
        self.lock_timeout_ms, self.statement_timeout_ms = timeout_lock_ms, timeout_statement_ms
        self.cur = None

    def acquire_lock(self) -> None:
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT pg_try_advisory_lock(%s, %s)", TASK_ADVISORY_LOCK)
        if not self.cur.fetchone()[0]: raise GovernanceError("TASK_ADVISORY_LOCK_CONFLICT")

    def begin_serializable(self) -> None:
        self.conn.autocommit = False
        self.cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        self.cur.execute("SELECT set_config('lock_timeout', %s, true)", (f"{self.lock_timeout_ms}ms",))
        self.cur.execute("SELECT set_config('statement_timeout', %s, true)", (f"{self.statement_timeout_ms}ms",))

    def apply_bootstrap(self) -> None:
        self.cur.execute(BOOTSTRAP_SQL.read_text())

    def validate_ledger(self) -> bool:
        self.cur.execute("""SELECT pg_get_userbyid(c.relowner), count(*)
          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='mlb' AND c.relname='schema_migration_ledger_v1' GROUP BY c.relowner""")
        row = self.cur.fetchone()
        if not row or row[0] != "postgres": return False
        self.cur.execute("""SELECT count(*) FROM information_schema.columns WHERE table_schema='mlb'
          AND table_name='schema_migration_ledger_v1' AND column_name IN
          ('migration_id','migration_sha256','migration_kind','description','applied_at_utc','applied_by','tool_identity','parent_migration_id','target_database_identity_sha256','metadata')""")
        if self.cur.fetchone()[0] != 10: return False
        self.cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema='mlb' AND table_name='schema_migration_ledger_v1' AND column_name='applied_at_utc'")
        if self.cur.fetchone() != ("timestamp with time zone",): return False
        self.cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema='mlb' AND table_name='schema_migration_ledger_v1' AND column_name='metadata'")
        if self.cur.fetchone() != ("jsonb",): return False
        self.cur.execute("""SELECT count(*) FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
          JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='mlb'
          AND r.relname='schema_migration_ledger_v1' AND c.contype='p' AND c.conname='schema_migration_ledger_v1_pkey'""")
        if self.cur.fetchone()[0] != 1: return False
        self.cur.execute("""SELECT count(*) FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
          JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='mlb'
          AND r.relname='schema_migration_ledger_v1' AND c.contype='f' AND c.conname='schema_migration_ledger_v1_parent_migration_id_fkey'""")
        if self.cur.fetchone()[0] != 1: return False
        self.cur.execute("""SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
          JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='mlb'
          AND c.relname='schema_migration_ledger_v1' AND NOT t.tgisinternal
          AND t.tgname='schema_migration_ledger_v1_append_only' AND t.tgtype=58""")
        if self.cur.fetchone()[0] != 1: return False
        self.cur.execute("""SELECT pg_get_userbyid(p.proowner), p.prosecdef FROM pg_proc p
          JOIN pg_namespace n ON n.oid=p.pronamespace
          WHERE n.nspname='mlb' AND p.proname='reject_schema_migration_ledger_v1_mutation'""")
        if self.cur.fetchone() != ("postgres", False): return False
        self.cur.execute("""SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
          JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='mlb'
          AND r.relname='schema_migration_ledger_v1' AND c.contype='c'""")
        constraint_definitions = [row[0] for row in self.cur.fetchall()]
        if not any(checksum_constraint_pattern(definition) == LEDGER_SHA256_PATTERN
                   for definition in constraint_definitions): return False
        self.cur.execute("SELECT has_table_privilege('postgres','mlb.schema_migration_ledger_v1','SELECT'), has_table_privilege('postgres','mlb.schema_migration_ledger_v1','INSERT')")
        if self.cur.fetchone() != (True, True): return False
        self.cur.execute("""SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace,
          LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
          WHERE n.nspname='mlb' AND c.relname='schema_migration_ledger_v1'
          AND (a.grantee=0 OR a.grantee<>c.relowner)""")
        return self.cur.fetchone()[0] == 0

    def insert_record(self, record: Any) -> None:
        self.cur.execute(f"""INSERT INTO {LEDGER}
          (migration_id,migration_sha256,migration_kind,description,tool_identity,parent_migration_id,target_database_identity_sha256,metadata)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
          (record.migration_id, record.migration_sha256, record.migration_kind, record.description,
           RUNNER_VERSION, record.parent_migration_id, self.auth.target_database_identity, json.dumps({"contract": "MLB_PROJECT_OWNED_APPEND_ONLY_LEDGER_V1"})))

    def apply_exact_schema(self) -> None:
        validate_exact_migration_allowlist()
        self.cur.execute(EXACT_SQL.read_text())

    def validate_exact_empty(self) -> bool:
        self.cur.execute("SELECT count(*) FROM mlb.player_game_feature_state_v1")
        empty = self.cur.fetchone()[0] == 0
        self.cur.execute("SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='mlb' AND c.relname='player_game_feature_state_v1'")
        owner = self.cur.fetchone()
        self.cur.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='mlb' AND tablename='player_game_feature_state_v1' AND indexname IN ('idx_player_game_feature_state_v1_game','idx_player_game_feature_state_v1_cutoff')")
        indexes = self.cur.fetchone()[0] == 2
        self.cur.execute("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='mlb' AND c.relname='player_game_feature_state_v1' AND t.tgname='player_game_feature_state_v1_append_only' AND t.tgtype=27 AND NOT t.tgisinternal")
        trigger = self.cur.fetchone()[0] == 1
        self.cur.execute("""SELECT pg_get_userbyid(p.proowner) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
          WHERE n.nspname='mlb' AND p.proname='reject_player_game_feature_state_v1_mutation'""")
        function_owner = self.cur.fetchone()
        self.cur.execute("""SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace,
          LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
          WHERE n.nspname='mlb' AND c.relname='player_game_feature_state_v1'
          AND (a.grantee=0 OR a.grantee<>c.relowner)""")
        grants_bounded = self.cur.fetchone()[0] == 0
        return empty and owner == ("postgres",) and indexes and trigger and function_owner == ("postgres",) and grants_bounded

    def validate_ledger_records(self, records: tuple[Any, Any]) -> bool:
        txids = set()
        for record in records:
            self.cur.execute(f"SELECT migration_sha256,migration_kind,parent_migration_id,target_database_identity_sha256,tool_identity,metadata,transaction_id,applied_at_utc FROM {LEDGER} WHERE migration_id=%s", (record.migration_id,))
            row = self.cur.fetchone()
            if not row or row[:5] != (record.migration_sha256, record.migration_kind, record.parent_migration_id,
                                      self.auth.target_database_identity, RUNNER_VERSION): return False
            if row[5] != {"contract": "MLB_PROJECT_OWNED_APPEND_ONLY_LEDGER_V1"} or row[7] is None: return False
            txids.add(row[6])
        self.cur.execute(f"SELECT count(*) FROM {LEDGER} WHERE migration_id IN (%s,%s)", (records[0].migration_id, records[1].migration_id))
        return self.cur.fetchone()[0] == 2 and len(txids) == 1

    def validate_legacy(self, definition_hashes: Any, game_evidence: Any) -> bool:
        if not definition_hashes or set(definition_hashes) != set(game_evidence): return False
        self.cur.execute("SELECT set_config('statement_timeout',%s,true)", (f"{min(self.statement_timeout_ms,5000)}ms",))
        for relation, expected_hash in definition_hashes.items():
            if relation not in EXPECTED_LEGACY_RELATIONS: return False
            if _legacy_definition(self.cur, relation) != expected_hash: return False
            if _bounded_game_pk_evidence(self.cur, relation) != game_evidence[relation]: return False
        return True

    def commit(self) -> None:
        self.conn.commit()
        self.conn.autocommit = True
        self.cur.execute("SELECT pg_advisory_unlock(%s, %s)", TASK_ADVISORY_LOCK)

    def rollback(self) -> None:
        self.conn.rollback()
        self.conn.autocommit = True
        self.cur.execute("SELECT pg_advisory_unlock(%s, %s)", TASK_ADVISORY_LOCK)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("inspect", "plan", "apply", "verify"), default="plan")
    parser.add_argument("--authorization-artifact", type=Path)
    parser.add_argument("--nonce-registry", type=Path, help="Local append-only record of consumed artifact nonces; required for apply")
    parser.add_argument("--target-database-identity", help="Expected lowercase SHA-256 of host/port/database identity")
    parser.add_argument("--expected-bootstrap-sha256")
    parser.add_argument("--expected-exact-migration-sha256")
    parser.add_argument("--lock-timeout-ms", type=int, default=10000)
    parser.add_argument("--statement-timeout-ms", type=int, default=60000)
    args = parser.parse_args()
    try:
        plan = build_plan()
        if args.mode == "plan":
            validate_exact_migration_allowlist()
            print(json.dumps({"mode": args.mode, "plan": plan.steps(), "objects": plan.objects,
                              "bootstrap_sha256": plan.bootstrap.migration_sha256,
                              "exact_sha256": plan.exact.migration_sha256,
                              "operational_execution": "NOT RUN"}, indent=2))
            return 0
        if args.mode == "inspect":
            if not args.target_database_identity:
                raise GovernanceError("EXPECTED_TARGET_IDENTITY_REQUIRED_FOR_INSPECT")
            validate_exact_migration_allowlist()
            conn = connect_readonly()
            try:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                report = inspect_database(conn, args.target_database_identity)
                print(json.dumps(report, sort_keys=True, indent=2))
                conn.rollback()
                return 0 if report["readiness"] == "READY_FOR_GOVERNED_REVIEW" else 2
            finally:
                conn.close()
        if args.authorization_artifact is None: raise GovernanceError("AUTHORIZATION_ARTIFACT_REQUIRED")
        auth = AuthorizationV1.load(args.authorization_artifact)
        require_exact_artifact_scope(auth)
        if auth.bootstrap.migration_sha256 != plan.bootstrap.migration_sha256 or auth.exact.migration_sha256 != plan.exact.migration_sha256:
            raise GovernanceError("MIGRATION_INPUT_HASH_MISMATCH")
        if args.mode == "apply" and (args.expected_bootstrap_sha256 != plan.bootstrap.migration_sha256
                                      or args.expected_exact_migration_sha256 != plan.exact.migration_sha256):
            raise GovernanceError("EXPLICIT_MIGRATION_SHA_CONFIRMATION_REQUIRED_OR_MISMATCH")
        if not args.target_database_identity or args.target_database_identity != auth.target_database_identity:
            raise GovernanceError("EXACT_TARGET_IDENTITY_REQUIRED_OR_MISMATCH")
        conn = connect_readonly()
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            if target_identity(conn) != args.target_database_identity: raise GovernanceError("CONNECTED_TARGET_IDENTITY_MISMATCH")
            if args.mode == "verify":
                snapshot = inspect(conn, auth)
                p = build_plan()
                auth.verify(target_identity=args.target_database_identity,
                            now_utc=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                            used_nonces=set())
                verifier = PsycopgBackend(conn, auth, args.lock_timeout_ms, args.statement_timeout_ms)
                target_rows = snapshot["present_objects"].get("mlb.player_game_feature_state_v1", {}).get("row_count")
                if (not verifier.validate_ledger()
                        or not verifier.validate_ledger_records((p.bootstrap, p.exact))
                        or not verifier.validate_exact_empty() or target_rows != 0):
                    raise GovernanceError("VERIFY_LEDGER_RECORDS_OR_EMPTY_SCHEMA_FAILED")
                if (snapshot["legacy_relation_definition_hashes"] != auth.expected_pre_state["legacy_relation_definition_hashes"]
                        or snapshot["legacy_game_pk_evidence"] != auth.expected_pre_state["legacy_game_pk_evidence"]
                        or snapshot["postgres_server_version_num"] != auth.expected_pre_state["postgres_server_version_num"]):
                    raise GovernanceError("VERIFY_LEGACY_BOUNDARY_FAILED")
                print(json.dumps({"status": "VERIFIED", "records": [auth.bootstrap.migration_id, auth.exact.migration_id], "exact_game_row_count": target_rows,
                                  "postgres_server_version_num": snapshot["postgres_server_version_num"],
                                  "legacy_relation_definition_hashes": snapshot["legacy_relation_definition_hashes"], "legacy_game_pk_evidence": snapshot["legacy_game_pk_evidence"]}, sort_keys=True, indent=2))
                return 0
            if args.mode != "apply": raise GovernanceError("RUNNER_MODE_INVALID")
            snapshot = inspect(conn, auth)
            cur = conn.cursor()
            cur.execute("SELECT current_user")
            if cur.fetchone()[0] != "postgres": raise GovernanceError("RUNNER_ROLE_MUST_BE_POSTGRES")
            cur.close()
            if snapshot["supabase_internal_ledger_present"]: raise GovernanceError("BLOCKED_SUPABASE_INTERNAL_LEDGER_REJECTED")
            if (set(snapshot["absent_objects"]) != set(auth.expected_pre_state["absent_objects"])
                    or set(snapshot["present_objects"]) != set(auth.expected_pre_state.get("present_objects", []))
                    or snapshot["legacy_relation_definition_hashes"] != auth.expected_pre_state["legacy_relation_definition_hashes"]
                    or snapshot["legacy_game_pk_evidence"] != auth.expected_pre_state["legacy_game_pk_evidence"]
                    or snapshot["postgres_server_version_num"] != auth.expected_pre_state["postgres_server_version_num"]):
                raise GovernanceError("UNEXPECTED_PRESTATE")
            auth.verify(target_identity=args.target_database_identity, now_utc=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(), used_nonces=set())
            if args.nonce_registry is None:
                raise GovernanceError("AUTHORIZATION_NONCE_REGISTRY_REQUIRED")
            consume_nonce(args.nonce_registry, auth.nonce)
            print(json.dumps({"precondition_report": "CLEAN", "snapshot": snapshot}, sort_keys=True, indent=2))
            backend = PsycopgBackend(conn, auth, args.lock_timeout_ms, args.statement_timeout_ms)
            # Existing sessions holding the task lock are rejected; then the transaction runner owns it.
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=%s AND objid=%s AND granted", TASK_ADVISORY_LOCK)
            if cur.fetchone()[0]: raise GovernanceError("CONFLICTING_TASK_LOCK_SESSION")
            cur.close()
            conn.rollback()
            conn.set_session(readonly=False, isolation_level="READ COMMITTED", autocommit=True)
            from backend.mlb.migration_ledger_bootstrap_v1 import GuardedRunnerV1, PreflightV1
            pf = PreflightV1("bootstrap", False, None, None, False, True, True, False,
                             tuple(auth.expected_pre_state["absent_objects"]), (), {},
                             args.target_database_identity, auth.expected_pre_state["legacy_relation_definition_hashes"],
                             auth.expected_pre_state["legacy_game_pk_evidence"])
            print(GuardedRunnerV1().execute(pf, backend, auth, mode="apply"))
            return 0
        finally:
            conn.close()
    except GovernanceError as exc:
        print(json.dumps({"status": "BLOCKED", "classification": str(exc)}, file=sys.stderr))
        return 2
    except Exception as exc:
        print(json.dumps({"status": "BLOCKED", "classification": "DATABASE_OR_RUNNER_ERROR",
                          "error_type": type(exc).__name__}, file=sys.stderr))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

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
    dsn = os.environ.get("MLB_MIGRATION_DSN")
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
    cur.execute(f'SELECT count(*) FROM "{schema}"."{name}"')
    count = cur.fetchone()[0]
    return True, owner, count


def inspect(conn: Any, auth: AuthorizationV1) -> dict[str, Any]:
    require_exact_artifact_scope(auth)
    cur = conn.cursor()
    try:
        identity = target_identity(conn)
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
        legacy_hashes: dict[str, str] = {}
        legacy_counts: dict[str, int] = {}
        expected_hashes = auth.expected_pre_state.get("legacy_state_hashes", {})
        if set(expected_hashes) != set(auth.expected_pre_state.get("legacy_counts", {})):
            raise GovernanceError("AUTHORIZATION_LEGACY_SNAPSHOT_KEYS_MISMATCH")
        for rel in sorted(expected_hashes):
            if rel not in EXPECTED_LEGACY_RELATIONS:
                raise GovernanceError("AUTHORIZATION_LEGACY_SCOPE_TOO_BROAD")
            cur.execute(f"SELECT to_jsonb(t)::text FROM {rel} AS t ORDER BY to_jsonb(t)::text")
            digest, count = hashlib.sha256(), 0
            for (row_json,) in cur:
                encoded = row_json.encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
                count += 1
            legacy_hashes[rel], legacy_counts[rel] = digest.hexdigest(), count
        return {"target_identity": identity, "supabase_internal_ledger_present": internal,
                "present_objects": present, "absent_objects": absent, "migration_ids": migration_ids,
                "migration_sha256s": migration_shas,
                "legacy_state_hashes": legacy_hashes, "legacy_counts": legacy_counts}
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
        if file_sha256(EXACT_SQL) != EXACT_SHA256: raise GovernanceError("EXACT_MIGRATION_INPUT_BYTE_IDENTITY_MISMATCH")
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

    def validate_legacy(self, hashes: Any, counts: Any) -> bool:
        # Fail closed until the authorization supplies a concrete protected legacy snapshot.
        if not hashes and not counts: return False
        if set(hashes) != set(counts): return False
        for relation, expected_count in counts.items():
            if relation not in EXPECTED_LEGACY_RELATIONS: return False
            self.cur.execute(f"SELECT to_jsonb(t)::text FROM {relation} AS t ORDER BY to_jsonb(t)::text")
            digest, count = hashlib.sha256(), 0
            for (row_json,) in self.cur:
                encoded = row_json.encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
                count += 1
            if count != expected_count or digest.hexdigest() != hashes[relation]: return False
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
        if args.mode in ("plan", "inspect"):
            print(json.dumps({"mode": args.mode, "plan": plan.steps(), "objects": plan.objects,
                              "bootstrap_sha256": plan.bootstrap.migration_sha256,
                              "exact_sha256": plan.exact.migration_sha256,
                              "operational_execution": "NOT RUN"}, indent=2))
            return 0
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
            if target_identity(conn) != args.target_database_identity: raise GovernanceError("CONNECTED_TARGET_IDENTITY_MISMATCH")
            if args.mode == "verify":
                conn.set_session(readonly=True)
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
                if snapshot["legacy_state_hashes"] != auth.expected_pre_state["legacy_state_hashes"] or snapshot["legacy_counts"] != auth.expected_pre_state["legacy_counts"]:
                    raise GovernanceError("VERIFY_LEGACY_BOUNDARY_FAILED")
                print(json.dumps({"status": "VERIFIED", "records": [auth.bootstrap.migration_id, auth.exact.migration_id], "exact_game_row_count": target_rows,
                                  "legacy_state_hashes": snapshot["legacy_state_hashes"], "legacy_counts": snapshot["legacy_counts"]}, sort_keys=True, indent=2))
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
                    or snapshot["legacy_state_hashes"] != auth.expected_pre_state["legacy_state_hashes"]
                    or snapshot["legacy_counts"] != auth.expected_pre_state["legacy_counts"]):
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
            from backend.mlb.migration_ledger_bootstrap_v1 import GuardedRunnerV1, PreflightV1
            pf = PreflightV1("bootstrap", False, None, None, False, True, True, False,
                             tuple(auth.expected_pre_state["absent_objects"]), (), {},
                             args.target_database_identity, auth.expected_pre_state["legacy_state_hashes"],
                             auth.expected_pre_state["legacy_counts"])
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

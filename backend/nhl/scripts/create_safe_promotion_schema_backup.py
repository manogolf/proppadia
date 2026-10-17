#!/usr/bin/env python3
"""Create a protected, schema-only NHL ACL backup before safe promotion."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import psycopg
from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[3]
ENV_FILE = ROOT / "backend/.env"
OUTPUT_DIR = ROOT / "artifacts/activation/nhl_safe_promotion/pre_migration"
PG_DUMP = Path("/opt/homebrew/opt/libpq/bin/pg_dump")
PG_RESTORE = Path("/opt/homebrew/opt/libpq/bin/pg_restore")
EXPECTED_PROJECT_REF = "cnwwhhmpashijqbspvhf"
ROLE_NAMES = (
    "proppadia_nhl_ingest",
    "proppadia_nhl_daily_login",
    "proppadia_nhl_source_correction",
    "proppadia_nhl_research_login",
    "proppadia_nhl_operations_login",
    "proppadia_nhl_ops_writer",
    "proppadia_research_ro",
    "anon",
    "authenticated",
    "service_role",
)
EXPECTED_INGEST_UPDATE = [
    "assists", "blocked_shots_taken", "blocks", "ev_shot_attempts", "ev_sog",
    "fenwick_for", "game_date", "giveaways", "goals", "hits", "is_home",
    "missed_shots", "opponent_id", "penalties_drawn", "penalties_taken", "points",
    "pp_shot_attempts", "pp_sog", "pp_toi_minutes", "rebounds_for",
    "sh_shot_attempts", "sh_sog", "shot_attempts", "shots_on_goal", "takeaways",
    "team_id", "toi_minutes",
]


def rows(cursor: psycopg.Cursor) -> list[dict[str, object]]:
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def query_rows(cursor: psycopg.Cursor, sql: str, params: tuple = ()) -> list[dict[str, object]]:
    cursor.execute(sql, params)
    return rows(cursor)


def project_ref(value: str) -> str:
    parsed = urlsplit(value)
    username = unquote(parsed.username or "")
    if "." in username:
        return username.rsplit(".", 1)[-1]
    host = parsed.hostname or ""
    return host.split(".")[1] if host.startswith("db.") and len(host.split(".")) > 1 else ""


def run_checked(command: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, env=env, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(f"LOCAL_TOOL_FAILED:{Path(command[0]).name}:{result.returncode}")
    return result


def main() -> int:
    os.umask(0o077)
    if not PG_DUMP.is_file() or not PG_RESTORE.is_file():
        raise RuntimeError("POSTGRES_BACKUP_TOOLS_UNAVAILABLE")

    config = dotenv_values(ENV_FILE)
    dsn = config.get("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("APPROVED_PRODUCTION_CONNECTION_MISSING")
    if project_ref(dsn) != EXPECTED_PROJECT_REF:
        raise RuntimeError("PRODUCTION_PROJECT_REFERENCE_MISMATCH")
    for key in (
        "DATABASE_URL", "NHL_INGEST_DATABASE_URL", "NHL_RESEARCH_DATABASE_URL",
        "NHL_OPERATIONS_DATABASE_URL",
    ):
        value = config.get(key)
        if not value or project_ref(value) != EXPECTED_PROJECT_REF:
            raise RuntimeError(f"PRODUCTION_ROUTE_REFERENCE_MISMATCH:{key}")
    test_dsn = config.get("NHL_PP_TOI_TEST_DSN")
    if test_dsn and project_ref(test_dsn) == EXPECTED_PROJECT_REF:
        raise RuntimeError("PRODUCTION_AND_PREVIEW_REFERENCES_NOT_DISTINCT")

    parsed = urlsplit(dsn)
    password = unquote(parsed.password or "")
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not password:
        raise RuntimeError("PRODUCTION_CONNECTION_URL_INVALID")

    # Confirm identity and collect ACL/membership evidence in one enforced
    # read-only transaction before opening pg_dump's read-only snapshot.
    conn = psycopg.connect(
        dsn,
        connect_timeout=15,
        application_name="nhl_safe_promotion_pre_migration_backup",
        options="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=5000",
    )
    try:
        conn.execute("BEGIN READ ONLY")
        cur = conn.cursor()
        cur.execute(
            "SELECT current_user, session_user, current_database(), version(), "
            "current_setting('transaction_read_only')"
        )
        user, session_user, database, version, read_only = cur.fetchone()
        if user != "postgres" or session_user != "postgres" or read_only != "on":
            raise RuntimeError("PRODUCTION_IDENTITY_OR_READ_ONLY_MISMATCH")
        if "PostgreSQL 15.8" not in version:
            raise RuntimeError("PRODUCTION_VERSION_MISMATCH")

        role_inventory = query_rows(
            cur,
            "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, "
            "rolreplication, rolbypassrls, rolinherit "
            "FROM pg_roles WHERE rolname = ANY(%s) ORDER BY rolname",
            (list(ROLE_NAMES),),
        )
        if {row["rolname"] for row in role_inventory} != set(ROLE_NAMES):
            raise RuntimeError("REQUIRED_ROLE_INVENTORY_MISMATCH")

        relation = query_rows(
            cur,
            "SELECT n.nspname AS schema_name, c.relname, owner.rolname AS owner, "
            "c.relkind, c.relrowsecurity "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_roles owner ON owner.oid=c.relowner "
            "WHERE n.nspname='nhl' AND c.relname='skater_game_logs_raw'",
        )
        if len(relation) != 1 or relation[0]["owner"] != "postgres":
            raise RuntimeError("RAW_TABLE_IDENTITY_MISMATCH")

        raw_update_columns = query_rows(
            cur,
            "SELECT a.attname AS column_name FROM pg_attribute a "
            "WHERE a.attrelid=to_regclass('nhl.skater_game_logs_raw') "
            "AND a.attnum>0 AND NOT a.attisdropped "
            "AND has_column_privilege('proppadia_nhl_ingest', "
            "'nhl.skater_game_logs_raw', a.attname, 'UPDATE') ORDER BY a.attname",
        )
        ingest_update_columns = [str(row["column_name"]) for row in raw_update_columns]
        if ingest_update_columns != EXPECTED_INGEST_UPDATE:
            raise RuntimeError("PRE_MIGRATION_INGEST_UPDATE_GRANT_MISMATCH")

        raw_insert_columns = query_rows(
            cur,
            "SELECT a.attname AS column_name FROM pg_attribute a "
            "WHERE a.attrelid=to_regclass('nhl.skater_game_logs_raw') "
            "AND a.attnum>0 AND NOT a.attisdropped "
            "AND has_column_privilege('proppadia_nhl_ingest', "
            "'nhl.skater_game_logs_raw', a.attname, 'INSERT') ORDER BY a.attname",
        )
        role_memberships = query_rows(
            cur,
            "SELECT member.rolname AS member, granted.rolname AS granted_role, "
            "grantor.rolname AS grantor, m.admin_option "
            "FROM pg_auth_members m JOIN pg_roles member ON member.oid=m.member "
            "JOIN pg_roles granted ON granted.oid=m.roleid "
            "JOIN pg_roles grantor ON grantor.oid=m.grantor "
            "WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s) "
            "ORDER BY member.rolname, granted.rolname",
            (list(ROLE_NAMES), list(ROLE_NAMES)),
        )
        table_acl = query_rows(
            cur,
            "SELECT COALESCE(grantee.rolname,'PUBLIC') AS grantee, "
            "grantor.rolname AS grantor, a.privilege_type, a.is_grantable "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
            "LEFT JOIN pg_roles grantee ON grantee.oid=a.grantee "
            "JOIN pg_roles grantor ON grantor.oid=a.grantor "
            "WHERE n.nspname='nhl' AND c.relname='skater_game_logs_raw' "
            "ORDER BY grantee, grantor, a.privilege_type",
        )
        column_acl = query_rows(
            cur,
            "SELECT a.attname AS column_name, COALESCE(grantee.rolname,'PUBLIC') AS grantee, "
            "grantor.rolname AS grantor, x.privilege_type, x.is_grantable "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
            "CROSS JOIN LATERAL aclexplode(a.attacl) x "
            "LEFT JOIN pg_roles grantee ON grantee.oid=x.grantee "
            "JOIN pg_roles grantor ON grantor.oid=x.grantor "
            "WHERE n.nspname='nhl' AND c.relname='skater_game_logs_raw' AND a.attacl IS NOT NULL "
            "ORDER BY a.attname, grantee, grantor, x.privilege_type",
        )
        schema_acl = query_rows(
            cur,
            "SELECT COALESCE(grantee.rolname,'PUBLIC') AS grantee, "
            "grantor.rolname AS grantor, a.privilege_type, a.is_grantable "
            "FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a "
            "LEFT JOIN pg_roles grantee ON grantee.oid=a.grantee "
            "JOIN pg_roles grantor ON grantor.oid=a.grantor "
            "WHERE n.nspname='nhl' ORDER BY grantee, grantor, a.privilege_type",
        )
        default_acls = query_rows(
            cur,
            "SELECT owner.rolname AS owner, COALESCE(n.nspname,'') AS schema_name, "
            "d.defaclobjtype, d.defaclacl::text AS acl "
            "FROM pg_default_acl d JOIN pg_roles owner ON owner.oid=d.defaclrole "
            "LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace "
            "WHERE n.nspname='nhl' ORDER BY owner.rolname,d.defaclobjtype",
        )
        schema_objects = query_rows(
            cur,
            "SELECT c.relkind, count(*) AS object_count FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='nhl' "
            "GROUP BY c.relkind ORDER BY c.relkind",
        )
        memberships = {row["member"]: row["granted_role"] for row in role_memberships}
        if not any(row["member"] == "proppadia_nhl_daily_login"
                   and row["granted_role"] == "proppadia_nhl_ingest" for row in role_memberships):
            raise RuntimeError("DAILY_INGEST_MEMBERSHIP_MISSING")
        if any(row["member"] == "proppadia_nhl_daily_login"
               and row["granted_role"] == "proppadia_nhl_source_correction" for row in role_memberships):
            raise RuntimeError("DAILY_CORRECTION_MEMBERSHIP_PRESENT")

        identity = {
            "database": database,
            "current_user": user,
            "session_user": session_user,
            "postgres_version": version.split(",", 1)[0],
            "transaction_read_only": read_only,
            "production_project_reference": EXPECTED_PROJECT_REF,
        }
        pre_migration_acl = {
            "raw_table": relation[0],
            "ingest_effective_update_columns": ingest_update_columns,
            "ingest_effective_insert_columns": [str(row["column_name"]) for row in raw_insert_columns],
            "correction_effective_update_columns": [
                str(row["column_name"]) for row in query_rows(
                    cur,
                    "SELECT a.attname AS column_name FROM pg_attribute a "
                    "WHERE a.attrelid=to_regclass('nhl.skater_game_logs_raw') "
                    "AND a.attnum>0 AND NOT a.attisdropped "
                    "AND has_column_privilege('proppadia_nhl_source_correction', "
                    "'nhl.skater_game_logs_raw', a.attname, 'UPDATE') ORDER BY a.attname",
                )
            ],
            "explicit_raw_table_acl": table_acl,
            "explicit_raw_column_acl": column_acl,
            "nhl_schema_acl": schema_acl,
            "nhl_default_acls": default_acls,
            "relevant_role_memberships": role_memberships,
            "roles": role_inventory,
            "relation_inventory_by_kind": schema_objects,
            "expected_ingest_update_set_match": True,
        }
        if pre_migration_acl["correction_effective_update_columns"] != ["pp_toi_minutes", "shot_attempts"]:
            raise RuntimeError("CORRECTION_UPDATE_GRANT_MISMATCH")

        cur.execute(
            "SELECT to_regprocedure('nhl.promote_skater_game_log_protected_fields(date)'), "
            "to_regprocedure('nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)')"
        )
        functions = cur.fetchone()
        if functions != (None, None):
            raise RuntimeError("SAFE_PROMOTION_FUNCTION_ALREADY_DEPLOYED")
        identity["safe_promotion_functions_preexisting"] = False
        conn.rollback()
    finally:
        conn.close()

    OUTPUT_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    OUTPUT_DIR.chmod(0o700)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dump_path = OUTPUT_DIR / f"nhl_schema_acl_pre_migration_{timestamp}.dump"
    sql_path = OUTPUT_DIR / f".verify_{timestamp}.sql"
    query = parse_qs(parsed.query)
    child_env = dict(os.environ)
    child_env["PGPASSWORD"] = password
    child_env["PGSSLMODE"] = query.get("sslmode", ["require"])[0]
    child_env["PGAPPNAME"] = "nhl_safe_promotion_schema_acl_backup"
    dump_command = [
        str(PG_DUMP), "--no-password", "--format=custom", "--schema-only",
        "--schema=nhl", "--file", str(dump_path),
        "--host", str(parsed.hostname), "--port", str(parsed.port or 5432),
        "--username", unquote(parsed.username or "postgres"),
        "--dbname", unquote(parsed.path.lstrip("/")) or "postgres",
    ]
    run_checked(dump_command, env=child_env)
    dump_path.chmod(0o600)
    if not dump_path.is_file() or dump_path.stat().st_size == 0:
        raise RuntimeError("SCHEMA_DUMP_EMPTY")

    toc = run_checked([str(PG_RESTORE), "--list", str(dump_path)], env=child_env).stdout
    toc_entries = [line for line in toc.splitlines() if line and not line.lstrip().startswith(";")]
    acl_entries = [line for line in toc_entries if re.search(r"\bACL\b", line)]
    data_entries = [line for line in toc_entries if re.search(r"\b(TABLE DATA|SEQUENCE SET|BLOB)\b", line)]
    if not toc_entries or data_entries:
        raise RuntimeError("SCHEMA_DUMP_TOC_OR_DATA_CONTENT_MISMATCH")

    run_checked([str(PG_RESTORE), "--schema-only", "--file", str(sql_path), str(dump_path)], env=child_env)
    sql_path.chmod(0o600)
    sql = sql_path.read_text(encoding="utf-8")
    if "GRANT " not in sql or "OWNER TO " not in sql:
        raise RuntimeError("SCHEMA_DUMP_ACL_OR_OWNERSHIP_NOT_RENDERED")
    if re.search(r"(?im)^COPY\s+.+\s+FROM\s+stdin;", sql):
        raise RuntimeError("SCHEMA_DUMP_CONTAINS_TABLE_DATA")
    raw_protected_acls = re.findall(
        r"(?im)^GRANT\s+[^;\n]*?UPDATE\s*\(([^)]*)\)\s+ON\s+TABLE\s+"
        r"(?:\"?nhl\"?\.)?\"?skater_game_logs_raw\"?\s+TO\s+proppadia_nhl_ingest\s*;",
        sql,
    )
    correction_acls = re.findall(
        r"(?im)^GRANT\s+[^;\n]*?UPDATE\s*\(([^)]*)\)\s+ON\s+TABLE\s+"
        r"(?:\"?nhl\"?\.)?\"?skater_game_logs_raw\"?\s+TO\s+proppadia_nhl_source_correction\s*;",
        sql,
    )
    if not raw_protected_acls or not correction_acls:
        raise RuntimeError("SCHEMA_DUMP_RAW_COLUMN_ACLS_NOT_RENDERED")
    raw_acl_columns = {
        part.strip().strip('"')
        for grant_columns in raw_protected_acls
        for part in grant_columns.split(",")
    }
    correction_columns = {
        part.strip().strip('"')
        for grant_columns in correction_acls
        for part in grant_columns.split(",")
    }
    if raw_acl_columns != set(EXPECTED_INGEST_UPDATE):
        raise RuntimeError("SCHEMA_DUMP_INGEST_UPDATE_SET_MISMATCH")
    if correction_columns != {"shot_attempts", "pp_toi_minutes"}:
        raise RuntimeError("SCHEMA_DUMP_CORRECTION_UPDATE_SET_MISMATCH")

    sql_counts = {
        "grant_statements": len(re.findall(r"(?im)^GRANT\s", sql)),
        "revoke_statements": len(re.findall(r"(?im)^REVOKE\s", sql)),
        "owner_statements": len(re.findall(r"(?im)^ALTER\s+.+\s+OWNER\s+TO\s", sql)),
        "function_definitions": len(re.findall(r"(?im)^CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s", sql)),
        "table_definitions": len(re.findall(r"(?im)^CREATE\s+TABLE\s", sql)),
        "copy_data_statements": len(re.findall(r"(?im)^COPY\s", sql)),
    }
    sql_path.unlink()
    digest = hashlib.sha256(dump_path.read_bytes()).hexdigest()
    created = datetime.now(timezone.utc).isoformat(timespec="seconds")
    manifest = {
        "created_at_utc": created,
        "backup_file": str(dump_path.relative_to(ROOT)),
        "backup_format": "PostgreSQL custom archive; schema-only; schema=nhl; ACL and ownership included",
        "file_size_bytes": dump_path.stat().st_size,
        "sha256": digest,
        "database_identity": identity,
        "pre_migration_acl_and_role_evidence": pre_migration_acl,
        "archive_verification": {
            "pg_restore_list_entries": len(toc_entries),
            "acl_toc_entries": len(acl_entries),
            "table_data_sequence_or_blob_entries": len(data_entries),
            "rendered_sql_checks": sql_counts,
            "raw_ingest_update_columns_in_archive": sorted(raw_acl_columns),
            "correction_update_columns_in_archive": sorted(correction_columns),
            "archive_readable": True,
            "acl_verified": bool(acl_entries) and True,
            "table_data_absent": True,
        },
        "limitations": [
            "pg_dump schema selection does not include global role definitions or role memberships; relevant role attributes and memberships are in this manifest.",
            "This archive was inspected and rendered but not restored. Do not restore it wholesale over production; use the committed migration rollback first when applicable and selectively restore reviewed DDL/ACL only with separate authorization.",
            "The dump covers the nhl schema only; dependencies in other schemas and Supabase-managed objects are not included.",
        ],
    }
    manifest_path = OUTPUT_DIR / "pre_migration_manifest.json"
    temporary_manifest = OUTPUT_DIR / ".pre_migration_manifest.json.tmp"
    temporary_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary_manifest.chmod(0o600)
    temporary_manifest.replace(manifest_path)
    manifest_path.chmod(0o600)

    print("BACKUP=CREATED")
    print("TARGET=PRODUCTION_REFERENCE_MATCH; DATABASE=postgres; SESSION_ROLE=postgres; READ_ONLY=on")
    print(f"BACKUP_FILE={manifest['backup_file']}")
    print(f"SIZE_BYTES={manifest['file_size_bytes']}; SHA256={digest}")
    print(f"PG_RESTORE_TOC_ENTRIES={len(toc_entries)}; ACL_ENTRIES={len(acl_entries)}; DATA_ENTRIES={len(data_entries)}")
    print(f"GRANT_STATEMENTS={sql_counts['grant_statements']}; OWNER_STATEMENTS={sql_counts['owner_statements']}; FUNCTIONS={sql_counts['function_definitions']}; TABLES={sql_counts['table_definitions']}")
    print("RAW_INGEST_UPDATE_COLUMNS=27; CORRECTION_UPDATE_COLUMNS=shot_attempts,pp_toi_minutes")
    print(f"MANIFEST={manifest_path.relative_to(ROOT)}; PERMISSIONS=0600; DIRECTORY_PERMISSIONS=0700")
    print("RESTORE=NOT_PERFORMED; PRODUCTION_UNCHANGED=YES")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        sqlstate = getattr(error, "sqlstate", None)
        code = f"; SQLSTATE={sqlstate}" if sqlstate else ""
        # Exception text from database drivers and subprocesses can contain
        # endpoint, user, or other connection details. Report only its type
        # and SQLSTATE; the local protected artifact directory is the only
        # place where diagnostic outputs may be retained.
        safe_reason = ""
        if isinstance(error, RuntimeError) and re.fullmatch(r"[A-Z0-9_:.-]+", str(error)):
            safe_reason = f"; CHECK={error}"
        print(f"BACKUP=BLOCKED; REASON={type(error).__name__}{safe_reason}{code}")
        raise SystemExit(1)

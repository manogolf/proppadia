#!/usr/bin/env python3
"""Create and verify a protected, schema-only MLB ACL backup.

This utility reads `SUPABASE_DB_URL` from backend/.env, verifies the pinned
production reference, and exports only the MLB schema definition. It never
exports table rows or role password material.
"""
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
OUTPUT_DIR = ROOT / "artifacts/activation/mlb_restricted_access/pre_migration"
PG_DUMP = Path("/opt/homebrew/opt/libpq/bin/pg_dump")
PG_RESTORE = Path("/opt/homebrew/opt/libpq/bin/pg_restore")
EXPECTED_PROJECT_REF = "cnwwhhmpashijqbspvhf"
EXPECTED_RELATIONS = (
    "game_info", "model_training_props", "player_derived_stats", "player_ids",
    "player_stats", "prop_features_precomputed",
)


def project_ref(dsn: str) -> str:
    parsed = urlsplit(dsn)
    username = unquote(parsed.username or "")
    if "." in username:
        return username.rsplit(".", 1)[-1]
    host = parsed.hostname or ""
    return host.split(".")[1] if host.startswith("db.") and len(host.split(".")) > 1 else ""


def checked(command: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, env=env, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(f"LOCAL_TOOL_FAILED:{Path(command[0]).name}:{result.returncode}")
    return result


def query_rows(cur: psycopg.Cursor, sql: str, params: tuple = ()) -> list[dict[str, object]]:
    cur.execute(sql, params)
    names = [column.name for column in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def main() -> int:
    os.umask(0o077)
    if not PG_DUMP.is_file() or not PG_RESTORE.is_file():
        raise RuntimeError("POSTGRES_BACKUP_TOOLS_UNAVAILABLE")
    config = dotenv_values(ENV_FILE)
    dsn = config.get("SUPABASE_DB_URL")
    if not dsn or project_ref(dsn) != EXPECTED_PROJECT_REF:
        raise RuntimeError("APPROVED_PRODUCTION_REFERENCE_MISMATCH")
    preview_dsn = config.get("NHL_PP_TOI_TEST_DSN")
    if preview_dsn and project_ref(preview_dsn) == EXPECTED_PROJECT_REF:
        raise RuntimeError("PRODUCTION_AND_PREVIEW_REFERENCES_NOT_DISTINCT")

    parsed = urlsplit(dsn)
    password = unquote(parsed.password or "")
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not password:
        raise RuntimeError("APPROVED_PRODUCTION_CONNECTION_INVALID")

    conn = psycopg.connect(
        dsn,
        connect_timeout=15,
        application_name="mlb_schema_acl_backup_readonly",
        options="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=5000",
    )
    try:
        conn.execute("BEGIN READ ONLY")
        cur = conn.cursor()
        cur.execute("SELECT current_user, session_user, current_database(), version(), current_setting('transaction_read_only')")
        user, session_user, database, version, read_only = cur.fetchone()
        if (user, session_user, database) != ("postgres", "postgres", "postgres") or read_only != "on":
            raise RuntimeError("PRODUCTION_IDENTITY_OR_READ_ONLY_MISMATCH")
        if "PostgreSQL 15.8" not in version:
            raise RuntimeError("PRODUCTION_VERSION_MISMATCH")

        roles = query_rows(
            cur,
            "SELECT rolname,rolcanlogin,rolinherit,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls "
            "FROM pg_catalog.pg_roles WHERE rolname IN ('postgres','anon','authenticated','service_role') "
            "OR rolname LIKE 'proppadia_mlb_%%' ORDER BY rolname",
        )
        relations = query_rows(
            cur,
            "SELECT c.relname,owner.rolname AS owner,c.relkind,c.relrowsecurity,c.relforcerowsecurity "
            "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_catalog.pg_roles owner ON owner.oid=c.relowner "
            "WHERE n.nspname='mlb' AND c.relkind IN ('r','p','v','m','S','f') ORDER BY c.relname",
        )
        missing = sorted(set(EXPECTED_RELATIONS) - {str(row["relname"]) for row in relations})
        if missing:
            raise RuntimeError("EXPECTED_MLB_RELATION_MISSING")
        schema_acl = query_rows(
            cur,
            "SELECT COALESCE(grantee.rolname,'PUBLIC') AS grantee,grantor.rolname AS grantor, "
            "x.privilege_type,x.is_grantable FROM pg_catalog.pg_namespace n "
            "CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) x "
            "LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid=x.grantee "
            "JOIN pg_catalog.pg_roles grantor ON grantor.oid=x.grantor WHERE n.nspname='mlb' "
            "ORDER BY grantee,grantor,x.privilege_type",
        )
        relation_acl = query_rows(
            cur,
            "SELECT c.relname,COALESCE(grantee.rolname,'PUBLIC') AS grantee,grantor.rolname AS grantor, "
            "x.privilege_type,x.is_grantable FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) x "
            "LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid=x.grantee "
            "JOIN pg_catalog.pg_roles grantor ON grantor.oid=x.grantor WHERE n.nspname='mlb' "
            "ORDER BY c.relname,grantee,grantor,x.privilege_type",
        )
        column_acl = query_rows(
            cur,
            "SELECT c.relname,a.attname,COALESCE(grantee.rolname,'PUBLIC') AS grantee,grantor.rolname AS grantor, "
            "x.privilege_type,x.is_grantable FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
            "CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) x "
            "LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid=x.grantee "
            "JOIN pg_catalog.pg_roles grantor ON grantor.oid=x.grantor WHERE n.nspname='mlb' "
            "ORDER BY c.relname,a.attname,grantee,grantor,x.privilege_type",
        )
        function_acl = query_rows(
            cur,
            "SELECT p.proname,pg_catalog.pg_get_function_identity_arguments(p.oid) AS args, "
            "owner.rolname AS owner,p.prosecdef AS security_definer,COALESCE(grantee.rolname,'PUBLIC') AS grantee, "
            "grantor.rolname AS grantor,x.privilege_type,x.is_grantable FROM pg_catalog.pg_proc p "
            "JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
            "JOIN pg_catalog.pg_roles owner ON owner.oid=p.proowner "
            "CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner))) x "
            "LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid=x.grantee "
            "JOIN pg_catalog.pg_roles grantor ON grantor.oid=x.grantor WHERE n.nspname='mlb' "
            "ORDER BY p.proname,args,grantee,grantor,x.privilege_type",
        )
        memberships = query_rows(
            cur,
            "SELECT member.rolname AS member,granted.rolname AS granted_role,grantor.rolname AS grantor,m.admin_option "
            "FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles member ON member.oid=m.member "
            "JOIN pg_catalog.pg_roles granted ON granted.oid=m.roleid JOIN pg_catalog.pg_roles grantor ON grantor.oid=m.grantor "
            "WHERE member.rolname LIKE 'proppadia_%%' OR granted.rolname LIKE 'proppadia_%%' "
            "ORDER BY member,granted_role",
        )
        default_acls = query_rows(
            cur,
            "SELECT owner.rolname AS owner,n.nspname AS schema_name,d.defaclobjtype,d.defaclacl::text AS acl "
            "FROM pg_catalog.pg_default_acl d JOIN pg_catalog.pg_roles owner ON owner.oid=d.defaclrole "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='mlb' "
            "ORDER BY owner,schema_name,d.defaclobjtype",
        )
        conn.rollback()
    finally:
        conn.close()

    OUTPUT_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    OUTPUT_DIR.chmod(0o700)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dump_path = OUTPUT_DIR / f"mlb_schema_acl_preflight_{stamp}.dump"
    sql_path = OUTPUT_DIR / f".render_{stamp}.sql"
    parsed_query = parse_qs(parsed.query)
    child_env = dict(os.environ)
    child_env["PGPASSWORD"] = password
    child_env["PGSSLMODE"] = parsed_query.get("sslmode", ["require"])[0]
    child_env["PGAPPNAME"] = "mlb_schema_acl_backup"
    command = [
        str(PG_DUMP), "--no-password", "--format=custom", "--schema-only", "--schema=mlb",
        "--file", str(dump_path), "--host", str(parsed.hostname), "--port", str(parsed.port or 5432),
        "--username", unquote(parsed.username or "postgres"), "--dbname", unquote(parsed.path.lstrip("/")) or "postgres",
    ]
    checked(command, env=child_env)
    dump_path.chmod(0o600)
    if not dump_path.is_file() or dump_path.stat().st_size == 0:
        raise RuntimeError("SCHEMA_ARCHIVE_EMPTY")
    toc = checked([str(PG_RESTORE), "--list", str(dump_path)], env=child_env).stdout
    entries = [line for line in toc.splitlines() if line and not line.lstrip().startswith(";")]
    data_entries = [line for line in entries if re.search(r"\b(TABLE DATA|SEQUENCE SET|BLOB)\b", line)]
    acl_entries = [line for line in entries if re.search(r"\bACL\b", line)]
    checked([str(PG_RESTORE), "--schema-only", "--file", str(sql_path), str(dump_path)], env=child_env)
    sql_path.chmod(0o600)
    rendered = sql_path.read_text(encoding="utf-8")
    if data_entries or not acl_entries or "GRANT " not in rendered or "OWNER TO " not in rendered:
        raise RuntimeError("SCHEMA_ARCHIVE_ACL_OWNER_OR_DATA_CHECK_FAILED")
    actual_tables = {str(row["relname"]) for row in relations if row["relkind"] in {"r", "p"}}
    missing_tables = [name for name in actual_tables if not any(re.search(rf"\bTABLE\s+mlb\s+{re.escape(name)}\b", line) for line in entries)]
    if missing_tables:
        raise RuntimeError("EXPECTED_TABLE_NOT_IN_ARCHIVE")
    if not any("FUNCTION mlb" in line for line in entries):
        raise RuntimeError("MLB_FUNCTIONS_NOT_IN_ARCHIVE")
    if re.search(r"(?im)^COPY\s+.+\s+FROM\s+stdin;", rendered):
        raise RuntimeError("SCHEMA_ARCHIVE_CONTAINS_TABLE_ROWS")
    sql_path.unlink()

    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "backup_file": str(dump_path.relative_to(ROOT)),
        "format": "PostgreSQL custom archive; schema-only; mlb schema; ACL and ownership included",
        "file_size_bytes": dump_path.stat().st_size,
        "sha256": hashlib.sha256(dump_path.read_bytes()).hexdigest(),
        "identity": {
            "project_reference_matches_pinned_production": True,
            "database": database,
            "current_user": user,
            "session_user": session_user,
            "postgresql_version": str(version).split(",", 1)[0],
            "read_only_metadata_transaction": True,
        },
        "role_inventory": roles,
        "memberships": memberships,
        "mlb_relations": relations,
        "schema_acl": schema_acl,
        "relation_acl": relation_acl,
        "column_acl": column_acl,
        "function_acl": function_acl,
        "default_acls": default_acls,
        "archive_verification": {
            "pg_restore_list_entries": len(entries),
            "acl_toc_entries": len(acl_entries),
            "data_entries": len(data_entries),
            "table_data_absent": True,
            "rendered_owner_and_grant_statements": True,
            "rendered_function_definitions": len(re.findall(r"(?im)^CREATE\s+(?:OR REPLACE\s+)?FUNCTION\s", rendered)),
            "archive_readable": True,
        },
        "limitations": [
            "Role password material is not exported.",
            "Role attributes and Proppadia membership edges are captured as metadata; restore requires target roles to exist first.",
            "Archive was inspected and rendered but not restored; do not restore wholesale without a separately reviewed recovery plan.",
        ],
    }
    manifest_path = OUTPUT_DIR / f"mlb_schema_acl_preflight_{stamp}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest_path.chmod(0o600)
    print("MLB_SCHEMA_ACL_BACKUP=CREATED")
    print("PRODUCTION_IDENTITY=VERIFIED; DB=postgres; ROLE=postgres; PG=15.8")
    print(f"FILE={manifest['backup_file']}")
    print(f"SIZE_BYTES={manifest['file_size_bytes']}")
    print(f"SHA256={manifest['sha256']}")
    print(f"ACL_ENTRIES={len(acl_entries)}; TABLE_DATA_ENTRIES={len(data_entries)}; ROLE_MEMBERSHIPS={len(memberships)}")
    print(f"MANIFEST={manifest_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # keep DSNs and provider diagnostics out of output
        detail = str(exc) if isinstance(exc, RuntimeError) and str(exc).isupper() else "SANITIZED"
        sqlstate = getattr(exc, "sqlstate", None)
        sqlstate_text = sqlstate if isinstance(sqlstate, str) and re.fullmatch(r"[0-9A-Z]{5}", sqlstate) else "NONE"
        print(f"MLB_SCHEMA_ACL_BACKUP=BLOCKED; ERROR={type(exc).__name__}; SQLSTATE={sqlstate_text}; DETAIL={detail}")
        raise SystemExit(2)

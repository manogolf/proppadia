#!/usr/bin/env python3
"""Sanitized NHL database preflight; the connection value is environment-only."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from backend.nhl.database_access import OWNER_ROLES, database_url, restricted_mode  # noqa: E402


# Restricted logins must have no owner reach: not superuser, no BYPASSRLS, and
# no membership in an owner role or service_role.
PRIVILEGE_QUERY = """
SELECT r.rolsuper OR r.rolbypassrls OR EXISTS (
    SELECT 1 FROM pg_roles o
    WHERE o.rolname = ANY(%s) AND pg_has_role(r.oid, o.oid, 'MEMBER'))
FROM pg_roles r WHERE r.rolname = current_user
"""
REFERENCE = re.compile(r"^\s*\$(?:\{[A-Za-z_][A-Za-z0-9_]*\}|[A-Za-z_][A-Za-z0-9_]*)\s*$")


def main() -> int:
    value = (os.environ.get("SUPABASE_DB_URL") or "").strip()
    alias = (os.environ.get("DATABASE_URL") or "").strip()
    if not value or REFERENCE.fullmatch(value) or not alias or REFERENCE.fullmatch(alias) or value != alias:
        raise SystemExit("NHL_DATABASE_CREDENTIAL_ENVIRONMENT_INVALID")
    try:
        # Restricted mode: the aliases must carry the research login, never a
        # legacy owner value.
        if value != database_url("research"):
            raise SystemExit("NHL_DATABASE_CREDENTIAL_NOT_RESEARCH_ROUTE")
    except RuntimeError:
        raise SystemExit("NHL_DATABASE_CREDENTIAL_ENVIRONMENT_INVALID") from None
    try:
        expected_database = conninfo_to_dict(value).get("dbname")
        if not expected_database:
            raise RuntimeError("DATABASE_TARGET_IDENTITY_UNAVAILABLE")
        with psycopg.connect(value) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            if cursor.fetchone()[0] != expected_database:
                raise RuntimeError("DATABASE_TARGET_IDENTITY_CHECK_FAILED")
            if restricted_mode():
                cursor.execute(PRIVILEGE_QUERY, ([*sorted(OWNER_ROLES), "service_role"],))
                privileged = cursor.fetchone()
                owner_reach = privileged is None or privileged[0] is not False
    except Exception:
        raise SystemExit("NHL_DATABASE_CONNECTION_OR_TARGET_CHECK_FAILED") from None
    if restricted_mode() and owner_reach:
        raise SystemExit("NHL_DATABASE_LOGIN_HAS_OWNER_REACH")
    print("NHL_DATABASE_CONNECTION_AND_TARGET_CHECK_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

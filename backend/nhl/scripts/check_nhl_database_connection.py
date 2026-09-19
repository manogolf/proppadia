#!/usr/bin/env python3
"""Sanitized NHL database preflight; the connection value is environment-only."""
from __future__ import annotations

import os
import re

import psycopg
from psycopg.conninfo import conninfo_to_dict


REFERENCE = re.compile(r"^\s*\$(?:\{[A-Za-z_][A-Za-z0-9_]*\}|[A-Za-z_][A-Za-z0-9_]*)\s*$")


def main() -> int:
    value = (os.environ.get("SUPABASE_DB_URL") or "").strip()
    alias = (os.environ.get("DATABASE_URL") or "").strip()
    if not value or REFERENCE.fullmatch(value) or not alias or REFERENCE.fullmatch(alias) or value != alias:
        raise SystemExit("NHL_DATABASE_CREDENTIAL_ENVIRONMENT_INVALID")
    try:
        expected_database = conninfo_to_dict(value).get("dbname")
        if not expected_database:
            raise RuntimeError("DATABASE_TARGET_IDENTITY_UNAVAILABLE")
        with psycopg.connect(value) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            if cursor.fetchone()[0] != expected_database:
                raise RuntimeError("DATABASE_TARGET_IDENTITY_CHECK_FAILED")
    except Exception:
        raise SystemExit("NHL_DATABASE_CONNECTION_OR_TARGET_CHECK_FAILED") from None
    print("NHL_DATABASE_CONNECTION_AND_TARGET_CHECK_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

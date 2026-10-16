"""Fail-closed execution of separately authorized NHL source corrections.

The administrative URL is retrieved from macOS Keychain at runtime and is
never accepted from argv, dotenv, or process environment. Role membership,
role activation, the correction statement, role reset, and membership revoke
are one database transaction. A new connection independently checks that no
membership remains after commit or rollback.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
from dataclasses import dataclass
from typing import Callable, Sequence
from urllib.parse import unquote, urlsplit

import psycopg


KEYCHAIN_SERVICE = "proppadia.nhl.source-correction.admin-dsn"
ADMIN_LOGIN = "postgres"
CORRECTION_ROLE = "proppadia_nhl_source_correction"
RAW_TABLE = "nhl.skater_game_logs_raw"
ALLOWED_COLUMNS = frozenset({"shot_attempts", "pp_toi_minutes"})
EXPECTED_SERVER_VERSION = 150008


class SourceCorrectionError(RuntimeError):
    """An error with a stable, secret-safe public message."""


@dataclass(frozen=True)
class CorrectionResult:
    affected_rows: int
    sql_sha256: str
    authorization_reference: str


def _tokenize_sql(sql: str) -> list[tuple[str, str]]:
    """Tokenize enough PostgreSQL syntax to reject multiple/control statements."""
    tokens: list[tuple[str, str]] = []
    i = 0
    size = len(sql)
    while i < size:
        char = sql[i]
        if char.isspace():
            i += 1
            continue
        if sql.startswith("--", i):
            newline = sql.find("\n", i + 2)
            i = size if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", i):
            depth = 1
            i += 2
            while i < size and depth:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise SourceCorrectionError("CORRECTION_SQL_INVALID_COMMENT")
            continue
        if char == "'":
            i += 1
            while i < size:
                if sql[i] == "'":
                    if i + 1 < size and sql[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                if sql[i] == "\\":
                    raise SourceCorrectionError("CORRECTION_SQL_BACKSLASH_STRING_UNSUPPORTED")
                i += 1
            else:
                raise SourceCorrectionError("CORRECTION_SQL_INVALID_STRING")
            tokens.append(("literal", ""))
            continue
        if char == '"':
            i += 1
            value: list[str] = []
            while i < size:
                if sql[i] == '"':
                    if i + 1 < size and sql[i + 1] == '"':
                        value.append('"')
                        i += 2
                        continue
                    i += 1
                    break
                value.append(sql[i])
                i += 1
            else:
                raise SourceCorrectionError("CORRECTION_SQL_INVALID_IDENTIFIER")
            tokens.append(("identifier", "".join(value).lower()))
            continue
        if char == "$":
            match = re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", sql[i:])
            if match:
                delimiter = match.group(0)
                end = sql.find(delimiter, i + len(delimiter))
                if end < 0:
                    raise SourceCorrectionError("CORRECTION_SQL_INVALID_DOLLAR_STRING")
                i = end + len(delimiter)
                tokens.append(("literal", ""))
                continue
        if char.isalpha() or char == "_":
            match = re.match(r"[A-Za-z_][A-Za-z_0-9$]*", sql[i:])
            assert match is not None
            value = match.group(0)
            tokens.append(("identifier", value.lower()))
            i += len(value)
            continue
        if char == ";":
            tokens.append(("semicolon", char))
        elif char in ".,=()":
            tokens.append(("punctuation", char))
        else:
            # Operators and numeric tokens are opaque. The server validates
            # their grammar when the reviewed statement is executed.
            tokens.append(("other", char))
        i += 1
    return tokens


def validate_correction_sql(sql: str) -> None:
    """Require exactly one UPDATE of the raw table and approved columns."""
    if not sql.strip() or len(sql.encode("utf-8")) > 1_000_000:
        raise SourceCorrectionError("CORRECTION_SQL_EMPTY_OR_TOO_LARGE")
    tokens = _tokenize_sql(sql)
    if tokens and tokens[-1][0] == "semicolon":
        tokens.pop()
    if not tokens or any(kind == "semicolon" for kind, _ in tokens):
        raise SourceCorrectionError("CORRECTION_SQL_MULTIPLE_STATEMENTS_FORBIDDEN")
    if tokens[0] != ("identifier", "update"):
        raise SourceCorrectionError("CORRECTION_SQL_UPDATE_ONLY")

    forbidden = {
        "begin", "commit", "rollback", "savepoint", "release",
        "reset", "grant", "revoke", "call", "do", "copy", "insert",
        "delete", "truncate", "alter", "create", "drop", "execute",
        "prepare", "deallocate", "vacuum", "analyze",
    }
    if any(kind == "identifier" and value in forbidden for kind, value in tokens):
        raise SourceCorrectionError("CORRECTION_SQL_CONTROL_STATEMENT_FORBIDDEN")

    # UPDATE [ONLY] nhl.skater_game_logs_raw SET ... WHERE ...
    pos = 1
    if tokens[pos:pos + 1] == [("identifier", "only")]:
        pos += 1
    expected_target = [
        ("identifier", "nhl"), ("punctuation", "."),
        ("identifier", "skater_game_logs_raw"), ("identifier", "set"),
    ]
    if tokens[pos:pos + len(expected_target)] != expected_target:
        raise SourceCorrectionError("CORRECTION_SQL_TARGET_FORBIDDEN")
    pos += len(expected_target)

    assignments: list[list[tuple[str, str]]] = []
    current: list[tuple[str, str]] = []
    depth = 0
    where_index: int | None = None
    for index in range(pos, len(tokens)):
        token = tokens[index]
        if token == ("punctuation", "("):
            depth += 1
        elif token == ("punctuation", ")"):
            depth -= 1
            if depth < 0:
                raise SourceCorrectionError("CORRECTION_SQL_UNBALANCED_PARENS")
        if depth == 0 and token == ("identifier", "where"):
            where_index = index
            if current:
                assignments.append(current)
            break
        if depth == 0 and token == ("punctuation", ","):
            assignments.append(current)
            current = []
        else:
            current.append(token)
    if depth != 0 or where_index is None or where_index == len(tokens) - 1:
        raise SourceCorrectionError("CORRECTION_SQL_WHERE_REQUIRED")
    if not assignments:
        raise SourceCorrectionError("CORRECTION_SQL_ASSIGNMENT_REQUIRED")
    assigned_columns: set[str] = set()
    for assignment in assignments:
        if len(assignment) < 3 or assignment[0][0] != "identifier" or assignment[1] != ("punctuation", "="):
            raise SourceCorrectionError("CORRECTION_SQL_ASSIGNMENT_INVALID")
        column = assignment[0][1]
        if column not in ALLOWED_COLUMNS:
            raise SourceCorrectionError("CORRECTION_SQL_COLUMN_FORBIDDEN")
        if column in assigned_columns:
            raise SourceCorrectionError("CORRECTION_SQL_DUPLICATE_ASSIGNMENT")
        assigned_columns.add(column)


def load_admin_dsn(
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    *,
    environ: dict[str, str] | None = None,
) -> str:
    """Load the approved admin URL from Keychain without exposing it."""
    import os

    env = os.environ if environ is None else environ
    if env.get("NHL_SOURCE_CORRECTION_ADMIN_DSN"):
        raise SourceCorrectionError("ADMIN_CREDENTIAL_MUST_NOT_USE_ENVIRONMENT")
    try:
        result = runner(
            ["/usr/bin/security", "find-generic-password", "-s", KEYCHAIN_SERVICE,
             "-a", ADMIN_LOGIN, "-w"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        raise SourceCorrectionError("ADMIN_CREDENTIAL_UNAVAILABLE") from None
    dsn = (result.stdout or "").strip()
    if result.returncode != 0 or not dsn:
        raise SourceCorrectionError("ADMIN_CREDENTIAL_UNAVAILABLE")
    validate_admin_dsn(dsn)
    return dsn


def validate_admin_dsn(dsn: str) -> None:
    try:
        parsed = urlsplit(dsn)
        username = unquote(parsed.username or "").split(".", 1)[0]
        database = parsed.path.lstrip("/")
        query = dict(part.split("=", 1) for part in parsed.query.split("&") if "=" in part)
        valid = (
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.hostname
            and parsed.password
            and username == ADMIN_LOGIN
            and database == "postgres"
            and query.get("sslmode", "").lower() in {"require", "verify-ca", "verify-full"}
        )
    except Exception:
        valid = False
    if not valid:
        raise SourceCorrectionError("ADMIN_CREDENTIAL_INVALID")


def _fetchone(conn: psycopg.Connection, statement: str, params: Sequence[object] = ()):
    with conn.cursor() as cursor:
        cursor.execute(statement, params)
        return cursor.fetchone()


def _check_admin_session(
    conn: psycopg.Connection, *, require_clean: bool, require_grant_authority: bool = True,
) -> None:
    row = _fetchone(
        conn,
        """SELECT session_user, current_user,
                  current_setting('server_version_num')::integer,
                  (SELECT rolcreaterole FROM pg_roles WHERE rolname = session_user),
                  pg_has_role(session_user, %s, 'MEMBER')""",
        (CORRECTION_ROLE,),
    )
    if not row or row[0] != ADMIN_LOGIN or row[1] != ADMIN_LOGIN or row[2] != EXPECTED_SERVER_VERSION:
        raise SourceCorrectionError("ADMIN_SESSION_IDENTITY_OR_VERSION_MISMATCH")
    if require_grant_authority and not row[3]:
        raise SourceCorrectionError("ADMIN_ROLE_GRANT_AUTHORITY_MISSING")
    if require_clean and row[4]:
        raise SourceCorrectionError("CORRECTION_ROLE_MEMBERSHIP_ALREADY_PRESENT")


def _check_correction_role(conn: psycopg.Connection) -> None:
    row = _fetchone(
        conn,
        """SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
                  rolreplication, rolbypassrls
             FROM pg_roles WHERE rolname = %s""",
        (CORRECTION_ROLE,),
    )
    if row != (False, False, False, False, False, False):
        raise SourceCorrectionError("CORRECTION_ROLE_ATTRIBUTES_MISMATCH")


def _check_exact_privileges(conn: psycopg.Connection) -> None:
    row = _fetchone(
        conn,
        """SELECT has_database_privilege(%s, current_database(), 'CONNECT'),
                  has_schema_privilege(%s, 'nhl', 'USAGE'),
                  has_schema_privilege(%s, 'nhl', 'CREATE'),
                  has_table_privilege(%s, %s, 'SELECT'),
                  has_table_privilege(%s, %s, 'INSERT'),
                  has_table_privilege(%s, %s, 'UPDATE'),
                  has_table_privilege(%s, %s, 'DELETE'),
                  has_table_privilege(%s, %s, 'TRUNCATE'),
                  ARRAY(SELECT a.attname::text FROM pg_attribute a
                        WHERE a.attrelid = %s::regclass AND a.attnum > 0
                          AND NOT a.attisdropped
                          AND has_column_privilege(%s, %s, a.attname, 'UPDATE')
                        ORDER BY a.attnum),
                  EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n
                            ON n.oid = c.relnamespace
                          WHERE n.nspname = 'nhl' AND c.relkind IN ('r','p','v','m','f')
                            AND c.oid <> %s::regclass
                            AND (has_table_privilege(%s, c.oid, 'SELECT')
                              OR has_table_privilege(%s, c.oid, 'INSERT')
                              OR has_table_privilege(%s, c.oid, 'UPDATE')
                              OR has_table_privilege(%s, c.oid, 'DELETE')
                              OR has_table_privilege(%s, c.oid, 'TRUNCATE')
                              OR has_table_privilege(%s, c.oid, 'REFERENCES')
                              OR has_table_privilege(%s, c.oid, 'TRIGGER')))""",
        (CORRECTION_ROLE, CORRECTION_ROLE, CORRECTION_ROLE,
         CORRECTION_ROLE, RAW_TABLE, CORRECTION_ROLE, RAW_TABLE,
         CORRECTION_ROLE, RAW_TABLE, CORRECTION_ROLE, RAW_TABLE,
         CORRECTION_ROLE, RAW_TABLE, RAW_TABLE, CORRECTION_ROLE, RAW_TABLE,
         RAW_TABLE, CORRECTION_ROLE, CORRECTION_ROLE, CORRECTION_ROLE,
         CORRECTION_ROLE, CORRECTION_ROLE, CORRECTION_ROLE, CORRECTION_ROLE),
    )
    if not row:
        raise SourceCorrectionError("CORRECTION_PRIVILEGE_CHECK_FAILED")
    expected_updates = {"shot_attempts", "pp_toi_minutes"}
    valid = (
        row[0] is True and row[1] is True and row[2] is False and row[3] is True
        and all(value is False for value in row[4:8])
        and set(row[8]) == expected_updates and row[9] is False
    )
    if not valid:
        raise SourceCorrectionError("CORRECTION_PRIVILEGE_SCOPE_MISMATCH")


def _is_member(conn: psycopg.Connection) -> bool:
    row = _fetchone(conn, "SELECT pg_has_role(%s, %s, 'MEMBER')", (ADMIN_LOGIN, CORRECTION_ROLE))
    return bool(row and row[0])


def _quiet_close(conn) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _ensure_membership_revoked(dsn: str, connect: Callable = psycopg.connect) -> None:
    """Cleanup if needed, then confirm absent on a fresh independent session."""
    check = None
    try:
        check = connect(dsn)
        _check_admin_session(check, require_clean=False, require_grant_authority=False)
        membership_present = _is_member(check)
    except Exception:
        raise SourceCorrectionError("INDEPENDENT_MEMBERSHIP_VERIFICATION_FAILED") from None
    finally:
        if check is not None:
            _quiet_close(check)

    cleanup_failed = False
    if membership_present:
        cleanup = None
        try:
            cleanup = connect(dsn)
            _check_admin_session(cleanup, require_clean=False)
            with cleanup.cursor() as cursor:
                cursor.execute(f"REVOKE {CORRECTION_ROLE} FROM {ADMIN_LOGIN}")
            cleanup.commit()
        except Exception:
            if cleanup is not None:
                try:
                    cleanup.rollback()
                except Exception:
                    pass
            cleanup_failed = True
        finally:
            if cleanup is not None:
                _quiet_close(cleanup)

    verify = None
    try:
        verify = connect(dsn)
        _check_admin_session(verify, require_clean=False, require_grant_authority=False)
        if _is_member(verify):
            code = "TEMPORARY_MEMBERSHIP_CLEANUP_FAILED" if cleanup_failed else "TEMPORARY_MEMBERSHIP_STILL_PRESENT"
            raise SourceCorrectionError(code)
    except SourceCorrectionError:
        raise
    except Exception:
        raise SourceCorrectionError("INDEPENDENT_MEMBERSHIP_VERIFICATION_FAILED") from None
    finally:
        if verify is not None:
            _quiet_close(verify)


def execute_authorized_correction(
    *,
    dsn: str,
    sql: str,
    sql_sha256: str,
    authorization_reference: str,
    connect: Callable = psycopg.connect,
) -> CorrectionResult:
    """Execute one approved raw-table UPDATE with transactional role cleanup."""
    validate_admin_dsn(dsn)
    validate_correction_sql(sql)
    digest = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", sql_sha256 or "") or digest != sql_sha256:
        raise SourceCorrectionError("CORRECTION_SQL_DIGEST_MISMATCH")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}", authorization_reference or ""):
        raise SourceCorrectionError("OWNER_AUTHORIZATION_REFERENCE_REQUIRED")

    conn = None
    transaction_started = False
    membership_may_have_changed = False
    failure: SourceCorrectionError | None = None
    affected_rows = 0
    try:
        conn = connect(dsn)
        _check_admin_session(conn, require_clean=True)
        _check_correction_role(conn)
        _check_exact_privileges(conn)

        # psycopg starts a transaction on the first catalog SELECT above. Keep
        # the temporary grant, role switch, correction, reset, and revoke in it.
        transaction_started = True
        membership_may_have_changed = True
        with conn.cursor() as cursor:
            cursor.execute(f"GRANT {CORRECTION_ROLE} TO {ADMIN_LOGIN}")
            cursor.execute(f"SET ROLE {CORRECTION_ROLE}")
        active = _fetchone(
            conn,
            """SELECT session_user, current_user,
                      pg_has_role(session_user, current_user, 'MEMBER')""",
        )
        if active != (ADMIN_LOGIN, CORRECTION_ROLE, True):
            raise SourceCorrectionError("CORRECTION_SET_ROLE_VERIFICATION_FAILED")
        _check_exact_privileges(conn)

        with conn.cursor() as cursor:
            cursor.execute(sql)
            affected_rows = cursor.rowcount
            cursor.execute("RESET ROLE")
        identity = _fetchone(conn, "SELECT session_user, current_user")
        if identity != (ADMIN_LOGIN, ADMIN_LOGIN):
            raise SourceCorrectionError("CORRECTION_RESET_ROLE_VERIFICATION_FAILED")
        with conn.cursor() as cursor:
            cursor.execute(f"REVOKE {CORRECTION_ROLE} FROM {ADMIN_LOGIN}")
        if _is_member(conn):
            raise SourceCorrectionError("TEMPORARY_MEMBERSHIP_REVOKE_FAILED")
        conn.commit()
        transaction_started = False
    except SourceCorrectionError as exc:
        failure = exc
    except Exception:
        failure = SourceCorrectionError("CORRECTION_TRANSACTION_FAILED")
    finally:
        if conn is not None:
            if transaction_started:
                try:
                    conn.rollback()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass

    if membership_may_have_changed:
        try:
            _ensure_membership_revoked(dsn, connect=connect)
        except SourceCorrectionError as exc:
            raise exc from None
    if failure is not None:
        raise failure from None
    return CorrectionResult(affected_rows, digest, authorization_reference)

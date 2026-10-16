from __future__ import annotations

import hashlib
import subprocess
import unittest
from unittest.mock import Mock

from backend.nhl.source_correction_access import (
    ADMIN_LOGIN,
    CORRECTION_ROLE,
    KEYCHAIN_SERVICE,
    SourceCorrectionError,
    execute_authorized_correction,
    load_admin_dsn,
    validate_admin_dsn,
    validate_correction_sql,
)


ADMIN_DSN = "postgresql://postgres:placeholder@db.invalid/postgres?sslmode=require"
GOOD_SQL = "UPDATE nhl.skater_game_logs_raw SET pp_toi_minutes = 2 WHERE player_id = 1;"


class FakeDatabase:
    def __init__(self, *, fail_update: bool = False, membership: bool = False):
        self.membership = membership
        self.fail_update = fail_update
        self.connections: list[FakeConnection] = []

    def connect(self, _dsn: str) -> "FakeConnection":
        conn = FakeConnection(self)
        self.connections.append(conn)
        return conn


class FakeConnection:
    def __init__(self, database: FakeDatabase):
        self.database = database
        self.current_user = ADMIN_LOGIN
        self.rowcount = -1
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.snapshot: tuple[bool, str] | None = None
        self.statements: list[str] = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True
        self.snapshot = (self.database.membership, self.current_user)

    def rollback(self):
        self.rolled_back = True
        if self.snapshot is not None:
            self.database.membership, self.current_user = self.snapshot
        else:
            self.database.membership = False
            self.current_user = ADMIN_LOGIN

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, conn: FakeConnection):
        self.conn = conn
        self.row = None
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement: str, _params=()):
        normalized = " ".join(statement.lower().split())
        self.conn.statements.append(normalized)
        if normalized == "begin":
            self.conn.snapshot = (self.conn.database.membership, self.conn.current_user)
        elif normalized.startswith("grant "):
            self.conn.database.membership = True
        elif normalized == f"set role {CORRECTION_ROLE}":
            if not self.conn.database.membership:
                raise RuntimeError("membership missing")
            self.conn.current_user = CORRECTION_ROLE
        elif normalized == "reset role":
            self.conn.current_user = ADMIN_LOGIN
        elif normalized.startswith("revoke "):
            self.conn.database.membership = False
        elif normalized.startswith("update "):
            if self.conn.database.fail_update:
                raise RuntimeError("simulated correction failure")
            self.rowcount = 3
            self.conn.rowcount = 3
        elif "current_setting('server_version_num')" in normalized:
            self.row = (ADMIN_LOGIN, self.conn.current_user, 150008, True, self.conn.database.membership)
        elif "from pg_roles where rolname = %s" in normalized:
            self.row = (False, False, False, False, False, False)
        elif "has_database_privilege" in normalized:
            self.row = (True, True, False, True, False, False, False, False,
                        ["shot_attempts", "pp_toi_minutes"], False)
        elif "pg_has_role(session_user, current_user" in normalized:
            self.row = (ADMIN_LOGIN, self.conn.current_user, self.conn.database.membership)
        elif normalized == "select session_user, current_user":
            self.row = (ADMIN_LOGIN, self.conn.current_user)
        elif "select pg_has_role(%s, %s, 'member')" in normalized:
            self.row = (self.conn.database.membership,)
        else:
            raise AssertionError(f"unexpected SQL in offline test: {statement}")

    def fetchone(self):
        return self.row


class NHLSourceCorrectionAccessTests(unittest.TestCase):
    def test_keychain_lookup_is_secret_safe_and_fails_closed(self):
        secret = "postgresql://postgres:do-not-print@db.invalid/postgres?sslmode=require"
        runner = Mock(return_value=subprocess.CompletedProcess([], 1, stdout=secret, stderr=secret))
        with self.assertRaisesRegex(SourceCorrectionError, "ADMIN_CREDENTIAL_UNAVAILABLE") as caught:
            load_admin_dsn(runner=runner, environ={})
        self.assertNotIn("do-not-print", str(caught.exception))
        self.assertEqual(runner.call_args.args[0][3], KEYCHAIN_SERVICE)
        self.assertEqual(runner.call_args.args[0][5], ADMIN_LOGIN)
        self.assertNotIn(secret, repr(runner.call_args.args[0]))

    def test_environment_admin_dsn_is_rejected(self):
        with self.assertRaisesRegex(SourceCorrectionError, "ADMIN_CREDENTIAL_MUST_NOT_USE_ENVIRONMENT"):
            load_admin_dsn(runner=Mock(), environ={"NHL_SOURCE_CORRECTION_ADMIN_DSN": "secret"})

    def test_admin_dsn_requires_postgres_role_database_and_tls(self):
        validate_admin_dsn(ADMIN_DSN)
        for unsafe in (
            "postgresql://research:placeholder@db.invalid/postgres?sslmode=require",
            "postgresql://postgres:placeholder@db.invalid/postgres?sslmode=disable",
        ):
            with self.subTest(unsafe=unsafe), self.assertRaises(SourceCorrectionError):
                validate_admin_dsn(unsafe)

    def test_sql_accepts_only_single_update_of_approved_columns(self):
        validate_correction_sql(GOOD_SQL)
        validate_correction_sql(
            "UPDATE ONLY nhl.skater_game_logs_raw SET \"shot_attempts\" = 4 WHERE game_id = 7"
        )
        for unsafe in (
            "UPDATE nhl.skater_game_logs_raw SET team_id = 4 WHERE player_id = 1",
            "UPDATE public.users SET pp_toi_minutes = 2 WHERE id = 1",
            "UPDATE nhl.skater_game_logs_raw SET pp_toi_minutes = 2 WHERE player_id = 1; RESET ROLE",
            r"UPDATE nhl.skater_game_logs_raw SET pp_toi_minutes = 'x\'; RESET ROLE' WHERE player_id = 1",
            "COMMIT",
        ):
            with self.subTest(unsafe=unsafe), self.assertRaises(SourceCorrectionError):
                validate_correction_sql(unsafe)

    def test_transaction_resets_revokes_and_verifies_on_new_connections(self):
        database = FakeDatabase()
        digest = hashlib.sha256(GOOD_SQL.encode()).hexdigest()
        result = execute_authorized_correction(
            dsn=ADMIN_DSN,
            sql=GOOD_SQL,
            sql_sha256=digest,
            authorization_reference="owner-approved-change-123",
            connect=database.connect,
        )
        self.assertEqual(result.affected_rows, 3)
        self.assertFalse(database.membership)
        self.assertEqual(len(database.connections), 3)
        statements = database.connections[0].statements
        self.assertLess(statements.index(f"grant {CORRECTION_ROLE} to {ADMIN_LOGIN}"),
                        statements.index(f"set role {CORRECTION_ROLE}"))
        self.assertLess(statements.index(GOOD_SQL.lower()), statements.index("reset role"))
        self.assertLess(statements.index("reset role"),
                        statements.index(f"revoke {CORRECTION_ROLE} from {ADMIN_LOGIN}"))
        self.assertTrue(database.connections[1].closed)
        self.assertTrue(database.connections[2].closed)

    def test_failed_correction_rolls_back_membership_and_verifies_absence(self):
        database = FakeDatabase(fail_update=True)
        digest = hashlib.sha256(GOOD_SQL.encode()).hexdigest()
        with self.assertRaisesRegex(SourceCorrectionError, "CORRECTION_TRANSACTION_FAILED"):
            execute_authorized_correction(
                dsn=ADMIN_DSN,
                sql=GOOD_SQL,
                sql_sha256=digest,
                authorization_reference="owner-approved-change-456",
                connect=database.connect,
            )
        self.assertFalse(database.membership)
        self.assertEqual(len(database.connections), 3)
        self.assertTrue(database.connections[0].rolled_back)
        self.assertTrue(database.connections[1].closed)
        self.assertTrue(database.connections[2].closed)

    def test_preexisting_membership_fails_closed_without_revoking_it(self):
        database = FakeDatabase(membership=True)
        digest = hashlib.sha256(GOOD_SQL.encode()).hexdigest()
        with self.assertRaisesRegex(SourceCorrectionError, "CORRECTION_ROLE_MEMBERSHIP_ALREADY_PRESENT"):
            execute_authorized_correction(
                dsn=ADMIN_DSN,
                sql=GOOD_SQL,
                sql_sha256=digest,
                authorization_reference="owner-approved-change-789",
                connect=database.connect,
            )
        self.assertTrue(database.membership)
        self.assertEqual(len(database.connections), 1)
        self.assertFalse(any(s.startswith("grant ") or s.startswith("revoke ")
                             for s in database.connections[0].statements))

    def test_digest_mismatch_fails_before_database_connection(self):
        connect = Mock(side_effect=AssertionError("database should not be contacted"))
        with self.assertRaisesRegex(SourceCorrectionError, "CORRECTION_SQL_DIGEST_MISMATCH"):
            execute_authorized_correction(
                dsn=ADMIN_DSN,
                sql=GOOD_SQL,
                sql_sha256="0" * 64,
                authorization_reference="owner-approved-change-987",
                connect=connect,
            )
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()

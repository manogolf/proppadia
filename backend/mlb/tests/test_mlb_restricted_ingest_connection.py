"""Offline checks for the process-scoped MLB restricted writer route."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from backend.shared.db import pg


_OK_ROW = {
    "session_user": "proppadia_mlb_ingest_login",
    "current_user": "proppadia_mlb_ingest",
    "login_ok": True,
    "group_ok": True,
    "group_isolated": True,
    "membership_ok": True,
    "schema_usage": True,
    "schema_create_denied": True,
    "functions_ok": True,
    "source_update_denied": True,
    "source_insert_denied": True,
    "training_insert_denied": True,
    "outcomes_protected": True,
    "destructive_denied": True,
    "adjacent_direct_writes_denied": True,
    "streak_read_contract": True,
}


class _Cursor:
    def __init__(self, row=None, error=None):
        self.row = row
        self.error = error
        self.statements = []
        self.parameters = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params=None):
        self.statements.append(statement)
        self.parameters.append(params)
        if self.error is not None:
            raise self.error

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, row=None, error=None):
        self.test_cursor = _Cursor(row=row, error=error)
        self.closed = False
        self.cursor_count = 0

    def cursor(self):
        self.cursor_count += 1
        return self.test_cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def close(self):
        self.closed = True


class MlbRestrictedIngestConnectionTests(unittest.TestCase):
    def test_success_sets_role_and_verifies_same_connection_identity_and_acl(self):
        conn = _Connection(row=_OK_ROW)

        identity = pg.configure_mlb_ingest_connection(conn)

        self.assertEqual(
            identity,
            {
                "session_user": "proppadia_mlb_ingest_login",
                "current_user": "proppadia_mlb_ingest",
            },
        )
        self.assertEqual(conn.cursor_count, 1)
        self.assertEqual(conn.test_cursor.statements[0], "SET ROLE proppadia_mlb_ingest")
        self.assertIn("session_user::text", conn.test_cursor.statements[1])
        self.assertIn("current_user::text", conn.test_cursor.statements[1])
        self.assertEqual(
            conn.test_cursor.parameters[1],
            (
                list(pg._MLB_INGEST_FUNCTIONS),
                10,
                "proppadia_mlb_ingest",
                len(pg._MLB_INGEST_PROTECTED_RELATIONS),
                list(pg._MLB_INGEST_PROTECTED_RELATIONS),
                False,
            ),
        )

    def test_receipt_identity_is_read_from_writer_connection(self):
        conn = _Connection(row=_OK_ROW)

        evidence = pg.mlb_ingest_session_identity(conn)

        self.assertEqual(
            evidence,
            {
                "session_user": "proppadia_mlb_ingest_login",
                "current_user": "proppadia_mlb_ingest",
            },
        )
        self.assertEqual(conn.test_cursor.statements, ["SELECT session_user::text, current_user::text"])

    def test_receipt_identity_rejects_role_reset(self):
        conn = _Connection(row={**_OK_ROW, "current_user": "proppadia_mlb_ingest_login"})

        with self.assertRaisesRegex(RuntimeError, "role identity assertion failed"):
            pg.mlb_ingest_session_identity(conn)

    def test_wrong_login_or_role_fails_closed(self):
        conn = _Connection(row={**_OK_ROW, "current_user": "postgres"})

        with self.assertRaisesRegex(RuntimeError, "identity or permission assertion failed"):
            pg.configure_mlb_ingest_connection(conn)

    def test_permission_drift_fails_closed(self):
        conn = _Connection(row={**_OK_ROW, "destructive_denied": False})

        with self.assertRaisesRegex(RuntimeError, "identity or permission assertion failed"):
            pg.configure_mlb_ingest_connection(conn)

    def test_missing_stat_derived_streak_read_contract_fails_closed(self):
        conn = _Connection(row={**_OK_ROW, "streak_read_contract": False})

        with self.assertRaisesRegex(RuntimeError, "identity or permission assertion failed"):
            pg.configure_mlb_ingest_connection(conn, require_streak_read=True)

    def test_stat_derived_streak_read_contract_passes_when_required(self):
        conn = _Connection(row=_OK_ROW)

        identity = pg.configure_mlb_ingest_connection(conn, require_streak_read=True)

        self.assertEqual(identity["current_user"], "proppadia_mlb_ingest")
        self.assertTrue(conn.test_cursor.parameters[1][-1])

    def test_database_error_is_sanitized_to_sqlstate(self):
        class _DatabaseError(Exception):
            sqlstate = "42501"

        conn = _Connection(error=_DatabaseError("contains a fake secret DSN"))

        with self.assertRaises(RuntimeError) as raised:
            pg.configure_mlb_ingest_connection(conn)

        self.assertEqual(
            str(raised.exception),
            "restricted MLB role activation or verification failed (SQLSTATE 42501)",
        )
        self.assertNotIn("fake secret", str(raised.exception))

    def test_blank_override_does_not_fall_back_to_shared_alias(self):
        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_OPT_IN_ENV: "1", pg.MLB_INGEST_DATABASE_URL_ENV: "   "},
        ), patch.object(pg, "get_database_url", return_value="shared-url"), patch.object(
            pg.psycopg, "connect"
        ) as connect:
            with self.assertRaisesRegex(RuntimeError, "opt-in is set but its URL is missing"):
                pg._connect()
        connect.assert_not_called()

    def test_stored_override_does_not_change_shared_routing_without_process_opt_in(self):
        conn = _Connection(row=_OK_ROW)
        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_DATABASE_URL_ENV: "restricted-url"},
            clear=True,
        ), patch.object(pg, "get_database_url", return_value="shared-url"), patch.object(
            pg.psycopg, "connect", return_value=conn
        ) as connect:
            self.assertIs(pg._connect(), conn)

        self.assertEqual(connect.call_args.args[0], "shared-url")
        self.assertEqual(conn.cursor_count, 0)

    def test_shared_alias_behavior_is_unchanged_when_override_is_absent(self):
        conn = _Connection(row=_OK_ROW)
        with patch.dict(os.environ, {}, clear=True), patch.object(
            pg, "get_database_url", return_value="shared-url"
        ), patch.object(pg.psycopg, "connect", return_value=conn) as connect:
            self.assertIs(pg._connect(), conn)

        self.assertEqual(connect.call_args.args[0], "shared-url")
        self.assertEqual(conn.cursor_count, 0)

    def test_explicit_override_activates_role_and_sanitizes_connect_error(self):
        class _DatabaseError(Exception):
            sqlstate = "28P01"

        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_OPT_IN_ENV: "1", pg.MLB_INGEST_DATABASE_URL_ENV: "restricted-url"},
        ), patch.object(pg, "verify_mlb_ingest_production_target") as verify_target, patch.object(
            pg.psycopg, "connect", side_effect=_DatabaseError("contains a fake secret DSN")
        ) as connect:
            with self.assertRaises(RuntimeError) as raised:
                pg._connect()

        verify_target.assert_called_once_with("restricted-url")
        self.assertEqual(connect.call_args.args[0], "restricted-url")
        self.assertEqual(str(raised.exception), "restricted MLB database connection failed (SQLSTATE 28P01)")
        self.assertNotIn("fake secret", str(raised.exception))

    def test_project_reference_mismatch_fails_before_connect(self):
        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_OPT_IN_ENV: "1", pg.MLB_INGEST_DATABASE_URL_ENV: "wrong-target"},
        ), patch.object(
            pg, "verify_mlb_ingest_production_target", side_effect=RuntimeError("project reference mismatch")
        ), patch.object(pg.psycopg, "connect") as connect:
            with self.assertRaisesRegex(RuntimeError, "project reference mismatch"):
                pg._connect()
        connect.assert_not_called()

    def test_roster_uses_explicit_override_without_shared_alias_fallback(self):
        from backend.mlb.scripts import refresh_mlb_players_rosters as roster

        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_OPT_IN_ENV: "1", pg.MLB_INGEST_DATABASE_URL_ENV: "restricted-url"},
        ):
            self.assertEqual(roster._db_url(), "restricted-url?sslmode=require&gssencmode=disable")

    def test_roster_keeps_shared_alias_when_only_restricted_dsn_is_stored(self):
        from backend.mlb.scripts import refresh_mlb_players_rosters as roster

        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_DATABASE_URL_ENV: "restricted-url", "SUPABASE_DB_URL": "shared-url"},
            clear=True,
        ):
            self.assertEqual(roster._db_url(), "shared-url?sslmode=require&gssencmode=disable")

    def test_roster_shared_alias_fallback_is_unchanged_without_override(self):
        from backend.mlb.scripts import refresh_mlb_players_rosters as roster

        with patch.dict(os.environ, {"SUPABASE_DB_URL": "shared-url"}, clear=True):
            self.assertEqual(roster._db_url(), "shared-url?sslmode=require&gssencmode=disable")

    def test_roster_verifies_target_before_connect_and_sets_role(self):
        from backend.mlb.scripts import refresh_mlb_players_rosters as roster

        conn = _Connection(row=_OK_ROW)
        order = []

        def verify(_dsn):
            order.append("target")

        def connect(_dsn, **_kwargs):
            order.append("connect")
            return conn

        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_OPT_IN_ENV: "1", pg.MLB_INGEST_DATABASE_URL_ENV: "restricted-url"},
        ), patch.object(
            roster, "verify_mlb_ingest_production_target", side_effect=verify
        ), patch.object(roster.psycopg, "connect", side_effect=connect), patch.object(
            roster, "configure_mlb_ingest_connection", wraps=pg.configure_mlb_ingest_connection
        ) as configure:
            with roster._connect_db("restricted-url") as returned:
                self.assertIs(returned, conn)

        self.assertEqual(order, ["target", "connect"])
        configure.assert_called_once_with(conn)

    def test_roster_target_mismatch_prevents_connection(self):
        from backend.mlb.scripts import refresh_mlb_players_rosters as roster

        with patch.dict(
            os.environ,
            {pg.MLB_INGEST_OPT_IN_ENV: "1", pg.MLB_INGEST_DATABASE_URL_ENV: "wrong-target"},
        ), patch.object(
            roster, "verify_mlb_ingest_production_target", side_effect=RuntimeError("project reference mismatch")
        ), patch.object(roster.psycopg, "connect") as connect:
            with self.assertRaisesRegex(RuntimeError, "project reference mismatch"):
                roster._connect_db("wrong-target")
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()

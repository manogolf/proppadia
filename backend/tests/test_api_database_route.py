"""Hosted API database route: the API login replaces the owner aliases."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from backend.mlb.scripts.create_mlb_schema_acl_backup import EXPECTED_PROJECT_REF
from backend.shared.db import pg

API_URL = f"postgresql://proppadia_api_login.{EXPECTED_PROJECT_REF}:pw@pooler.invalid:6543/postgres"
OWNER_URL = f"postgresql://postgres.{EXPECTED_PROJECT_REF}:pw@pooler.invalid:6543/postgres"
LEGACY_URL = "postgresql://legacy.invalid/postgres"


def _identity(**overrides):
    row = {
        "session_user": pg.API_LOGIN,
        "current_user": pg.API_LOGIN,
        "role_inherited": True,
        "attributes_ok": True,
        "no_owner_reach": True,
    }
    row.update(overrides)
    return row


def _fake_conn(row):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = row
    return conn, cur


class ApiDatabaseRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = (pg._api_route_selected, pg._api_identity_verified)
        pg._api_route_selected = False
        pg._api_identity_verified = False

    def tearDown(self) -> None:
        pg._api_route_selected, pg._api_identity_verified = self._saved

    def test_scripts_never_use_the_api_login_without_selection(self) -> None:
        with patch.dict(os.environ, {pg.API_DATABASE_URL_ENV: API_URL}, clear=True):
            self.assertIsNone(pg._api_route_url())
            self.assertEqual(pg.api_database_route_state(), "NOT_SELECTED")

    def test_unset_api_url_keeps_legacy_aliases_for_staged_cutover(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(pg.select_api_database_route())
            self.assertEqual(pg.api_database_route_state(), "LEGACY_ALIASES")
            conn, _ = _fake_conn(None)
            with patch.object(pg, "_db_url", return_value=LEGACY_URL), \
                 patch.object(pg.psycopg, "connect", return_value=conn) as connect:
                pg.pg_connect()
        self.assertEqual(connect.call_args.args[0], LEGACY_URL)

    def test_selected_route_connects_as_api_login_and_checks_identity_once(self) -> None:
        conn, cur = _fake_conn(_identity())
        with patch.dict(os.environ, {pg.API_DATABASE_URL_ENV: API_URL}, clear=True), \
             patch.object(pg, "_db_url", side_effect=AssertionError("legacy alias read")), \
             patch.object(pg.psycopg, "connect", return_value=conn) as connect:
            self.assertTrue(pg.select_api_database_route())
            self.assertEqual(pg.api_database_route_state(), "API_LOGIN")
            pg.pg_connect()
            pg.pg_connect()
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(connect.call_args.args[0], API_URL)
        self.assertEqual(cur.execute.call_count, 1)
        self.assertEqual(cur.execute.call_args.args[1][0], pg.API_ROLE)

    def test_owner_login_in_api_variable_is_refused_before_connecting(self) -> None:
        with patch.dict(os.environ, {pg.API_DATABASE_URL_ENV: OWNER_URL}, clear=True), \
             patch.object(pg.psycopg, "connect") as connect:
            with self.assertRaisesRegex(RuntimeError, "does not log in as proppadia_api_login") as ctx:
                pg.select_api_database_route()
            with self.assertRaisesRegex(RuntimeError, "does not log in as proppadia_api_login"):
                pg.pg_connect()
        connect.assert_not_called()
        self.assertNotIn("pw", str(ctx.exception))

    def test_other_project_is_refused(self) -> None:
        other = API_URL.replace(EXPECTED_PROJECT_REF, "otherprojectref")
        with patch.dict(os.environ, {pg.API_DATABASE_URL_ENV: other}, clear=True), \
             patch.object(pg.psycopg, "connect") as connect:
            with self.assertRaisesRegex(RuntimeError, "project reference mismatch"):
                pg.select_api_database_route()
        connect.assert_not_called()

    def test_ingest_opt_in_cannot_combine_with_api_route(self) -> None:
        env = {pg.API_DATABASE_URL_ENV: API_URL, pg.MLB_INGEST_OPT_IN_ENV: "1"}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(RuntimeError, "MLB ingest opt-in"):
                pg.select_api_database_route()

    def test_owner_reach_or_wrong_identity_fails_closed(self) -> None:
        for bad in (
            _identity(no_owner_reach=False),
            _identity(attributes_ok=False),
            _identity(role_inherited=False),
            _identity(session_user="postgres", current_user="postgres"),
            None,
        ):
            pg._api_identity_verified = False
            conn, _ = _fake_conn(bad)
            with patch.dict(os.environ, {pg.API_DATABASE_URL_ENV: API_URL}, clear=True), \
                 patch.object(pg.psycopg, "connect", return_value=conn):
                pg.select_api_database_route()
                with self.assertRaisesRegex(RuntimeError, "identity or owner-reach"):
                    pg.pg_connect()
            conn.close.assert_called_once()
            self.assertFalse(pg._api_identity_verified)

    def test_connection_failure_message_has_no_driver_text(self) -> None:
        class Failure(Exception):
            sqlstate = "28P01"

        with patch.dict(os.environ, {pg.API_DATABASE_URL_ENV: API_URL}, clear=True), \
             patch.object(pg.psycopg, "connect", side_effect=Failure("password for user x at host y")):
            pg.select_api_database_route()
            with self.assertRaises(RuntimeError) as ctx:
                pg.pg_fetchone("SELECT 1")
        self.assertEqual(str(ctx.exception), "API database connection failed (SQLSTATE 28P01)")

    def test_api_server_selects_the_route_at_import(self) -> None:
        from backend.app import api_server  # noqa: F401  (import selects the route)
        import importlib

        with patch.object(pg, "select_api_database_route") as select:
            importlib.reload(api_server)
        select.assert_called_once_with()

    def test_ping_db_error_omits_driver_message(self) -> None:
        from backend.app import deps

        class Failure(Exception):
            sqlstate = "28P01"

        with patch.object(deps, "shared_pg_fetchone", side_effect=Failure("user postgres at host")):
            ok, row, err = deps.pg_fetchone("SELECT 1")
        self.assertFalse(ok)
        self.assertEqual(err, "Failure (SQLSTATE 28P01)")

    def test_nhl_user_props_ddl_is_skipped_on_the_api_login(self) -> None:
        from backend.domains.nhl.repository import prop_repository

        with patch.dict(os.environ, {pg.API_DATABASE_URL_ENV: API_URL}, clear=True), \
             patch.object(prop_repository, "_user_props_ensured", False), \
             patch.object(prop_repository, "pg_connect", side_effect=AssertionError("DDL ran")):
            pg.select_api_database_route()
            prop_repository.ensure_user_props_table()
            self.assertTrue(prop_repository._user_props_ensured)


if __name__ == "__main__":
    unittest.main()

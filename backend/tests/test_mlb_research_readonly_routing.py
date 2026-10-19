from __future__ import annotations

import os
import re
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from backend.shared.db import mlb_research
from backend.shared.db import pg


ROOT = Path(__file__).resolve().parents[2]
RESEARCH_READERS = (
    "report_mlb_hits_environment.py",
    "run_mlb_hits_lane_selector_daily.py",
    "run_mlb_hits_lane_selector_report.py",
    "build_mlb_overlap_role_profile_watch.py",
    "report_mlb_hits_recency_vs_baseline.py",
    "report_mlb_hits_interaction_failure.py",
    "audit_mlb_hits_feature_parity.py",
    "assemble_mlb_hits05_2026_season_to_date_evidence_v1.py",
    "update_mlb_starter_skill_workload_observation.py",
    "build_mlb_starter_skill_workload_research.py",
)


class MlbResearchReadRoutingTests(unittest.TestCase):
    def test_only_in_process_opt_in_selects_research_url(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(mlb_research.research_database_url("postgresql://legacy"), "postgresql://legacy")
            with self.assertRaises(RuntimeError):
                mlb_research.research_database_url()
        with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True):
            self.assertEqual(mlb_research.research_database_url("postgresql://legacy"), "postgresql://research")

    def test_research_engine_starts_read_only_and_checks_login_and_role(self) -> None:
        listeners: list[tuple[object, str, object]] = []
        fake_engine = object()

        def listen_for(target, name):
            def decorate(callback):
                listeners.append((target, name, callback))
                return callback
            return decorate

        with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True), \
             patch.object(mlb_research, "create_engine", return_value=fake_engine) as create_engine, \
             patch.object(mlb_research.event, "listens_for", side_effect=listen_for):
            self.assertIs(mlb_research.create_mlb_research_readonly_engine("postgresql://legacy"), fake_engine)
        create_engine.assert_called_once_with("postgresql://research")
        self.assertEqual(len(listeners), 1)
        conn = MagicMock()
        conn.execute.return_value.mappings.return_value.one.return_value = {
            "session_user": mlb_research.MLB_RESEARCH_LOGIN,
            "current_user": mlb_research.MLB_RESEARCH_LOGIN,
            "role_inherited": True,
        }
        listeners[0][2](conn)
        self.assertEqual(conn.exec_driver_sql.call_args.args[0], "SET TRANSACTION READ ONLY")
        self.assertEqual(
            conn.execute.call_args.args[1], {"role_name": mlb_research.MLB_RESEARCH_ROLE}
        )

    def test_wrong_research_identity_fails_closed(self) -> None:
        conn = MagicMock()
        conn.execute.return_value.mappings.return_value.one.return_value = {
            "session_user": "unexpected_login",
            "current_user": "unexpected_login",
            "role_inherited": False,
        }
        with self.assertRaisesRegex(RuntimeError, "identity or inherited role mismatch"):
            mlb_research._assert_research_identity(conn)

    def test_shared_reader_returns_mapping_rows_and_uses_research_login_check(self) -> None:
        cursor = Mock()
        cursor.fetchall.return_value = [{"player_id": 42, "hits": 1}]
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = {
            "session_user": pg.MLB_RESEARCH_LOGIN,
            "current_user": pg.MLB_RESEARCH_LOGIN,
            "role_inherited": True,
        }
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True), \
             patch.object(pg, "psycopg") as psycopg_mock:
            psycopg_mock.rows.dict_row = object()
            psycopg_mock.connect.return_value = conn
            rows = pg.pg_fetchall_mlb_research_readonly("SELECT player_id, hits FROM mlb.player_stats")
        self.assertEqual(rows, [{"player_id": 42, "hits": 1}])
        self.assertEqual(conn.execute.call_args_list[0].args[0], "BEGIN READ ONLY")
        cursor.execute.assert_called_once_with("SELECT player_id, hits FROM mlb.player_stats", ())
        conn.close.assert_called_once()

    def test_generic_reader_route_does_not_consume_research_override(self) -> None:
        with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True), \
             patch.object(pg, "_connect") as legacy_connect:
            legacy_conn = MagicMock()
            legacy_cursor = legacy_conn.cursor.return_value.__enter__.return_value
            legacy_cursor.fetchall.return_value = [{"n": 3}]
            legacy_connect.return_value.__enter__.return_value = legacy_conn
            rows = pg.pg_fetchall("SELECT 3 AS n")
        self.assertEqual(rows, [{"n": 3}])
        legacy_connect.assert_called_once_with()

    def test_named_consumers_use_only_reader_connection_paths(self) -> None:
        script_dir = ROOT / "backend" / "mlb" / "scripts"
        for filename in RESEARCH_READERS:
            source = (script_dir / filename).read_text(encoding="utf-8")
            with self.subTest(consumer=filename):
                if filename in {
                    "report_mlb_hits_environment.py",
                    "update_mlb_starter_skill_workload_observation.py",
                    "build_mlb_starter_skill_workload_research.py",
                }:
                    self.assertIn("pg_fetchall_mlb_research_readonly", source)
                elif filename == "assemble_mlb_hits05_2026_season_to_date_evidence_v1.py":
                    self.assertIn("mlb_research_readonly_connection", source)
                else:
                    self.assertIn("create_mlb_research_readonly_engine", source)
                self.assertNotRegex(source, r"\b(?:INSERT\s+INTO|UPDATE\s+mlb\.|DELETE\s+FROM|TRUNCATE\s+TABLE)\b")

    def test_moneyline_and_bvp_connections_do_not_use_reader_route(self) -> None:
        scripts = ROOT / "backend" / "mlb" / "scripts"
        moneyline = (scripts / "audit_mlb_moneyline_probability_region_premise_v1.py").read_text(encoding="utf-8")
        bvp = (scripts / "run_mlb_bvp_inline_daily.py").read_text(encoding="utf-8")
        self.assertNotIn("create_mlb_research_readonly_engine", moneyline)
        self.assertNotIn("mlb_research_readonly_connection", moneyline)
        self.assertNotIn("create_mlb_research_readonly_engine", bvp)
        self.assertNotIn("mlb_research_readonly_connection", bvp)
        self.assertIn("MLB_MONEYLINE_AUDIT_DATABASE_URL", moneyline)
        self.assertIn("MLB_MONEYLINE_AUDIT_DATABASE_URL", bvp)

    def test_daily_make_targets_scope_research_url_to_approved_reader_commands(self) -> None:
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

        def target_body(name: str) -> str:
            match = re.search(rf"(?m)^{re.escape(name)}:", makefile)
            if match is None:
                raise AssertionError(f"Make target not found: {name}")
            next_target = re.search(r"(?m)^[^\s#][^:\n]*:", makefile[match.end():])
            end = match.end() + next_target.start() if next_target else len(makefile)
            return makefile[match.start():end]

        expected = {
            "mlb-hits-environment-report": "report_mlb_hits_environment.py",
            "mlb-refresh-overlap-role-profile-watch": "build_mlb_overlap_role_profile_watch.py",
            "mlb-daily-upload-prep": "run_mlb_hits_lane_selector_report",
            "mlb-predictions-wide": "build_mlb_predictions_wide.py",
        }
        for target, reader in expected.items():
            with self.subTest(target=target):
                body = target_body(target)
                self.assertIn(reader, body)
                self.assertIn("MLB_RESEARCH_DATABASE_URL=", body)
                self.assertIn("MLB_MONEYLINE_AUDIT_DATABASE_URL", body)

        # These readers need the prepared mlb.game_info column grant before
        # their recipes may set the research URL.
        for target in ("mlb-slate-output", "mlb-book-upload"):
            with self.subTest(grant_pending_target=target):
                self.assertNotIn("MLB_RESEARCH_DATABASE_URL=", target_body(target))

        writer_targets = ("mlb-roster-refresh-all", "mlb-stat-derived-refresh")
        for target in writer_targets:
            with self.subTest(writer_target=target):
                self.assertNotIn("MLB_RESEARCH_DATABASE_URL=", target_body(target))


class MlbDailyReaderResearchRouteTests(unittest.TestCase):
    """Daily predictions-wide, slate-output, and book-upload database reads."""

    def _dbapi_conn(self, identity):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = identity
        return conn, cursor

    def test_dbapi_session_is_read_only_and_identity_checked(self) -> None:
        conn, cursor = self._dbapi_conn(
            (mlb_research.MLB_RESEARCH_LOGIN, mlb_research.MLB_RESEARCH_LOGIN, True)
        )
        mlb_research.configure_mlb_research_dbapi_session(conn)
        self.assertEqual(
            cursor.execute.call_args_list[0].args[0],
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        )
        self.assertEqual(cursor.execute.call_args_list[1].args[1], (mlb_research.MLB_RESEARCH_ROLE,))
        conn.commit.assert_called_once()
        conn.close.assert_not_called()

    def test_dbapi_session_wrong_identity_closes_and_fails(self) -> None:
        conn, _ = self._dbapi_conn(("postgres", "postgres", False))
        with self.assertRaisesRegex(RuntimeError, "identity or inherited role mismatch"):
            mlb_research.configure_mlb_research_dbapi_session(conn)
        conn.close.assert_called_once()

    def test_predictions_wide_reads_use_research_only_when_opted_in(self) -> None:
        from backend.mlb.scripts import build_mlb_predictions_wide as wide

        source = (ROOT / "backend/mlb/scripts/build_mlb_predictions_wide.py").read_text(encoding="utf-8")
        self.assertNotIn("with pg_connect()", source)
        self.assertEqual(source.count("with _read_connection() as conn"), 2)
        with patch.object(wide, "pg_connect", return_value="legacy") as legacy, \
             patch.object(wide, "mlb_research_readonly_connection", return_value="research") as research:
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(wide._read_connection(), "legacy")
            with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True):
                self.assertEqual(wide._read_connection(), "research")
        legacy.assert_called_once_with()
        research.assert_called_once_with()

    def test_slate_output_and_book_upload_connections_prefer_opted_in_research(self) -> None:
        import psycopg2

        from backend.mlb.scripts import build_mlb_slate_output as slate
        from backend.mlb.scripts import export_mlb_book_upload as book

        for module in (slate, book):
            with self.subTest(module=module.__name__):
                with patch.object(psycopg2, "connect", return_value=MagicMock()) as connect, \
                     patch("backend.shared.db.mlb_research.configure_mlb_research_dbapi_session") as configure_shared, \
                     patch.object(slate, "configure_mlb_research_dbapi_session") as configure_slate:
                    env = {"MLB_RESEARCH_DATABASE_URL": "postgresql://research", "SUPABASE_DB_URL": "postgresql://legacy"}
                    with patch.dict(os.environ, env, clear=True):
                        conn = module._get_db_conn()
                    connect.assert_called_once_with("postgresql://research")
                    configure = configure_slate if module is slate else configure_shared
                    configure.assert_called_once_with(conn)

                    connect.reset_mock()
                    configure.reset_mock()
                    with patch.dict(os.environ, {"SUPABASE_DB_URL": "postgresql://legacy"}, clear=True):
                        module._get_db_conn()
                    connect.assert_called_once_with("postgresql://legacy")
                    configure.assert_not_called()


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
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

    def test_eight_named_consumers_use_only_reader_connection_paths(self) -> None:
        script_dir = ROOT / "backend" / "mlb" / "scripts"
        for filename in RESEARCH_READERS:
            source = (script_dir / filename).read_text(encoding="utf-8")
            with self.subTest(consumer=filename):
                if filename == "report_mlb_hits_environment.py":
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


if __name__ == "__main__":
    unittest.main()

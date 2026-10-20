"""Daily MLB readers that silently degraded once the owner aliases were removed (run 97)."""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from backend.shared.db import mlb_research

ROOT = Path(__file__).resolve().parents[3]
MIGRATIONS = ROOT / "backend/mlb/sql/migrations"
FORWARD = MIGRATIONS / "20261019_mlb_research_daily_fallback_read.sql"
ROLLBACK = MIGRATIONS / "20261019_mlb_research_daily_fallback_read_rollback.sql"

FETCH_READERS = (
    "backend/domains/mlb/prop_workflow.py",
    "backend/domains/mlb/repository/player_repository.py",
    "backend/mlb/shared/bvp_identity.py",
    "backend/mlb/scripts/analyze_mlb_prediction_quality.py",
    "backend/mlb/scripts/audit_mlb_prediction_flow.py",
    "backend/mlb/scripts/report_mlb_feature_health.py",
    "backend/mlb/scripts/report_mlb_bvp_impact.py",
    "backend/mlb/scripts/run_mlb_hits_15_tier_backtest.py",
    "backend/mlb/scripts/score_mlb_hits05_full_board_shadow_v1.py",
    "backend/mlb/scripts/build_mlb_hits_nonmarket_player_game_feature_spine.py",
    "backend/mlb/scripts/run_mlb_review_aid_performance_tracker.py",
)
CONNECTION_READERS = (
    "backend/mlb/scripts/capture_mlb_market_strong_agreement_live_v4.py",
    "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py",
    "backend/mlb/scripts/cleanroom_v1/routine_outcome_reconciliation.py",
)


def _grants(sql: str, keyword: str) -> dict[str, set[str]]:
    pattern = rf"{keyword}\s*\((.*?)\)\s*ON\s+([\w.]+)\s+(?:TO|FROM)\s+proppadia_mlb_research_ro"
    return {
        relation: {column.strip() for column in columns.split(",")}
        for columns, relation in re.findall(pattern, sql, flags=re.IGNORECASE | re.DOTALL)
    }


class ReaderRouteSourceTests(unittest.TestCase):
    def test_fetch_readers_import_only_research_variants(self) -> None:
        statement = re.compile(r"from backend\.shared\.db(?:\.pg)? import (\([^)]*\)|[^\n]+)")
        for path in FETCH_READERS:
            source = (ROOT / path).read_text(encoding="utf-8")
            with self.subTest(reader=path):
                items = [
                    " ".join(item.split())
                    for match in statement.findall(source)
                    for item in match.strip("()").split(",")
                    if item.strip()
                ]
                fetch_items = [item for item in items if item.endswith(("pg_fetchall", "pg_fetchone"))]
                self.assertTrue(fetch_items)
                for item in fetch_items:
                    self.assertRegex(item, r"^pg_fetch(all|one)_mlb_research_readonly as pg_fetch\1$")

    def test_connection_readers_use_the_research_or_current_connection(self) -> None:
        for path in CONNECTION_READERS:
            source = (ROOT / path).read_text(encoding="utf-8")
            with self.subTest(reader=path):
                self.assertIn("mlb_research_or_current_connection", source)
                self.assertNotRegex(source, r"\bpg_connect\(\)")

    def test_durable_store_routes_only_the_prediction_read(self) -> None:
        source = (ROOT / "backend/mlb/public_game_predictions/durable_store_v1.py").read_text(encoding="utf-8")
        body = source[source.index("def fetch_prediction_rows"):]
        body = body[: body.index("\ndef ", 1)]
        self.assertIn("with mlb_research_or_current_connection() as conn", body)
        # Moneyline lifecycle writes stay on their current route (a separate writer decision).
        self.assertEqual(source.count("with mlb_research_or_current_connection()"), 1)
        self.assertGreaterEqual(source.count("with pg_connect() as conn"), 6)

    def test_raw_connection_readers_prefer_the_research_url(self) -> None:
        exact = (ROOT / "backend/mlb/scripts/run_mlb_exact_game_feature_shadow_v1.py").read_text(encoding="utf-8")
        self.assertIn('dsn = os.getenv("MLB_RESEARCH_DATABASE_URL", "").strip() or os.getenv("SUPABASE_DB_URL")', exact)
        ubo5 = (ROOT / "backend/mlb/scripts/build_mlb_ubo5_tb15_daily_closeout.py").read_text(encoding="utf-8")
        self.assertIn("connect_mlb_research_psycopg(row_factory=dict_row)", ubo5)

    def test_daily_wrapper_exports_research_url_before_any_command(self) -> None:
        wrapper = (ROOT / "bin/proppadia_mlb_refresh_daily.sh").read_text(encoding="utf-8")
        export = 'export MLB_RESEARCH_DATABASE_URL="${MLB_RESEARCH_DATABASE_URL:-${MLB_MONEYLINE_AUDIT_DATABASE_URL:-}}"'
        self.assertIn(export, wrapper)
        position = wrapper.index(export)
        self.assertLess(wrapper.index("source backend/.env"), position)
        self.assertLess(position, wrapper.index("\nmake "))
        self.assertLess(position, wrapper.index("bin/mlb_public_game_moneyline_daily_hook.sh"))
        # Never an owner-alias fallback.
        self.assertNotIn("MLB_RESEARCH_DATABASE_URL:-${SUPABASE_DB_URL", wrapper)


class ResearchHelperTests(unittest.TestCase):
    def test_connection_helper_uses_research_only_when_opted_in(self) -> None:
        with patch.dict(os.environ, {}, clear=True), \
             patch("backend.shared.db.pg.pg_connect", return_value="current") as current:
            self.assertEqual(mlb_research.mlb_research_or_current_connection(), "current")
        current.assert_called_once_with()
        with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True), \
             patch.object(mlb_research, "mlb_research_readonly_connection", return_value="research") as research, \
             patch("backend.shared.db.pg.pg_connect") as current:
            self.assertEqual(mlb_research.mlb_research_or_current_connection(), "research")
        research.assert_called_once_with()
        current.assert_not_called()

    def test_psycopg_helper_checks_session_then_sets_row_factory(self) -> None:
        conn = MagicMock()
        factory = object()
        with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True), \
             patch("psycopg.connect", return_value=conn) as connect, \
             patch.object(mlb_research, "configure_mlb_research_dbapi_session") as configure:
            self.assertIs(mlb_research.connect_mlb_research_psycopg(row_factory=factory), conn)
        connect.assert_called_once_with("postgresql://research")
        configure.assert_called_once_with(conn)
        self.assertIs(conn.row_factory, factory)
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                mlb_research.connect_mlb_research_psycopg()

    def test_ubo5_certified_read_uses_research_connection(self) -> None:
        from backend.mlb.scripts import build_mlb_ubo5_tb15_daily_closeout as closeout

        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.execute.return_value.fetchall.return_value = []
        with patch.dict(os.environ, {"MLB_RESEARCH_DATABASE_URL": "postgresql://research"}, clear=True), \
             patch("backend.shared.db.mlb_research.connect_mlb_research_psycopg", return_value=conn) as research, \
             patch("psycopg.connect") as raw:
            outcomes, status = closeout.load_certified_player_stats("2026-10-18")
        research.assert_called_once()
        raw.assert_not_called()
        self.assertEqual(outcomes, {})
        self.assertNotEqual(status, "DATABASE_URL_UNAVAILABLE")


class FallbackGrantContractTests(unittest.TestCase):
    def test_grant_is_column_scoped_and_rollback_matches(self) -> None:
        forward = FORWARD.read_text(encoding="utf-8")
        rollback = ROLLBACK.read_text(encoding="utf-8")
        expected = {
            "mlb.public_game_moneyline_predictions": {"created_at", "prediction_payload"},
            "mlb.public_game_moneyline_outcomes": {"grading_timestamp_utc"},
            "mlb.player_ids": {"id", "created_at", "updated_at"},
            "mlb.game_info": {"starting_pitcher_id_home", "starting_pitcher_id_away"},
            "mlb.player_props": {
                "prop_type", "game_date", "over_under", "outcome", "predicted_outcome", "was_correct",
                "confidence_score", "prop_source", "game_id", "player_id", "created_at", "status",
                "prop_value", "user_id",
            },
            "mlb.model_training_props": {
                "over_under", "outcome", "predicted_outcome", "was_correct", "confidence_score",
                "player_name", "team",
            },
            "mlb.player_derived_stats": {"id", "is_home", "created_at", "updated_at"},
        }
        self.assertEqual(_grants(forward, "GRANT SELECT"), expected)
        self.assertEqual(_grants(rollback, "REVOKE SELECT"), expected)
        upper = forward.upper()
        for forbidden in ("GRANT SELECT ON", "GRANT ALL", "CREATE POLICY", "ALTER ROLE", "CREATE ROLE", "GRANT INSERT", "GRANT UPDATE"):
            self.assertNotIn(forbidden, upper)
        self.assertIn("NOT APPLIED", forward)
        # The account-identifier column is called out for operator review.
        self.assertIn("user_id (an account identifier)", forward)

    def test_grant_covers_the_new_reader_columns(self) -> None:
        granted = _grants(FORWARD.read_text(encoding="utf-8"), "GRANT SELECT")
        spine = (ROOT / "backend/mlb/scripts/build_mlb_hits_nonmarket_player_game_feature_spine.py").read_text()
        self.assertIn("starting_pitcher_id_home, starting_pitcher_id_away", spine)
        quality = (ROOT / "backend/mlb/scripts/analyze_mlb_prediction_quality.py").read_text()
        for column in ("over_under", "outcome", "predicted_outcome", "was_correct", "confidence_score"):
            self.assertIn(column, quality)
            self.assertIn(column, granted["mlb.model_training_props"])
            self.assertIn(column, granted["mlb.player_props"])
        agreement = (ROOT / "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py").read_text()
        self.assertIn("grading_timestamp_utc", agreement)


if __name__ == "__main__":
    unittest.main()

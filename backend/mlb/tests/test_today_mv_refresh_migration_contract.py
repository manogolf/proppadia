"""Offline contract checks for the MLB today refresh capability migration."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/mlb/sql/migrations/20261017_mlb_today_workspace_restricted_refresh.sql"
ROLLBACK = ROOT / "backend/mlb/sql/migrations/20261017_mlb_today_workspace_restricted_refresh_rollback.sql"
EXPECTED = (
    "today_market_snapshot",
    "today_market_timing_signal",
    "today_player_context",
    "today_workspace_mlb",
)


class TodayMaterializedViewRefreshMigrationTests(unittest.TestCase):
    def test_migration_has_only_fixed_concurrent_refresh_targets_in_order(self):
        sql = MIGRATION.read_text()
        refreshes = [
            line.strip().removesuffix(";")
            for line in sql.splitlines()
            if line.strip().upper().startswith("REFRESH MATERIALIZED VIEW")
        ]
        self.assertEqual(
            refreshes,
            [f"REFRESH MATERIALIZED VIEW CONCURRENTLY mlb.{name}" for name in EXPECTED],
        )
        self.assertNotIn("EXECUTE format", sql.upper())
        self.assertNotIn("$1", sql)

    def test_migration_pins_definer_and_limits_execution(self):
        sql = MIGRATION.read_text().lower()
        self.assertIn("security definer", sql)
        self.assertIn("set search_path = pg_catalog, pg_temp", sql)
        self.assertIn("revoke all on function mlb.refresh_today_workspace_views() from public", sql)
        self.assertIn(
            "grant execute on function mlb.refresh_today_workspace_views() to proppadia_mlb_ingest",
            sql,
        )

    def test_rollback_removes_only_the_refresh_capability(self):
        sql = ROLLBACK.read_text().lower()
        self.assertIn("revoke execute on function mlb.refresh_today_workspace_views() from proppadia_mlb_ingest", sql)
        self.assertIn("drop function mlb.refresh_today_workspace_views()", sql)
        self.assertNotIn("drop materialized view", sql)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/nhl/sql/migrations/20261017_safe_skater_source_promotion.sql"
ROLE_MIGRATION = ROOT / "backend/nhl/sql/migrations/20261016_prepare_nhl_write_roles.sql"
ROLLBACK = ROOT / "backend/nhl/sql/migrations/20261017_safe_skater_source_promotion_rollback.sql"
FIXTURE_SETUP = ROOT / "backend/nhl/sql/tests/nhl_safe_promotion_fixture_setup.sql"


def update_columns(sql: str) -> set[str]:
    grant = sql.lower().split("grant update (", 1)[1].split(")\n  on nhl.skater_game_logs_raw", 1)[0]
    return {column.strip() for column in grant.replace("\n", " ").split(",")}


class SafeSourcePromotionContractTests(unittest.TestCase):
    def test_migration_has_fixed_security_definer_null_fill_functions(self):
        sql = MIGRATION.read_text().lower()
        self.assertEqual(sql.count("create or replace function"), 2)
        self.assertEqual(sql.count("security definer"), 2)
        self.assertEqual(sql.count("set search_path = pg_catalog, nhl"), 2)
        self.assertNotRegex(sql, r"\bexecute\s+(format|sql)\b")
        self.assertIn("raw.shot_attempts is null", sql)
        self.assertIn("raw.pp_toi_minutes is null", sql)
        self.assertIn("stage.game_date = p_game_date", sql)
        self.assertIn("raw.pp_toi_minutes is null", sql)
        self.assertIn("shiftcharts_raw", sql)
        self.assertIn("game_manpower_segments", sql)

    def test_migration_revokes_direct_updates_and_restricts_function_execution(self):
        sql = MIGRATION.read_text().lower()
        self.assertIn("revoke update (shot_attempts, pp_toi_minutes)", sql)
        self.assertIn("proppadia_nhl_daily_login", sql)
        self.assertIn("proppadia_nhl_operations_login", sql)
        self.assertIn("proppadia_nhl_ingest", sql)
        self.assertEqual(sql.count("grant execute on function"), 2)
        self.assertIn("from public, anon, authenticated, service_role", sql)
        assertions = (ROOT / "backend/nhl/sql/tests/nhl_write_role_permission_assertions.sql").read_text().lower()
        self.assertIn("has_column_privilege(ingest_login, raw_table, 'shot_attempts', 'update')", assertions)
        self.assertIn("nhl_safe_promotion_function_execute_privilege_mismatch", assertions)
        self.assertIn("nhl_correction_update_column_set_mismatch", assertions)

    def test_fixture_covers_replay_conflict_and_denied_write_cases(self):
        sql = (ROOT / "backend/nhl/sql/tests/nhl_safe_promotion_fixture_test.sql").read_text().lower()
        for expected in (
            "insert into nhl.skater_game_logs_raw",
            "on conflict (player_id,game_id)",
            "fixture_conflict_overwrote_populated_source",
            "fixture_verified_null_fill_failed",
            "fixture_shift_overlap_fill_failed",
            "fixture_direct_attempts_update_was_allowed",
            "fixture_direct_pp_toi_update_was_allowed",
            "fixture_raw_delete_was_allowed",
            "fixture_raw_truncate_was_allowed",
            "fixture_stage_replay_not_idempotent",
            "fixture_pp_toi_replay_not_idempotent",
        ):
            self.assertIn(expected, sql)

    def test_disposable_fixture_defines_migration_roles_and_update_columns(self):
        migration = MIGRATION.read_text().lower()
        setup = (ROOT / "backend/nhl/sql/tests/nhl_safe_promotion_fixture_setup.sql").read_text().lower()
        fixture = (ROOT / "backend/nhl/sql/tests/nhl_safe_promotion_fixture_test.sql").read_text().lower()
        required_roles = (
            "proppadia_nhl_ingest", "proppadia_nhl_daily_login",
            "proppadia_nhl_source_correction", "proppadia_nhl_research_login",
            "proppadia_nhl_operations_login", "proppadia_research_ro",
            "proppadia_nhl_ops_writer", "anon", "authenticated", "service_role",
        )
        for role in required_roles:
            self.assertIn(f"rolname='{role}'", setup)
        self.assertIn("grant proppadia_nhl_ingest to proppadia_nhl_daily_login", setup)
        self.assertIn("set local role proppadia_nhl_daily_login", fixture)

        grant = migration.split("grant update (", 1)[1].split(")\n  on nhl.skater_game_logs_raw", 1)[0]
        columns = {column.strip() for column in grant.replace("\n", " ").split(",")}
        raw_table = setup.split("create table nhl.skater_game_logs_raw(", 1)[1].split(");", 1)[0]
        for column in columns:
            self.assertRegex(raw_table, rf"\b{column}\b")

    def test_fixture_baseline_and_rollback_match_original_ingest_update_contract(self):
        original = ROLE_MIGRATION.read_text()
        rollback = ROLLBACK.read_text()
        setup = FIXTURE_SETUP.read_text()
        original_columns = update_columns(original)
        self.assertEqual(update_columns(rollback), original_columns)
        self.assertEqual(update_columns(setup), original_columns)

    def test_fixture_cleanup_requires_exact_restored_update_columns(self):
        cleanup = (ROOT / "backend/nhl/sql/tests/nhl_safe_promotion_fixture_cleanup.sql").read_text().lower()
        self.assertIn("fixture_cleanup_ingest_update_column_set_mismatch", cleanup)
        self.assertIn("fixture_cleanup_correction_update_column_set_mismatch", cleanup)

    def test_all_active_raw_promoters_use_the_safe_entry_points(self):
        cli = (ROOT / "backend/nhl/cli.py").read_text()
        postgame = (ROOT / "backend/nhl/scripts/run_nhl_postgame_reconciliation.py").read_text()
        fill = (ROOT / "backend/nhl/scripts/fill_pp_toi_minutes_for_date.py").read_text()
        self.assertIn("promote_skater_game_log_protected_fields", cli)
        self.assertIn("promote_skater_game_log_protected_fields", postgame)
        self.assertIn("fill_pp_toi_minutes_from_shift_overlap", fill)
        self.assertNotIn("SET shot_attempts = EXCLUDED.shot_attempts", cli)
        self.assertNotIn("shot_attempts=excluded.shot_attempts", postgame)
        self.assertNotIn("SET pp_toi_minutes = %s", fill)


if __name__ == "__main__":
    unittest.main()

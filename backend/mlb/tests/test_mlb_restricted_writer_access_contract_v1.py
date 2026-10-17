"""Offline guardrails for the prepared MLB restricted-writer contract.

These checks document current SQL behavior; PostgreSQL ACL semantics still
require the separately authorized disposable fixture run.
"""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "docs/contracts/mlb_restricted_writer_access_contract_v1.md"
FIXTURE = ROOT / "backend/mlb/tests/fixtures/restricted_writer_access_v1/README.md"
ROSTER = ROOT / "backend/mlb/scripts/refresh_mlb_players_rosters.py"
STAT_DERIVED = ROOT / "backend/mlb/scripts/insert_mlb_stat_derived.py"
BVP = ROOT / "backend/mlb/scripts/refresh_mlb_bvp_pvb.py"
WORKFLOW = ROOT / ".github/workflows/mlb-refresh-player-ids.yml"


class MlbRestrictedWriterContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = CONTRACT.read_text()
        cls.fixture = FIXTURE.read_text()
        cls.contract_one_line = re.sub(r"\s+", " ", cls.contract)
        cls.fixture_one_line = re.sub(r"\s+", " ", cls.fixture)
        cls.roster = ROSTER.read_text()
        cls.stat = STAT_DERIVED.read_text()
        cls.bvp = BVP.read_text()
        cls.workflow = WORKFLOW.read_text()

    def test_roster_writer_conflict_and_inactive_operations_are_documented(self):
        migration = (ROOT / "backend/mlb/sql/migrations/20261019_mlb_adjacent_writer_entrypoints.sql").read_text()
        self.assertIn("mlb.safe_upsert_player_roster(%s::jsonb)", self.roster)
        self.assertIn("mlb.safe_mark_player_roster_inactive(%s::bigint[],%s,%s)", self.roster)
        self.assertIn("COALESCE(EXCLUDED.%I,mlb.player_ids.%I)", migration)
        self.assertIn("p_successful_teams<GREATEST(p_total_teams-1,1)", migration)
        self.assertIn("mlb.safe_insert_player_id_if_absent(%s::jsonb)", self.stat)
        self.assertIn("ordinary role needs no direct table DML or SELECT", self.contract)
        for field in ("player_name", "team", "team_id", "status", "updated_at"):
            self.assertIn(f"`{field}`", self.contract)
        self.assertIn("Production has no `active` or `status` column", self.contract)
        self.assertIn("no timestamp-only update", self.contract)
        self.assertIn("successful-team threshold remains validated", self.contract)

    def test_stat_source_upsert_null_fills_and_quarantines_populated_conflicts(self):
        migration = (ROOT / "backend/mlb/sql/migrations/20261017_safe_stat_training_write_entrypoints.sql").read_text()
        self.assertIn("mlb.safe_upsert_player_stats(%s::jsonb)", self.stat)
        self.assertIn("INSERT INTO mlb.player_stats", migration)
        self.assertIn("ON CONFLICT (player_id, game_id)", migration)
        self.assertIn("COALESCE(mlb.player_stats.at_bats, EXCLUDED.at_bats)", migration)
        self.assertIn("COALESCE(mlb.player_stats.hits, EXCLUDED.hits)", migration)
        self.assertIn("SECURITY DEFINER", migration)
        self.assertIn("REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON mlb.player_stats", migration)
        self.assertIn("COALESCE(at_bats, 0) = 0", migration)
        self.assertIn("safe_fill_player_stats_at_bats", self.stat)
        self.assertIn("POPULATED_SOURCE_STAT_CONFLICT", self.stat)
        self.assertIn("differing populated incoming fields", self.contract_one_line)
        self.assertIn("verified NULL fill should pass", self.fixture_one_line)

    def test_pa_backfill_routes_through_fill_only_entrypoint(self):
        migration = (ROOT / "backend/mlb/sql/migrations/20261018_safe_player_stats_pa_backfill.sql").read_text()
        backfill = (ROOT / "backend/mlb/scripts/backfill_mlb_player_pa.py").read_text()
        rollback = (ROOT / "backend/mlb/sql/migrations/20261018_safe_player_stats_pa_backfill_rollback.sql").read_text()
        self.assertIn("mlb.safe_fill_player_stats_pa(%s,%s,%s,%s,%s,%s,%s,%s)", backfill)
        self.assertNotIn("UPDATE mlb.player_stats", backfill)
        self.assertIn("p_source IS DISTINCT FROM 'statsapi'", migration)
        self.assertIn("USING ERRCODE = '23514'", migration)
        self.assertIn("COALESCE(plate_appearances, p_plate_appearances)", migration)
        self.assertIn("source_payload_sha256", backfill)
        self.assertIn("source_path.write_bytes(raw_box)", backfill)
        self.assertIn("REVOKE ALL ON FUNCTION", rollback)
        self.assertNotIn("GRANT UPDATE", migration)

    def test_derived_collision_quarantines_and_training_outcomes_are_protected(self):
        self.assertNotIn("DELETE FROM mlb.player_derived_stats", self.stat)
        self.assertIn("LegacyDerivedIdentityCollision", self.stat)
        self.assertIn("INSERT INTO mlb.player_derived_stats", self.stat)
        self.assertIn("UPDATE mlb.model_training_props", self.stat)
        self.assertIn("mlb.safe_upsert_model_training_props(%s::jsonb)", self.stat)
        self.assertIn("ON CONFLICT (player_id,game_id,prop_type,prop_source)", (ROOT / "backend/mlb/sql/migrations/20261017_safe_stat_training_write_entrypoints.sql").read_text())
        self.assertIn("HISTORICAL_OUTCOME_CONFLICT", self.stat)
        self.assertIn("No unrestricted `DELETE` should be granted", self.contract)
        self.assertIn("established result tuple", self.contract_one_line)

    def test_restricted_role_provisioning_and_derived_acl_are_narrow(self):
        role_migration = (ROOT / "backend/mlb/sql/migrations/20261016_mlb_restricted_ingest_role.sql").read_text()
        role_rollback = (ROOT / "backend/mlb/sql/migrations/20261016_mlb_restricted_ingest_role_rollback.sql").read_text()
        derived_migration = (ROOT / "backend/mlb/sql/migrations/20261018_mlb_derived_stats_writer_grants.sql").read_text()
        derived_rollback = (ROOT / "backend/mlb/sql/migrations/20261018_mlb_derived_stats_writer_grants_rollback.sql").read_text()
        self.assertIn("CREATE ROLE proppadia_mlb_ingest NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS", role_migration)
        self.assertIn("refusing to drop a role not created by the paired migration", role_rollback)
        self.assertIn("GRANT INSERT (", derived_migration)
        self.assertIn("GRANT UPDATE (", derived_migration)
        self.assertIn("d7_plate_appearances", derived_migration)
        self.assertIn("REVOKE INSERT, UPDATE, DELETE, TRUNCATE", derived_migration)
        self.assertIn("REVOKE INSERT, UPDATE, DELETE, TRUNCATE", derived_rollback)
        self.assertNotIn("GRANT DELETE", derived_migration)
        self.assertNotIn("GRANT TRUNCATE", derived_migration)

    def test_game_phase_conflicts_and_bvp_upsert_are_recorded(self):
        migration = (ROOT / "backend/mlb/sql/migrations/20261019_mlb_adjacent_writer_entrypoints.sql").read_text()
        self.assertNotIn("source_game_type", migration)
        self.assertIn("canonical phase/source evidence remains", self.contract.lower())
        self.assertIn("mlb.safe_upsert_game_info(%s::jsonb)", self.stat)
        self.assertIn("mlb.safe_upsert_prop_features_precomputed(%s::jsonb)", self.bvp)
        self.assertIn("ON CONFLICT(prop_type,player_id,game_id,feature_set_tag)", migration)
        self.assertIn("game_date=EXCLUDED.game_date", migration)
        self.assertIn("home_team_id=COALESCE(mlb.game_info.home_team_id,EXCLUDED.home_team_id)", migration)
        self.assertIn("prop_features_precomputed", self.contract)

    def test_prediction_backfill_uses_initial_only_fixed_entrypoint(self):
        migration = (ROOT / "backend/mlb/sql/migrations/20261020_safe_prediction_backfill_entrypoint.sql").read_text()
        rollback = (ROOT / "backend/mlb/sql/migrations/20261020_safe_prediction_backfill_entrypoint_rollback.sql").read_text()
        backfill = (ROOT / "backend/mlb/modeling/backfill_predictions.py").read_text()
        self.assertIn("mlb.safe_fill_prediction_backfill", migration)
        self.assertIn("FOR UPDATE", migration)
        self.assertIn("USING ERRCODE='23514'", migration)
        self.assertIn("GRANT EXECUTE", migration)
        self.assertIn("DROP FUNCTION", rollback)
        self.assertIn("mlb.safe_fill_prediction_backfill", backfill)
        self.assertNotIn('"was_correct": was_correct', backfill)

    def test_restricted_ingest_rls_policies_preserve_acl_boundary(self):
        migration = (ROOT / "backend/mlb/sql/migrations/20261021_mlb_restricted_ingest_rls_policies.sql").read_text()
        rollback = (ROOT / "backend/mlb/sql/migrations/20261021_mlb_restricted_ingest_rls_policies_rollback.sql").read_text()
        self.assertIn("FOR SELECT TO proppadia_mlb_ingest", migration)
        self.assertIn("FOR UPDATE TO proppadia_mlb_ingest", migration)
        self.assertIn("FOR INSERT TO proppadia_mlb_ingest", migration)
        self.assertIn("expected RLS-enabled, non-FORCE MLB writer relations", migration)
        self.assertNotIn("GRANT INSERT", migration)
        self.assertNotIn("GRANT UPDATE", migration)
        self.assertIn("DROP POLICY IF EXISTS proppadia_mlb_ingest", rollback)
        self.assertNotIn("ALTER TABLE", rollback)

    def test_streak_profile_read_migration_is_column_and_row_limited(self):
        migration = (ROOT / "backend/mlb/sql/migrations/20261022_mlb_ingest_streak_profile_read.sql").read_text()
        rollback = (ROOT / "backend/mlb/sql/migrations/20261022_mlb_ingest_streak_profile_read_rollback.sql").read_text()
        expected_columns = "player_id, prop_type, prop_source, last_game_date, streak_type, streak_count"
        self.assertIn(f"GRANT SELECT ({expected_columns})", migration)
        self.assertIn("USING (prop_source = 'mlb_api' AND last_game_date IS NOT NULL)", migration)
        self.assertNotIn("GRANT INSERT", migration)
        self.assertNotIn("GRANT UPDATE", migration)
        self.assertNotIn("GRANT DELETE", migration)
        self.assertNotIn("GRANT TRUNCATE", migration)
        self.assertIn("REVOKE SELECT (player_id, prop_type, prop_source, last_game_date, streak_type, streak_count)", rollback)
        self.assertIn("DROP POLICY proppadia_mlb_ingest_streak_select", rollback)

    def test_hosted_workflow_uses_separate_secret_without_secret_inspection(self):
        self.assertIn("secrets.SUPABASE_DB_URL", self.workflow)
        self.assertIn("its value was not inspected", self.contract_one_line)
        self.assertIn("identity and a bounded hosted run remain unverified", self.contract_one_line)

    def test_fixture_is_explicitly_disposable_and_denies_destructive_privileges(self):
        self.assertIn("never canonical production tables", self.fixture_one_line)
        self.assertIn("must not have direct DELETE or TRUNCATE", self.fixture_one_line)
        self.assertIn("isolated project/branch identity", self.fixture_one_line)
        self.assertIn("the real production login", self.fixture_one_line)


if __name__ == "__main__":
    unittest.main()

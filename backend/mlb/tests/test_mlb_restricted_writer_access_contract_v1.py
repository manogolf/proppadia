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
        self.assertIn("INSERT INTO mlb.player_ids", self.roster)
        self.assertIn("ON CONFLICT (player_id)", self.roster)
        self.assertIn("COALESCE(EXCLUDED.player_name, player_ids.player_name)", self.roster)
        self.assertIn("WHERE NOT (player_id = ANY(%s::bigint[]))", self.roster)
        self.assertIn("SELECT(player_id)", self.contract)
        self.assertIn("`ON CONFLICT` expressions read existing values", self.contract)
        for field in ("player_name", "team", "team_id", "active", "status", "updated_at"):
            self.assertIn(f"`{field}`", self.contract)
        self.assertIn("at least `total - 1` team fetches succeeded", self.contract)

    def test_stat_source_upsert_is_classified_as_overwrite_risk(self):
        self.assertIn("INSERT INTO mlb.player_stats", self.stat)
        self.assertIn("ON CONFLICT (player_id, game_id)", self.stat)
        self.assertIn("at_bats = EXCLUDED.at_bats", self.stat)
        self.assertIn("hits = EXCLUDED.hits", self.stat)
        self.assertIn("COALESCE(at_bats, 0) = 0", self.stat)
        self.assertIn("would permit historical overwrites", self.contract)
        self.assertIn("expected to fail the protected-overwrite assertion", self.fixture_one_line)

    def test_derived_delete_and_training_row_writes_are_not_granted_as_general_ingest(self):
        self.assertIn("DELETE FROM mlb.player_derived_stats", self.stat)
        self.assertIn("INSERT INTO mlb.player_derived_stats", self.stat)
        self.assertIn("UPDATE mlb.model_training_props", self.stat)
        self.assertIn("ON CONFLICT (player_id, game_id, prop_type, prop_source)", self.stat)
        self.assertIn("No unrestricted `DELETE` should be granted", self.contract)
        self.assertIn("Exclude from routine ingestion grants", self.contract)

    def test_game_phase_conflicts_and_bvp_upsert_are_recorded(self):
        self.assertIn("CANONICAL_GAME_PHASE_CONFLICT", self.stat)
        self.assertIn("INSERT INTO mlb.game_info", self.stat)
        self.assertIn("ON CONFLICT (prop_type, player_id, game_id, feature_set_tag)", self.bvp)
        self.assertIn("prop_features_precomputed", self.contract)

    def test_hosted_workflow_uses_separate_secret_without_secret_inspection(self):
        self.assertIn("secrets.SUPABASE_DB_URL", self.workflow)
        self.assertIn("its value must not be inspected", self.contract_one_line)
        self.assertIn("must not be inspected", self.contract_one_line)

    def test_fixture_is_explicitly_disposable_and_denies_destructive_privileges(self):
        self.assertIn("never canonical production tables", self.fixture_one_line)
        self.assertIn("must not have direct DELETE or TRUNCATE", self.fixture_one_line)
        self.assertIn("isolated project/branch identity", self.fixture_one_line)
        self.assertIn("the real production login", self.fixture_one_line)


if __name__ == "__main__":
    unittest.main()

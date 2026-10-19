"""NHL research readers and the morning preflight use the restricted route."""
import importlib.util
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "backend/nhl/scripts"
READERS = [
    "assess_nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance",
    "build_nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test",
    "build_nhl_moneyline_team_goalie_feature_spine",
    "build_nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1",
    "build_nhl_season_2025_player_prop_market_archive_canonical_index",
    "build_nhl_sog_cold_start_platform_v1",
    "calibrate_sog_segmented_recency",
    "certify_nhl_full_game_moneyline_population",
    "certify_nhl_sog_outcome_denominator",
    "export_sog_candidate_book_upload",
    "export_sog_denali_book_upload",
    "run_nhl_september19_independent_lane_predictions",
]
OWNER = "postgresql://postgres.ownerref:owner-secret@db.invalid/postgres"
RESEARCH = "postgresql://proppadia_nhl_research_login.ref:r@db.invalid/postgres"
RESTRICTED = {"NHL_DB_ACCESS_MODE": "restricted", "NHL_RESEARCH_DATABASE_URL": RESEARCH,
              "SUPABASE_DB_URL": OWNER, "DATABASE_URL": OWNER}


def load(name):
    spec = importlib.util.spec_from_file_location(f"route_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReaderRouteTests(unittest.TestCase):
    def test_readers_resolve_through_database_access_not_legacy_aliases(self):
        legacy = re.compile(r"""environ(?:\.get)?[\[(]\s*["'](?:SUPABASE_DB_URL|DATABASE_URL)""")
        for name in READERS:
            with self.subTest(name=name):
                source = (SCRIPTS / f"{name}.py").read_text()
                self.assertIn('database_url("research")', source)
                self.assertIsNone(legacy.search(source))

    def test_export_reader_connects_with_the_research_login_in_restricted_mode(self):
        module = load("export_sog_candidate_book_upload")
        psycopg2 = MagicMock()
        with patch.dict("os.environ", RESTRICTED, clear=True), patch.dict(sys.modules, {"psycopg2": psycopg2}):
            module.get_db_conn()
        psycopg2.connect.assert_called_once_with(RESEARCH)


class PreflightRouteTests(unittest.TestCase):
    def setUp(self):
        self.module = load("check_nhl_database_connection")

    def run_check(self, env, owner_reach=False):
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("postgres",), (owner_reach,)]
        with patch.dict("os.environ", env, clear=True), \
                patch.object(self.module.psycopg, "connect", return_value=connection) as connect:
            return self.module.main(), connect

    def test_research_aliases_pass_and_connect_as_research(self):
        env = dict(RESTRICTED, SUPABASE_DB_URL=RESEARCH, DATABASE_URL=RESEARCH)
        code, connect = self.run_check(env)
        self.assertEqual(code, 0)
        connect.assert_called_once_with(RESEARCH)

    def test_research_login_with_owner_reach_is_refused(self):
        env = dict(RESTRICTED, SUPABASE_DB_URL=RESEARCH, DATABASE_URL=RESEARCH)
        with self.assertRaisesRegex(SystemExit, "NHL_DATABASE_LOGIN_HAS_OWNER_REACH"):
            self.run_check(env, owner_reach=True)

    def test_owner_aliases_are_refused_in_restricted_mode(self):
        with self.assertRaisesRegex(SystemExit, "NHL_DATABASE_CREDENTIAL_NOT_RESEARCH_ROUTE"):
            self.run_check(RESTRICTED)

    def test_restricted_mode_without_research_route_is_refused(self):
        env = {"NHL_DB_ACCESS_MODE": "restricted", "SUPABASE_DB_URL": OWNER, "DATABASE_URL": OWNER}
        with self.assertRaisesRegex(SystemExit, "NHL_DATABASE_CREDENTIAL_ENVIRONMENT_INVALID"):
            self.run_check(env)


class CrossMarketPriorDayChildTests(unittest.TestCase):
    def test_prior_day_children_inherit_no_owner_login(self):
        module = load("run_nhl_mainline_cross_market_capture_warn_only")
        env = dict(RESTRICTED, PGPASSWORD="p", KEEP="yes")
        result = MagicMock(returncode=0)
        with patch.dict("os.environ", env, clear=True), \
                patch.object(module.subprocess, "run", return_value=result) as runner:
            module._run_prior_day_child("stage", ["true"])
        child = runner.call_args.kwargs["env"]
        self.assertEqual(child["NHL_RESEARCH_DATABASE_URL"], RESEARCH)
        self.assertEqual(child["KEEP"], "yes")
        for key in ("SUPABASE_DB_URL", "DATABASE_URL", "PGPASSWORD"):
            self.assertNotIn(key, child)


if __name__ == "__main__":
    unittest.main()

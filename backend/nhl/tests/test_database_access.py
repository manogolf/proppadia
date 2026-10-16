import unittest
import os
from types import SimpleNamespace
from unittest.mock import patch

from backend.nhl.database_access import (
    database_url,
    restricted_mode,
    routed_database_environment,
)


class NHLDatabaseAccessTests(unittest.TestCase):
    def test_legacy_mode_preserves_existing_url_precedence(self):
        env = {
            "SUPABASE_DB_URL": "postgresql://postgres:secret@db.invalid/postgres",
            "DATABASE_URL": "postgresql://fallback:secret@db.invalid/postgres",
        }
        self.assertFalse(restricted_mode(env))
        self.assertEqual(database_url("research", environ=env), env["SUPABASE_DB_URL"])

    def test_restricted_research_route_requires_expected_role(self):
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_RESEARCH_DATABASE_URL": "postgresql://proppadia_nhl_research_login:secret@db.invalid/postgres",
            "SUPABASE_DB_URL": "postgresql://postgres:secret@db.invalid/postgres",
        }
        self.assertEqual(database_url("research", environ=env), env["NHL_RESEARCH_DATABASE_URL"])

    def test_restricted_pooler_username_may_include_project_suffix(self):
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_INGEST_DATABASE_URL": "postgresql://proppadia_nhl_daily_login.projectref:secret@pool.invalid/postgres",
        }
        self.assertEqual(database_url("ingestion", environ=env), env["NHL_INGEST_DATABASE_URL"])

    def test_restricted_mode_never_falls_back_to_legacy_owner_url(self):
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "SUPABASE_DB_URL": "postgresql://postgres:secret@db.invalid/postgres",
            "DATABASE_URL": "postgresql://postgres:secret@db.invalid/postgres",
            "DB": "postgresql://postgres:secret@db.invalid/postgres",
            "PGUSER": "postgres",
            "PGPASSWORD": "owner-secret",
            "PGHOST": "owner-db.invalid",
            "PGSERVICE": "owner-service",
            "PGSERVICEFILE": "/tmp/owner-service.conf",
            "DB": "postgresql://postgres:secret@db.invalid/postgres",
            "PGUSER": "postgres",
            "PGPASSWORD": "owner-secret",
            "PGHOST": "owner-db.invalid",
        }
        with self.assertRaisesRegex(RuntimeError, "NHL_RESTRICTED_DATABASE_URL_REQUIRED"):
            database_url("research", environ=env)

    def test_restricted_mode_rejects_owner_role_in_research_slot(self):
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_RESEARCH_DATABASE_URL": "postgresql://postgres:secret@db.invalid/postgres",
        }
        with self.assertRaisesRegex(RuntimeError, "NHL_RESTRICTED_DATABASE_ROLE_MISMATCH"):
            database_url("research", environ=env)

    def test_child_environment_contains_only_selected_role_aliases(self):
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_RESEARCH_DATABASE_URL": "postgresql://proppadia_nhl_research_login:secret@db.invalid/postgres",
        }
        routed = routed_database_environment("research", environ=env)
        self.assertEqual(routed["SUPABASE_DB_URL"], env["NHL_RESEARCH_DATABASE_URL"])
        self.assertEqual(routed["DATABASE_URL"], env["NHL_RESEARCH_DATABASE_URL"])
        self.assertNotIn("NHL_INGEST_DATABASE_URL", routed)

    def test_restricted_operations_role_is_separate_from_ingestion(self):
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_OPERATIONS_DATABASE_URL": "postgresql://proppadia_nhl_operations_login:secret@db.invalid/postgres",
        }
        self.assertEqual(database_url("operations", environ=env), env["NHL_OPERATIONS_DATABASE_URL"])

    def test_daily_cli_routes_read_query_to_research_role(self):
        from backend.nhl import cli

        research = "postgresql://proppadia_nhl_research_login:secret@db.invalid/postgres"
        ingest = "postgresql://proppadia_nhl_daily_login:secret@db.invalid/postgres"
        operations = "postgresql://proppadia_nhl_operations_login:secret@db.invalid/postgres"
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_RESEARCH_DATABASE_URL": research,
            "NHL_INGEST_DATABASE_URL": ingest,
            "NHL_OPERATIONS_DATABASE_URL": operations,
            "SUPABASE_DB_URL": "postgresql://postgres:secret@db.invalid/postgres",
            "DATABASE_URL": "postgresql://postgres:secret@db.invalid/postgres",
        }
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        with patch.dict(os.environ, env, clear=False), patch.object(cli.sp, "run", return_value=result) as runner:
            cli.run(["psql", env["SUPABASE_DB_URL"], "-c", "SELECT 1"], database_write_capable=False)
        command = runner.call_args.args[0]
        child_env = runner.call_args.kwargs["env"]
        self.assertEqual(command[1], research)
        self.assertEqual(child_env["SUPABASE_DB_URL"], research)
        self.assertEqual(child_env["DATABASE_URL"], research)
        self.assertNotIn("DB", child_env)
        self.assertNotIn("PGUSER", child_env)
        self.assertNotIn("PGPASSWORD", child_env)
        self.assertNotIn("PGHOST", child_env)
        self.assertNotIn("PGSERVICE", child_env)
        self.assertNotIn("PGSERVICEFILE", child_env)
        self.assertNotIn("NHL_INGEST_DATABASE_URL", child_env)
        self.assertNotIn("NHL_OPERATIONS_DATABASE_URL", child_env)

    def test_daily_cli_routes_writer_to_ingestion_role(self):
        from backend.nhl import cli

        research = "postgresql://proppadia_nhl_research_login:secret@db.invalid/postgres"
        ingest = "postgresql://proppadia_nhl_daily_login:secret@db.invalid/postgres"
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_RESEARCH_DATABASE_URL": research,
            "NHL_INGEST_DATABASE_URL": ingest,
            "SUPABASE_DB_URL": "postgresql://postgres:secret@db.invalid/postgres",
            "DATABASE_URL": "postgresql://postgres:secret@db.invalid/postgres",
        }
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        with patch.dict(os.environ, env, clear=False), patch.object(cli.sp, "run", return_value=result) as runner:
            cli.run(["./bin/python", "seed_skater_logs_for_date.py"], database_write_capable=True)
        child_env = runner.call_args.kwargs["env"]
        self.assertEqual(child_env["SUPABASE_DB_URL"], ingest)
        self.assertEqual(child_env["DATABASE_URL"], ingest)
        self.assertNotIn("NHL_RESEARCH_DATABASE_URL", child_env)
        self.assertNotIn("NHL_OPERATIONS_DATABASE_URL", child_env)

    def test_prediction_writer_routes_to_operations_role(self):
        from backend.nhl import cli

        research = "postgresql://proppadia_nhl_research_login:secret@db.invalid/postgres"
        ingest = "postgresql://proppadia_nhl_daily_login:secret@db.invalid/postgres"
        operations = "postgresql://proppadia_nhl_operations_login:secret@db.invalid/postgres"
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_RESEARCH_DATABASE_URL": research,
            "NHL_INGEST_DATABASE_URL": ingest,
            "NHL_OPERATIONS_DATABASE_URL": operations,
            "SUPABASE_DB_URL": "postgresql://postgres:secret@db.invalid/postgres",
            "DATABASE_URL": "postgresql://postgres:secret@db.invalid/postgres",
        }
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        with patch.dict(os.environ, env, clear=False), patch.object(cli.sp, "run", return_value=result) as runner:
            cli.run(["./bin/python", "load_nhl_predictions_generic.py"])
        child_env = runner.call_args.kwargs["env"]
        self.assertEqual(child_env["SUPABASE_DB_URL"], operations)
        self.assertNotIn("NHL_INGEST_DATABASE_URL", child_env)

    def test_daily_evaluator_is_classified_as_a_database_writer(self):
        from backend.nhl import cli

        self.assertTrue(cli._command_is_database_write_capable(
            ["./bin/python", "backend/nhl/scripts/evaluate_sog_predictions.py"]
        ))
        self.assertEqual(
            cli._command_database_access(["./bin/python", "seed_skater_logs_for_date.py"], write_capable=True),
            "ingestion",
        )
        self.assertEqual(
            cli._command_database_access(["./bin/python", "load_nhl_predictions_generic.py"], write_capable=True),
            "operations",
        )
        self.assertEqual(
            cli._command_database_access(
                ["./bin/python", "backfill_shot_on_goal_events_from_pbp.py"],
                write_capable=True,
            ),
            "ingestion",
        )
        self.assertEqual(
            cli._command_database_access(
                ["./bin/python", "build_nhl_season_2025_frozen_moneyline_replay.py"],
                write_capable=False,
            ),
            "research",
        )
        self.assertEqual(
            cli._command_database_access(
                ["psql", "postgresql://hidden", "-c", "UPDATE nhl.skater_game_logs_raw SET blocks=1"],
                write_capable=True,
            ),
            "ingestion",
        )
        self.assertEqual(
            cli._command_database_access(
                ["psql", "postgresql://hidden", "-c", "UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 SET pace_index=1"],
                write_capable=True,
            ),
            "operations",
        )


if __name__ == "__main__":
    unittest.main()

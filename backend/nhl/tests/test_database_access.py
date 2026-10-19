import unittest
import os
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch

from backend.nhl.database_access import (
    database_url,
    psql_dsn_without_password,
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

    def test_psql_argv_dsn_does_not_contain_password(self):
        safe_url, password = psql_dsn_without_password(
            "postgresql://role.projectref:p%40ssword@pool.invalid:5432/postgres?sslmode=require"
        )
        self.assertEqual(password, "p@ssword")
        self.assertEqual(
            safe_url,
            "postgresql://role.projectref@pool.invalid:5432/postgres?sslmode=require",
        )

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
            "DATABASE_URL": "postgresql://mlb-shared:secret@db.invalid/postgres",
            "SUPABASE_DB_URL": "postgresql://mlb-shared-supabase:secret@db.invalid/postgres",
        }
        routed = routed_database_environment("research", environ=env)
        self.assertEqual(routed["SUPABASE_DB_URL"], env["NHL_RESEARCH_DATABASE_URL"])
        self.assertEqual(routed["DATABASE_URL"], env["NHL_RESEARCH_DATABASE_URL"])
        self.assertNotIn("NHL_INGEST_DATABASE_URL", routed)
        # Routing is for the child only; shared MLB configuration in the
        # parent process must remain untouched.
        self.assertEqual(env["DATABASE_URL"], "postgresql://mlb-shared:secret@db.invalid/postgres")
        self.assertEqual(env["SUPABASE_DB_URL"], "postgresql://mlb-shared-supabase:secret@db.invalid/postgres")

    def test_inherited_database_environment_precedes_dotenv_files(self):
        # The shared Supabase resolver loads dotenv files with override=False.
        # Exercise that behavior in a clean child process so the real local
        # environment and imported module globals are not changed by this test.
        inherited_database = "postgresql://inherited-mlb:placeholder@db.invalid/postgres"
        inherited_supabase = "postgresql://inherited-supabase:placeholder@db.invalid/postgres"
        child_env = {
            "PATH": os.environ.get("PATH", ""),
            "DATABASE_URL": inherited_database,
            "SUPABASE_DB_URL": inherited_supabase,
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from backend.supabase import supabase_utils as s; "
                "assert s.DATABASE_URL == 'postgresql://inherited-mlb:placeholder@db.invalid/postgres'; "
                "assert s.SUPABASE_DB_URL == 'postgresql://inherited-supabase:placeholder@db.invalid/postgres'",
            ],
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")),
            env=child_env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, "dotenv loading replaced inherited DB configuration")

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
        # Keep the real process environment (which may contain credentials)
        # out of assertion representations if a routing check fails.
        with patch.dict(os.environ, env, clear=True), patch.object(cli.sp, "run", return_value=result) as runner:
            cli.run(["psql", env["SUPABASE_DB_URL"], "-c", "SELECT 1"], database_write_capable=False)
        command = runner.call_args.args[0]
        child_env = runner.call_args.kwargs["env"]
        self.assertEqual(
            command[1],
            "postgresql://proppadia_nhl_research_login@db.invalid/postgres",
        )
        self.assertEqual(child_env["SUPABASE_DB_URL"], research)
        self.assertEqual(child_env["DATABASE_URL"], research)
        self.assertEqual(child_env["NHL_RESEARCH_DATABASE_URL"], research)
        self.assertEqual(child_env["PGPASSWORD"], "secret")
        self.assertNotIn(":secret@", command[1])
        self.assertNotIn("DB", child_env)
        self.assertNotIn("PGUSER", child_env)
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
        with patch.dict(os.environ, env, clear=True), patch.object(cli.sp, "run", return_value=result) as runner:
            cli.run(["./bin/python", "seed_skater_logs_for_date.py"], database_write_capable=True)
        child_env = runner.call_args.kwargs["env"]
        self.assertEqual(child_env["SUPABASE_DB_URL"], ingest)
        self.assertEqual(child_env["DATABASE_URL"], ingest)
        self.assertEqual(child_env["NHL_INGEST_DATABASE_URL"], ingest)
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
        with patch.dict(os.environ, env, clear=True), patch.object(cli.sp, "run", return_value=result) as runner:
            cli.run(["./bin/python", "load_nhl_predictions_generic.py"])
        child_env = runner.call_args.kwargs["env"]
        self.assertEqual(child_env["SUPABASE_DB_URL"], operations)
        self.assertEqual(child_env["NHL_OPERATIONS_DATABASE_URL"], operations)
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

    def test_owner_free_child_environment_drops_owner_and_libpq_routes(self):
        from backend.nhl.database_access import owner_free_child_environment
        research = "postgresql://proppadia_nhl_research_login.ref:secret@db.invalid/postgres"
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_RESEARCH_DATABASE_URL": research,
            "SUPABASE_DB_URL": "postgresql://postgres.ref:secret@db.invalid/postgres",
            "DATABASE_URL": "postgresql://postgres.ref:secret@db.invalid/postgres",
            "OTHER_DSN": "postgresql://supabase_admin:secret@db.invalid/postgres",
            "PGPASSWORD": "secret", "PGAPPNAME": "nhl", "KEEP": "yes",
        }
        child = owner_free_child_environment(env)
        self.assertEqual(sorted(child), ["KEEP", "NHL_DB_ACCESS_MODE", "NHL_RESEARCH_DATABASE_URL", "PGAPPNAME"])
        legacy = dict(env, NHL_DB_ACCESS_MODE="legacy")
        self.assertEqual(owner_free_child_environment(legacy), legacy)

    def test_owner_login_keys_in_file_reports_names_and_follows_references(self):
        import tempfile
        from pathlib import Path
        from backend.nhl.database_access import owner_login_keys_in_file
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / ".env"
            path.write_text(
                "SUPABASE_DB_URL='postgresql://postgres.ref:secret@db.invalid/postgres'\n"
                "DATABASE_URL=${SUPABASE_DB_URL}\n"
                "export NHL_RESEARCH_DATABASE_URL=postgresql://proppadia_nhl_research_login:s@db.invalid/postgres\n"
                "# OLD=postgresql://postgres:secret@db.invalid/postgres\n")
            self.assertEqual(owner_login_keys_in_file(path), ["DATABASE_URL", "SUPABASE_DB_URL"])
            self.assertEqual(owner_login_keys_in_file(Path(raw) / "missing.env"), [])

    def test_daily_cli_child_never_receives_an_owner_login(self):
        from backend.nhl import cli

        ingest = "postgresql://proppadia_nhl_daily_login:secret@db.invalid/postgres"
        env = {
            "NHL_DB_ACCESS_MODE": "restricted",
            "NHL_INGEST_DATABASE_URL": ingest,
            "NHL_PP_TOI_TEST_DSN": "postgresql://postgres.testref:secret@db.invalid/postgres",
            "SUPABASE_DB_URL": "postgresql://postgres:secret@db.invalid/postgres",
        }
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        with patch.dict(os.environ, env, clear=True), patch.object(cli.sp, "run", return_value=result) as runner:
            cli.run(["./bin/python", "seed_skater_logs_for_date.py"], database_write_capable=True)
        child_env = runner.call_args.kwargs["env"]
        self.assertNotIn("NHL_PP_TOI_TEST_DSN", child_env)
        self.assertEqual(child_env["SUPABASE_DB_URL"], ingest)


if __name__ == "__main__":
    unittest.main()

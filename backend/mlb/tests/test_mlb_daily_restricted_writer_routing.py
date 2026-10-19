"""Verify restricted ingest is passed only to ordinary daily writer processes."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
TRACKED_WRAPPER = ROOT / "bin/proppadia_mlb_refresh_daily.sh"
INSTALLED_WRAPPER = Path.home() / "bin/proppadia_mlb_refresh_daily.sh"
OPERATIONS_RUNBOOK = ROOT / "docs/Prod12 Automation Runbook.md"
WORKSPACE_BUILD_CALL = "MLB_USE_RESTRICTED_INGEST=1 .venv/bin/python backend/mlb/scripts/build_mlb_today_workspace.py"
WORKSPACE_BUILD_CALL_PREVIOUS = ".venv/bin/python backend/mlb/scripts/build_mlb_today_workspace.py"
WORKSPACE_BUILD_COMMENT = "# Staging writes go through mlb.replace_today_workspace_staging() on the ingest login.\n"


class MlbDailyRestrictedWriterRoutingTests(unittest.TestCase):
    def test_pending_status_handoff_is_tracked_but_not_yet_installed(self):
        if not INSTALLED_WRAPPER.exists():
            self.skipTest(f"installed daily wrapper not present: {INSTALLED_WRAPPER}")
        if INSTALLED_WRAPPER.read_bytes() == TRACKED_WRAPPER.read_bytes():
            return

        # The installation keeps the restricted refresh route; the restricted
        # workspace staging build stays tracked-only until a separately
        # authorized installation.
        tracked = TRACKED_WRAPPER.read_text()
        installed = INSTALLED_WRAPPER.read_text()
        self.assertIn("SELECT mlb.refresh_today_workspace_views();", tracked)
        self.assertIn("SELECT mlb.refresh_today_workspace_views();", installed)
        self.assertIn(WORKSPACE_BUILD_CALL, tracked)
        self.assertEqual(
            tracked.replace(WORKSPACE_BUILD_CALL, WORKSPACE_BUILD_CALL_PREVIOUS)
            .replace(WORKSPACE_BUILD_COMMENT, ""),
            installed,
        )

    def test_runbook_preserves_installed_wrapper_routing_recovery(self):
        runbook = OPERATIONS_RUNBOOK.read_text()
        normalized_runbook = " ".join(runbook.split())
        self.assertIn("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh", runbook)
        self.assertIn(
            "maintained source is `bin/proppadia_mlb_refresh_daily.sh`", normalized_runbook
        )
        self.assertIn(
            "Do not use it to replace the active wrapper wholesale", normalized_runbook
        )
        self.assertIn("Do not export this flag in the parent wrapper", normalized_runbook)
        self.assertIn(
            "Today-workspace restricted refresh installation",
            runbook,
        )
        self.assertIn(
            'MLB_USE_RESTRICTED_INGEST=1 bin/mlb_bvp_inline_daily_hook.sh "$MLB_DATE_ET"',
            runbook,
        )
        self.assertIn(
            "backend.mlb.tests.test_mlb_daily_restricted_writer_routing", runbook
        )

    def test_daily_wrapper_scopes_flag_to_roster_and_bvp_writer_calls(self):
        wrapper = TRACKED_WRAPPER.read_text()
        self.assertIn(
            'MLB_USE_RESTRICTED_INGEST=1 MLB_ROSTER_DATE="$MLB_DATE_ET" \\\n  make mlb-roster-refresh-all',
            wrapper,
        )
        self.assertIn(
            'MLB_USE_RESTRICTED_INGEST=1 bin/mlb_bvp_inline_daily_hook.sh "$MLB_DATE_ET"',
            wrapper,
        )
        stat_call = wrapper.split("make mlb-stat-derived-refresh \\\n", 1)[1].split("\n", 1)[0]
        self.assertNotIn("MLB_USE_RESTRICTED_INGEST", stat_call)
        self.assertIn("SELECT mlb.refresh_today_workspace_views();", wrapper)
        self.assertNotIn("REFRESH MATERIALIZED VIEW mlb.today_", wrapper)
        self.assertEqual(wrapper.count(WORKSPACE_BUILD_CALL_PREVIOUS), 1)
        self.assertIn(WORKSPACE_BUILD_CALL, wrapper)

    def test_today_workspace_refresh_is_scoped_fixed_and_has_no_owner_fallback(self):
        wrapper = TRACKED_WRAPPER.read_text()
        self.assertIn(
            'MLB_SLATE_OUTPUT_GATE_STATUS_FILE="artifacts/ops/mlb_predictive_gate_status/${MLB_DATE_ET}/${MLB_RUN_TAG}.txt"',
            wrapper,
        )
        self.assertIn(
            'MLB_SLATE_OUTPUT_GATE_STATUS_FILE="$MLB_SLATE_OUTPUT_GATE_STATUS_FILE" \\\n  bin/mlb_slate_output_guarded.sh MLB_DATE="$MLB_DATE_ET"',
            wrapper,
        )
        self.assertIn(
            '--qualification-status-file "$MLB_SLATE_OUTPUT_GATE_STATUS_FILE"',
            wrapper,
        )
        self.assertIn(
            'elif [[ "$today_workspace_build_rc" -eq 3 ]]; then\n'
            '  echo "[$(date -u +%FT%TZ)] SKIP mlb-today workspace build and materialized view refresh status=EXPECTED_SKIP classification=NO_QUALIFIED_PREDICTION_SLATE reason=NO_QUALIFIED_MLB_MODEL"',
            wrapper,
        )
        command_start = wrapper.index('  if [[ -z "${MLB_INGEST_DATABASE_URL:-}" ]]')
        command_end = wrapper.index(
            '\n  fi\nelif [[ "$today_workspace_build_rc" -eq 3 ]]', command_start
        ) + len("\n  fi")
        command_block = wrapper[command_start:command_end]
        self.assertIn("SET ROLE proppadia_mlb_ingest;", command_block)
        self.assertIn("SELECT mlb.refresh_today_workspace_views();", command_block)
        self.assertNotIn("SUPABASE_DB_URL", command_block)
        self.assertNotIn("REFRESH MATERIALIZED VIEW", command_block)
        self.assertIn("today_workspace_mv_rc=78", command_block)

        with tempfile.TemporaryDirectory(prefix="mlb-today-refresh-route-") as temp_dir:
            temp = Path(temp_dir)
            log = temp / "calls.log"
            fake_psql = temp / "psql"
            fake_psql.write_text(
                "#!/bin/sh\n"
                "printf 'url=%s\\n' \"$1\" >> \"$ROUTING_TEST_LOG\"\n"
                "printf 'flag=%s\\n' \"${MLB_USE_RESTRICTED_INGEST-unset}\" >> \"$ROUTING_TEST_LOG\"\n"
                "shift\n"
                "printf 'args=%s\\n' \"$*\" >> \"$ROUTING_TEST_LOG\"\n"
                "exit \"${PSQL_TEST_RC:-0}\"\n"
            )
            fake_psql.chmod(0o755)
            env = os.environ.copy()
            env.pop("MLB_USE_RESTRICTED_INGEST", None)
            env.update(
                {
                    "PSQL_BIN": str(fake_psql),
                    "MLB_INGEST_DATABASE_URL": "restricted-test-url",
                    "SUPABASE_DB_URL": "owner-test-url",
                    "ROUTING_TEST_LOG": str(log),
                    "PSQL_TEST_RC": "42",
                }
            )
            script = (
                "set +e\n"
                f"{command_block}\n"
                "printf 'captured_rc=%s\\n' \"$today_workspace_mv_rc\"\n"
                "printf 'parent_flag=%s\\n' \"${MLB_USE_RESTRICTED_INGEST-unset}\"\n"
            )
            result = subprocess.run(
                ["/bin/zsh", "-c", script],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("captured_rc=42", result.stdout)
            self.assertIn("parent_flag=unset", result.stdout)
            calls = log.read_text().splitlines()
            self.assertEqual(calls[0], "url=restricted-test-url")
            self.assertEqual(calls[1], "flag=1")
            self.assertIn(
                "SET ROLE proppadia_mlb_ingest; SELECT mlb.refresh_today_workspace_views();",
                calls[2],
            )
            self.assertNotIn("owner-test-url", "\n".join(calls))
            self.assertEqual(sum(line.startswith("url=") for line in calls), 1)

            log.unlink()
            missing_url_env = env.copy()
            missing_url_env.pop("MLB_INGEST_DATABASE_URL")
            missing = subprocess.run(
                ["/bin/zsh", "-c", script],
                cwd=ROOT,
                env=missing_url_env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(missing.returncode, 0, missing.stdout + missing.stderr)
            self.assertIn("captured_rc=78", missing.stdout)
            self.assertIn("MLB_INGEST_DATABASE_URL is required", missing.stderr)
            self.assertFalse(log.exists())

    def test_slate_gate_adapter_exposes_existing_status_file_contract(self):
        adapter = (ROOT / "bin/mlb_slate_output_guarded.sh").read_text()
        guard = (ROOT / "bin/mlb_predictive_command_guarded.sh").read_text()
        self.assertIn('status_args=(--status-file "${MLB_SLATE_OUTPUT_GATE_STATUS_FILE}")', adapter)
        self.assertIn('write_status "SUCCESS operation=${operation}"', guard)
        self.assertIn('write_status "SKIPPED operation=${operation}"', guard)
        self.assertIn('write_status "FAILED operation=${operation} rc=${command_rc}"', guard)

    def test_slate_gate_writes_success_and_explicit_skip_statuses(self):
        with tempfile.TemporaryDirectory(prefix="mlb-slate-gate-status-") as temp_dir:
            temp = Path(temp_dir)
            fake_make = temp / "make"
            fake_make.write_text(
                "#!/bin/sh\n"
                "if [ \"${GATE_TEST_MODE:-success}\" = skip ]; then\n"
                "  echo 'MLBPredictiveModelBlocked: MLB_PREDICTIVE_MODEL_BLOCKED_NO_QUALIFIED_MODEL: operation=production_slate_generation' >&2\n"
                "  exit 1\n"
                "fi\n"
                "exit 0\n"
            )
            fake_make.chmod(0o755)
            status_file = temp / "gate_status.txt"
            env = os.environ.copy()
            env["PATH"] = f"{temp}:{env.get('PATH', '')}"
            env["MLB_SLATE_OUTPUT_GATE_STATUS_FILE"] = str(status_file)

            qualified = subprocess.run(
                ["/bin/zsh", str(ROOT / "bin/mlb_slate_output_guarded.sh"), "MLB_DATE=2026-10-17"],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(qualified.returncode, 0, qualified.stdout + qualified.stderr)
            self.assertEqual(
                status_file.read_text().strip(),
                "SUCCESS operation=production_slate_generation",
            )

            env["GATE_TEST_MODE"] = "skip"
            skipped = subprocess.run(
                ["/bin/zsh", str(ROOT / "bin/mlb_slate_output_guarded.sh"), "MLB_DATE=2026-10-17"],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(skipped.returncode, 0, skipped.stdout + skipped.stderr)
            self.assertEqual(
                status_file.read_text().strip(),
                "SKIPPED operation=production_slate_generation",
            )

    @unittest.skipUnless(shutil.which("make"), "GNU make is required")
    def test_wrapper_roster_and_bvp_calls_deliver_flag_to_only_the_child(self):
        wrapper = TRACKED_WRAPPER.read_text()
        with tempfile.TemporaryDirectory(prefix="mlb-wrapper-route-") as temp_dir:
            temp = Path(temp_dir)
            (temp / "bin").mkdir()
            log = temp / "calls.log"
            fake_command = "#!/bin/sh\nprintf '%s|%s\\n' \"$ROUTE_TEST_NAME\" \"${MLB_USE_RESTRICTED_INGEST-unset}\" >> \"$ROUTING_TEST_LOG\"\n"
            real_make = shutil.which("make")
            fake_make = temp / "make"
            fake_make.write_text(
                f"#!/bin/sh\nexec {real_make} -f {ROOT / 'Makefile'} \"$@\"\n"
            )
            fake_make.chmod(0o755)
            fake_python = temp / "fake-python"
            fake_python.write_text(
                "#!/bin/sh\n"
                "case \"$*\" in *refresh_mlb_players_rosters*) ROUTE_TEST_NAME=roster ;; "
                "*) ROUTE_TEST_NAME=unexpected ;; esac\n"
                "printf '%s|%s\\n' \"$ROUTE_TEST_NAME\" "
                "\"${MLB_USE_RESTRICTED_INGEST-unset}\" >> \"$ROUTING_TEST_LOG\"\n"
            )
            fake_python.chmod(0o755)
            fake_hook = temp / "bin/mlb_bvp_inline_daily_hook.sh"
            fake_hook.write_text(fake_command)
            fake_hook.chmod(0o755)

            roster_line_start = wrapper.index('  MLB_USE_RESTRICTED_INGEST=1 MLB_ROSTER_DATE=')
            roster_line_end = wrapper.index("\n  roster_rc=$?", roster_line_start)
            bvp_line_start = wrapper.index(
                'MLB_USE_RESTRICTED_INGEST=1 bin/mlb_bvp_inline_daily_hook.sh '
            )
            bvp_line_end = wrapper.index('\nMLB_BVP_INLINE_RC=$?', bvp_line_start)
            env = os.environ.copy()
            env.pop("MLB_USE_RESTRICTED_INGEST", None)
            env.update(
                {
                    "PATH": f"{temp}:/usr/bin:/bin",
                    "ROUTING_TEST_LOG": str(log),
                    "VENV_PY": str(fake_python),
                    "MLB_DATE_ET": "2026-10-17",
                    "MLB_RUN_TAG": "offline-route-test",
                    "MLB_WRAPPER_RUN_STARTED_AT_UTC": "2026-10-17T00:00:00Z",
                    "MLB_BVP_INLINE_RESULT": "unused.json",
                }
            )
            for name, fragment in (
                ("roster", wrapper[roster_line_start:roster_line_end]),
                ("hook", wrapper[bvp_line_start:bvp_line_end]),
            ):
                child_env = env | {"ROUTE_TEST_NAME": name}
                result = subprocess.run(
                    ["/bin/zsh", "-c", fragment],
                    cwd=temp,
                    env=child_env,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(log.read_text().splitlines(), ["roster|1", "hook|1"])

    @unittest.skipUnless(shutil.which("make"), "GNU make is required")
    def test_stat_refresh_flags_writer_submake_but_not_validator(self):
        with tempfile.TemporaryDirectory(prefix="mlb-ingest-route-") as temp_dir:
            temp = Path(temp_dir)
            log = temp / "calls.log"
            fake_python = temp / "fake-python"
            fake_python.write_text(
                "#!/bin/sh\n"
                "case \"$*\" in\n"
                "  *insert_mlb_stat_derived.py*) child=writer ;;\n"
                "  *validate_mlb_stat_derived_recent.py*) child=validator ;;\n"
                "  *) child=other ;;\n"
                "esac\n"
                "printf '%s|%s\\n' \"$child\" \"${MLB_USE_RESTRICTED_INGEST-unset}\" >> \"$ROUTING_TEST_LOG\"\n"
            )
            fake_python.chmod(0o755)
            env = os.environ.copy()
            env.pop("MLB_USE_RESTRICTED_INGEST", None)
            # The validator reads through the research URL; the fake child never connects.
            env["MLB_MONEYLINE_AUDIT_DATABASE_URL"] = "postgresql://research.invalid/placeholder"
            env["ROUTING_TEST_LOG"] = str(log)
            result = subprocess.run(
                [
                    "make",
                    "-s",
                    "mlb-stat-derived-refresh",
                    f"VENV_PY={fake_python}",
                    "MLB_STAT_DAYS_AGO=0",
                    "MLB_STAT_DERIVED_DAYS=1",
                    "MLB_STAT_DERIVED_MIN=0",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            calls = log.read_text().splitlines()
            self.assertEqual(calls, ["writer|1", "validator|unset"])


if __name__ == "__main__":
    unittest.main()

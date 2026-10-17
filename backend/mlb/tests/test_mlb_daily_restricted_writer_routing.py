"""Verify restricted ingest is passed only to ordinary daily writer processes."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
DAILY_WRAPPER = Path.home() / "bin/proppadia_mlb_refresh_daily.sh"


class MlbDailyRestrictedWriterRoutingTests(unittest.TestCase):
    def test_daily_wrapper_scopes_flag_to_roster_and_bvp_writer_calls(self):
        if not DAILY_WRAPPER.exists():
            self.skipTest(f"installed daily wrapper not present: {DAILY_WRAPPER}")
        wrapper = DAILY_WRAPPER.read_text()
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
        for view in (
            "today_market_snapshot",
            "today_market_timing_signal",
            "today_player_context",
            "today_workspace_mlb",
        ):
            self.assertIn(f"REFRESH MATERIALIZED VIEW mlb.{view};", wrapper)

    @unittest.skipUnless(shutil.which("make"), "GNU make is required")
    def test_wrapper_roster_and_bvp_calls_deliver_flag_to_only_the_child(self):
        if not DAILY_WRAPPER.exists():
            self.skipTest(f"installed daily wrapper not present: {DAILY_WRAPPER}")
        wrapper = DAILY_WRAPPER.read_text()
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

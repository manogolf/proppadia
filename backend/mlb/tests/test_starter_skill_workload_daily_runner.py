import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.mlb.scripts import run_mlb_starter_skill_workload_daily_research as runner


class StarterSkillWorkloadDailyRunnerTests(unittest.TestCase):
    def test_one_generated_timestamp_is_reused_by_builder_and_runner(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            args = runner.parse_args(
                [
                    "--date",
                    "2026-08-05",
                    "--skip-schedule-check",
                    "--output-root",
                    str(root / "daily"),
                    "--observation-root",
                    str(root / "observations"),
                    "--launchagent-artifact-root",
                    str(root / "agent"),
                ]
            )
            generated = "2026-08-05T14:00:00+00:00"
            build_result = {
                "run_dir": str(root / "daily" / "2026-08-05" / "runs" / "test"),
                "latest_dir": str(root / "daily" / "2026-08-05" / "latest"),
            }
            with patch.object(runner, "_utc_now", return_value=generated), patch.object(
                runner, "build_research", return_value=build_result
            ) as build, patch.object(runner, "update_observation", return_value={"status": "PASS"}):
                result = runner.run(args)

            self.assertEqual(build.call_args.args[0].generated_at_utc, generated)
            self.assertEqual(result["generated_at_utc"], generated)
            written = json.loads(
                (root / "agent" / "2026-08-05" / "starter_skill_workload_runner_latest.json").read_text()
            )
            self.assertEqual(written["generated_at_utc"], generated)

    def test_controlled_noop_reuses_execution_timestamp(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            args = runner.parse_args(
                [
                    "--date",
                    "2026-08-05",
                    "--validate-no-games",
                    "--noop-root",
                    str(root),
                ]
            )
            generated = "2026-08-05T14:00:00+00:00"
            with patch.object(runner, "_utc_now", return_value=generated):
                runner.run(args)
            payload = json.loads((root / "2026-08-05" / "starter_skill_workload_noop.json").read_text())
            self.assertEqual(payload["generated_at_utc"], generated)


if __name__ == "__main__":
    unittest.main()

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
            ) as build, patch.object(runner, "update_observation", return_value={"status": "PASS"}), patch.object(
                runner, "_select_research_read_route"
            ) as select_route:
                result = runner.run(args)
            select_route.assert_called_once_with()

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


class RebindPendingCompletedSlatesTests(unittest.TestCase):
    def test_only_completed_pending_slates_other_than_today_are_retried(self):
        from backend.mlb.scripts import update_mlb_starter_skill_workload_observation as updater

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "starter_skill_workload_observation_index.csv").write_text(
                "slate_date,outcome_binding_status\n"
                "2026-10-26,pending_current_or_future_slate\n"
                "2026-10-27,bound_sufficient\n"
                "2026-10-28,pending_or_unavailable\n"
                "2099-01-01,pending_current_or_future_slate\n"
            )
            args = argparse.Namespace(date="2026-10-28", run_dir="/today/run", daily_root="daily",
                                      observation_root=str(root), min_starter_match_pct=0.8, no_db=False)
            calls = []

            def fake_update(retry):
                calls.append((retry.date, retry.run_dir))
                return {"updated_row": {"outcome_binding_status": "bound_sufficient"}}

            with patch.object(updater, "update_observation", side_effect=fake_update):
                result = updater.rebind_pending_completed_slates(args)
        # Today's slate (10-28), bound slates and future slates are left alone;
        # the retry uses that slate's own latest pregame run, not today's.
        self.assertEqual(calls, [("2026-10-26", "")])
        self.assertEqual(result, [{"slate_date": "2026-10-26", "outcome_binding_status": "bound_sufficient"}])


if __name__ == "__main__":
    unittest.main()

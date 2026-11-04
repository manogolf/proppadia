"""Scheduled daily run: skips, step order, stop on a failed daily, export CSV capture."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.nhl.scripts import run_nhl_scheduled_daily as job


class ScheduledDailyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(dir=job.ROOT / "tmp"))
        self.patches = [mock.patch.object(job, "OUT", self.tmp / "out"),
                        mock.patch.object(job, "manual_daily_running", return_value=False),
                        mock.patch.object(job, "wait_for_morning_prep", return_value=True),
                        mock.patch.object(job, "has_cross_market_package", return_value=True),
                        mock.patch.object(job, "slate_today", return_value="2026-11-04")]
        for p in self.patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.patches])

    def fake_steps(self, codes, classification="READY", csv="artifacts/x.csv"):
        calls = []

        def run_step(name, command, run_dir, timeout):
            calls.append(name)
            log = run_dir / f"{name}.log"
            log.write_text(f"NHL 8RAIN EXPORT READY\nCSV: {csv}\n" if name == "03_8rain_export" else "")
            return {"step": name, "exit_code": codes[name], "log": str(log.relative_to(job.ROOT))}
        receipt = {"final_classification": classification, "path": "receipt"} if classification else None
        return calls, mock.patch.object(job, "run_step", run_step), mock.patch.object(job, "latest_daily_receipt", return_value=receipt)

    def summary(self):
        return json.loads(next((self.tmp / "out" / "2026-11-04").glob("*/summary.json")).read_text())

    def test_all_steps_run_in_order_and_record_the_upload_csv(self):
        calls, a, b = self.fake_steps({"01_daily_with_odds": 0, "02_cross_market_capture": 0, "03_8rain_export": 0})
        with a, b:
            self.assertEqual(job.main([]), 0)
        self.assertEqual(calls, ["01_daily_with_odds", "02_cross_market_capture", "03_8rain_export"])
        self.assertEqual((self.summary()["status"], self.summary()["upload_csv"]), ("READY", "artifacts/x.csv"))

    def test_a_daily_run_that_is_not_ready_stops_before_the_export(self):
        calls, a, b = self.fake_steps({"01_daily_with_odds": 0}, classification="FAILED_BUDGET_GUARD")
        with a, b:
            self.assertEqual(job.main([]), 1)
        self.assertEqual(calls, ["01_daily_with_odds"])
        self.assertEqual(self.summary()["status"], "FAILED_DAILY")

    def test_a_failed_game_line_refresh_still_exports_with_a_warning(self):
        calls, a, b = self.fake_steps({"01_daily_with_odds": 0, "02_cross_market_capture": 2, "03_8rain_export": 0})
        with a, b:
            self.assertEqual(job.main([]), 0)
        self.assertEqual(self.summary()["cross_market_warning"], "capture exit 2")

    def test_skips_when_a_manual_daily_is_running(self):
        with mock.patch.object(job, "manual_daily_running", return_value=True), \
                mock.patch.object(job, "run_step") as run_step:
            self.assertEqual(job.main([]), 0)
        run_step.assert_not_called()

    def test_morning_prep_still_running_after_the_wait_fails_the_run(self):
        with mock.patch.object(job, "wait_for_morning_prep", return_value=False), \
                mock.patch.object(job, "run_step") as run_step:
            self.assertEqual(job.main([]), 1)
        run_step.assert_not_called()

    def test_first_run_of_the_day_uses_auto_then_refresh(self):
        for exists, phase in ((False, "AUTO"), (True, "REFRESH")):
            with mock.patch.object(job, "has_cross_market_package", return_value=exists), \
                    mock.patch("builtins.print") as printed:
                job.main(["--dry-run"])
            steps = json.loads(printed.call_args[0][0])["steps"]
            self.assertIn(phase, steps[1])
            self.assertEqual(steps[0][-2:], ["daily", "--with-odds"])
            self.assertEqual(steps[2][-1], "--latest-refresh")


if __name__ == "__main__":
    unittest.main()

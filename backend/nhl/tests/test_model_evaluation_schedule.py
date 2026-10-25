"""Weekly evaluation schedule (status, gated build) and the improvement log."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from backend.nhl import model_evaluation_schedule as sched
from backend.nhl.scripts import build_nhl_weekly_model_evaluation as ev


def make_root(enabled: bool, reports: list[str]) -> Path:
    root = Path(tempfile.mkdtemp(dir=Path(__file__).resolve().parents[3] / "tmp"))
    (root / "docs/nhl").mkdir(parents=True)
    for d in reports:
        (root / f"docs/nhl/weekly_model_evaluation_{d}.md").write_text("x")
    config = json.loads(sched.CONFIG.read_text())
    config["enabled"] = enabled
    (root / "config.json").write_text(json.dumps(config))
    return root


class ScheduleTests(unittest.TestCase):
    def test_shipped_schedule_is_switched_off(self):
        self.assertFalse(sched.load_config()["enabled"])

    def test_status_across_the_week(self):
        root = make_root(False, ["2026-10-24"])
        config = sched.load_config(root / "config.json")
        self.assertTrue(sched.status("2026-10-25", config, root)["current"])          # Sunday after the report
        self.assertEqual(sched.status("2026-10-31", config, root)["next_run_et"], "2026-11-01")
        due = sched.status("2026-11-01", config, root)
        self.assertTrue(due["due_today"]) and self.assertEqual(due["expected_report_as_of"], "2026-10-31")
        self.assertTrue(sched.status("2026-11-03", config, root)["overdue"])

    def test_disabled_never_builds(self):
        root = make_root(False, ["2026-10-24"])
        calls = []
        result = sched.check_and_run("2026-11-01", config_path=root / "config.json", root=root,
                                     runner=lambda *a, **k: calls.append(a))
        self.assertEqual(result["status"], "OVERDUE_SCHEDULE_DISABLED")
        self.assertEqual(calls, [])
        self.assertIn("switched off", sched.console_line(result))

    def test_enabled_builds_once_for_the_expected_week(self):
        root = make_root(True, ["2026-10-24"])
        calls = []

        def runner(cmd, **kwargs):
            calls.append(cmd)
            (root / "docs/nhl/weekly_model_evaluation_2026-10-31.md").write_text("built")
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        result = sched.check_and_run("2026-11-01", config_path=root / "config.json", root=root, runner=runner)
        self.assertEqual(result["status"], "BUILT")
        self.assertIn("2026-10-31", calls[0])
        self.assertEqual(sched.check_and_run("2026-11-01", config_path=root / "config.json", root=root,
                                             runner=runner)["status"], "CURRENT")
        self.assertEqual(len(calls), 1)

    def test_build_failure_and_bad_config_do_not_raise(self):
        root = make_root(True, [])
        failed = sched.check_and_run("2026-11-01", config_path=root / "config.json", root=root,
                                     runner=lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="boom"))
        self.assertEqual(failed["status"], "BUILD_FAILED_NONBLOCKING")
        (root / "bad.json").write_text("{}")
        self.assertEqual(sched.check_and_run("2026-11-01", config_path=root / "bad.json", root=root)["status"],
                         "SCHEDULE_ERROR")


def seg(gap: float, n: int = 100, band: str = ".55-.60") -> pd.DataFrame:
    return pd.DataFrame([{"lane": "sog", "line": 2.5, "selected_side": "OVER", "band": band, "n": n,
                          "wins": 50, "win_rate": 0.5, "predicted": 0.5 - gap, "gap": gap}])


class ImprovementLogTests(unittest.TestCase):
    def test_new_recurring_clear_and_manual_status(self):
        log, new, rec = ev.update_improvement_log({}, seg(-0.10), "2026-10-24")
        self.assertEqual((new, rec), (["IMP-001"], []))
        log, new, rec = ev.update_improvement_log(log, seg(-0.08), "2026-10-31")
        self.assertEqual((new, rec), ([], ["IMP-001"]))
        self.assertEqual(log["entries"][0]["weeks_flagged"], 2)
        log, _, _ = ev.update_improvement_log(log, seg(-0.08), "2026-10-31")   # same week rerun
        self.assertEqual(len(log["entries"][0]["history"]), 2)
        for week in ("2026-11-07", "2026-11-14", "2026-11-21"):
            log, _, _ = ev.update_improvement_log(log, seg(-0.01), week)
        self.assertEqual(log["entries"][0]["status"], "OPEN_CLEAR_3_WEEKS")
        log["entries"][0]["status"] = "TESTING"
        log, _, _ = ev.update_improvement_log(log, seg(-0.01), "2026-11-28")
        self.assertEqual(log["entries"][0]["status"], "TESTING")

    def test_small_or_mild_segments_are_not_flagged(self):
        log, new, _ = ev.update_improvement_log({}, pd.concat([seg(-0.10, n=10), seg(-0.02, band=".60-.70")]), "2026-10-24")
        self.assertEqual((new, log["entries"]), ([], []))


if __name__ == "__main__":
    unittest.main()

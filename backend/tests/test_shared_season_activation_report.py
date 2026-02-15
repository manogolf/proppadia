import unittest
from pathlib import Path
from unittest.mock import patch

from backend.scripts import season_activation_report as sar


class TestSharedSeasonActivationReport(unittest.TestCase):
    def test_build_report_passes_when_all_green(self):
        with patch.object(sar.phase_status_snapshot, "build_snapshot", return_value={"ok": True}), patch.object(
            sar.season_activation_status, "build_status", return_value={"ok": True}
        ), patch.object(sar.check_season_baseline_artifacts, "build_payload", return_value={"ok": True}), patch.object(
            sar, "_history_tail", return_value={"history_count": 0, "returned": 0, "rows": []}
        ):
            payload = sar.build_report(Path("x"), 5, 0)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pass")
        self.assertIn("season_activation_history", payload)

    def test_build_report_fails_when_any_check_fails(self):
        with patch.object(sar.phase_status_snapshot, "build_snapshot", return_value={"ok": True}), patch.object(
            sar.season_activation_status, "build_status", return_value={"ok": False}
        ), patch.object(sar.check_season_baseline_artifacts, "build_payload", return_value={"ok": True}), patch.object(
            sar, "_history_tail", return_value={"history_count": 0, "returned": 0, "rows": []}
        ):
            payload = sar.build_report(Path("x"), 5, 0)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")

    def test_history_tail_reports_new_blockers(self):
        with patch.object(
            sar.season_activation_last,
            "_load_history",
            return_value=[
                {"readiness": {"blockers": ["a"]}},
                {"readiness": {"blockers": ["a", "b"]}},
            ],
        ):
            payload = sar._history_tail(Path("x"), 2)
        self.assertEqual(payload["history_count"], 2)
        self.assertEqual(payload["rows"][1]["new_blockers"], ["b"])

    def test_main_non_strict_returns_zero_on_fail_payload(self):
        with patch.object(
            sar,
            "build_report",
            return_value={
                "ok": False,
                "status": "fail",
                "phase_status": {},
                "season_activation": {},
                "baseline_check": {},
                "season_activation_history": {},
            },
        ):
            rc = sar.main([])
        self.assertEqual(rc, 0)

    def test_main_strict_returns_nonzero_on_fail_payload(self):
        with patch.object(
            sar,
            "build_report",
            return_value={
                "ok": False,
                "status": "fail",
                "phase_status": {},
                "season_activation": {},
                "baseline_check": {},
                "season_activation_history": {},
            },
        ):
            rc = sar.main(["--strict"])
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()

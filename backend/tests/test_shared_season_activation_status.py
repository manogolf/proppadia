import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.scripts import season_activation_status as sas


class TestSharedSeasonActivationStatus(unittest.TestCase):
    def test_build_status_with_no_baselines(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            plan = root / "Execution Plan.md"
            baselines = root / "season_baselines"
            plan.write_text(
                "\n".join(
                    [
                        "## Phase Status Tracker",
                        "- Phase 6.1 Preseason dry run: in progress",
                        "- Phase 6.2 In-season cadence cutover: pending",
                        "- Phase 6.3 Baseline lock: in progress",
                    ]
                ),
                encoding="utf-8",
            )
            payload = sas.build_status(plan, baselines)
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["status"], "fail")
            self.assertFalse(payload["baseline_artifacts"]["has_mlb"])
            self.assertFalse(payload["baseline_artifacts"]["has_nhl"])
            self.assertGreater(len(payload["next_steps"]), 0)
            self.assertIn("phase_6_1_incomplete", payload["readiness"]["blockers"])
            self.assertIn("phase_6_2_incomplete", payload["readiness"]["blockers"])
            self.assertIn("phase_6_3_incomplete", payload["readiness"]["blockers"])
            self.assertIn("baseline_artifacts_missing", payload["readiness"]["blockers"])
            self.assertIn("season_cutover_history_missing", payload["readiness"]["blockers"])
            self.assertIn("Run: make season-cutover-log", payload["next_steps"])

    def test_build_status_with_baselines_present(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            plan = root / "Execution Plan.md"
            baselines = root / "season_baselines"
            baselines.mkdir(parents=True, exist_ok=True)
            cutover = root / "season_cutover_history.jsonl"
            (baselines / "mlb_quality_games_30_120.json").write_text("{}", encoding="utf-8")
            (baselines / "nhl_quality_2025-12-01_2025-12-31.json").write_text("{}", encoding="utf-8")
            cutover.write_text('{"status":"ok"}\n', encoding="utf-8")
            plan.write_text(
                "\n".join(
                    [
                        "## Phase Status Tracker",
                        "- Phase 6.1 Preseason dry run: complete",
                        "- Phase 6.2 In-season cadence cutover: complete",
                        "- Phase 6.3 Baseline lock: complete",
                    ]
                ),
                encoding="utf-8",
            )
            payload = sas.build_status(plan, baselines, cutover)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["status"], "pass")
            self.assertTrue(payload["baseline_artifacts"]["has_mlb"])
            self.assertTrue(payload["baseline_artifacts"]["has_nhl"])
            self.assertTrue(payload["season_cutover"]["has_history"])
            self.assertIn("Review: make season-baseline-last", payload["next_steps"])
            self.assertEqual(payload["readiness"]["blockers"], [])

    def test_build_status_requires_phase_6_3_complete_even_with_baselines(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            plan = root / "Execution Plan.md"
            baselines = root / "season_baselines"
            cutover = root / "season_cutover_history.jsonl"
            baselines.mkdir(parents=True, exist_ok=True)
            (baselines / "mlb_quality_games_30_120.json").write_text("{}", encoding="utf-8")
            (baselines / "nhl_quality_2025-12-01_2025-12-31.json").write_text("{}", encoding="utf-8")
            cutover.write_text('{"status":"ok"}\n', encoding="utf-8")
            plan.write_text(
                "\n".join(
                    [
                        "## Phase Status Tracker",
                        "- Phase 6.1 Preseason dry run: complete",
                        "- Phase 6.2 In-season cadence cutover: complete",
                        "- Phase 6.3 Baseline lock: in progress",
                    ]
                ),
                encoding="utf-8",
            )
            payload = sas.build_status(plan, baselines, cutover)
            self.assertFalse(payload["ok"])
            self.assertIn("phase_6_3_incomplete", payload["readiness"]["blockers"])
            self.assertIn("Review: make season-baseline-last", payload["next_steps"])

    def test_main_strict_returns_nonzero_when_not_ready(self):
        with mock.patch.object(sas, "build_status", return_value={"ok": False, "status": "fail"}):
            rc = sas.main(["--strict"])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()

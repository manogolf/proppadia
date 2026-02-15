import tempfile
import unittest
from pathlib import Path

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
            self.assertTrue(payload["ok"])
            self.assertFalse(payload["baseline_artifacts"]["has_mlb"])
            self.assertFalse(payload["baseline_artifacts"]["has_nhl"])
            self.assertGreater(len(payload["next_steps"]), 0)

    def test_build_status_with_baselines_present(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            plan = root / "Execution Plan.md"
            baselines = root / "season_baselines"
            baselines.mkdir(parents=True, exist_ok=True)
            (baselines / "mlb_quality_games_30_120.json").write_text("{}", encoding="utf-8")
            (baselines / "nhl_quality_2025-12-01_2025-12-31.json").write_text("{}", encoding="utf-8")
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
            payload = sas.build_status(plan, baselines)
            self.assertTrue(payload["baseline_artifacts"]["has_mlb"])
            self.assertTrue(payload["baseline_artifacts"]["has_nhl"])
            self.assertIn("complete", payload["next_steps"][0].lower())


if __name__ == "__main__":
    unittest.main()


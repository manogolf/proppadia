import tempfile
import unittest
from pathlib import Path

from backend.scripts import phase_status_snapshot as pss


class TestSharedPhaseStatusSnapshot(unittest.TestCase):
    def test_build_snapshot_counts_statuses(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            plan = root / "Execution Plan.md"
            plan.write_text(
                "\n".join(
                    [
                        "# Plan",
                        "## Phase Status Tracker",
                        "- Phase 1.1 Workflow decommission pass: complete",
                        "- Phase 1.2 Runtime path lock: in progress",
                        "- Phase 1.3 Frontend route hardening regression pack: pending",
                        "## Next Section",
                        "- ignored",
                    ]
                ),
                encoding="utf-8",
            )
            payload = pss.build_snapshot(plan)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["total"], 3)
            self.assertEqual(payload["counts"].get("complete"), 1)
            self.assertEqual(payload["counts"].get("in_progress"), 1)
            self.assertEqual(payload["counts"].get("pending"), 1)

    def test_build_snapshot_ignores_invalid_tracker_lines(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            plan = root / "Execution Plan.md"
            plan.write_text(
                "\n".join(
                    [
                        "## Phase Status Tracker",
                        "- not a phase line",
                        "- Phase X.Y bad: pending",
                    ]
                ),
                encoding="utf-8",
            )
            payload = pss.build_snapshot(plan)
            self.assertEqual(payload["total"], 0)


if __name__ == "__main__":
    unittest.main()


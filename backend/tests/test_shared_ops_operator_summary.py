import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import ops_operator_summary as ops


class TestSharedOpsOperatorSummary(unittest.TestCase):
    def test_collect_summary_shape(self):
        with patch.object(
            ops.cron_governance_snapshot,
            "build_snapshot",
            return_value={"ok": True, "status": "pass"},
        ), patch.object(
            ops.mlb_readiness_snapshot,
            "collect_snapshot",
            return_value={"ok": True, "status": "pass", "checks": {}},
        ), patch.object(
            ops.season_activation_report,
            "build_report",
            return_value={"ok": True, "status": "pass"},
        ):
            payload = ops.collect_summary(
                stat_days=30,
                stat_require_min=0,
                roster_require_min=1,
                roster_stale_hours=30,
                season_history_input="artifacts/season_activation_history.jsonl",
                season_history_limit=10,
                season_max_age_hours=0,
            )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pass")
        self.assertIn("governance", payload)
        self.assertIn("mlb_readiness", payload)
        self.assertIn("season_activation_report", payload)

    def test_main_non_strict_returns_zero_on_fail(self):
        with patch.object(
            ops,
            "collect_summary",
            return_value={"ok": False, "status": "fail", "governance": {}, "mlb_readiness": {}, "season_activation_report": {}},
        ):
            out = StringIO()
            with redirect_stdout(out):
                rc = ops.main(["--json"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])

    def test_main_strict_returns_nonzero_on_fail(self):
        with patch.object(
            ops,
            "collect_summary",
            return_value={"ok": False, "status": "fail", "governance": {}, "mlb_readiness": {}, "season_activation_report": {}},
        ):
            rc = ops.main(["--strict"])
        self.assertEqual(rc, 1)

    def test_compact_summary_shape(self):
        payload = {
            "ok": True,
            "status": "pass",
            "governance": {"ok": True, "status": "pass", "governance_ok": True, "season_activation_ok": True},
            "mlb_readiness": {
                "ok": True,
                "status": "pass",
                "captured_at": "2026-02-15T08:00:00+00:00",
                "checks": {
                    "stat_derived": {"count": 123, "latest_game_date": "2025-08-15"},
                    "roster": {"total_players": 1197, "stale": False},
                },
            },
            "season_activation_report": {
                "ok": True,
                "status": "pass",
                "season_activation": {"blockers": ["none"]},
            },
        }
        compact = ops.compact_summary(payload)
        self.assertEqual(compact["captured_at"], "2026-02-15T08:00:00+00:00")
        self.assertTrue(compact["ok"])
        self.assertEqual(compact["mlb_readiness"]["stat_count"], 123)
        self.assertEqual(compact["season_activation"]["blocker_count"], 1)

    def test_main_compact_outputs_compact_json(self):
        with patch.object(
            ops,
            "collect_summary",
            return_value={
                "ok": True,
                "status": "pass",
                "governance": {"ok": True, "status": "pass", "governance_ok": True, "season_activation_ok": True},
                "mlb_readiness": {"ok": True, "status": "pass", "checks": {}},
                "season_activation_report": {"ok": True, "status": "pass", "season_activation": {"blockers": []}},
            },
        ):
            out = StringIO()
            with redirect_stdout(out):
                rc = ops.main(["--compact"])
        self.assertEqual(rc, 0)
        compact = json.loads(out.getvalue())
        self.assertIn("season_activation", compact)
        self.assertNotIn("season_activation_report", compact)


if __name__ == "__main__":
    unittest.main()

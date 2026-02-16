import unittest
from unittest.mock import patch

from backend.scripts import mlb_pipeline_check as pipeline


class TestSharedMlbPipelineCheck(unittest.TestCase):
    def test_collect_pipeline_check_success_shape(self):
        responses = [
            (0, {"ok": True, "status": "pass"}),
            (0, {"ok": True, "status": "pass"}),
            (0, {"ok": True, "status": "pass"}),
        ]
        with patch.object(pipeline, "run_json_check", side_effect=responses) as mock_runner:
            payload = pipeline.collect_pipeline_check(
                base_url=None,
                date="2025-08-15",
                sample_size=10,
                require_min_success=3,
                prop_types="hits,total_bases",
                quality_window_mode="games",
                quality_window_days=120,
                quality_games_back=30,
                quality_min_total=100,
                quality_min_accuracy=50.0,
                coverage_window_mode="games",
                coverage_window_days=30,
                coverage_games_back=30,
                coverage_required_props="hits,total_bases",
                coverage_min_graded_per_prop=20,
            )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["failures"], [])
        self.assertEqual(len(payload["checks"]), 3)
        self.assertEqual(mock_runner.call_count, 3)
        self.assertIn("captured_at", payload)

    def test_collect_pipeline_check_marks_failure_from_nonzero_or_bad_ok(self):
        responses = [
            (0, {"ok": True, "status": "pass"}),
            (0, {"ok": False, "status": "fail"}),
            (1, {"ok": False, "status": "fail"}),
        ]
        with patch.object(pipeline, "run_json_check", side_effect=responses):
            payload = pipeline.collect_pipeline_check(
                base_url="https://example.test",
                date="2025-08-15",
                sample_size=10,
                require_min_success=3,
                prop_types="hits",
                quality_window_mode="days",
                quality_window_days=120,
                quality_games_back=30,
                quality_min_total=100,
                quality_min_accuracy=50.0,
                coverage_window_mode="days",
                coverage_window_days=30,
                coverage_games_back=30,
                coverage_required_props="hits",
                coverage_min_graded_per_prop=20,
            )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")
        self.assertIn("prediction_flow_audit", payload["failures"])
        self.assertIn("prop_coverage", payload["failures"])

    def test_main_returns_nonzero_when_not_ok(self):
        with patch.object(
            pipeline,
            "collect_pipeline_check",
            return_value={"ok": False, "status": "fail", "failures": ["prediction_gate"], "checks": []},
        ):
            rc = pipeline.main([])
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()

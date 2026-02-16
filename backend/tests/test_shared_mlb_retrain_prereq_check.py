import unittest
from unittest.mock import patch

from backend.scripts import mlb_retrain_prereq_check as prereq


class TestSharedMlbRetrainPrereqCheck(unittest.TestCase):
    def test_collect_checklist_pass_shape(self):
        responses = [
            (0, {"status": "pass"}),
            (0, {"ok": True, "status": "pass"}),
            (0, {"ok": True, "status": "pass"}),
        ]
        with patch.object(prereq.json_check_runner, "run_json_check", side_effect=responses), patch.object(
            prereq.season_baseline_last,
            "build_payload",
            return_value={"latest": {"mlb": {"exists": True, "age_hours": 2.0, "path": "x"}}},
        ):
            payload = prereq.collect_retrain_prereq_checklist(
                freshness_days=7,
                freshness_min_rows=1,
                coverage_window_mode="games",
                coverage_window_days=30,
                coverage_games_back=30,
                coverage_required_props="hits,total_bases",
                coverage_min_training_source_per_prop=20,
                coverage_training_prop_sources="mlb_api,user_added",
                grading_window_mode="games",
                grading_window_days=30,
                grading_games_back=30,
                grading_prop_types="hits,total_bases",
                grading_min_total=1000,
                baseline_dir="artifacts/season_baselines",
                baseline_max_age_hours=0,
            )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["failures"], [])
        self.assertEqual(len(payload["checks"]), 4)
        self.assertIn("captured_at", payload)

    def test_collect_checklist_fails_when_any_check_fails(self):
        responses = [
            (1, {"status": "fail"}),
            (0, {"ok": True, "status": "pass"}),
            (0, {"ok": False, "status": "fail"}),
        ]
        with patch.object(prereq.json_check_runner, "run_json_check", side_effect=responses), patch.object(
            prereq.season_baseline_last,
            "build_payload",
            return_value={"latest": {"mlb": {"exists": False, "age_hours": None, "path": None}}},
        ):
            payload = prereq.collect_retrain_prereq_checklist(
                freshness_days=7,
                freshness_min_rows=1,
                coverage_window_mode="games",
                coverage_window_days=30,
                coverage_games_back=30,
                coverage_required_props="hits,total_bases",
                coverage_min_training_source_per_prop=20,
                coverage_training_prop_sources="mlb_api,user_added",
                grading_window_mode="games",
                grading_window_days=30,
                grading_games_back=30,
                grading_prop_types="hits,total_bases",
                grading_min_total=1000,
                baseline_dir="artifacts/season_baselines",
                baseline_max_age_hours=0,
            )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")
        self.assertIn("data_freshness", payload["failures"])
        self.assertIn("grading_completeness", payload["failures"])
        self.assertIn("baseline_comparison_availability", payload["failures"])

    def test_main_returns_nonzero_on_fail(self):
        with patch.object(
            prereq,
            "collect_retrain_prereq_checklist",
            return_value={"ok": False, "status": "fail", "failures": ["data_freshness"], "checks": []},
        ):
            rc = prereq.main([])
        self.assertEqual(rc, 1)

    def test_collect_handles_check_exception(self):
        with patch.object(
            prereq.json_check_runner, "run_json_check", side_effect=RuntimeError("db down")
        ), patch.object(
            prereq.season_baseline_last,
            "build_payload",
            return_value={"latest": {"mlb": {"exists": True, "age_hours": 2.0, "path": "x"}}},
        ):
            payload = prereq.collect_retrain_prereq_checklist(
                freshness_days=7,
                freshness_min_rows=1,
                coverage_window_mode="games",
                coverage_window_days=30,
                coverage_games_back=30,
                coverage_required_props="hits,total_bases",
                coverage_min_training_source_per_prop=20,
                coverage_training_prop_sources="mlb_api,user_added",
                grading_window_mode="games",
                grading_window_days=30,
                grading_games_back=30,
                grading_prop_types="hits,total_bases",
                grading_min_total=1000,
                baseline_dir="artifacts/season_baselines",
                baseline_max_age_hours=0,
            )
        self.assertFalse(payload["ok"])
        self.assertIn("data_freshness", payload["failures"])
        first = payload["checks"][0]["payload"]
        self.assertEqual(first.get("status"), "fail")
        self.assertIn("error", first)


if __name__ == "__main__":
    unittest.main()

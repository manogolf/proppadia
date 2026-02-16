import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import report_mlb_prop_coverage as coverage


class TestSharedMlbPropCoverage(unittest.TestCase):
    def test_pass_with_required_props_present_and_graded(self):
        side_effects = [
            [  # player_props aggregate
                {
                    "prop_type": "hits",
                    "total_predictions": 100,
                    "resolved_count": 90,
                    "graded_count": 80,
                    "wins": 42,
                    "losses": 38,
                    "pushes": 5,
                    "dnps": 5,
                },
                {
                    "prop_type": "total_bases",
                    "total_predictions": 80,
                    "resolved_count": 70,
                    "graded_count": 60,
                    "wins": 30,
                    "losses": 30,
                    "pushes": 5,
                    "dnps": 5,
                },
            ],
            [  # stat_derived aggregate
                {"prop_type": "hits", "training_source_count": 250},
                {"prop_type": "total_bases", "training_source_count": 240},
            ],
        ]
        out = StringIO()
        with patch.object(coverage, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = coverage.main(
                ["--window-days", "30", "--required-props", "hits,total_bases", "--min-graded-per-prop", "10"]
            )
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["missing_required_props"], [])
        self.assertEqual(payload["under_min_required_props"], [])

    def test_fail_when_required_prop_missing_or_under_min(self):
        side_effects = [
            [
                {
                    "prop_type": "hits",
                    "total_predictions": 10,
                    "resolved_count": 10,
                    "graded_count": 2,
                    "wins": 1,
                    "losses": 1,
                    "pushes": 0,
                    "dnps": 0,
                }
            ],
            [{"prop_type": "hits", "training_source_count": 30}],
        ]
        out = StringIO()
        with patch.object(coverage, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = coverage.main(
                ["--required-props", "hits,total_bases", "--min-graded-per-prop", "5"]
            )
        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertIn("total_bases", payload["missing_required_props"])
        self.assertIn("hits", payload["under_min_required_props"])

    def test_pass_when_gate_metric_training_source_meets_min(self):
        side_effects = [
            [
                {
                    "prop_type": "hits",
                    "total_predictions": 5,
                    "resolved_count": 5,
                    "graded_count": 1,
                    "wins": 1,
                    "losses": 0,
                    "pushes": 0,
                    "dnps": 0,
                },
                {
                    "prop_type": "total_bases",
                    "total_predictions": 3,
                    "resolved_count": 3,
                    "graded_count": 1,
                    "wins": 1,
                    "losses": 0,
                    "pushes": 0,
                    "dnps": 0,
                },
            ],
            [
                {"prop_type": "hits", "training_source_count": 40},
                {"prop_type": "total_bases", "training_source_count": 35},
            ],
        ]
        out = StringIO()
        with patch.object(coverage, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = coverage.main(
                [
                    "--required-props",
                    "hits,total_bases",
                    "--min-graded-per-prop",
                    "20",
                    "--gate-metric",
                    "training_source",
                ]
            )
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["gate_metric"], "training_source")

    def test_training_prop_sources_are_applied_to_training_query(self):
        side_effects = [
            [],
            [],
        ]
        out = StringIO()
        with patch.object(coverage, "pg_fetchall", side_effect=side_effects) as mock_fetch, redirect_stdout(out):
            rc = coverage.main(["--training-prop-sources", "mlb_api,stat_derived"])
        self.assertEqual(rc, 0)
        calls = mock_fetch.call_args_list
        self.assertEqual(len(calls), 2)
        training_query, training_params = calls[1].args
        self.assertIn("prop_source IN (%s, %s)", training_query)
        self.assertEqual(training_params[0], "mlb_api")
        self.assertEqual(training_params[1], "stat_derived")


if __name__ == "__main__":
    unittest.main()

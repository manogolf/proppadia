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
                {"prop_type": "hits", "stat_derived_count": 250},
                {"prop_type": "total_bases", "stat_derived_count": 240},
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
            [{"prop_type": "hits", "stat_derived_count": 30}],
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


if __name__ == "__main__":
    unittest.main()

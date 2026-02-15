import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import analyze_mlb_prediction_quality as quality


class TestSharedMlbPredictionQuality(unittest.TestCase):
    def test_main_pass_with_sufficient_rows(self):
        side_effects = [
            [{"total": 100, "correct": 56}],  # overall
            [{"prop_type": "hits", "total": 60, "correct": 34}],  # by_prop
            [{"confidence_bucket": "high", "total": 40, "correct": 26}],  # by_bucket
            [  # drift
                {"bucket": "last_14d", "total": 50, "correct": 30},
                {"bucket": "prev_14d", "total": 50, "correct": 26},
            ],
        ]
        out = StringIO()
        with patch.object(quality, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = quality.main(["--window-days", "120", "--min-total", "1"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["overall"]["accuracy_pct"], 56.0)
        self.assertEqual(payload["drift_14d"]["delta_pct"], 8.0)

    def test_main_fails_when_overall_total_below_min(self):
        side_effects = [
            [{"total": 0, "correct": 0}],
            [],
            [],
            [],
        ]
        out = StringIO()
        with patch.object(quality, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = quality.main(["--window-days", "120", "--min-total", "1"])
        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["overall"]["total"], 0)

    def test_main_games_mode_uses_games_back_window(self):
        side_effects = [
            [{"total": 20, "correct": 12}],  # overall
            [{"prop_type": "hits", "total": 20, "correct": 12}],  # by_prop
            [{"confidence_bucket": "mid", "total": 20, "correct": 12}],  # by_bucket
            [],  # drift
        ]
        out = StringIO()
        with patch.object(quality, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = quality.main(["--window-mode", "games", "--games-back", "30", "--min-total", "1"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["overall"]["window_mode"], "games")
        self.assertEqual(payload["overall"]["window_value"], 30)


if __name__ == "__main__":
    unittest.main()

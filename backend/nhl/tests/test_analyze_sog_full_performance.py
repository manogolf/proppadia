import unittest

from backend.nhl.scripts.analyze_sog_full_performance import (
    auc_ap,
    paired_bootstrap,
    score,
)


class SogFullPerformanceMetricsTest(unittest.TestCase):
    def test_probability_metrics_and_auc(self):
        rows = [
            {"y": 1, "p": 0.8, "side": "OVER", "player_id": "a"},
            {"y": 0, "p": 0.2, "side": "UNDER", "player_id": "b"},
        ]
        metrics = score(rows)
        self.assertEqual(metrics["accuracy"], 1.0)
        self.assertEqual(metrics["auc"], 1.0)
        self.assertAlmostEqual(metrics["brier"], 0.04)
        self.assertEqual(auc_ap(rows), (1.0, 1.0))

    def test_paired_bootstrap_keeps_player_pairs(self):
        pairs = [
            ({"player_id": "a", "y": 1, "p_over": 0.6, "side": "OVER"},
             {"player_id": "a", "y": 1, "p": 0.7, "side": "OVER"}),
            ({"player_id": "b", "y": 0, "p_over": 0.4, "side": "UNDER"},
             {"player_id": "b", "y": 0, "p": 0.3, "side": "UNDER"}),
        ]
        ci = paired_bootstrap(pairs, reps=100, seed=7)
        self.assertEqual(len(ci), 6)
        self.assertEqual(ci[0], 0.0)
        self.assertEqual(ci[1], 0.0)
        self.assertLess(ci[2], 0.0)
        self.assertLess(ci[4], 0.0)


if __name__ == "__main__":
    unittest.main()

import unittest

import pandas as pd

from backend.nhl.scripts.characterize_nhl_sog_production_baseline import metrics
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail


class ProductionSogCharacterizationTests(unittest.TestCase):
    def test_poisson_half_line_probabilities_are_nested(self):
        probabilities = [_poisson_tail(1.7, threshold) for threshold in (2, 3, 4)]
        self.assertTrue(all(0 <= p <= 1 for p in probabilities))
        self.assertGreaterEqual(probabilities[0], probabilities[1])
        self.assertGreaterEqual(probabilities[1], probabilities[2])

    def test_metrics_residual_sign_is_actual_minus_prediction(self):
        rows = pd.DataFrame({
            "slate_date": ["2026-10-10", "2026-10-11"],
            "game_id": [1, 2], "player_id": [7, 7], "line": [1.5, 1.5],
            "realized_over": [1, 0], "p_production": [0.8, 0.2],
            "lambda_production": [1.0, 2.0], "actual_sog": [2, 1],
        })
        result = metrics(rows)
        self.assertAlmostEqual(result["mean_residual_actual_minus_predicted"], 0.0)
        self.assertAlmostEqual(result["count_mae"], 1.0)
        self.assertEqual(result["over_selections"], 1)
        self.assertEqual(result["under_selections"], 1)


if __name__ == "__main__":
    unittest.main()

import unittest

import numpy as np

from backend.nhl.scripts.analyze_nhl_puck_line_goal_margin_distribution import (
    CLASSES, class_from_margin, cls, poisson_margin, score,
)


class GoalMarginDistributionTests(unittest.TestCase):
    def test_margin_class_contract(self):
        self.assertEqual([class_from_margin(x) for x in (-3, -2, -1, 1, 2, 3)], [0, 0, 1, 1, 2, 2])
        self.assertEqual(CLASSES, ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME", "HOME_BY_2_PLUS"])

    def test_poisson_grid_produces_normalized_three_class_distribution(self):
        margin = poisson_margin(3.1, 2.9)
        classes = cls(margin)
        self.assertAlmostEqual(float(margin.sum()), 1.0)
        self.assertAlmostEqual(float(classes.sum()), 1.0)
        self.assertEqual(len(classes), 3)
        self.assertGreater(classes[1], 0)

    def test_score_returns_proper_scores_and_actual_rows_predicted_columns(self):
        result = score(np.array([0, 1, 2]), np.eye(3))
        self.assertEqual(result["accuracy"], 1.0)
        self.assertEqual(result["balanced_accuracy"], 1.0)
        self.assertEqual(result["confusion_actual_rows_predicted_columns"], [[1, 0, 0], [0, 1, 0], [0, 0, 1]])
        self.assertAlmostEqual(result["multiclass_brier_sum"], 0.0)


if __name__ == "__main__":
    unittest.main()

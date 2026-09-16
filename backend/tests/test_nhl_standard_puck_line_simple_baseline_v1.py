import unittest

import numpy as np

from backend.nhl.scripts.build_nhl_standard_puck_line_simple_baseline_v1 import (
    CLASSES,
    class_from_margin,
    cover_probabilities,
    mechanical_probabilities,
    multiclass_metrics,
)


class TestNhlStandardPuckLineSimpleBaselineV1(unittest.TestCase):
    def test_margin_classes_are_exhaustive_for_nhl_final_margins(self) -> None:
        expected = {
            -5: "AWAY_BY_2_PLUS", -2: "AWAY_BY_2_PLUS",
            -1: "ONE_GOAL_GAME", 1: "ONE_GOAL_GAME",
            2: "HOME_BY_2_PLUS", 7: "HOME_BY_2_PLUS",
        }
        self.assertEqual({margin: class_from_margin(margin) for margin in expected}, expected)

    def test_zero_margin_is_invalid_for_certified_nhl_final(self) -> None:
        with self.assertRaises(ValueError):
            class_from_margin(0)

    def test_cover_probabilities_are_coherent_and_push_free(self) -> None:
        probs = np.array([[0.25, 0.40, 0.35], [0.10, 0.50, 0.40]])
        covers = cover_probabilities(probs)
        np.testing.assert_allclose(covers["home_minus_1_5_probability"] + covers["away_plus_1_5_probability"], 1.0)
        np.testing.assert_allclose(covers["away_minus_1_5_probability"] + covers["home_plus_1_5_probability"], 1.0)
        np.testing.assert_allclose(covers["home_minus_1_5_probability"], probs[:, 2])
        np.testing.assert_allclose(covers["away_minus_1_5_probability"], probs[:, 0])

    def test_mechanical_v2_benchmark_is_symmetric_and_normalized(self) -> None:
        probs = mechanical_probabilities(np.array([0.2, 0.5, 0.8]))
        np.testing.assert_allclose(probs.sum(axis=1), 1.0)
        np.testing.assert_allclose(probs[0], probs[2][::-1])
        np.testing.assert_allclose(probs[1], [0.25, 0.50, 0.25])

    def test_multiclass_metrics_reward_exact_prediction(self) -> None:
        y = np.array([0, 1, 2])
        probs = np.eye(3)
        metrics = multiclass_metrics(y, probs)
        self.assertAlmostEqual(metrics["multiclass_log_loss"], 0.0)
        self.assertAlmostEqual(metrics["multiclass_brier_score"], 0.0)
        self.assertAlmostEqual(metrics["ranked_probability_score"], 0.0)
        self.assertEqual(metrics["class_accuracy"], 1.0)

    def test_class_order_is_ordinal(self) -> None:
        self.assertEqual(CLASSES, ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME", "HOME_BY_2_PLUS"])


if __name__ == "__main__":
    unittest.main()

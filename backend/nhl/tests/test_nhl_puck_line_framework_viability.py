import unittest

import pandas as pd

from backend.nhl.scripts.analyze_nhl_puck_line_framework_viability import CLASSES, score


class PuckLineFrameworkViabilityTests(unittest.TestCase):
    def test_multiclass_score_uses_canonical_probability_order(self):
        frame = pd.DataFrame({
            "actual_margin_class": ["AWAY_BY_2_PLUS", "HOME_BY_2_PLUS"],
            "predicted_v1": ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME"],
            "away_by_2_plus_probability_v1": [.8, .1],
            "one_goal_game_probability_v1": [.1, .8],
            "home_by_2_plus_probability_v1": [.1, .1],
        })
        result = score(frame, "v1")
        self.assertEqual(CLASSES, ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME", "HOME_BY_2_PLUS"])
        self.assertEqual(result["correct"], 1)
        self.assertAlmostEqual(result["multiclass_log_loss"], -(__import__("math").log(.8)+__import__("math").log(.1))/2)
        self.assertEqual(result["confusion_actual_rows_predicted_columns"]["HOME_BY_2_PLUS"]["ONE_GOAL_GAME"], 1)


if __name__ == "__main__":
    unittest.main()

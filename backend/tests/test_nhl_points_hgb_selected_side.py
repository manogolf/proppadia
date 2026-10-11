import unittest

import pandas as pd

from backend.nhl.performance_summary import render_markdown
from backend.nhl.scripts.grade_nhl_points_hgb_shadow import grade


class PointsHgbSelectedSideTests(unittest.TestCase):
    def setUp(self):
        self.predictions = pd.DataFrame({
            "game_id": [1, 2, 3, 4], "player_id": [11, 22, 33, 44],
            "expected_points": [0.2, 1.3, 2.1, 0.4],
            "prob_over_0_5": [0.5, 0.49, 0.8, 0.1],
            "prob_over_1_5": [0.1, 0.5, 0.9, 0.2],
            "prob_over_2_5": [0.1, 0.2, 0.5, 0.3],
        })
        self.outcomes = pd.DataFrame({
            "game_id": [1, 2, 3, 4], "player_id": [11, 22, 33, 44],
            "official_points": [0, 2, 3, None],
            "participation_state": ["PARTICIPATED", "PARTICIPATED", "PARTICIPATED", "UNRESOLVED"],
            "official_final": [True, True, True, True],
        })

    def test_selected_sides_lines_unresolved_and_reconciliation(self):
        result = grade(self.predictions, self.outcomes)
        record = result["selected_side_record"]
        self.assertEqual(record["settled"], 9)
        self.assertEqual(record["unresolved"], 1)
        self.assertEqual(record["pushes"], 0)
        self.assertEqual(record["by_line"]["0.5"]["wins"], 1)
        self.assertEqual(record["by_line"]["1.5"]["wins"], 3)
        self.assertEqual(record["by_line"]["2.5"]["wins"], 3)
        self.assertEqual(sum(x["wins"] + x["losses"] for x in record["by_line"].values()), 9)
        self.assertEqual(sum(x["settled"] for x in record["by_selected_side"].values()), 9)
        self.assertEqual(record["by_line"]["0.5"]["predicted_over_count"], 2)

    def test_half_point_lines_have_binary_realized_sides(self):
        result = grade(self.predictions, self.outcomes)["selected_side_record"]
        self.assertEqual(result["by_line"]["0.5"]["realized_over_count"], 2)
        self.assertEqual(result["by_line"]["1.5"]["realized_over_count"], 2)
        self.assertEqual(result["by_line"]["2.5"]["realized_over_count"], 1)

    def test_probability_metrics_and_summary_serialization(self):
        result = grade(self.predictions, self.outcomes)
        self.assertAlmostEqual(result["thresholds"]["prob_over_0_5"]["brier"], 0.18336666666666666)
        summary = {"slate_date": "2026-10-10", "game_count": 1,
                   "canonical_phase": "REGULAR_SEASON", "reconciliation_status": "REUSED",
                   "official_outcomes_status": "FINAL", "models": {
                       "points": {"reference": {"overall": {"n": 1}},
                                  "authority_context": {
                                      "production": {"model": "NHL_POINTS_COUNT_HGB_V1"},
                                      "incumbent_shadow": {"model": "phoenix_v2", "status": "BOUND"},
                                      "hgb_evaluation": {
                                          "role": "PRODUCTION", "metrics": result,
                                          "phoenix_same_capture_selected_side_record": record_for_render(),
                                          "selected_side_comparison": {
                                              "common_player_game_identities": 3,
                                              "common_settled_line_predictions": 9,
                                              "side_disagreements": 2,
                                              "hgb_wins_on_disagreement_rows": 1,
                                              "phoenix_wins_on_disagreement_rows": 1,
                                          }}}},
                       "moneyline": {"reference": {"correct": 0, "incorrect": 0, "graded": 0, "unresolved": 0}},
                       "puck_line": {"reference": {"correct": 0, "incorrect": 0, "graded": 0, "unresolved": 0}},
                       "sog": {}, "saves": {}}}
        markdown = render_markdown(summary)
        self.assertIn("Same-capture selected-side record", markdown)
        self.assertIn("HGB 7-2", markdown)
        self.assertIn("Phoenix 5-4", markdown)
        self.assertIn("Line 0.5", markdown)


def record_for_render():
    return {"wins": 5, "losses": 4, "win_rate": 5 / 9,
            "by_line": {"0.5": {"wins": 2, "losses": 1},
                        "1.5": {"wins": 2, "losses": 1},
                        "2.5": {"wins": 1, "losses": 2}}}


if __name__ == "__main__":
    unittest.main()

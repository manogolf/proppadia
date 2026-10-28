from __future__ import annotations

import unittest
from unittest import mock

import pandas as pd

from backend.nhl import performance_summary, two_sided_lines


def _rows() -> pd.DataFrame:
    return pd.DataFrame([
        {"game_id": 1, "player_id": 10, "line": 1.5, "selected_side": "OVER", "grading_state": "WIN"},
        {"game_id": 1, "player_id": 10, "line": 3.5, "selected_side": "UNDER", "grading_state": "WIN"},
        {"game_id": 1, "player_id": 11, "line": 1.5, "selected_side": "UNDER", "grading_state": "LOSS"},
        {"game_id": 1, "player_id": 11, "line": 3.5, "selected_side": "UNDER", "grading_state": "WIN"},
        {"game_id": 1, "player_id": 12, "line": 1.5, "selected_side": "UNDER", "grading_state": "NO_APPEARANCE_UNGRADED"},
    ])


class TwoSidedLevelTest(unittest.TestCase):
    def test_two_sided_is_a_subset_of_all_predictions(self):
        result = two_sided_lines.level(_rows(), {(1, 10, 1.5), (1, 11, 1.5)})
        self.assertEqual(result["all"], {"wins": 3, "losses": 1, "settled": 4, "win_rate": 0.75})
        self.assertEqual(result["two_sided"], {"wins": 1, "losses": 1, "settled": 2, "win_rate": 0.5})
        self.assertEqual(result["by_line"]["3.5"]["two_sided"]["settled"], 0)
        self.assertEqual(result["by_line"]["1.5"]["by_side"]["UNDER"]["two_sided"]["losses"], 1)
        self.assertFalse(result["affects_grading_denominator"])

    def test_render_shows_both_levels_and_unavailable_reason(self):
        text = "\n".join(two_sided_lines.render(
            two_sided_lines.level(_rows(), {(1, 10, 1.5)}), "SOG production"))
        self.assertIn("| 1.5 | All | 1-1 (50.0%) | 1-0 (100.0%) | 1 |", text)
        self.assertIn("**3-1 (75.0%)**", text)
        missing = "\n".join(two_sided_lines.render(two_sided_lines.unavailable("NO_ODDS"), "Points"))
        self.assertIn("UNAVAILABLE (NO_ODDS)", missing)

    def test_same_book_must_quote_both_sides(self):
        candidate = {"slate_date": "2026-10-27", "market_key": "shots_on_goal", "model_role": "PRODUCTION"}
        quotes = [
            # Book A quotes both sides at 2.5: two-sided.
            {"market_key": "shots_on_goal", "game_id": "1", "player_id": "10", "market_line": "2.5",
             "sportsbook_key": "a", "quote_side": "OVER"},
            {"market_key": "shots_on_goal", "game_id": "1", "player_id": "10", "market_line": "2.5",
             "sportsbook_key": "a", "quote_side": "UNDER"},
            # Over from one book and Under from another: not two-sided.
            {"market_key": "shots_on_goal", "game_id": "1", "player_id": "11", "market_line": "3.5",
             "sportsbook_key": "a", "quote_side": "OVER"},
            {"market_key": "shots_on_goal", "game_id": "1", "player_id": "11", "market_line": "3.5",
             "sportsbook_key": "b", "quote_side": "UNDER"},
        ]
        export = two_sided_lines.candidate_export
        with mock.patch.object(export, "load_candidates", return_value=([candidate], [])), \
                mock.patch.object(export, "add_captured_sog_arms", side_effect=lambda rows, *_: rows), \
                mock.patch.object(export, "scope_regular_season_pregame", side_effect=lambda rows: (rows, {})), \
                mock.patch.object(export, "deduplicate_candidates", side_effect=lambda rows: rows), \
                mock.patch.object(export, "load_retained_quotes", return_value=(quotes, [])) as load, \
                mock.patch.object(export, "deduplicate_quotes", side_effect=lambda rows: rows):
            evidence = two_sided_lines.load_evidence("unused", "2026-10-27")
        self.assertTrue(load.call_args.kwargs["price_both_sides"])
        self.assertEqual(evidence["keys"]["sog"], {(1, 10, 2.5)})
        # No production Points candidates: no evidence, not zero two-sided lines.
        self.assertIsNone(evidence["keys"]["points"])

    def test_two_sided_level_does_not_change_grade_projection(self):
        with_level = {"models": {"sog": {"reference": {"overall": {"wins": 1},
                                                       "two_sided_lines": {"status": "AVAILABLE"}}}}}
        without = {"models": {"sog": {"reference": {"overall": {"wins": 1}}}}}
        self.assertEqual(performance_summary._grade_projection(with_level),
                         performance_summary._grade_projection(without))


if __name__ == "__main__":
    unittest.main()

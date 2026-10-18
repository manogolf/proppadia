import unittest

from backend.nhl.scripts.evaluate_nhl_sog_paired_blends_full_window import (
    paired_probability_delta,
    qualified_quote_books,
)


class PairedProbabilityDeltaTests(unittest.TestCase):
    def test_delta_uses_identical_rows_and_reports_side_disagreements(self):
        rows = [
            {
                "actual_sog": "3", "market_line": "2.5",
                "production_d10_probability_over": "0.6",
                "blend_25_75_probability_over": "0.7",
                "production_d10_selected_side": "OVER",
                "blend_25_75_selected_side": "OVER",
            },
            {
                "actual_sog": "2", "market_line": "2.5",
                "production_d10_probability_over": "0.4",
                "blend_25_75_probability_over": "0.6",
                "production_d10_selected_side": "UNDER",
                "blend_25_75_selected_side": "OVER",
            },
        ]

        result = paired_probability_delta(rows, "blend_25_75")

        self.assertAlmostEqual(result["brier_delta_vs_production"], 0.065)
        self.assertEqual(result["brier_improved_rows"], 1)
        self.assertEqual(result["brier_worsened_rows"], 1)
        self.assertEqual(result["selected_side_disagreements"], 1)

    def test_empty_slice_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "at least one row"):
            paired_probability_delta([], "blend_25_75")

    def test_quote_books_require_selected_side_and_full_pregame_chronology(self):
        labels = ("production_d10", "blend_25_75", "blend_50_50")
        paired = [{
            **{f"{label}_candidate_key": f"{label}-candidate" for label in labels},
            **{f"{label}_selected_side": "OVER" for label in labels},
            **{f"{label}_prediction_timestamp_utc": "2026-10-10T20:00:00Z" for label in labels},
            "scheduled_start_utc": "2026-10-10T23:00:00Z",
        }]
        quotes = [{
            "candidate_key": "production_d10-candidate",
            "sportsbook_key": "betonlineag",
            "quote_qualification_status": "QUALIFIED_SOURCE_AND_OBSERVATION_PRECEDE_PREDICTION",
            "quote_side": "OVER",
            "source_quote_timestamp_utc": "2026-10-10T19:00:00Z",
            "observation_timestamp_utc": "2026-10-10T19:30:00Z",
        }]

        self.assertEqual(qualified_quote_books(paired, quotes), {"production_d10-candidate": {"betonlineag"}})
        quotes[0]["observation_timestamp_utc"] = "2026-10-10T20:01:00Z"
        with self.assertRaisesRegex(ValueError, "chronology failed"):
            qualified_quote_books(paired, quotes)


if __name__ == "__main__":
    unittest.main()

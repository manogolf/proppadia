import unittest

import pandas as pd

from backend.mlb.scripts import audit_mlb_alternative_book_joint_strength_transfer_v1 as audit


class AlternativeBookTransferAuditTest(unittest.TestCase):
    def test_primary_selection_is_coverage_first_and_excludes_non_sportsbooks(self):
        frame = pd.DataFrame([
            {"normalized_source_name": "high_roi_not_known_here", "source_type": "SPORTSBOOK",
             "joint_strength_coverage": 9, "joint_post_prediction_coverage": 9, "joint_two_sided_coverage": 9,
             "longest_consecutive_game_date_run": 5, "modal_half_hour_capture_share": .2, "full_471_game_coverage": 59},
            {"normalized_source_name": "coverage_winner", "source_type": "SPORTSBOOK",
             "joint_strength_coverage": 10, "joint_post_prediction_coverage": 8, "joint_two_sided_coverage": 10,
             "longest_consecutive_game_date_run": 4, "modal_half_hour_capture_share": .1, "full_471_game_coverage": 58},
            {"normalized_source_name": "exchange", "source_type": "EXCHANGE_OR_PREDICTION_MARKET",
             "joint_strength_coverage": 13, "joint_post_prediction_coverage": 13, "joint_two_sided_coverage": 13,
             "longest_consecutive_game_date_run": 6, "modal_half_hour_capture_share": .9, "full_471_game_coverage": 74},
            {"normalized_source_name": "unknown", "source_type": "AMBIGUOUS_IDENTITY",
             "joint_strength_coverage": 14, "joint_post_prediction_coverage": 14, "joint_two_sided_coverage": 14,
             "longest_consecutive_game_date_run": 6, "modal_half_hour_capture_share": .9, "full_471_game_coverage": 75},
        ])
        record, names = audit.select_primary(frame)
        self.assertEqual(names, ["coverage_winner"])
        self.assertEqual(record.loc[record.normalized_source_name.eq("exchange"),
                                    "selection_or_exclusion_reason"].iloc[0], "NOT_A_SPORTSBOOK")

    def test_effectively_tied_sources_are_co_primary(self):
        keys = {"source_type": "SPORTSBOOK", "joint_strength_coverage": 9,
                "joint_post_prediction_coverage": 8, "joint_two_sided_coverage": 9,
                "longest_consecutive_game_date_run": 5, "modal_half_hour_capture_share": .2,
                "full_471_game_coverage": 59}
        _, names = audit.select_primary(pd.DataFrame([
            {"normalized_source_name": "a", **keys}, {"normalized_source_name": "b", **keys}]))
        self.assertEqual(names, ["a", "b"])

    def test_economics_uses_flat_stake_returns(self):
        rows = pd.DataFrame({"game_id": [1, 2], "game_date": ["2026-08-01", "2026-08-02"],
                             "evaluated_win": [1, 0], "evaluated_american_price": [-150, -120],
                             "evaluated_decimal_price": [1.6666666667, 1.8333333333],
                             "evaluated_paid_break_even_probability": [.6, .5454545455],
                             "evaluated_no_vig_market_probability": [.58, .54]})
        result = audit.economics(rows)
        self.assertAlmostEqual(result["net_units"], -.3333333333)
        self.assertAlmostEqual(result["captured_price_hypothetical_roi"], -.16666666665)
        self.assertAlmostEqual(result["expected_wins_from_no_vig_market_probability"], 1.12)

    def test_boundaries_are_frozen(self):
        self.assertEqual(audit.STRONG, .60)
        self.assertEqual(audit.ROBUSTNESS_MIN_JOINT, 38)
        self.assertEqual(len(audit.SNAPSHOTS), 10)


if __name__ == "__main__":
    unittest.main()

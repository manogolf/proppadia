import unittest

import pandas as pd

from backend.mlb.scripts import acquire_and_audit_mlb_oddsapi_historical_joint_strength_transfer_v1 as audit


class HistoricalJointTransferTest(unittest.TestCase):
    def test_frozen_book_list_and_cost_contract(self):
        selected, rationale = audit.bookmaker_frames()
        self.assertEqual(len(selected), 10)
        self.assertIn("pinnacle", set(selected.bookmaker_key))
        self.assertIn("betonlineag", set(selected.bookmaker_key))
        self.assertNotIn("fliff", set(selected.bookmaker_key))
        self.assertEqual(rationale.loc[rationale.bookmaker_key.eq("fliff"), "selected"].iloc[0], False)
        self.assertEqual(29 * audit.EXPECTED_COST_PER_REQUEST, 290)
        self.assertLessEqual(290, audit.MAX_CREDITS)

    def test_secret_is_removed(self):
        self.assertEqual(audit.safe_params({"apiKey": "secret", "markets": "h2h"}), {"markets": "h2h"})

    def test_economics(self):
        rows = pd.DataFrame({"game_id": [1, 2], "game_date": ["2026-08-01", "2026-08-02"],
                             "admission_class": ["VALID_PREGAME_PRICE", "VALID_PREGAME_PRICE"],
                             "evaluated_win": [1, 0], "selected_american_price": [-150, -110],
                             "selected_decimal_price": [1.666666666667, 1.909090909091]})
        result = audit.economics(rows)
        self.assertAlmostEqual(result["net_units"], -1/3)
        self.assertAlmostEqual(result["hypothetical_flat_risk_roi"], -1/6)

    def test_team_normalization(self):
        self.assertEqual(audit.norm_team("N.Y. Yankees"), "new york yankees")


if __name__ == "__main__": unittest.main()

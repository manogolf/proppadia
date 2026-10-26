import unittest

import numpy as np
import pandas as pd

from backend.nhl.scripts import analyze_nhl_odds_line_movement as a


class LineMovementTests(unittest.TestCase):
    def test_implied_probability_and_book_groups(self):
        np.testing.assert_allclose(a.implied(pd.Series([-150, 150, 100])), [0.6, 0.4, 0.5])
        self.assertEqual(a.book_group("kalshi"), "prediction_markets")
        self.assertEqual(a.book_group("betonlineag"), "betonline")
        self.assertEqual(a.book_group("draftkings"), "sportsbooks")
        self.assertEqual(a.norm_name("Tim Stützle"), "tim stutzle")
        self.assertEqual(a.PLACEHOLDER_PRICE, -10000)  # Novig posts -99900 for a side with no offer

    def test_earlier_offer_is_compared_with_the_games_last_snapshot(self):
        t0, t1 = pd.Timestamp("2026-10-20T14:00Z"), pd.Timestamp("2026-10-20T20:00Z")
        base = dict(slate="2026-10-20", game_id=1, bookmaker="draftkings", market="player_shots_on_goal",
                    player="a b", point=1.5, hold_sum=1.05, pt_hour=7, lane="SOG", group="sportsbooks")
        rows = [
            {**base, "side": "OVER", "snapshot": "s0", "obs_utc": t0, "hours_to_puck": 9.0, "implied": 0.50, "price": 100},
            {**base, "side": "OVER", "snapshot": "s1", "obs_utc": t1, "hours_to_puck": 3.0, "implied": 0.52, "price": -108},
            {**base, "side": "UNDER", "snapshot": "s0", "obs_utc": t0, "hours_to_puck": 9.0, "implied": 0.55, "price": -122},
        ]
        m, matched = a.pairs_vs_latest(pd.DataFrame(rows))
        self.assertEqual(len(m), 2)
        self.assertEqual(int(m.gone_later.sum()), 1)  # UNDER disappeared later
        self.assertAlmostEqual(float(matched.delta_pp.iloc[0]), 2.0)  # price got worse by 2 points
        self.assertEqual(str(matched.bucket.iloc[0]), "6-10h")


if __name__ == "__main__":
    unittest.main()

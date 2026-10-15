import unittest

from backend.nhl.scripts.analyze_nhl_points_market_loss_concentration import amer, logloss, metrics


class PointsMarketLossConcentrationTests(unittest.TestCase):
    def test_american_price_conversion(self):
        self.assertEqual(amer(0.5), -100.0)
        self.assertEqual(amer(0.25), 300.0)
        self.assertEqual(amer(0.75), -300.0)

    def test_log_loss_uses_over_outcome(self):
        self.assertAlmostEqual(logloss(0.8, 1), -__import__("math").log(0.8))

    def test_expected_and_observed_losses_are_separate(self):
        rows = [
            {"model_win": 1, "model_side": "OVER", "model_selected_p": .8, "model_p_over": .8, "over_outcome": 1,
             "model_logloss": logloss(.8, 1), "market_p_over": .7, "market_logloss": logloss(.7, 1)},
            {"model_win": 0, "model_side": "OVER", "model_selected_p": .6, "model_p_over": .6, "over_outcome": 0,
             "model_logloss": logloss(.6, 0), "market_p_over": .4, "market_logloss": logloss(.4, 0)},
        ]
        got = metrics(rows)
        self.assertEqual(got["losses"], 1)
        self.assertAlmostEqual(got["expected_losses"], .6)
        self.assertAlmostEqual(got["excess_losses"], .4)
        self.assertAlmostEqual(got["model_brier"], .2)


if __name__ == "__main__":
    unittest.main()

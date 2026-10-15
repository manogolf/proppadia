import unittest

from backend.nhl.scripts.analyze_nhl_points_consideration_funnel import enrich, implied, metrics


class PointsConsiderationFunnelTests(unittest.TestCase):
    def test_american_implied(self):
        self.assertAlmostEqual(implied(-110), 110 / 210)
        self.assertAlmostEqual(implied(150), .4)
        self.assertIsNone(implied(0))

    def test_selected_side_no_vig_gap_and_edge_are_distinct(self):
        row = enrich({"line": "0.5", "p_over": ".3", "price_over": "-110", "price_under": "-110",
                      "edge_over": str(.3 - 110/210), "edge_under": str(.7 - 110/210), "official_points": "0"})
        self.assertEqual(row["model_side"], "UNDER")
        self.assertAlmostEqual(row["market_selected_p"], .5)
        self.assertAlmostEqual(row["signed_selected_model_minus_no_vig_market_gap"], .2)
        self.assertAlmostEqual(row["selected_production_edge"], .7 - 110/210)
        self.assertNotAlmostEqual(row["selected_production_edge"], row["signed_selected_model_minus_no_vig_market_gap"])
        self.assertEqual(row["model_win"], 1)

    def test_metrics_expected_vs_observed(self):
        rows = [enrich({"line": ".5", "p_over": ".7", "price_over": "-110", "price_under": "-110",
                        "official_points": "1"})]
        scored = metrics(rows)
        self.assertEqual(scored["wins"], 1)
        self.assertAlmostEqual(scored["expected_losses"], .3)
        self.assertAlmostEqual(scored["excess_losses"], -.3)


if __name__ == "__main__":
    unittest.main()

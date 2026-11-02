"""Weekly plus-money price view: best prices per scope, settlement on both sides, readiness."""
import unittest

import pandas as pd

from backend.nhl.scripts import report_nhl_sog_points_price_view as view


def quote(book, side, decimal, player=7, line=0.5, market="player_points"):
    return {"market_key": market, "game_id": "2026020300", "player_id": str(player), "market_line": str(line),
            "quote_side": side, "sportsbook_key": book, "decimal_price": str(decimal)}


class PlusMoneyPriceViewTests(unittest.TestCase):
    def test_best_price_is_taken_per_scope_and_my_books_exclude_others(self):
        prices = view.best_prices([
            quote("draftkings", "UNDER", 2.30), quote("novig", "UNDER", 2.10), quote("pinnacle", "UNDER", 2.05),
            quote("betonlineag", "OVER", 1.70), quote("fanduel", "OVER", 1.75),
            quote("novig", "OVER", 1.9, market="h2h")])  # not a prop market
        under = prices[prices.quote_side == "UNDER"].set_index("price_scope")
        self.assertEqual(under.loc["all_books", "sportsbook_key"], "draftkings")
        self.assertEqual(under.loc["my_books", "sportsbook_key"], "novig")
        self.assertEqual(under.loc["my_books", "decimal_price"], 2.10)
        self.assertEqual(set(prices.lane), {"points"})

    def test_settlement_bands_and_readiness(self):
        rows = pd.DataFrame([{"slate_date": "2026-11-03", "game_id": 2026020300, "player_id": 7, "line": 0.5,
                              "p_over": 0.40, "selected_side": "UNDER", "grading_state": "WIN", "lane": "points"}])
        prices = view.best_prices([quote("novig", "UNDER", 2.10), quote("novig", "OVER", 1.80)])
        priced = view.weekly_priced(rows, prices)
        pick = priced[(priced.price_scope == "my_books") & (priced.relation == "MODEL_PICK")].iloc[0]
        self.assertEqual((pick.side, pick.american_price, pick.plus_band, pick.fair_band),
                         ("UNDER", 110, "+100 to +129", "-133 to -200"))
        self.assertAlmostEqual(pick.profit, 1.10)
        against = priced[(priced.price_scope == "my_books") & (priced.relation == "AGAINST_MODEL")].iloc[0]
        self.assertEqual((against.side, bool(against.won), against.profit), ("OVER", False, -1.0))
        ready = view.readiness(priced).set_index("market")
        self.assertEqual((ready.loc["points", "picks"], bool(ready.loc["points", "ready"])), (1, False))

    def test_points_slates_follow_the_production_identity(self):
        current = view.current_points_identity()
        summary = lambda identity: {"models": {"points": {"authority_context": {
            "production": {"fitted_model_identity_sha256": identity}}}}}
        days = view.lane_days({"2026-10-31": summary("old"), "2026-11-01": summary(current),
                               "2026-11-02": summary(current)}, "2026-11-01")
        self.assertEqual(days["points"], ["2026-11-01"])
        self.assertEqual(days["sog"], ["2026-10-31", "2026-11-01"])


if __name__ == "__main__":
    unittest.main()

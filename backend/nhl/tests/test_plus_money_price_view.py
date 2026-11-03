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
        prices = view.best_prices([quote("novig", "UNDER", 1.80), quote("novig", "OVER", 2.80)])
        priced = view.weekly_priced(rows, prices)
        pick = priced[(priced.price_scope == "my_books") & (priced.relation == "MODEL_PICK")].iloc[0]
        self.assertEqual((pick.side, pick.fair_american, pick.fair_band), ("UNDER", -150, "-133 to -200"))
        self.assertAlmostEqual(pick.profit, 0.80)
        # The model's underdog side: 40% is fair +150; the book's +180 is longer, so positive EV.
        dog = view.underdogs(priced[priced.price_scope == "my_books"]).iloc[0]
        self.assertEqual((dog.side, dog.fair_american, dog.fair_band, dog.american_price),
                         ("OVER", 150, "+130 to +169", 180))
        self.assertAlmostEqual(dog.model_ev, 0.12)
        self.assertEqual((bool(dog.won), dog.profit), (False, -1.0))
        ready = view.readiness(priced).set_index(["market", "fair_band"])
        self.assertEqual(ready.loc[("points", "+130 to +169"), "picks"], 1)
        self.assertEqual(ready.loc[("points", "+100 to +129"), "picks"], 0)
        self.assertEqual(view.lane_readiness(ready.reset_index())[1],
                         {"market": "points", "picks": 0, "needed": 100, "ready": False})
        self.assertEqual(int(ready.loc[("sog over 1.5", "+130 to +169"), "picks"]), 0)
        self.assertNotIn(("sog over 1.5", "+100 to +129"), ready.index)

    def test_sog_over_1_5_is_tracked_on_its_own(self):
        rows = pd.DataFrame([{"slate_date": "2026-11-03", "game_id": 2026020300, "player_id": 7, "line": 1.5,
                              "p_over": 0.45, "selected_side": "UNDER", "grading_state": "LOSS", "lane": "sog"}])
        prices = view.best_prices([quote("novig", "OVER", 2.40, line=1.5, market="shots_on_goal"),
                                   quote("novig", "UNDER", 1.60, line=1.5, market="shots_on_goal")])
        ready = view.readiness(view.weekly_priced(rows, prices)).set_index(["market", "fair_band"])
        # 45% is fair +122; the book's +140 is longer, so a positive-EV underdog Over 1.5.
        self.assertEqual(int(ready.loc[("sog over 1.5", "+120 to +129"), "picks"]), 1)
        self.assertEqual(int(ready.loc[("sog over 1.5", "+100 to +109"), "picks"]), 0)
        self.assertEqual(int(ready.loc[("sog", "+100 to +129"), "picks"]), 1)
        self.assertEqual([r["market"] for r in view.lane_readiness(ready.reset_index())],
                         ["sog", "points", "sog over 1.5"])

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

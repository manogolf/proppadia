"""8rain bet report: download parsing, de-duplication, fair-odds and EV bands, records."""
import unittest

import pandas as pd

from backend.nhl.scripts import report_nhl_8rain_bets as report

COLUMNS = ["Wager ID", "Event Date", "League", "Market", "Bet", "Book", "Grade", "$ W/L", "Amount", "Odds",
           "No-Vig", "Positive EV", "Closing Line Value", "CLV Venue %"]


def row(wager, bet, grade, pnl, odds, fair, ev, market="player_prop_1", clv=""):
    return dict(zip(COLUMNS, [wager, "11/01/2026 17:00:00", "NHL", market, bet, "Novig", grade, pnl, "10",
                              odds, fair, ev, clv, ""]))


class BetReportTests(unittest.TestCase):
    def setUp(self):
        raw = pd.DataFrame([
            row("1", "Under Dylan Cozens Points 0.5", "W", "11.7", "117", "101", "0.0796", clv="0.0093"),
            row("2", "Over Jake Sanderson Shots On Goal 1.5", "L", "-10", "-189.85", "-164", "-0.0516"),
            row("3", "Ottawa Senators", "W", "8", "-125", "-150", "0.10", market="h2h"),
            {**row("4", "Over Leo Carlsson Points 0.5", "W", "5.99", "-167", "-169", "0.0045"), "League": "NBA"},
        ])
        self.bets = report.parse_bets(pd.concat([raw, raw.iloc[[0]]]).drop_duplicates("Wager ID"))

    def test_parsing_and_bands(self):
        cozens = self.bets.set_index("wager_id").loc["1"]
        self.assertEqual((cozens.market, cozens.side, cozens.line, cozens.player),
                         ("points", "UNDER", 0.5, "Dylan Cozens"))
        self.assertEqual((cozens.fair_band, cozens.model_side, cozens.price_type, cozens.ev_band),
                         ("+100 to +129", "model underdog", "plus money", "5-10%"))
        self.assertAlmostEqual(cozens.edge, 100 / 201 - 100 / 217)
        sanderson = self.bets.set_index("wager_id").loc["2"]
        self.assertEqual((sanderson.market, sanderson.fair_band, sanderson.ev_band),
                         ("sog", "-133 to -200", "negative"))
        self.assertEqual(set(self.bets.market), {"points", "sog", "moneyline"})  # the NBA row is dropped

    def test_band_edges_follow_8rain(self):
        self.assertEqual([report.fair_band(a) for a in (-133, -200, -201, -500, -501, 100, 129, 130, 250)],
                         ["-133 to -200", "-133 to -200", "-200 to -300", "-300 to -500", "beyond -500",
                          "+100 to +129", "+100 to +129", "+130 to +169", "+250 or longer"])

    def test_record_and_report(self):
        rec = report.record(self.bets)
        self.assertEqual((rec["bets"], rec["W-L-P"], rec["P/L"]), (3, "2-1-0", 9.7))
        text, summary = report.build(self.bets.assign(prediction_price=float("nan")), ["a.csv"])
        self.assertIn("## Plus-money prices taken", text)
        self.assertEqual(summary["clv_bets"], 1)


if __name__ == "__main__":
    unittest.main()

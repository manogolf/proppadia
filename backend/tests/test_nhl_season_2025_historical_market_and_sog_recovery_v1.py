import json
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.analysis_package_guard import verify_manifest


ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "artifacts/analysis/model_development/nhl_season_2025_historical_market_and_sog_recovery_v1/2026-09-15"


class HistoricalMarketAndSogRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.decision = json.loads((PACKAGE / "decision.json").read_text())
        cls.moneyline = pd.read_csv(PACKAGE / "normalized_moneyline_history.csv")
        cls.puck = pd.read_csv(PACKAGE / "graded_standard_puck_line_history.csv")
        cls.sog = pd.read_parquet(PACKAGE / "normalized_sog_history.parquet")
        cls.requests = pd.read_csv(PACKAGE / "persistent_request_ledger.csv")

    def test_manifest_and_credit_arithmetic(self):
        verify_manifest(PACKAGE)
        self.assertEqual(self.decision["NHL_HISTORICAL_TASK_CREDITS_USED"], 28030)
        self.assertEqual(83862 - 28030 - 244, self.decision["ODDS_API_ENDING_REMAINING_CREDITS"])
        self.assertEqual(self.decision["LIVE_OPERATIONS_RESERVE_AFTER_TASK"], "PRESERVED")

    def test_no_repeated_charged_requests(self):
        charged = self.requests[pd.to_numeric(self.requests.x_requests_last, errors="coerce").fillna(0).gt(0)]
        self.assertFalse(charged.request_id.duplicated().any())
        self.assertEqual(int(pd.to_numeric(charged.x_requests_last).sum()), 28030)

    def test_moneyline_no_vig_and_coverage(self):
        valid = self.moneyline[self.moneyline.qualification_status.eq("VALID_STRICT_PRESTART")]
        self.assertEqual(valid.game_id.nunique(), 1312)
        pairs = valid.dropna(subset=["no_vig_probability"]).groupby(["game_id", "bookmaker_key"]).no_vig_probability.sum()
        self.assertTrue(((pairs - 1).abs() < 1e-12).all())

    def test_puck_line_orientation(self):
        self.assertEqual(self.puck.game_id.nunique(), 899)
        self.assertTrue(self.puck.loc[self.puck.side_orientation.eq("HOME"), "point"].eq(-1.5).all())
        self.assertTrue(self.puck.loc[self.puck.side_orientation.eq("AWAY"), "point"].eq(1.5).all())
        self.assertTrue(self.puck.financial_label.eq("HYPOTHETICAL_NO_WAGER_PLACED").all())

    def test_sog_coverage_identity_and_timing(self):
        valid = self.sog[self.sog.qualification_status.eq("VALID_STRICT_PRESTART") & self.sog.two_sided_line]
        self.assertEqual(valid.game_id.nunique(), 1312)
        names = valid[["player_name_normalized", "player_id"]].drop_duplicates()
        self.assertGreater(names.player_id.notna().mean(), .99)
        self.assertTrue(valid.strictly_pregame.all())


if __name__ == "__main__":
    unittest.main()

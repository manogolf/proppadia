import unittest

import pandas as pd

from backend.nhl.scripts.characterize_nhl_sog_shot_rate import auc_score, select_rate


class ShotRateCharacterizationTests(unittest.TestCase):
    def test_production_fallback_order(self):
        frame = pd.DataFrame({
            "d10_sog_per60": [2.0, None, None, None],
            "d20_sog_per60": [3.0, 4.0, None, None],
            "d5_sog_per60": [5.0, 6.0, 7.0, None],
        })
        rate, source = select_rate(frame)
        self.assertEqual(rate.iloc[:3].tolist(), [2.0, 4.0, 7.0])
        self.assertEqual(source.iloc[:3].tolist(), ["d10_sog_per60", "d20_sog_per60", "d5_sog_per60"])
        self.assertTrue(pd.isna(rate.iloc[3]))
        self.assertEqual(source.iloc[3], "UNAVAILABLE")

    def test_auc_uses_rank_statistic_and_handles_single_class(self):
        self.assertEqual(auc_score(pd.Series([.1, .9]), pd.Series([0, 1])), 1.0)
        self.assertTrue(pd.isna(auc_score(pd.Series([.1, .9]), pd.Series([1, 1]))))


if __name__ == "__main__":
    unittest.main()

import unittest

import numpy as np
import pandas as pd

from backend.nhl.scripts.characterize_nhl_sog_toi_exposure import coalesce_exposure


class TestExposureCharacterizationHelpers(unittest.TestCase):
    def test_exposure_source_fallback_order_matches_scorer(self):
        frame = pd.DataFrame({
            "d10_toi_min_avg": [12.0, np.nan, np.inf, np.nan],
            "d20_toi_min_avg": [20.0, 14.0, np.nan, np.nan],
            "d5_toi_min_avg": [30.0, 30.0, 15.0, np.nan],
            "szn_toi_per_game_5on5": [40.0, 40.0, 40.0, 18.0],
            "szn_toi_per_game_pp": [3.0, 3.0, 3.0, 2.0],
        })
        values, sources = coalesce_exposure(frame)
        self.assertEqual(values.tolist(), [12.0, 14.0, 15.0, 20.0])
        self.assertEqual(sources.tolist(), ["d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg", "SEASON_TOI_5V5_PLUS_PP"])

    def test_exposure_unavailable_when_all_fallbacks_missing(self):
        frame = pd.DataFrame({"d10_toi_min_avg": [np.nan]})
        values, sources = coalesce_exposure(frame)
        self.assertTrue(pd.isna(values.iloc[0]))
        self.assertEqual(sources.iloc[0], "UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()

import math
import unittest

import numpy as np
import pandas as pd

from backend.nhl.scripts.evaluate_nhl_2025_v2_market_and_sog_cross_market_v1 import (
    GAP_LABELS,
    add_game_states,
    invert_poisson_tail,
    poisson_tail,
    score_prepared_features,
)


class TestNhl2025CrossMarketEvaluationV1(unittest.TestCase):
    def test_poisson_inversion_reproduces_probability(self) -> None:
        for line in (0.5, 1.5, 2.0, 2.5, 3.5, 4.5):
            for lam in (0.2, 1.0, 2.75, 6.0):
                probability = poisson_tail(lam, line)
                rebuilt = invert_poisson_tail(probability, line)
                self.assertAlmostEqual(rebuilt, lam, places=11)

    def test_strict_prior_feature_fallback_order(self) -> None:
        row = {
            "d5_sog_per60": 9.0, "d10_sog_per60": np.nan, "d20_sog_per60": 6.0,
            "d5_toi_min_avg": 12.0, "d10_toi_min_avg": np.nan, "d20_toi_min_avg": np.nan,
            "szn_toi_per_game_5on5": 10.0, "szn_toi_per_game_pp": 2.0,
            "season_5on5_icetime_per_game": np.nan, "season_5on4_icetime_per_game": np.nan,
        }
        scored = score_prepared_features(pd.DataFrame([row])).iloc[0]
        self.assertEqual(scored.selected_rate_source, "d20_sog_per60")
        self.assertEqual(scored.selected_toi_source, "d5_toi_min_avg")
        self.assertAlmostEqual(scored.expected_sog, 1.2)
        self.assertEqual(scored.missingness_fallback_state, "FALLBACK_USED")

    def test_missing_input_defaults_lambda_to_zero(self) -> None:
        cols = [
            "d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "d5_toi_min_avg",
            "d10_toi_min_avg", "d20_toi_min_avg", "szn_toi_per_game_5on5",
            "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game",
        ]
        scored = score_prepared_features(pd.DataFrame([{c: np.nan for c in cols}])).iloc[0]
        self.assertEqual(scored.expected_sog, 0.0)
        self.assertEqual(scored.selected_rate_source, "MISSING")

    def test_gap_bands_are_frozen_absolute_bands(self) -> None:
        frame = pd.DataFrame({
            "game_date": ["2025-10-01"] * 5,
            "v2_home_win_probability": [0.50, 0.525, 0.55, 0.575, 0.60],
            "market_median_no_vig_home_probability": [0.50] * 5,
        })
        result = add_game_states(frame)
        self.assertEqual(result.gap_band.tolist(), GAP_LABELS)

    def test_poisson_tail_half_line_semantics(self) -> None:
        lam = 2.0
        expected = 1.0 - math.exp(-lam) * (1 + lam)
        self.assertAlmostEqual(poisson_tail(lam, 1.5), expected)


if __name__ == "__main__":
    unittest.main()

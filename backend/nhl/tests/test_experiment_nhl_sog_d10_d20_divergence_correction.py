import unittest
from math import exp

import pandas as pd

from backend.nhl.scripts.experiment_nhl_sog_d10_d20_divergence_correction import prepare, poisson_over


class DivergenceCorrectionExperimentTests(unittest.TestCase):
    def test_only_frozen_material_rules_change_rates(self):
        frame = pd.DataFrame({
            "game_date": ["2026-01-01"] * 3,
            "game_id": [1, 1, 1],
            "player_id": [10, 11, 12],
            "d10": [8.0, 8.6, 7.4],
            "d20": [8.0, 8.0, 8.0],
            "lam": [2.0, 2.15, 1.85],
            "actual": [2.0, 2.0, 2.0],
            "selected_toi_minutes": [15.0] * 3,
        })
        got = prepare(frame, "game_date")
        self.assertEqual(got.loc[0, "lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1"], 2.0)
        self.assertEqual(got.loc[0, "lambda_HIGH_D10_ONLY_SHRINK_V1"], 2.0)
        self.assertAlmostEqual(got.loc[1, "lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1"], 2.075)
        self.assertAlmostEqual(got.loc[1, "lambda_HIGH_D10_ONLY_SHRINK_V1"], 2.075)
        self.assertAlmostEqual(got.loc[2, "lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1"], 1.925)
        self.assertAlmostEqual(got.loc[2, "lambda_HIGH_D10_ONLY_SHRINK_V1"], 1.85)

    def test_poisson_threshold_probability(self):
        self.assertAlmostEqual(float(poisson_over(pd.Series([2.0]), 1.5).iloc[0]), 1 - 3 * exp(-2), places=6)


if __name__ == "__main__":
    unittest.main()

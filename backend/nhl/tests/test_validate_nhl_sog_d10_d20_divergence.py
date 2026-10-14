import unittest

import pandas as pd

from backend.nhl.scripts.validate_nhl_sog_d10_d20_divergence import MATERIAL, count_stats


class SogD10D20ValidationTests(unittest.TestCase):
    def test_prior_material_threshold_and_residual_sign(self):
        self.assertEqual(MATERIAL, 0.5)
        rows = pd.DataFrame({
            "d10": [6.0, 4.0], "d20": [5.0, 5.0], "lam": [2.0, 2.0],
            "actual": [1.0, 3.0],
        })
        rows["divergence"] = rows.d10 - rows.d20
        rows["residual"] = rows.actual - rows.lam
        result = count_stats(rows)
        self.assertAlmostEqual(result["mean_residual"], 0.0)
        self.assertAlmostEqual(result["mean_divergence"], 0.0)
        self.assertAlmostEqual(result["mae"], 1.0)


if __name__ == "__main__":
    unittest.main()

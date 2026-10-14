import unittest

import numpy as np
import pandas as pd

from backend.nhl.scripts.characterize_nhl_sog_extreme_rate_after_divergence import (
    assign_groups,
    metrics,
    tail,
)


class ExtremeRateAfterDivergenceTests(unittest.TestCase):
    def test_poisson_threshold_tail(self):
        self.assertAlmostEqual(tail(0.0, 2), 0.0)
        self.assertAlmostEqual(tail(1.0, 2), 1 - 2 / np.e)

    def test_count_metrics_and_residual_sign(self):
        frame = pd.DataFrame({"actual_sog": [3.0, 0.0], "production_lambda": [2.0, 1.0]})
        result = metrics(frame, "fixture")
        self.assertEqual(result["n"], 2)
        self.assertEqual(result["mean_residual"], 0.0)
        self.assertEqual(result["mae"], 1.0)

    def test_frozen_cutpoints_assign_nested_groups(self):
        frame = pd.DataFrame({"d10": [1.0, 2.0, 3.0, 4.0, 5.0], "selected_toi_minutes": [10]*5,
                              "production_lambda": [1, 2, 3, 4, 5]})
        cuts = pd.DataFrame([{"population": "historical", "group": "bottom_5pct", "cutpoint": 1.0},
                             {"population": "historical", "group": "bottom_decile", "cutpoint": 2.0},
                             {"population": "historical", "group": "top_decile", "cutpoint": 4.0},
                             {"population": "historical", "group": "top_5pct", "cutpoint": 5.0}])
        result = assign_groups(frame, cuts, "historical")
        self.assertEqual(result.loc[0, "rate_group"], "bottom_5pct")
        self.assertEqual(result.loc[1, "rate_group"], "bottom_decile")
        self.assertEqual(result.loc[3, "rate_group"], "top_decile")
        self.assertEqual(result.loc[4, "rate_group"], "top_5pct")


if __name__ == "__main__":
    unittest.main()

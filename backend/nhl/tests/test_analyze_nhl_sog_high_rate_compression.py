import unittest

import pandas as pd

from backend.nhl.scripts.analyze_nhl_sog_high_rate_compression import apply_rules, poisson_over


class HighRateCompressionRulesTest(unittest.TestCase):
    def test_soft_and_cap_only_apply_after_control_above_threshold(self):
        d10 = pd.Series([10.0, 10.0, 8.0, 10.0])
        d20 = pd.Series([10.0, 8.0, 8.0, 10.0])
        toi = pd.Series([30.0] * 4)
        production = pd.Series([5.0] * 4)
        got = apply_rules(d10, d20, production, toi)

        self.assertEqual(got.divergence_lambda.iloc[0], 5.0)  # unchanged D10
        self.assertEqual(got.divergence_lambda.iloc[1], 4.5)  # midpoint control
        self.assertEqual(got.soft_lambda.iloc[0], 4.75)       # halfway from 5 to 4.5? rate scale
        self.assertEqual(got.hard_cap_lambda.iloc[0], 4.5)
        self.assertEqual(got.soft_lambda.iloc[1], got.divergence_lambda.iloc[1])
        self.assertEqual(got.hard_cap_lambda.iloc[1], got.divergence_lambda.iloc[1])
        self.assertFalse(got.high_rate_compression_active.iloc[1])
        self.assertFalse(got.high_rate_compression_active.iloc[2])
        self.assertTrue(got.divergence_correction_active.iloc[1])

    def test_poisson_over_known_probabilities_and_invalid_rate(self):
        self.assertAlmostEqual(poisson_over(0.0, 2), 0.0)
        self.assertAlmostEqual(poisson_over(1.0, 1), 1.0 - 1.0 / 2.718281828459045)
        self.assertTrue(pd.isna(poisson_over(float("nan"), 2)))


if __name__ == "__main__":
    unittest.main()

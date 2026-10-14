import unittest

import pandas as pd

from backend.nhl.scripts.analyze_nhl_sog_expanded_structural_validation_v1 import rule_rates


class RuleRatesTests(unittest.TestCase):
    def test_divergence_boundary_is_strict(self):
        got = rule_rates(pd.Series([8.0, 8.0]), pd.Series([7.5, 7.49]))
        self.assertFalse(bool(got.loc[0, "divergence_active"]))
        self.assertTrue(bool(got.loc[1, "divergence_active"]))
        self.assertEqual(got.loc[0, "divergence_rate"], 8.0)
        self.assertAlmostEqual(got.loc[1, "divergence_rate"], 7.745)

    def test_soft_compression_requires_both_frozen_conditions(self):
        got = rule_rates(pd.Series([9.0, 9.5, 10.0]), pd.Series([9.0, 8.0, 9.0]))
        self.assertFalse(bool(got.loc[0, "soft_active"]))
        self.assertFalse(bool(got.loc[1, "soft_active"]))
        self.assertTrue(bool(got.loc[2, "soft_active"]))
        self.assertAlmostEqual(got.loc[2, "soft_rate"], 9.25)

    def test_soft_rule_applies_after_midpoint_correction(self):
        got = rule_rates(pd.Series([10.4]), pd.Series([9.2]))
        self.assertTrue(bool(got.loc[0, "divergence_active"]))
        self.assertAlmostEqual(got.loc[0, "divergence_rate"], 9.8)
        self.assertAlmostEqual(got.loc[0, "soft_rate"], 9.4)


if __name__ == "__main__":
    unittest.main()

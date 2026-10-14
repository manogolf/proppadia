import unittest

from backend.nhl.scripts.audit_nhl_sog_full_season_reconstruction_v1 import poisson_tail


class FullSeasonSogAuditTests(unittest.TestCase):
    def test_zero_rate_has_zero_over_probability(self):
        self.assertEqual(poisson_tail(0.0, 2), 0.0)
        self.assertEqual(poisson_tail(0.0, 4), 0.0)

    def test_poisson_tail_is_monotone_in_expected_shots(self):
        low = poisson_tail(1.0, 3)
        high = poisson_tail(2.0, 3)
        self.assertGreater(high, low)
        self.assertGreaterEqual(low, 0.0)
        self.assertLessEqual(high, 1.0)


if __name__ == "__main__":
    unittest.main()

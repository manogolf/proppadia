import unittest

import pandas as pd

from backend.nhl.scripts.recover_nhl_sog_tier3_provenance_v1 import (
    poisson_tail,
    rolling_sog_rate,
    strict_prior_rows,
)


class Tier3ProvenanceHelpersTest(unittest.TestCase):
    def setUp(self):
        self.history = pd.DataFrame(
            {
                "game_date": ["2025-10-01", "2025-10-03", "2025-10-05"],
                "game_id": [1, 2, 3],
                "shots_on_goal": [1, 4, 99],
                "toi_minutes": [10.0, 20.0, 30.0],
            }
        )

    def test_strict_prior_excludes_same_day_and_future(self):
        prior = strict_prior_rows(self.history, "2025-10-03")
        self.assertEqual(prior.game_id.tolist(), [1])

    def test_rolling_rate_uses_ratio_of_sums(self):
        prior = strict_prior_rows(self.history, "2025-10-05")
        self.assertAlmostEqual(rolling_sog_rate(prior, 2), 10.0)

    def test_poisson_threshold_tail(self):
        self.assertAlmostEqual(poisson_tail(0.0, 2), 0.0)
        self.assertAlmostEqual(poisson_tail(1.0, 1), 1.0 - 1.0 / 2.718281828459045)


if __name__ == "__main__":
    unittest.main()

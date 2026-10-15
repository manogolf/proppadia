import unittest

import numpy as np

from backend.nhl.scripts import analyze_nhl_sog_poisson_distribution_characterization as study


class DistributionCharacterizationTests(unittest.TestCase):
    def test_poisson_probability_matches_scorer_tail(self):
        from backend.nhl.scripts import score_sog_poisson_baseline as scorer

        for mu in (0.0, 0.2, 1.5, 4.2):
            for line, threshold in ((1.5, 2), (2.5, 3), (3.5, 4)):
                self.assertAlmostEqual(
                    float(study.poisson_over(np.array([mu]), threshold)[0]),
                    float(scorer._poisson_tail(mu, threshold)),
                    places=12,
                )

    def test_negative_binomial_preserves_mean_and_poisson_limit(self):
        mu = np.array([0.0, 0.5, 1.8, 4.0])
        alpha = 0.12
        r = 1 / alpha
        self.assertTrue(np.allclose(r * (1 - r / (r + mu)) / (r / (r + mu)), mu))
        self.assertAlmostEqual(
            float(study.nb_over(np.array([1.7]), 3, 1e-7)[0]),
            float(study.poisson_over(np.array([1.7]), 3)[0]),
            places=5,
        )
        self.assertEqual(float(study.nb_over(np.array([0.0]), 2, alpha)[0]), 0.0)

    def test_zero_lambda_positive_outcome_is_impossible_support(self):
        frame = __import__("pandas").DataFrame(
            {"y": [0, 2, 3], "lambda_production": [0.0, 0.0, 2.0], "lambda_divergence": [0.0, 0.0, 2.0], "lambda_combined": [0.0, 0.0, 2.0]}
        )
        scores = study.metric_rows(frame, "fixture", "production")
        self.assertTrue(all(np.isinf(row["count_nll_all_rows"]) for row in scores))
        self.assertEqual(scores[0]["zero_lambda_positive_outcomes"], 1)

    def test_nb_logpmf_has_unit_probability_at_zero_mean_zero_count(self):
        self.assertEqual(float(study.nb_logpmf(np.array([0]), np.array([0.0]), 0.1)[0]), 0.0)


if __name__ == "__main__":
    unittest.main()

import unittest
import numpy as np

from backend.nhl.scripts.build_nhl_points_leader_validation import (
    fit_alpha, nb_probs, poisson_probs,
)


class PointsLeaderValidationTests(unittest.TestCase):
  def test_poisson_and_nb_thresholds_are_coherent_and_monotonic_in_mean(self):
    means = np.array([0.05, 0.2, 0.8, 1.5, 3.0])
    for probabilities in (poisson_probs(means), nb_probs(means, 0.15)):
      self.assertTrue(np.all(probabilities[:, 0] >= probabilities[:, 1]))
      self.assertTrue(np.all(probabilities[:, 1] >= probabilities[:, 2]))
      self.assertTrue(np.all(np.diff(probabilities, axis=0) >= 0))


  def test_nb_dispersion_estimator_returns_finite_nonnegative_value(self):
    y = np.array([0, 0, 0, 1, 1, 2, 3, 4, 0, 1] * 20)
    mu = np.full_like(y, 0.5, dtype=float)
    alpha = fit_alpha(y, mu)
    self.assertTrue(0 <= alpha <= 2)
    self.assertTrue(np.isfinite(alpha))


  def test_nb_probability_layer_converges_to_poisson_at_zero_dispersion(self):
    means = np.array([0.1, 0.5, 1.2])
    self.assertTrue(np.allclose(nb_probs(means, 1e-11), poisson_probs(means), atol=1e-10))


if __name__ == "__main__":
  unittest.main()

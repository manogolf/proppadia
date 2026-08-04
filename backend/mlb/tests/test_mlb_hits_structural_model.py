import numpy as np

from backend.mlb.scripts.run_mlb_hits_structural_model import poisson_binomial


def test_poisson_binomial_is_coherent_across_hit_thresholds():
    distribution = poisson_binomial([0.21, 0.24, 0.18, 0.20, 0.19])
    assert np.isclose(distribution.sum(), 1.0)
    assert np.all(distribution >= 0)
    assert distribution[2:].sum() <= 1 - distribution[0]


def test_zero_pa_has_certain_zero_hits():
    distribution = poisson_binomial([])
    assert np.array_equal(distribution, np.array([1.0, 0.0, 0.0, 0.0, 0.0]))

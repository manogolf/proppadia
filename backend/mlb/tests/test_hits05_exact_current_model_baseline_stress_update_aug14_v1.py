import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_hits05_exact_current_model_baseline_stress_update_aug14_v1 as stress


def test_score_uses_probability_proper_scores():
    result = stress.score(pd.Series([1, 0]), pd.Series([0.8, 0.2]))
    assert result["rows"] == 2
    assert abs(result["brier"] - 0.04) < 1e-12
    assert abs(result["log_loss"] + np.log(0.8)) < 1e-12


def test_hitter_shrinkage_formula_and_unseen_fallback():
    population_rate = 0.57
    known = (7 + stress.PSEUDO_GAMES * population_rate) / (10 + stress.PSEUDO_GAMES)
    unseen = (0 + stress.PSEUDO_GAMES * population_rate) / stress.PSEUDO_GAMES
    assert abs(known - 0.6422222222222222) < 1e-12
    assert unseen == population_rate


def test_fixed_ece_uses_governed_bins():
    target = pd.Series([1.0, 0.0])
    probability = pd.Series([0.51, 0.54])
    assert abs(stress.fixed_ece(target, probability) - 0.025) < 1e-12


def test_comparison_uses_identical_rows():
    frame = pd.DataFrame(
        {
            "target": [1.0, 0.0, np.nan],
            "p_over": [0.8, 0.2, 0.9],
            "baseline_a_population": [0.6, 0.6, 0.6],
            "baseline_b_hitter_shrunk": [0.7, 0.4, 0.8],
        }
    )
    result = stress.comparison(frame)
    assert result.rows.eq(2).all()
    assert result.forecast.tolist() == list(stress.FORECASTS)

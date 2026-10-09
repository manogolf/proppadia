import numpy as np
import pandas as pd
import pytest

from backend.nhl.scripts.grade_nhl_points_hgb_shadow import grade
from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, score


class FixedCountModel:
    def predict(self, x):
        assert list(x.columns) == FEATURES
        return np.array([0.5, 1.5])


def test_frozen_feature_order_scoring_and_poisson_coherence():
    frame = pd.DataFrame({"game_id": [1, 2], "player_id": [11, 22], **{c: [0.0, 1.0] for c in FEATURES}})
    result = score(frame, FixedCountModel())
    assert list(result.columns[-4:]) == ["expected_points", "prob_over_0_5", "prob_over_1_5", "prob_over_2_5"]
    assert np.all(result.prob_over_0_5 >= result.prob_over_1_5)
    assert np.all(result.prob_over_1_5 >= result.prob_over_2_5)


def test_shadow_scorer_fails_closed_on_missing_frozen_feature():
    frame = pd.DataFrame({"game_id": [1], "player_id": [11], **{c: [1.0] for c in FEATURES[:-1]}})
    with pytest.raises(ValueError, match="MISSING_FROZEN_HGB_FEATURES"):
        score(frame, FixedCountModel())


def test_grader_uses_official_goals_plus_assists_and_reports_metrics():
    predictions = pd.DataFrame({"game_id": [1, 2], "player_id": [11, 22], "expected_points": [0.5, 1.5],
        "prob_over_0_5": [0.4, 0.8], "prob_over_1_5": [0.1, 0.4], "prob_over_2_5": [0.01, 0.1]})
    outcomes = pd.DataFrame({"game_id": [1, 2], "player_id": [11, 22], "goals": [0, 1], "assists": [0, 1]})
    report = grade(predictions, outcomes)
    assert report["n"] == 2
    assert report["mean_observed"] == 1
    assert report["crossing_count"] == 0
    assert set(report["thresholds"]) == {"prob_over_0_5", "prob_over_1_5", "prob_over_2_5"}


def test_grader_rejects_unsettled_population():
    predictions = pd.DataFrame({"game_id": [1], "player_id": [11], "expected_points": [1.0],
        "prob_over_0_5": [0.6], "prob_over_1_5": [0.3], "prob_over_2_5": [0.1]})
    outcomes = pd.DataFrame(columns=["game_id", "player_id", "goals", "assists"])
    with pytest.raises(ValueError, match="UNSETTLED_OR_MISSING_OUTCOMES"):
        grade(predictions, outcomes)

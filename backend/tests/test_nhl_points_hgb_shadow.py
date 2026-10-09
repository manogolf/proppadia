import numpy as np
import pandas as pd
import pytest

from backend.nhl.scripts.grade_nhl_points_hgb_shadow import grade
from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, score
from backend.nhl.scripts.export_nhl_points_hgb_features import build_features, normalize_history, normalize_slate


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


def test_grader_rejects_population_without_settled_participants():
    predictions = pd.DataFrame({"game_id": [1], "player_id": [11], "expected_points": [1.0],
        "prob_over_0_5": [0.6], "prob_over_1_5": [0.3], "prob_over_2_5": [0.1]})
    outcomes = pd.DataFrame(columns=["game_id", "player_id", "goals", "assists"])
    with pytest.raises(ValueError, match="NO_SETTLED_PARTICIPANTS"):
        grade(predictions, outcomes)


def test_grader_classifies_nonparticipants_and_unresolved_rows():
    predictions = pd.DataFrame({"game_id": [1, 2, 3], "player_id": [11, 22, 33], "expected_points": [0.5, 1.0, 1.2],
        "prob_over_0_5": [0.4, 0.6, 0.7], "prob_over_1_5": [0.1, 0.3, 0.4], "prob_over_2_5": [0.01, 0.1, 0.2]})
    outcomes = pd.DataFrame({"game_id": [1, 2, 3], "player_id": [11, 22, 33], "official_points": [0, None, None],
        "participation_state": ["PARTICIPATED", "SCRATCHED", "UNRESOLVED"], "official_final": [True, True, True]})
    report = grade(predictions, outcomes)
    assert report["participated_graded_count"] == 1
    assert report["nonparticipant_count"] == 1
    assert report["unresolved_count"] == 1
    assert report["identity_reconciled_count"] == 3


def test_operational_feature_builder_enforces_120_day_and_strict_prior_membership():
    outcomes = pd.DataFrame([
        {"canonical_season":2026,"game_date":"2026-05-01","game_id":2025020001,"player_id":11,"team_id":1,"goals":1,"assists":0},
        {"canonical_season":2026,"game_date":"2026-09-30","game_id":2026020001,"player_id":11,"team_id":1,"goals":1,"assists":1},
        {"canonical_season":2026,"game_date":"2026-09-30","game_id":2026020001,"player_id":22,"team_id":1,"goals":0,"assists":0},
        # A same-day realized row in the source must not leak into the target.
        {"canonical_season":2026,"game_date":"2026-10-09","game_id":2026020066,"player_id":11,"team_id":1,"goals":3,"assists":2},
    ])
    logs = pd.DataFrame([
        {"game_id":2025020001,"player_id":11,"team_id":1,"is_home":True,"toi_minutes":20,"pp_toi_minutes":2,"shots_on_goal":9,"shot_attempts":12},
        {"game_id":2026020001,"player_id":11,"team_id":1,"is_home":True,"toi_minutes":30,"pp_toi_minutes":4,"shots_on_goal":3,"shot_attempts":6},
        {"game_id":2026020001,"player_id":22,"team_id":1,"is_home":False,"toi_minutes":25,"pp_toi_minutes":1,"shots_on_goal":2,"shot_attempts":4},
        {"game_id":2026020066,"player_id":11,"team_id":1,"is_home":True,"toi_minutes":99,"pp_toi_minutes":99,"shots_on_goal":99,"shot_attempts":99},
    ])
    slate = pd.DataFrame([{"game_id":2026020066,"player_id":11,"game_date":"2026-10-09",
        "game_start_utc":"2026-10-09T23:00:00Z","is_home":1,"home_team_id":1,"away_team_id":2}])
    features = build_features(normalize_history(logs, outcomes), normalize_slate(slate, 2026)).iloc[0]
    assert features.player_points_last10 == 2
    assert features.current_season_points_prior == 3
    assert features.current_season_games_prior == 2
    assert features.mean_toi_last10 == 30
    assert features.d10_sog_per60 == 6
    assert features.attempts_d10_per60 == 12
    assert features.team_d10_sf_per_game == 5
    assert features.last10_team_sog_share == 0.6

import unittest

import pandas as pd

from backend.nhl.cross_market_shadow.moneyline_challenger import (
    CHALLENGER_FEATURES,
    CHALLENGER_NAME,
    FINISHING_WEIGHT,
    MODEL_PATH,
    POLICY_VERSION,
    load_model,
    prior_strength_states,
    shot_prior_weight,
    build_challenger_predictions,
)


class MoneylinePriorStrengthChallengerV3Tests(unittest.TestCase):
    def test_model_is_separately_versioned_and_hashed(self):
        model = load_model()
        self.assertEqual(model["model_version"], CHALLENGER_NAME)
        self.assertEqual(model["feature_policy_version"], POLICY_VERSION)
        self.assertEqual(model["feature_order"], CHALLENGER_FEATURES)
        self.assertEqual(model["finishing_weight"], FINISHING_WEIGHT)
        self.assertEqual(len(model["parameter_file_sha256"]), 64)
        self.assertNotEqual(model["frozen_v2_parameter_sha256"], model["parameter_file_sha256"])
        self.assertFalse(model["production_promotion"])

    def test_shot_prior_weight_boundaries(self):
        self.assertEqual([shot_prior_weight(n) for n in (0, 1, 2, 3)], [1.0] * 4)
        self.assertEqual([shot_prior_weight(n) for n in (4, 5)], [.75, .75])
        self.assertEqual([shot_prior_weight(n) for n in (6, 10)], [.5, .5])
        self.assertEqual(shot_prior_weight(11), 0.0)

    def test_prior_state_is_regular_season_final_and_finishing_formula(self):
        prior = pd.DataFrame([
            {"game_id": 1, "game_type": 2, "database_game_status": "FINAL",
             "home_team_code": "BOS", "away_team_code": "UTA", "home_shots": 30,
             "away_shots": 20, "final_home_goals": 4, "final_away_goals": 1},
            {"game_id": 2, "game_type": 1, "database_game_status": "FINAL",
             "home_team_code": "BOS", "away_team_code": "UTA", "home_shots": 100,
             "away_shots": 0, "final_home_goals": 50, "final_away_goals": 0},
            {"game_id": 3, "game_type": 3, "database_game_status": "FINAL",
             "home_team_code": "UTA", "away_team_code": "BOS", "home_shots": 0,
             "away_shots": 100, "final_home_goals": 0, "final_away_goals": 50},
        ])
        states, rate = prior_strength_states(prior)
        self.assertEqual(set(states), {"BOS", "UTA"})
        self.assertAlmostEqual(states["BOS"]["shot_diff_pg"], 10.0)
        self.assertAlmostEqual(states["UTA"]["shot_diff_pg"], -10.0)
        self.assertAlmostEqual(states["BOS"]["finishing_residual_pg"], 4 - rate * 30)
        self.assertAlmostEqual(states["UTA"]["finishing_residual_pg"], 1 - rate * 20)
        self.assertEqual(states["BOS"]["games"], 1.0)
        self.assertEqual(MODEL_PATH.name, "moneyline_shot_finishing_v3.json")

    def test_fixed_half_finishing_shrinkage_and_probability_bounds(self):
        prior = pd.DataFrame([{
            "game_id": 7, "game_type": 2, "database_game_status": "FINAL",
            "home_team_code": "BOS", "away_team_code": "UTA", "home_shots": 30,
            "away_shots": 20, "final_home_goals": 4, "final_away_goals": 1,
        }])
        v2 = pd.DataFrame([{
            "game_id": 8, "canonical_season": 2026, "slate_date": "2026-09-30",
            "game_date": "2026-09-30", "scheduled_start_time_utc": "2026-09-30T23:30:00Z",
            "home_team_id": 1, "home_team": "BOS", "away_team_id": 2, "away_team": "UTA",
            "diff_std_goal_diff_pg": 0.2, "diff_r10_goal_diff_pg": 0.1,
            "diff_std_shot_diff_pg": float("nan"), "diff_days_rest": 1.0,
            "home_back_to_back": 0.0, "away_back_to_back": 0.0,
            "v2_home_win_probability": 0.53, "model_favored_team": "BOS",
            "v2_away_win_probability": 0.47,
        }])
        shot = pd.DataFrame([{"game_id": 8, "diff_std_shot_diff_pg": 20.0}])
        provenance = pd.DataFrame([{
            "game_id": 8, "current_home_shot_diff_pg": float("nan"),
            "current_away_shot_diff_pg": float("nan"), "current_home_games": 0,
            "current_away_games": 0, "home_prior_weight": 1.0, "away_prior_weight": 1.0,
            "blended_home_shot_diff_pg": 10.0, "blended_away_shot_diff_pg": -10.0,
        }])
        prediction, feature_provenance, _ = build_challenger_predictions(
            v2, shot, provenance, prior,
            prior_team_source_sha256="a" * 64,
            prior_outcome_source_sha256="b" * 64,
            schedule_source_sha256="c" * 64,
            history_source_sha256="d" * 64,
            odds_source_sha256="e" * 64,
        )
        self.assertAlmostEqual(
            prediction.iloc[0].shrunk_prior_finishing_residual_gap,
            FINISHING_WEIGHT * feature_provenance.iloc[0].raw_finishing_residual_gap,
        )
        self.assertTrue(prediction.iloc[0].challenger_home_win_probability > 0)
        self.assertTrue(prediction.iloc[0].challenger_home_win_probability < 1)
        self.assertEqual(feature_provenance.iloc[0].schedule_source_sha256, "c" * 64)


if __name__ == "__main__":
    unittest.main()

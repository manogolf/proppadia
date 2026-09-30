from __future__ import annotations

import unittest

import pandas as pd

from backend.nhl.cross_market_shadow.core import build_puck_line_predictions
from backend.nhl.cross_market_shadow.shot_prior import (
    CHALLENGER_NAME,
    POLICY_VERSION,
    build_shot_prior_challenger,
    prior_weight,
)


def fixture():
    start = "2026-10-01T00:00:00Z"
    schedule = pd.DataFrame([{
        "canonical_season": 2026, "slate_date": "2026-09-30", "game_id": 9001,
        "game_date": "2026-09-30", "scheduled_start_time_utc": start,
        "home_team_id": 100, "home_team": "UTA", "away_team_id": 200,
        "away_team": "BOS", "game_status": "SCHEDULED", "game_type_code": 2,
    }])
    base = {
        "canonical_season": 2026, "slate_date": "2026-09-30", "game_date": "2026-09-30",
        "game_status": "FINAL", "game_type_code": 2,
        "final_home_goals": 2, "final_away_goals": 1,
    }
    history = pd.DataFrame([
        {**base, "game_id": 1, "scheduled_start_time_utc": "2026-09-20T00:00:00Z", "home_team_id": 100, "home_team": "UTA", "away_team_id": 200, "away_team": "BOS", "final_home_shots": 30, "final_away_shots": 20},
        # Future result and non-regular-season rows must not contribute.
        {**base, "game_id": 2, "scheduled_start_time_utc": "2026-10-02T00:00:00Z", "home_team_id": 100, "home_team": "UTA", "away_team_id": 200, "away_team": "BOS", "final_home_shots": 60, "final_away_shots": 0},
        {**base, "game_id": 3, "game_type_code": 3, "scheduled_start_time_utc": "2026-09-25T00:00:00Z", "home_team_id": 100, "home_team": "UTA", "away_team_id": 200, "away_team": "BOS", "final_home_shots": 60, "final_away_shots": 0},
    ])
    v1 = pd.DataFrame([{
        **schedule.iloc[0].to_dict(), "game_type_code": 2,
        "diff_std_goal_diff_pg": 0.2, "diff_r10_goal_diff_pg": 0.3,
        "diff_std_shot_diff_pg": 10.0, "diff_days_rest": 1.0,
        "home_back_to_back": 0.0, "away_back_to_back": 1.0,
        "prediction_creation_time_utc": "2026-09-29T20:00:00Z",
        "prediction_status": "REGULAR_SEASON_SHADOW_ELIGIBLE",
        "regular_season_evaluation_eligible": True,
    }])
    prior = pd.DataFrame([
        {"game_type": 2, "database_game_status": "final", "home_team_code": "ARI", "away_team_code": "BOS", "home_shots": 35, "away_shots": 25},
        {"game_type": 2, "database_game_status": "final", "home_team_code": "BOS", "away_team_code": "NYR", "home_shots": 20, "away_shots": 30},
        {"game_type": 1, "database_game_status": "final", "home_team_code": "UTA", "away_team_code": "BOS", "home_shots": 100, "away_shots": 0},
    ])
    return schedule, history, v1, prior


class ShotPriorChallengerTests(unittest.TestCase):
    def test_weight_schedule_boundaries(self):
        for games, expected in [(0, 1), (1, 1), (3, 1), (4, .75), (5, .75), (6, .5), (10, .5), (11, 0)]:
            self.assertEqual(prior_weight(games), expected)

    def test_v1_raw_features_remain_unchanged_except_shot_feature(self):
        schedule, history, v1, prior = fixture()
        v1_before = v1.copy(deep=True)
        candidate, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="a" * 64, prior_outcome_sha256="f" * 64)
        pd.testing.assert_frame_equal(v1, v1_before)
        for column in ["diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_days_rest", "home_back_to_back", "away_back_to_back"]:
            self.assertEqual(candidate.iloc[0][column], v1.iloc[0][column])
        self.assertNotEqual(candidate.iloc[0].diff_std_shot_diff_pg, v1.iloc[0].diff_std_shot_diff_pg)
        self.assertEqual(audit.iloc[0].feature_policy_version, POLICY_VERSION)

    def test_zero_current_games_uses_prior_and_continuity(self):
        schedule, history, v1, prior = fixture()
        history = history.iloc[0:0]
        candidate, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="b" * 64, prior_outcome_sha256="f" * 64)
        self.assertEqual(audit.iloc[0].current_home_games, 0)
        self.assertEqual(audit.iloc[0].home_prior_weight, 1.0)
        # Arizona's prior franchise state maps to the current Utah franchise.
        self.assertEqual(audit.iloc[0].prior_home_full_season_shot_diff_pg, 10.0)
        self.assertEqual(candidate.iloc[0].diff_std_shot_diff_pg, 10.0 - (-10.0))

    def test_strict_start_and_game_type_filtering(self):
        schedule, history, v1, prior = fixture()
        candidate, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="c" * 64, prior_outcome_sha256="f" * 64)
        self.assertEqual(audit.iloc[0].current_home_games, 1)
        self.assertEqual(audit.iloc[0].current_away_games, 1)
        self.assertEqual(audit.iloc[0].current_home_shot_diff_pg, 10)

    def test_probabilities_normalize_and_challenger_is_identifiable(self):
        schedule, history, v1, prior = fixture()
        candidate_input, _ = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="d" * 64, prior_outcome_sha256="f" * 64)
        v1_scored = build_puck_line_predictions(v1)
        candidate_scored = build_puck_line_predictions(candidate_input)
        self.assertAlmostEqual(candidate_scored.iloc[0].probability_sum, 1.0)
        self.assertNotEqual(v1_scored.iloc[0].substantive_prediction_sha256, candidate_scored.iloc[0].substantive_prediction_sha256)
        self.assertEqual(CHALLENGER_NAME, "NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2")

    def test_missing_prior_uses_existing_shot_value(self):
        schedule, history, v1, prior = fixture()
        prior = prior.iloc[0:0]
        candidate, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="e" * 64, prior_outcome_sha256="f" * 64)
        self.assertEqual(candidate.iloc[0].diff_std_shot_diff_pg, v1.iloc[0].diff_std_shot_diff_pg)
        self.assertIn("PRIOR_UNAVAILABLE", audit.iloc[0].fallback_reason)

    def _with_home_history_count(self, count):
        schedule, history, v1, prior = fixture()
        rows = []
        for i in range(count):
            rows.append({
                "canonical_season": 2026, "slate_date": "2026-09-30", "game_date": "2026-09-20",
                "game_id": 100 + i, "scheduled_start_time_utc": f"2026-09-{20 + (i % 8):02d}T00:00:00Z",
                "home_team_id": 100, "home_team": "UTA", "away_team_id": 300 + i,
                "away_team": f"T{i}", "game_status": "FINAL", "game_type_code": 2,
                "final_home_goals": 1, "final_away_goals": 0,
                "final_home_shots": 30, "final_away_shots": 20,
            })
        # Remove the one baseline home game so the exact requested depth is tested.
        history = history[history.home_team_id.ne(100)]
        return schedule, pd.concat([history, pd.DataFrame(rows)], ignore_index=True), v1, prior

    def test_1_to_3_current_games_keep_full_prior_weight(self):
        schedule, history, v1, prior = self._with_home_history_count(3)
        _, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="a" * 64, prior_outcome_sha256="f" * 64)
        self.assertEqual(audit.iloc[0].home_prior_weight, 1.0)

    def test_4_to_5_current_games_use_three_quarter_prior(self):
        schedule, history, v1, prior = self._with_home_history_count(4)
        _, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="a" * 64, prior_outcome_sha256="f" * 64)
        self.assertEqual(audit.iloc[0].home_prior_weight, .75)

    def test_6_to_10_current_games_use_half_prior(self):
        schedule, history, v1, prior = self._with_home_history_count(6)
        _, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="a" * 64, prior_outcome_sha256="f" * 64)
        self.assertEqual(audit.iloc[0].home_prior_weight, .5)

    def test_more_than_10_current_games_use_no_prior_weight(self):
        schedule, history, v1, prior = self._with_home_history_count(11)
        _, audit = build_shot_prior_challenger(schedule, history, v1, prior, prior_source_sha256="a" * 64, prior_outcome_sha256="f" * 64)
        self.assertEqual(audit.iloc[0].home_prior_weight, 0.0)

    def test_both_priors_missing_preserve_v1_input_for_frozen_imputation(self):
        schedule, history, v1, prior = fixture()
        candidate, audit = build_shot_prior_challenger(
            schedule, history.iloc[0:0], v1, prior.iloc[0:0],
            prior_source_sha256="a" * 64, prior_outcome_sha256="f" * 64,
        )
        self.assertEqual(candidate.iloc[0].diff_std_shot_diff_pg, v1.iloc[0].diff_std_shot_diff_pg)
        self.assertIn("V1_PRESERVED", audit.iloc[0].fallback_reason)


if __name__ == "__main__":
    unittest.main()

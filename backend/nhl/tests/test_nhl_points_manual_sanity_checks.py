import unittest

import pandas as pd

from backend.nhl.scripts import audit_nhl_points_manual_sanity_checks as audit


class PointsManualSanityAuditTests(unittest.TestCase):
    def test_recent_history_is_strict_prior_and_binary_frequency_is_distinct(self):
        pred = pd.DataFrame([{"game_id": 30, "player_id": 7, "game_date": "2025-10-03", "points": 0,
                              "p_over_0_5": 0.4, "p_over_1_5": 0.1, "p_over_2_5": 0.02}])
        features = pd.DataFrame([{"game_id": 30, "player_id": 7, "game_date": "2025-10-03", "team_id": 1,
                                  "canonical_season": 2025, "is_home": 1}])
        history = pd.DataFrame([
            {"game_id": 10, "player_id": 7, "game_date": "2025-10-01", "canonical_season": 2025, "team_id": 1, "goals": 0, "assists": 1},
            {"game_id": 20, "player_id": 7, "game_date": "2025-10-02", "canonical_season": 2025, "team_id": 2, "goals": 0, "assists": 0},
            {"game_id": 30, "player_id": 7, "game_date": "2025-10-03", "canonical_season": 2025, "team_id": 1, "goals": 1, "assists": 0},
        ])
        logs = pd.DataFrame([{"game_id": i, "player_id": 7, "game_date": {10: "2025-10-01", 20: "2025-10-02", 30: "2025-10-03"}[i], "toi_minutes": 10.0, "pp_toi_minutes": 1.0, "shots_on_goal": 2} for i in (10, 20, 30)])
        row = audit.build_diagnostics(pred, features, history, logs).iloc[0]
        self.assertEqual(row.season_games, 2)
        self.assertEqual(row.season_points, 1)
        self.assertEqual(row.season_point_games, 1)
        self.assertEqual(row.season_point_hit_rate, 0.5)
        self.assertEqual(row.last5_games, 2)

    def test_same_opponent_counts_prior_meetings_with_explicit_denominator(self):
        pred = pd.DataFrame([{"game_id": 30, "player_id": 7, "game_date": "2025-10-03", "points": 1,
                              "p_over_0_5": 0.5, "p_over_1_5": 0.1, "p_over_2_5": 0.02}])
        features = pd.DataFrame([{"game_id": 30, "player_id": 7, "game_date": "2025-10-03", "team_id": 1,
                                  "canonical_season": 2025, "is_home": 1}])
        history = pd.DataFrame([
            {"game_id": 10, "player_id": 7, "game_date": "2025-10-01", "canonical_season": 2025, "team_id": 1, "goals": 1, "assists": 0},
            {"game_id": 10, "player_id": 8, "game_date": "2025-10-01", "canonical_season": 2025, "team_id": 2, "goals": 0, "assists": 0},
            {"game_id": 20, "player_id": 7, "game_date": "2025-10-02", "canonical_season": 2025, "team_id": 1, "goals": 0, "assists": 0},
            {"game_id": 20, "player_id": 8, "game_date": "2025-10-02", "canonical_season": 2025, "team_id": 2, "goals": 0, "assists": 0},
            {"game_id": 30, "player_id": 7, "game_date": "2025-10-03", "canonical_season": 2025, "team_id": 1, "goals": 0, "assists": 1},
            {"game_id": 30, "player_id": 9, "game_date": "2025-10-03", "canonical_season": 2025, "team_id": 2, "goals": 0, "assists": 0},
        ])
        dates={10:"2025-10-01",20:"2025-10-02",30:"2025-10-03"}
        logs = pd.DataFrame([{"game_id": i, "player_id": p, "game_date":dates[i], "toi_minutes": 10.0, "pp_toi_minutes": 0.0, "shots_on_goal": 1}
                             for i, p in ((10, 7), (20, 7), (30, 7))])
        row = audit.build_diagnostics(pred, features, history, logs).iloc[0]
        self.assertEqual(row.same_opp_games, 2)
        self.assertEqual(row.same_opp_point_games, 1)
        self.assertEqual(row.same_opp_point_hit_rate, 0.5)
        self.assertEqual(row.same_opp_most_recent_date, "2025-10-02")

    def test_signal_and_role_trend_cutoffs_are_transparent(self):
        self.assertEqual(audit.signal(0.09), "AGREEMENT")
        self.assertEqual(audit.signal(0.1), "MANUAL_HIGHER")
        self.assertEqual(audit.signal(-0.1), "MANUAL_LOWER")
        self.assertEqual(audit.trend(0.51), "RISING")
        self.assertEqual(audit.trend(-0.51), "FALLING")
        self.assertEqual(audit.trend(0.5), "STABLE")


if __name__ == "__main__":
    unittest.main()

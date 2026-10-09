import unittest

import pandas as pd

from backend.nhl.scripts.build_nhl_points_architecture_bakeoff import (
    ARMS, build_frame, prior_reverse,
)


class PointsBakeoffFrameTests(unittest.TestCase):
    def setUp(self):
        self.source = pd.DataFrame([
            # Same date observations must not leak into either target's history.
            [2023, "2023-10-01", "2023-10-01T18:00:00Z", 1, 10, 1, 1, 1, 0, 1, 20, 2, 4, 6],
            [2023, "2023-10-01", "2023-10-01T20:00:00Z", 2, 10, 1, 0, 0, 0, 0, 18, 1, 2, 4],
            [2024, "2024-12-20", "2024-12-20T18:00:00Z", 3, 10, 1, 1, 0, 1, 1, 22, 3, 5, 7],
            [2024, "2024-12-21", "2024-12-21T18:00:00Z", 4, 10, 1, 0, 0, 0, 0, 19, 2, 3, 5],
            [2024, "2024-12-22", "2024-12-22T18:00:00Z", 5, 10, 1, 1, 0, 0, 0, 21, 2, 4, 6],
        ], columns=["season", "game_date", "start_time_utc", "game_id", "player_id",
                    "team_id", "is_home", "realized_goals", "realized_assists",
                    "realized_points", "toi_minutes", "pp_toi_minutes", "shots_on_goal",
                    "shot_attempts"])

    def test_same_day_is_excluded_and_next_date_sees_prior_games(self):
        frame = build_frame(self.source)
        a = frame[(frame.game_id == 1) & (frame.history_contract == ARMS[0])].iloc[0]
        b = frame[(frame.game_id == 2) & (frame.history_contract == ARMS[0])].iloc[0]
        c = frame[(frame.game_id == 3) & (frame.history_contract == ARMS[0])].iloc[0]
        self.assertEqual(a.player_history_games, 0)
        self.assertEqual(b.player_history_games, 0)
        self.assertEqual(c.player_history_games, 2)
        self.assertEqual(c.current_season_games_prior, 0)
        self.assertEqual(c.player_points_last10, 0.5)

    def test_history_contracts_differ_at_season_boundary(self):
        frame = build_frame(self.source)
        row = frame[(frame.game_id == 3)].set_index("history_contract")
        self.assertEqual(row.loc[ARMS[0], "player_history_games"], 2)
        self.assertEqual(row.loc[ARMS[1], "player_history_games"], 0)
        self.assertEqual(row.loc[ARMS[2], "player_history_games"], 0)

    def test_prior_reversal_recovers_natural_probability(self):
        natural, prevalence = 0.30, 0.20
        weighted = (natural / prevalence) / ((natural / prevalence) + ((1-natural)/(1-prevalence)))
        self.assertAlmostEqual(float(prior_reverse([weighted], prevalence)[0]), natural)


if __name__ == "__main__":
    unittest.main()

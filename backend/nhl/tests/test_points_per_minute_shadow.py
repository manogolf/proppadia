"""Points per-minute shadow: strict-prior per-minute rates and safe capture handling."""
import unittest

import pandas as pd

from backend.nhl import points_per_minute_shadow as shadow


def frames():
    features = pd.DataFrame([
        {"game_id": 9, "player_id": 1, "game_date": "2026-11-06", "current_season_points_prior": 14,
         "current_season_games_prior": 14, "mean_toi_last10": 18.4},
        {"game_id": 9, "player_id": 2, "game_date": "2026-11-06", "current_season_points_prior": 0,
         "current_season_games_prior": 0, "mean_toi_last10": 12.0},   # no games, no prior season
    ])
    outcomes = pd.DataFrame([
        {"canonical_season": 2026, "slate_date": "2026-11-05", "player_id": 1, "official_goals": 7, "official_assists": 7},
        {"canonical_season": 2026, "slate_date": "2026-11-06", "player_id": 1, "official_goals": 5, "official_assists": 5},
    ])  # the 11-06 row is the target day: never counted
    logs = pd.DataFrame([{"canonical_season": 2026, "game_date": "2026-11-05", "player_id": 1, "toi_minutes": 250.0},
                         {"canonical_season": 2026, "game_date": "2026-11-06", "player_id": 1, "toi_minutes": 99.0}])
    totals = pd.DataFrame([{"player_id": 1, "season": 2025, "games": 76, "points": 22, "goals": 7, "assists": 15}])
    league = pd.DataFrame([{"season": 2025, "points": 0.46, "goals": 0.17, "assists": 0.29}])
    usage = pd.DataFrame([{"season": 2025, "player_id": 1, "games": 76, "toi_per_game": 13.2},
                          {"season": 2025, "player_id": 3, "games": 80, "toi_per_game": 20.0}])
    return features, outcomes, logs, totals, league, usage


class PerMinuteTests(unittest.TestCase):
    def test_rates_use_strict_prior_minutes_and_counts(self):
        out = shadow.per_minute_inputs(*frames(), season=2026).set_index("player_id")
        lg_min = (76 * 13.2 + 80 * 20.0) / 156
        expected = (22 + 14 + 30 * 0.46) / (76 * 13.2 + 250.0 + 30 * lg_min) * 18.4
        self.assertAlmostEqual(out.loc[1, "own_rate_toi"], expected)
        goals = (7 + 7 + 30 * 0.17) / (76 * 13.2 + 250.0 + 30 * lg_min) * 18.4
        self.assertAlmostEqual(out.loc[1, "goal_rate_toi"], goals)
        self.assertFalse(out.loc[1, "prior_minutes_estimated"])

    def test_new_player_falls_back_to_the_league_rate(self):
        out = shadow.per_minute_inputs(*frames(), season=2026).set_index("player_id")
        lg_min = (76 * 13.2 + 80 * 20.0) / 156
        self.assertTrue(out.loc[2, "prior_minutes_estimated"])
        self.assertAlmostEqual(out.loc[2, "own_rate_toi"], 0.46 / lg_min * 12.0)

    def test_season_of(self):
        self.assertEqual(shadow.season_of("2026-11-06"), 2026)
        self.assertEqual(shadow.season_of("2027-02-01"), 2026)


if __name__ == "__main__":
    unittest.main()

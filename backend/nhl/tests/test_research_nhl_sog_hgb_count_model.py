import unittest

import numpy as np
import pandas as pd

from backend.nhl.scripts import research_nhl_sog_hgb_count_model as m


def logs():
    rows = []
    gid = 2024020000
    # season 2024 for players 1 (F, home team 10) and 2 (D, away team 20); season 2025 continues
    for season, start in ((2024, "2024-10-10"), (2025, "2025-10-10")):
        for i in range(6):
            gid += 1
            date = pd.Timestamp(start) + pd.Timedelta(days=2 * i)
            for player, home, sog, toi in ((1, True, i % 4, 15.0 + i), (2, False, 1, 20.0)):
                rows.append({"player_id": player, "game_id": gid, "game_date": date, "season": season,
                             "game_type": "02", "is_home": home, "home_team_id": 10, "away_team_id": 20,
                             "shots_on_goal": float(sog), "toi_minutes": toi, "pp_toi_minutes": 2.0,
                             "shot_attempts": float(sog * 2)})
    # preseason row and a no-TOI row must not enter the clean features
    rows.append({**rows[-2], "game_id": 2025010001, "game_type": "01", "game_date": pd.Timestamp("2025-10-01"),
                 "shots_on_goal": 9.0})
    rows.append({**rows[-3], "game_id": 2025020999, "game_date": pd.Timestamp("2025-10-30"), "toi_minutes": None})
    return pd.DataFrame(rows)


POS = pd.DataFrame({"player_id": [1, 2], "group": ["F", "D"]})


class FeatureTests(unittest.TestCase):
    def setUp(self):
        self.frame = m.build(logs(), POS)

    def test_one_row_per_clean_game_and_features_are_strict_prior(self):
        self.assertEqual(len(self.frame), 24)
        self.assertNotIn(2025010001, set(self.frame.game_id))
        p1 = self.frame[self.frame.player_id.eq(1)].sort_values("game_date").reset_index(drop=True)
        self.assertTrue(np.isnan(p1.sog60_5.iloc[0]))
        first_two = p1.iloc[:2]
        expected = first_two.shots_on_goal.sum() / first_two.toi_minutes.sum() * 60
        self.assertAlmostEqual(p1.sog60_5.iloc[2], expected)
        self.assertEqual(p1.games_season.tolist()[:7], [0, 1, 2, 3, 4, 5, 0])

    def test_prior_season_rate_and_shrunk_prior_by_position(self):
        p2 = self.frame[self.frame.player_id.eq(2) & self.frame.season.eq(2025)]
        self.assertAlmostEqual(p2.sog60_prior_season.iloc[0], 6 / 120 * 60)
        self.assertAlmostEqual(p2.league_prior.iloc[0], 6 / 120 * 60)
        self.assertTrue(self.frame[self.frame.season.eq(2024)].league_prior.isna().all())

    def test_team_and_opponent_shots_are_prior_games_only(self):
        g = self.frame[self.frame.player_id.eq(1)].sort_values("game_date").reset_index(drop=True)
        self.assertTrue(np.isnan(g.team_sf_10.iloc[0]))
        self.assertAlmostEqual(g.team_sf_10.iloc[1], g.shots_on_goal.iloc[0])
        self.assertAlmostEqual(g.opp_sa_10.iloc[1], g.shots_on_goal.iloc[0])  # team 20 allowed player 1's shots


class InversionTests(unittest.TestCase):
    def test_lambda_from_p_over_round_trips(self):
        for lam in (0.3, 1.25, 2.6, 4.0):
            p = 1 - np.exp(-lam) * (1 + lam)
            self.assertAlmostEqual(m.lambda_from_p_over_1_5(p), lam, places=9)
        self.assertEqual(m.lambda_from_p_over_1_5(0.0), 0.0)


if __name__ == "__main__":
    unittest.main()

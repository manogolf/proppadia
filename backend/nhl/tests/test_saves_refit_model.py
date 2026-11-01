"""Saves refit module (P2.41): strict-prior inputs, monotone ladder, no calibration."""
import unittest

import numpy as np
import pandas as pd

from backend.nhl import saves_refit_model as srm


def logs(n_games=12, season=2026):
    rows = []
    for i in range(n_games):
        gid = season * 1_000_000 + 20_000 + i
        day = pd.Timestamp("2026-10-01") + pd.Timedelta(days=i)
        for team, sog in ((1, 30 + i), (2, 20)):
            for player in range(3):
                rows.append({"season": season, "game_date": day, "game_id": gid, "team_id": team,
                             "home_team_id": 1, "away_team_id": 2, "shots_on_goal": sog / 3})
    return pd.DataFrame(rows)


class TeamFeatureTests(unittest.TestCase):
    def test_target_rows_use_only_prior_games(self):
        g = srm.team_games(logs())
        slate = pd.DataFrame([{"game_id": 2026029999, "game_date": "2026-10-20", "home_team_id": 1, "away_team_id": 2}])
        f = srm.team_features(srm.add_targets(g, slate, 2026))
        target = f[(f.game_id == 2026029999) & (f.team_id == 1)].iloc[0]
        self.assertTrue(np.isnan(target.sf))
        # Team 1's last 10 shots for are games 2..11 (32..41), never the target game.
        self.assertAlmostEqual(target.sf_l10, np.mean([30 + i for i in range(2, 12)]))
        self.assertAlmostEqual(target.opp_sf_l10, 20.0)
        self.assertEqual(target.rest_days, 7)  # capped
        first = f[(f.team_id == 1)].sort_values("game_date").iloc[0]
        self.assertNotEqual(first.sf_l10, first.sf)  # first game has no own-game leakage

    def test_league_level_is_prior_dates_only(self):
        f = srm.team_features(srm.team_games(logs()))
        day2 = f[f.game_date == pd.Timestamp("2026-10-02")].league_spg_to_date.iloc[0]
        self.assertAlmostEqual(day2, (30 + 20) / 2)


class SaveRateTests(unittest.TestCase):
    def test_save_rate_shrinks_and_excludes_current_game(self):
        g = pd.DataFrame([
            {"season": 2025, "game_id": 1, "player_id": 9, "game_date": "2025-11-01", "toi": 60, "saves": 27, "shots": 30},
            {"season": 2026, "game_id": 2, "player_id": 9, "game_date": "2026-10-10", "toi": 60, "saves": 18, "shots": 20},
            {"season": 2026, "game_id": 3, "player_id": 9, "game_date": "2026-10-12", "toi": np.nan, "saves": np.nan, "shots": np.nan},
        ])
        r = srm.save_rates(g).set_index("game_id")
        league_2025 = 27 / 30
        expected = (27 + 18 + srm.K_SAVE * league_2025) / (30 + 20 + srm.K_SAVE)
        self.assertAlmostEqual(r.loc[3, "save_rate"], expected)
        self.assertEqual(r.loc[2, "cur_shots"], 0)


class ProbabilityTests(unittest.TestCase):
    def test_ladder_is_monotone_and_has_no_calibration(self):
        model = {"variance_ratio": 1.97, "lines": list(srm.LINES), "calibration_layer": "NONE"}
        p = srm.prob_over(model, np.array([18.0, 24.0, 31.0]))
        self.assertTrue((np.diff(p.to_numpy(), axis=1) <= 0).all())
        self.assertTrue((p.to_numpy() > 0).all() and (p.to_numpy() < 1).all())
        self.assertGreater(p.p_over_24_5.iloc[2], p.p_over_24_5.iloc[0])


if __name__ == "__main__":
    unittest.main()

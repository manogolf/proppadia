from __future__ import annotations

import unittest

import pandas as pd

from backend.nhl.points_offline_team_shots import reconstruct_team_shots


class OfflineTeamShotsTests(unittest.TestCase):
    def slate(self, day="2026-10-20", team=1):
        return pd.DataFrame([{"team_id": team, "game_id": 2026020999,
                              "player_id": 7, "game_date": day}])

    def games(self, dates, values, team=1, start_id=2026020001):
        return pd.DataFrame([{"team_id": team, "game_id": start_id + i,
                              "game_date": day, "team_sog": value}
                             for i, (day, value) in enumerate(zip(dates, values))])

    def test_zero_prior_games(self):
        empty = pd.DataFrame(columns=["team_id", "game_id", "game_date", "team_sog"])
        row = reconstruct_team_shots(empty, self.slate()).iloc[0]
        self.assertEqual(row.valid_prior_game_count, 0)
        self.assertEqual(row.team_d10_sf_per_game, 0.0)
        self.assertTrue(row.source_supported)

    def test_one_to_nine_valid_games_use_observed_denominator(self):
        for n in range(1, 10):
            vals = list(range(1, n + 1))
            dates = pd.date_range("2026-10-01", periods=n).strftime("%Y-%m-%d")
            row = reconstruct_team_shots(self.games(dates, vals), self.slate()).iloc[0]
            self.assertEqual(row.valid_prior_game_count, n)
            self.assertAlmostEqual(row.team_d10_sf_per_game, sum(vals) / n)

    def test_ten_game_window_uses_latest_ten(self):
        dates = pd.date_range("2026-10-01", periods=11).strftime("%Y-%m-%d")
        row = reconstruct_team_shots(self.games(dates, range(1, 12)), self.slate()).iloc[0]
        self.assertEqual(row.valid_prior_game_count, 10)
        self.assertEqual(row.prior_game_ids.split(";"), [str(2026020001 + i) for i in range(1, 11)])
        self.assertEqual(row.team_d10_sf_per_game, 6.5)

    def test_missing_official_value_is_not_zero(self):
        with self.assertRaisesRegex(ValueError, "MISSING_OFFICIAL_TEAM_SOG"):
            reconstruct_team_shots(self.games(["2026-10-01"], [None]), self.slate())

    def test_genuine_zero_is_a_valid_observation(self):
        row = reconstruct_team_shots(self.games(["2026-10-01"], [0]), self.slate()).iloc[0]
        self.assertEqual(row.valid_prior_game_count, 1)
        self.assertEqual(row.team_d10_sf_per_game, 0.0)
        self.assertEqual(row.prior_team_sog_values, "0")

    def test_explicit_team_alias_maps_to_canonical_team(self):
        row = reconstruct_team_shots(self.games(["2026-10-01"], [31], team=91),
                                     self.slate(team=1), team_aliases={91: 1}).iloc[0]
        self.assertEqual(row.valid_prior_game_count, 1)
        self.assertEqual(row.team_d10_sf_per_game, 31.0)

    def test_target_date_excluded_and_120_day_boundary_included(self):
        dates = ["2026-06-22", "2026-06-21", "2026-10-20"]
        row = reconstruct_team_shots(self.games(dates, [20, 90, 40]), self.slate()).iloc[0]
        self.assertEqual(row.prior_game_ids, str(2026020001))
        self.assertEqual(row.team_d10_sf_per_game, 20.0)

    def test_duplicate_contributing_games_rejected(self):
        games = self.games(["2026-10-01", "2026-10-02"], [20, 30])
        games.loc[1, "game_id"] = games.loc[0, "game_id"]
        with self.assertRaisesRegex(ValueError, "DUPLICATE_TEAM_GAME_SOURCE"):
            reconstruct_team_shots(games, self.slate())

    def test_incomplete_source_coverage_is_marked_unsupported(self):
        row = reconstruct_team_shots(self.games(["2026-10-01"], [20]),
                                     self.slate(), source_complete=False).iloc[0]
        self.assertFalse(row.source_supported)


if __name__ == "__main__":
    unittest.main()

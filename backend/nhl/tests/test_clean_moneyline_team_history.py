from __future__ import annotations

import hashlib
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.scripts import build_nhl_clean_moneyline_team_history as foundation


class CleanMoneylineTeamHistoryTests(unittest.TestCase):
    def games(self):
        return pd.DataFrame([
            {"season": 2026, "game_id": 2026020001, "game_date": "2026-09-29", "scheduled_start_time_utc": pd.Timestamp("2026-09-29T20:00:00Z"), "game_type": 2, "home_team_id": 1, "home_team_code": "AAA", "away_team_id": 2, "away_team_code": "BBB", "home_final_score": 3, "away_final_score": 2, "home_shots": 31, "away_shots": 28, "final_status": "OFF", "decision_type": "REG", "regular_season": True},
            {"season": 2026, "game_id": 2026020002, "game_date": "2026-09-29", "scheduled_start_time_utc": pd.Timestamp("2026-09-29T20:00:00Z"), "game_type": 2, "home_team_id": 3, "home_team_code": "CCC", "away_team_id": 4, "away_team_code": "DDD", "home_final_score": 1, "away_final_score": 2, "home_shots": 20, "away_shots": 22, "final_status": "OFF", "decision_type": "SO", "regular_season": True},
            {"season": 2026, "game_id": 2026020003, "game_date": "2026-09-30", "scheduled_start_time_utc": pd.Timestamp("2026-09-30T20:00:00Z"), "game_type": 2, "home_team_id": 1, "home_team_code": "AAA", "away_team_id": 4, "away_team_code": "DDD", "home_final_score": float("nan"), "away_final_score": float("nan"), "home_shots": float("nan"), "away_shots": float("nan"), "final_status": "SCHEDULED", "decision_type": None, "regular_season": True},
            {"season": 2026, "game_id": 2026030001, "game_date": "2026-09-25", "scheduled_start_time_utc": pd.Timestamp("2026-09-25T20:00:00Z"), "game_type": 3, "home_team_id": 1, "home_team_code": "AAA", "away_team_id": 4, "away_team_code": "DDD", "home_final_score": 4, "away_final_score": 1, "home_shots": 35, "away_shots": 17, "final_status": "OFF", "decision_type": "REG", "regular_season": False},
        ])

    def test_two_team_rows_orientation_winners_and_shots(self):
        games = self.games().iloc[:1]
        rows = foundation.make_team_games(games)
        foundation.validate_team_games(games, rows)
        self.assertEqual(len(rows), 2)
        home = rows.loc[rows.is_home.eq(1)].iloc[0]
        away = rows.loc[rows.is_home.eq(0)].iloc[0]
        self.assertEqual((home.goals_for, home.goals_against, home.won_game), (3, 2, 1))
        self.assertEqual((away.goals_for, away.goals_against, away.won_game), (2, 3, 0))
        self.assertEqual((home.shots_for, away.shots_for), (31, 28))

    def test_schedule_chronology_and_simultaneous_isolation(self):
        games = self.games()
        team = foundation.make_team_games(games)
        features = foundation.add_strict_prior(team)
        future = features[(features.game_id == 2026020003) & (features.team_id == 1)].iloc[0]
        self.assertEqual(future.prior_games_played, 1)
        self.assertEqual(int(future.prior_game_id), 2026020001)
        simultaneous = features[(features.game_id == 2026020002) & (features.team_id == 3)].iloc[0]
        self.assertEqual(simultaneous.prior_games_played, 0)

        # Synthetic simultaneous targets for one team exercise the strict
        # timestamp comparison independently of game ID tie-breaking.
        tied = team[team.game_id.eq(2026020001) & team.is_home.eq(1)].copy()
        tied2 = tied.copy()
        tied2["game_id"] = 2026020099
        tied_features = foundation.add_strict_prior(pd.concat([tied, tied2], ignore_index=True))
        self.assertEqual(tied_features.prior_games_played.tolist(), [0, 0])

    def test_regular_season_filter_and_future_handoff(self):
        games = self.games()
        matrix = foundation.make_matrix(games, foundation.make_team_games(games))
        self.assertNotIn(2026030001, set(matrix.game_id))
        self.assertNotIn(2026020003, set(matrix.game_id))
        rows = foundation.add_strict_prior(foundation.make_team_games(games))
        sep30_home = rows[(rows.game_id == 2026020003) & (rows.team_id == 1)].iloc[0]
        self.assertEqual(sep30_home.prior_games_played, 1)

    def test_deterministic_serialization_and_source_independence(self):
        games = self.games()
        first = foundation.make_team_games(games).to_csv(index=False, lineterminator="\n").encode()
        second = foundation.make_team_games(games.copy()).to_csv(index=False, lineterminator="\n").encode()
        self.assertEqual(hashlib.sha256(first).hexdigest(), hashlib.sha256(second).hexdigest())
        source = Path(foundation.__file__).read_text(encoding="utf-8")
        self.assertNotIn("skater_game_logs", source)

    def test_canonical_game_ids_unique(self):
        duplicate = self.games().iloc[[0, 0]].copy()
        with self.assertRaisesRegex(RuntimeError, "DUPLICATE_CANONICAL_GAME_ID"):
            foundation.validate_canonical(duplicate)


if __name__ == "__main__":
    unittest.main()

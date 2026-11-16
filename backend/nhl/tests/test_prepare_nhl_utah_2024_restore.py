"""P2.57 Utah 2024-25 restore: generated SQL is guarded, all-or-nothing and new-rows-only."""
import unittest

import pandas as pd

from backend.nhl.scripts import prepare_nhl_utah_2024_restore as u

GAMES = pd.DataFrame({"game_id": [2024020005, 2024020016], "game_date": ["2024-10-08", "2024-10-10"],
                      "start_time_utc": ["2024-10-09T02:00:00Z", "2024-10-10T23:30:00Z"],
                      "home_team_id": [68, 2], "away_team_id": [16, 68]})
SK = pd.DataFrame([{c: 1 for c in u.SKATER_COLS} | {"player_id": 8, "game_id": 2024020005, "team_id": 68,
                                                    "opponent_id": 16, "is_home": True, "game_date": "2024-10-08",
                                                    "toi_minutes": 17.05, "pp_toi_minutes": None}])
GL = pd.DataFrame([{c: 1 for c in u.GOALIE_COLS} | {"player_id": 9, "game_id": 2024020005, "team_id": 16,
                                                    "opponent_id": 68, "is_home": False, "game_date": "2024-10-08",
                                                    "start_flag": True}])
PLAYER = {"player_id": 9, "first_name": "O'Neil", "last_name": "X", "full_name": "O'Neil X", "team_id": 16}


class RestoreSqlTests(unittest.TestCase):
    def test_literals(self):
        self.assertEqual([u.lit(None), u.lit(True), u.lit(3), u.lit(1.234), u.lit("O'Neil")],
                         ["NULL", "TRUE", "3", "1.23", "'O''Neil'"])

    def test_games_fill_only_where_null_with_pinned_teams_and_exact_count(self):
        sql = u.games_sql(GAMES)
        self.assertIn("x.game_date IS NULL AND x.start_time_utc IS NULL", sql)
        self.assertIn("x.home_team_id = v.home AND x.away_team_id = v.away", sql)
        self.assertIn("WHEN c.matching_rows = 2 THEN TRUE", sql)
        self.assertIn("'2024-10-09T02:00:00+00:00'::timestamptz", sql)
        back = u.games_sql(GAMES, reverse=True)
        self.assertIn("SET game_date = NULL, start_time_utc = NULL", back)
        self.assertIn("x.game_date = v.d AND x.start_time_utc = v.t", back)

    def test_logs_insert_new_rows_only_with_count_and_team_guards(self):
        sql = u.logs_sql(GAMES, SK, GL, PLAYER)
        self.assertIn("RUN_THE_GAMES_BLOCK_FIRST", sql)
        self.assertIn("UTAH_2024_ROWS_ALREADY_PRESENT_REFUSING", sql)
        self.assertIn("<> 1", sql)
        self.assertIn("UTAH_2024_TEAM_GUARD_FAILED", sql)
        self.assertIn("'O''Neil X'", sql)
        self.assertNotIn("UPDATE nhl.skater_game_logs_raw", sql)
        self.assertNotIn("ON CONFLICT", sql)

    def test_editor_wrapper_grants_ingest_only_inside_the_transaction(self):
        text = u.wrap("SELECT 1;", "t")
        self.assertLess(text.index("BEGIN;"), text.index("GRANT proppadia_nhl_ingest TO postgres;"))
        self.assertLess(text.index("REVOKE proppadia_nhl_ingest FROM postgres;"), text.index("COMMIT;"))
        owner = u.wrap("SELECT 1;", "t", role=None)
        self.assertNotIn("GRANT", owner)


if __name__ == "__main__":
    unittest.main()

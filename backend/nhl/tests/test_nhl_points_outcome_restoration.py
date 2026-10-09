from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from backend.nhl.scripts.restore_official_points_outcomes_2025 import parse_boxscore, read_target


def boxscore(*, home_goals=1, home_score=None):
    return {
        "id": 2025020001, "season": 20252026, "gameType": 2, "gameDate": "2025-10-07",
        "gameState": "OFF", "homeTeam": {"id": 13, "score": home_goals if home_score is None else home_score},
        "awayTeam": {"id": 16, "score": 0},
        "playerByGameStats": {"homeTeam": {"forwards": [{"playerId": 11,"goals": home_goals,"assists": 0}],"defense": [],"goalies": []},
                             "awayTeam": {"forwards": [{"playerId": 12,"goals": 0,"assists": 0}],"defense": [],"goalies": []}},
    }


class PointsOutcomeRestorationTests(unittest.TestCase):
    def test_parser_emits_exact_player_game_targets_and_provenance(self):
        rows, check = parse_boxscore(boxscore(), "retained/game.json", "a" * 64)
        self.assertEqual([(r["game_id"],r["player_id"],r["realized_points"]) for r in rows],
                         [(2025020001,12,0),(2025020001,11,1)])
        self.assertEqual(check["home_player_goal_sum"], check["home_final_goals"])
        self.assertTrue(all(r["source_sha256"] == "a"*64 for r in rows))

    def test_scoring_total_mismatch_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "TEAM_GOAL_TOTAL_MISMATCH"):
            parse_boxscore(boxscore(home_goals=1, home_score=2), "retained/game.json", "a" * 64)

    def test_duplicate_player_game_identity_fails_closed(self):
        data = boxscore()
        data["playerByGameStats"]["homeTeam"]["defense"] = [{"playerId":11,"goals":0,"assists":0}]
        with self.assertRaisesRegex(ValueError, "DUPLICATE_PLAYER_IN_GAME"):
            parse_boxscore(data, "retained/game.json", "a" * 64)

    def test_database_target_population_rejects_duplicate_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/"target.csv"
            path.write_text("game_id,player_id,goals,assists\n1,2,,\n1,2,,\n")
            with self.assertRaisesRegex(ValueError, "DUPLICATE_TARGET_KEY"):
                read_target(path)


if __name__ == "__main__":
    unittest.main()

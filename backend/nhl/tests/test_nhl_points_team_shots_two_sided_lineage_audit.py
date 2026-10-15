import json
import unittest

import pandas as pd

from backend.nhl.scripts.build_nhl_points_team_shots_two_sided_lineage_audit import (
    strict_prior_team_history,
)
from backend.nhl.scripts.acquire_nhl_points_team_shots_official_boxscores import parse_boxscore


class TestNhlPointsTeamShotsTwoSidedLineageAudit(unittest.TestCase):
    def test_strict_prior_uses_latest_ten_valid_games_and_excludes_target_date(self):
        rows = []
        for day in range(1, 13):
            rows.append({"game_id": 100 + day, "game_date": f"2026-10-{day:02d}",
                         "official_team_sog": day})
        rows.extend([
            {"game_id": 1, "game_date": "2026-06-01", "official_team_sog": 40},
            {"game_id": 200, "game_date": "2026-10-15", "official_team_sog": 50},
            {"game_id": 201, "game_date": "2026-10-14", "official_team_sog": None},
        ])

        history = strict_prior_team_history(pd.DataFrame(rows), "2026-10-15")

        self.assertEqual(history.game_id.tolist(), list(range(103, 113)))
        self.assertEqual(int(history.official_team_sog.sum()), sum(range(3, 13)))
        self.assertEqual(float(history.official_team_sog.mean()), 7.5)
        self.assertFalse(history.game_date.eq(pd.Timestamp("2026-10-15")).any())

    def test_boxscore_parser_binds_team_totals_by_official_side(self):
        raw = json.dumps({
            "id": 2026020001,
            "gameType": 2,
            "gameDate": "2026-10-07",
            "startTimeUTC": "2026-10-08T00:00:00Z",
            "gameState": "OFF",
            "homeTeam": {"id": 1, "sog": 31},
            "awayTeam": {"id": 2, "sog": 27},
        }).encode()

        parsed = parse_boxscore(raw, 2026020001)

        self.assertEqual(parsed["home"], {"team_id": 1, "team_sog": 31})
        self.assertEqual(parsed["away"], {"team_id": 2, "team_sog": 27})

    def test_boxscore_parser_rejects_wrong_game_and_non_regular_season(self):
        base = {"id": 2026020001, "gameType": 2,
                "gameState": "OFF",
                "homeTeam": {"id": 1, "sog": 31}, "awayTeam": {"id": 2, "sog": 27}}
        with self.assertRaisesRegex(ValueError, "GAME_ID_MISMATCH"):
            parse_boxscore(json.dumps({**base, "id": 2026020002}).encode(), 2026020001)
        with self.assertRaisesRegex(ValueError, "NOT_REGULAR_SEASON"):
            parse_boxscore(json.dumps({**base, "gameType": 1}).encode(), 2026020001)
        with self.assertRaisesRegex(ValueError, "NOT_FINAL"):
            parse_boxscore(json.dumps({**base, "gameState": "FUT"}).encode(), 2026020001)


if __name__ == "__main__":
    unittest.main()

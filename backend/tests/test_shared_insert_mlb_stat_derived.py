import unittest
from unittest.mock import patch

from backend.scripts import insert_mlb_stat_derived as script


class TestSharedInsertMlbStatDerived(unittest.TestCase):
    def test_fetch_schedule_handles_empty_dates(self):
        with patch.object(script, "_fetch_json", return_value={"dates": []}):
            rows = script._fetch_schedule("2026-02-14")
        self.assertEqual(rows, [])

    def test_final_game_ids_include_all_final_when_not_locked(self):
        schedule = [
            {"gamePk": 1, "gameType": "R", "status": {"detailedState": "Final"}},
            {"gamePk": 2, "gameType": "S", "status": {"detailedState": "Final"}},
            {"gamePk": 3, "gameType": "R", "status": {"detailedState": "In Progress"}},
        ]
        out = script._final_game_ids(schedule, require_regular_season=False)
        self.assertEqual(out, [1, 2])

    def test_final_game_ids_filter_regular_when_locked(self):
        schedule = [
            {"gamePk": 1, "gameType": "R", "status": {"detailedState": "Final"}},
            {"gamePk": 2, "gameType": "S", "status": {"detailedState": "Final"}},
            {"gamePk": 3, "gameType": "P", "status": {"detailedState": "Final"}},
        ]
        out = script._final_game_ids(schedule, require_regular_season=True)
        self.assertEqual(out, [1])


if __name__ == "__main__":
    unittest.main()

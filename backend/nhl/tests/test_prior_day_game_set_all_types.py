"""Prior-day game sets include every game type and use the date's season (P2.24 E)."""
import importlib
import os
import sys
import unittest
from unittest.mock import MagicMock, patch


def _capture_query(module, function, *args):
    cursor = MagicMock()
    cursor.fetchall.return_value = [(2027030111,)]
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
    with patch.object(module.psycopg, "connect", return_value=connection):
        result = function(*args)
    sql, params = cursor.execute.call_args_list[-1].args
    return result, sql, params


class PriorDayGameSetTests(unittest.TestCase):
    def test_cross_market_prior_day_query_matches_reconciliation_admission(self):
        env = {"NHL_DB_ACCESS_MODE": "legacy",
               "SUPABASE_DB_URL": "postgresql://placeholder@db.invalid/postgres"}
        with patch.dict(os.environ, env):
            module = importlib.import_module(
                "backend.nhl.scripts.run_nhl_mainline_cross_market_capture_warn_only")
        result, sql, params = _capture_query(
            module, module._prior_day_regular_season_game_ids, "dsn", "2027-04-25")
        self.assertEqual(result, [2027030111])
        self.assertNotIn("game_type", sql)  # postseason (3) and preseason (1) included
        self.assertEqual(params, (2026, "2027-04-25"))  # April belongs to the 2026 season
        _, _, params = _capture_query(
            module, module._prior_day_regular_season_game_ids, "dsn", "2027-10-12")
        self.assertEqual(params, (2027, "2027-10-12"))


if __name__ == "__main__":
    unittest.main()

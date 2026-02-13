import unittest
from unittest.mock import patch

from backend.domains.nhl.repository.queries import (
    fetch_games_today,
    fetch_props_today,
    fetch_saves,
    fetch_sog,
)


class TestNhlRepositoryQueries(unittest.TestCase):
    def test_fetch_games_today_invalid_date(self):
        out = fetch_games_today("2026-99-99", limit=25, offset=0)
        self.assertFalse(out["ok"])
        self.assertIn("invalid date format", out["error"])

    @patch("backend.domains.nhl.repository.queries.pg_fetchall")
    def test_fetch_games_today_success(self, mock_fetchall):
        mock_fetchall.return_value = [{"game_id": 1}, {"game_id": 2}]
        out = fetch_games_today("2025-11-20", limit=25, offset=0)
        self.assertTrue(out["ok"])
        self.assertEqual(out["date"], "2025-11-20")
        self.assertEqual(out["count"], 2)
        self.assertEqual(len(out["rows"]), 2)
        mock_fetchall.assert_called_once()

    @patch("backend.domains.nhl.repository.queries.pg_fetchall")
    def test_fetch_props_today_db_error(self, mock_fetchall):
        mock_fetchall.side_effect = RuntimeError("db down")
        out = fetch_props_today("2025-11-20", limit=25, offset=0)
        self.assertFalse(out["ok"])
        self.assertIn("RuntimeError: db down", out["error"])

    @patch("backend.domains.nhl.repository.queries.pg_fetchall")
    def test_fetch_sog_forwards_date_and_paging(self, mock_fetchall):
        mock_fetchall.return_value = [{"player_id": 1}]
        out = fetch_sog("2025-11-20", limit=10, offset=5)
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 1)
        _sql, params = mock_fetchall.call_args.args
        self.assertEqual(params, ("2025-11-20", "2025-11-20", 10, 5))

    @patch("backend.domains.nhl.repository.queries.pg_fetchall")
    def test_fetch_saves_db_error(self, mock_fetchall):
        mock_fetchall.side_effect = ValueError("bad query")
        out = fetch_saves("2025-11-20", limit=10, offset=0)
        self.assertFalse(out["ok"])
        self.assertIn("ValueError: bad query", out["error"])


if __name__ == "__main__":
    unittest.main()

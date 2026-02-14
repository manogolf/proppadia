import unittest
from unittest.mock import patch

from backend.app.services.mlb.standings_service import (
    _standings_cache,
    fetch_standings,
    get_standings,
)


class TestMlbStandingsService(unittest.TestCase):
    def setUp(self):
        _standings_cache.clear()

    @patch("backend.app.services.mlb.standings_service.requests.get")
    def test_fetch_standings_builds_expected_url(self, mock_get):
        class _Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"records": []}

        mock_get.return_value = _Resp()
        out = fetch_standings(season=2025, league_ids="103,104")
        self.assertIn("records", out)
        called_url = mock_get.call_args.args[0]
        self.assertIn("api/v1/standings", called_url)
        self.assertIn("leagueId=103,104", called_url)
        self.assertIn("season=2025", called_url)

    @patch("backend.app.services.mlb.standings_service.fetch_standings")
    def test_get_standings_uses_cache_after_first_fetch(self, mock_fetch):
        mock_fetch.return_value = {"records": [{"teamRecords": []}]}
        out1 = get_standings(season=2025, league_ids="103,104")
        out2 = get_standings(season=2025, league_ids="103,104")
        self.assertTrue(out1["ok"])
        self.assertEqual(out1["source"], "upstream")
        self.assertEqual(out2["source"], "cache")
        self.assertFalse(out2["stale"])
        self.assertIsInstance(out2.get("records"), list)
        self.assertEqual(mock_fetch.call_count, 1)

    @patch("backend.app.services.mlb.standings_service.fetch_standings")
    def test_get_standings_returns_stale_cache_on_upstream_error(self, mock_fetch):
        mock_fetch.return_value = {"records": [{"teamRecords": []}]}
        first = get_standings(season=2025, league_ids="103,104")
        self.assertEqual(first["source"], "upstream")

        mock_fetch.side_effect = RuntimeError("boom")
        with patch("backend.app.services.mlb.standings_service.STANDINGS_CACHE_TTL_SECONDS", 0):
            second = get_standings(season=2025, league_ids="103,104")
        self.assertTrue(second["ok"])
        self.assertEqual(second["source"], "stale_cache")
        self.assertTrue(second["stale"])
        self.assertIn("warning", second)


if __name__ == "__main__":
    unittest.main()

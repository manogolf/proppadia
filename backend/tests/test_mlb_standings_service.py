import unittest
from unittest.mock import patch

from backend.app.services.mlb.standings_service import fetch_standings


class TestMlbStandingsService(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()


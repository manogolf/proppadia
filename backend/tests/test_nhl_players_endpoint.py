import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestNhlPlayersEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.nhl.fetch_players_directory")
    def test_players_directory_ok(self, mock_fetch):
        mock_fetch.return_value = [
            {
                "player_id": 8478402,
                "player_name": "Connor McDavid",
                "team_abbr": "EDM",
                "team": "EDM",
                "position": "F",
                "status": "active",
                "last_prop_date": "2026-02-13",
            }
        ]
        resp = self.client.get("/api/nhl/players?limit=5&offset=10&include_inactive=true")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(len(body), 1)
        self.assertEqual(body[0].get("team_abbr"), "EDM")
        mock_fetch.assert_called_once_with(5, 10, True)


if __name__ == "__main__":
    unittest.main()

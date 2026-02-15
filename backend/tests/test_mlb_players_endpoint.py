import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestMlbPlayersEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.mlb.list_players_mlb")
    def test_mlb_players_list_ok(self, mock_list):
        mock_list.return_value = [
            {
                "player_id": 660271,
                "player_name": "Shohei Ohtani",
                "team": "LAD",
                "last_prop_date": "2025-08-15",
            }
        ]
        resp = self.client.get("/api/mlb/players?limit=5")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(len(body), 1)
        self.assertEqual(body[0].get("player_name"), "Shohei Ohtani")
        mock_list.assert_called_once_with(limit=5)


if __name__ == "__main__":
    unittest.main()

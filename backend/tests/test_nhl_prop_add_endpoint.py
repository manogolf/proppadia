import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestNhlPropAddEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.nhl.add_prop")
    def test_props_add_ok(self, mock_add):
        mock_add.return_value = {"ok": True, "saved": True, "duplicate": False}
        resp = self.client.post(
            "/api/nhl/props/add",
            json={
                "player_id": 8478402,
                "game_id": 2025020011,
                "prop_type": "sog",
                "prop_value": 3.5,
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("saved"))
        payload = mock_add.call_args.args[0]
        self.assertEqual(payload.get("player_id"), 8478402)
        self.assertEqual(payload.get("game_id"), 2025020011)


if __name__ == "__main__":
    unittest.main()

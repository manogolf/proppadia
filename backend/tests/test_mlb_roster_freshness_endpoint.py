import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestMlbRosterFreshnessEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.mlb.get_roster_freshness")
    def test_roster_freshness_ok(self, mock_get):
        mock_get.return_value = {"ok": True, "status": "pass", "total_players": 1200}
        resp = self.client.get("/api/mlb/roster-freshness?stale_after_hours=24&require_min=100")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json().get("ok"), True)
        mock_get.assert_called_once_with(require_min=100, stale_after_hours=24)

    @patch("backend.app.routers.mlb.get_roster_freshness")
    def test_roster_freshness_handles_service_error(self, mock_get):
        mock_get.side_effect = RuntimeError("boom")
        resp = self.client.get("/api/mlb/roster-freshness")
        self.assertEqual(resp.status_code, 500)
        self.assertIn("RuntimeError", resp.json().get("detail", ""))


if __name__ == "__main__":
    unittest.main()


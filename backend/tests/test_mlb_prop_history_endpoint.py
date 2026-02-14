import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestMlbPropHistoryEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.mlb.get_prop_history")
    def test_props_history_ok(self, mock_history):
        mock_history.return_value = {
            "ok": True,
            "count": 1,
            "total": 7,
            "limit": 10,
            "offset": 0,
            "rows": [{"id": "abc-1"}],
        }
        resp = self.client.get("/api/props/history?limit=10&offset=0&user_id=user-1&status=pending")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get("ok"), True)
        self.assertEqual(body.get("count"), 1)
        self.assertEqual(body.get("total"), 7)
        self.assertEqual(body.get("rows", [{}])[0].get("id"), "abc-1")
        kwargs = mock_history.call_args.args[0]
        self.assertEqual(kwargs.get("limit"), 10)
        self.assertEqual(kwargs.get("offset"), 0)
        self.assertEqual(kwargs.get("user_id"), "user-1")
        self.assertEqual(kwargs.get("status"), "pending")

    def test_props_history_rejects_bad_date(self):
        resp = self.client.get("/api/props/history?from_date=2026-99-99")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("from_date must be YYYY-MM-DD", str(resp.json().get("detail")))


if __name__ == "__main__":
    unittest.main()

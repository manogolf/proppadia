import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestNhlSlateMetaEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.nhl.get_nhl_slate_meta")
    def test_slate_meta_ok(self, mock_meta):
        mock_meta.return_value = {
            "ok": True,
            "date": "2025-11-20",
            "components": {
                "games_today": {"ok": True, "count": 1, "error": None},
                "props_today": {"ok": True, "count": 2, "error": None},
                "sog": {"ok": True, "count": 3, "error": None},
                "saves": {"ok": True, "count": 4, "error": None},
            },
            "all_ok": True,
            "source": "upstream",
            "stale": False,
            "cached_at": "2026-02-14T12:00:00-05:00",
            "ttl_seconds": 300,
        }
        resp = self.client.get("/api/nhl/slate/meta?date=2025-11-20&limit=100")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("source"), "upstream")
        self.assertIn("components", body)
        kwargs = mock_meta.call_args.kwargs
        self.assertEqual(kwargs.get("date"), "2025-11-20")
        self.assertEqual(kwargs.get("limit"), 100)

    @patch("backend.app.routers.nhl.get_nhl_slate_meta")
    def test_slate_meta_bad_date(self, mock_meta):
        mock_meta.side_effect = ValueError("date must be YYYY-MM-DD")
        resp = self.client.get("/api/nhl/slate/meta?date=bad-date")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("YYYY-MM-DD", str(resp.json().get("detail")))


if __name__ == "__main__":
    unittest.main()


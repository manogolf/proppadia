import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestSharedPingEndpoints(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_mlb_ping(self):
        resp = self.client.get("/api/mlb/ping")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"sport": "mlb", "ok": True})

    def test_nhl_ping(self):
        resp = self.client.get("/api/nhl/ping")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"sport": "nhl", "ok": True})

    def test_mlb_ping_db(self):
        with patch(
            "backend.app.services.shared.db_health_service.pg_fetchone",
            return_value=(True, {"ok": 1}, None),
        ):
            resp = self.client.get("/api/mlb/ping-db")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get("ok"), True)
        self.assertIsNone(body.get("err"))

    def test_nhl_ping_db(self):
        with patch(
            "backend.app.services.shared.db_health_service.pg_fetchone",
            return_value=(False, None, "OperationalError: boom"),
        ):
            resp = self.client.get("/api/nhl/ping-db")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body.get("ok"), False)
        self.assertIn("OperationalError", str(body.get("err")))


if __name__ == "__main__":
    unittest.main()

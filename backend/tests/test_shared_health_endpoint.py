import unittest

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestSharedHealthEndpoint(unittest.TestCase):
    def test_health_ok(self):
        client = TestClient(app)
        resp = client.get("/api/health")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIs(body["ok"], True)
        self.assertIn(body["db_route"], {"API_LOGIN", "LEGACY_ALIASES"})


if __name__ == "__main__":
    unittest.main()

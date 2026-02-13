import unittest

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestSharedHealthEndpoint(unittest.TestCase):
    def test_health_ok(self):
        client = TestClient(app)
        resp = client.get("/api/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestNhlPropDeleteEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.nhl.delete_prop")
    def test_props_delete_ok(self, mock_delete):
        mock_delete.return_value = {"ok": True, "deleted": True, "id": "nhl-row-1"}
        resp = self.client.post(
            "/api/nhl/props/delete",
            json={"id": "nhl-row-1", "user_id": "user-1"},
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("deleted"))
        self.assertEqual(body.get("id"), "nhl-row-1")
        kwargs = mock_delete.call_args.args[0]
        self.assertEqual(kwargs.get("id"), "nhl-row-1")
        self.assertEqual(kwargs.get("user_id"), "user-1")

    def test_props_delete_missing_id(self):
        resp = self.client.post("/api/nhl/props/delete", json={"user_id": "user-1"})
        self.assertEqual(resp.status_code, 422)


if __name__ == "__main__":
    unittest.main()

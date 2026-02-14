import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestOpsRouter(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_deploy_status_requires_token(self):
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.get("/api/ops/render/deploy-status")
        self.assertEqual(resp.status_code, 403)

    def test_deploy_status_requires_configured_token(self):
        with patch.dict(os.environ, {"OPS_API_TOKEN": ""}, clear=False):
            resp = self.client.get("/api/ops/render/deploy-status", headers={"X-Ops-Token": "secret"})
        self.assertEqual(resp.status_code, 503)

    @patch("backend.app.routers.ops.fetch_latest_deploy")
    def test_deploy_status_ok(self, mock_status):
        mock_status.return_value = {"ok": True, "deploy": {"status": "live"}}
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.get("/api/ops/render/deploy-status", headers={"X-Ops-Token": "secret"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json().get("ok"), True)

    @patch("backend.app.routers.ops.trigger_redeploy")
    def test_redeploy_ok(self, mock_redeploy):
        mock_redeploy.return_value = {"ok": True, "deploy": {"status": "build_in_progress"}}
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.post(
                "/api/ops/render/redeploy",
                headers={"X-Ops-Token": "secret"},
                json={"clear_cache": True},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json().get("ok"), True)
        self.assertEqual(mock_redeploy.call_args.kwargs.get("clear_cache"), True)


if __name__ == "__main__":
    unittest.main()

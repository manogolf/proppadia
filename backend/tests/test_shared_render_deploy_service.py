import unittest
from unittest.mock import patch

from backend.app.services.shared.render_deploy_service import fetch_latest_deploy


class TestSharedRenderDeployService(unittest.TestCase):
    @patch("backend.app.services.shared.render_deploy_service._render_env")
    @patch("backend.app.services.shared.render_deploy_service._request")
    def test_fetch_latest_deploy_supports_list_shape(self, mock_request, mock_env):
        mock_env.return_value = ("key", "svc")
        mock_request.return_value = [
            {"id": "dep_1", "status": "building", "createdAt": "2026-02-14T00:00:00Z"}
        ]
        out = fetch_latest_deploy()
        self.assertTrue(out["ok"])
        self.assertEqual(out["service_id"], "svc")
        self.assertEqual(out["deploy"]["id"], "dep_1")
        self.assertEqual(out["deploy"]["status"], "building")

    @patch("backend.app.services.shared.render_deploy_service._render_env")
    @patch("backend.app.services.shared.render_deploy_service._request")
    def test_fetch_latest_deploy_supports_wrapped_shape(self, mock_request, mock_env):
        mock_env.return_value = ("key", "svc")
        mock_request.return_value = {
            "deploys": [
                {"id": "dep_2", "status": "live", "createdAt": "2026-02-14T00:00:00Z"}
            ]
        }
        out = fetch_latest_deploy()
        self.assertTrue(out["ok"])
        self.assertEqual(out["deploy"]["id"], "dep_2")
        self.assertEqual(out["deploy"]["status"], "live")

    @patch("backend.app.services.shared.render_deploy_service._render_env")
    @patch("backend.app.services.shared.render_deploy_service._request")
    def test_fetch_latest_deploy_supports_nested_deploy_in_list(self, mock_request, mock_env):
        mock_env.return_value = ("key", "svc")
        mock_request.return_value = [
            {
                "deploy": {
                    "id": "dep_3",
                    "status": "build_in_progress",
                    "createdAt": "2026-02-14T00:00:00Z",
                }
            }
        ]
        out = fetch_latest_deploy()
        self.assertTrue(out["ok"])
        self.assertEqual(out["deploy"]["id"], "dep_3")
        self.assertEqual(out["deploy"]["status"], "build_in_progress")

    @patch("backend.app.services.shared.render_deploy_service._render_env")
    @patch("backend.app.services.shared.render_deploy_service._request")
    def test_fetch_latest_deploy_supports_direct_nested_deploy_shape(self, mock_request, mock_env):
        mock_env.return_value = ("key", "svc")
        mock_request.return_value = {
            "deploy": {"id": "dep_4", "status": "live", "createdAt": "2026-02-14T00:00:00Z"}
        }
        out = fetch_latest_deploy()
        self.assertTrue(out["ok"])
        self.assertEqual(out["deploy"]["id"], "dep_4")
        self.assertEqual(out["deploy"]["status"], "live")


if __name__ == "__main__":
    unittest.main()

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

    @patch("backend.app.routers.ops.fetch_service_metrics")
    def test_metrics_ok(self, mock_metrics):
        mock_metrics.return_value = {"ok": True, "cpu": {"latest_value": 0.1}, "memory": {"latest_value": 123}}
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.get(
                "/api/ops/render/metrics?window_minutes=180&resolution_seconds=120",
                headers={"X-Ops-Token": "secret"},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json().get("ok"), True)
        self.assertEqual(mock_metrics.call_args.kwargs.get("window_minutes"), 180)
        self.assertEqual(mock_metrics.call_args.kwargs.get("resolution_seconds"), 120)

    @patch("backend.app.routers.ops.resolve_nhl_pending_props")
    def test_resolve_nhl_props_dry_run_ok(self, mock_resolve):
        mock_resolve.return_value = {"ok": True, "dry_run": True, "matched": 12, "updated": 0}
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.post(
                "/api/ops/nhl/resolve-props",
                headers={"X-Ops-Token": "secret"},
                json={"from_date": "2026-01-01", "to_date": "2026-01-31", "dry_run": True},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json().get("ok"))
        self.assertTrue(resp.json().get("dry_run"))
        self.assertEqual(mock_resolve.call_args.kwargs.get("from_date"), "2026-01-01")

    def test_resolve_nhl_props_rejects_bad_date(self):
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.post(
                "/api/ops/nhl/resolve-props",
                headers={"X-Ops-Token": "secret"},
                json={"from_date": "2026-99-99"},
            )
        self.assertEqual(resp.status_code, 400)

    @patch("backend.app.routers.ops.start_prod12_cycle")
    def test_trigger_mlb_prod12_cycle_ok(self, mock_start):
        mock_start.return_value = {"ok": True, "status": "running", "running": True, "run_id": "abc123"}
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.post(
                "/api/ops/mlb/prod12/trigger",
                headers={"X-Ops-Token": "secret"},
                json={
                    "mlb_date": "2025-08-15",
                    "mlb_prod12_daily_prop_types": "hits,total_bases,strikeouts_batting",
                    "mlb_replay_retry_attempts": 8,
                    "mlb_replay_retry_backoff_ms": 1500,
                },
            )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json().get("ok"))
        env_overrides = mock_start.call_args.kwargs.get("env_overrides") or {}
        self.assertEqual(env_overrides.get("MLB_CRON_RUN_MODE"), "daily")
        self.assertEqual(env_overrides.get("MLB_DATE"), "2025-08-15")
        self.assertEqual(
            env_overrides.get("MLB_PROD12_DAILY_PROP_TYPES"),
            "hits,total_bases,strikeouts_batting",
        )
        self.assertEqual(env_overrides.get("MLB_REPLAY_RETRY_ATTEMPTS"), 8)

    @patch("backend.app.routers.ops.start_prod12_cycle")
    def test_trigger_mlb_prod12_cycle_conflict(self, mock_start):
        mock_start.return_value = {
            "ok": False,
            "status": "already_running",
            "running": True,
            "run_id": "abc123",
            "pid": 999,
        }
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.post(
                "/api/ops/mlb/prod12/trigger",
                headers={"X-Ops-Token": "secret"},
                json={},
            )
        self.assertEqual(resp.status_code, 409)
        self.assertIn("already_running", str(resp.json()))

    @patch("backend.app.routers.ops.get_prod12_cycle_status")
    def test_mlb_prod12_status_ok(self, mock_status):
        mock_status.return_value = {"ok": True, "status": "running", "running": True, "log_tail": []}
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.get("/api/ops/mlb/prod12/status?tail_lines=25", headers={"X-Ops-Token": "secret"})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json().get("ok"))
        self.assertEqual(mock_status.call_args.kwargs.get("tail_lines"), 25)

    def test_trigger_mlb_prod12_cycle_rejects_bad_weekday(self):
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.post(
                "/api/ops/mlb/prod12/trigger",
                headers={"X-Ops-Token": "secret"},
                json={"run_mode": "auto", "weekly_day_utc": 9},
            )
        self.assertEqual(resp.status_code, 400)


if __name__ == "__main__":
    unittest.main()

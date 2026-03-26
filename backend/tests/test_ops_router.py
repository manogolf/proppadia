import os
import tempfile
import unittest
from pathlib import Path
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
                    "mlb_daily_stat_derived_enabled": 1,
                    "mlb_stat_days_ago": 2,
                    "mlb_stat_from_date": "2025-08-01",
                    "mlb_stat_to_date": "2025-08-15",
                    "mlb_stat_max_games": 0,
                    "mlb_stat_skip_existing_dates": 1,
                    "mlb_stat_derived_days": 7,
                    "mlb_stat_derived_min": 0,
                    "mlb_season_require_regular": 0,
                    "mlb_prod12_daily_prop_types": "hits,total_bases,strikeouts_batting",
                    "mlb_replay_retry_attempts": 8,
                    "mlb_replay_retry_backoff_ms": 1500,
                    "mlb_weekly_prop_sequence_enabled": 1,
                    "mlb_weekly_prop_sequence": "hits,runs_scored",
                    "mlb_weekly_prop_sequence_continue_on_error": 1,
                    "mlb_weekly_prop_sequence_sleep_sec": 8,
                    "mlb_candidate_min_total": 0,
                    "mlb_prod12_min_lift_pct": -5,
                    "mlb_prod12_max_prop_drop_pct": 3.5,
                },
            )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json().get("ok"))
        env_overrides = mock_start.call_args.kwargs.get("env_overrides") or {}
        self.assertEqual(env_overrides.get("MLB_CRON_RUN_MODE"), "daily")
        self.assertEqual(env_overrides.get("MLB_DATE"), "2025-08-15")
        self.assertEqual(env_overrides.get("MLB_DAILY_STAT_DERIVED_ENABLED"), 1)
        self.assertEqual(env_overrides.get("MLB_STAT_DAYS_AGO"), 2)
        self.assertEqual(env_overrides.get("MLB_STAT_FROM_DATE"), "2025-08-01")
        self.assertEqual(env_overrides.get("MLB_STAT_TO_DATE"), "2025-08-15")
        self.assertEqual(env_overrides.get("MLB_STAT_MAX_GAMES"), 0)
        self.assertEqual(env_overrides.get("MLB_STAT_SKIP_EXISTING_DATES"), 1)
        self.assertEqual(env_overrides.get("MLB_STAT_DERIVED_DAYS"), 7)
        self.assertEqual(env_overrides.get("MLB_STAT_DERIVED_MIN"), 0)
        self.assertEqual(env_overrides.get("MLB_SEASON_REQUIRE_REGULAR"), 0)
        self.assertEqual(
            env_overrides.get("MLB_PROD12_DAILY_PROP_TYPES"),
            "hits,total_bases,strikeouts_batting",
        )
        self.assertEqual(env_overrides.get("MLB_REPLAY_RETRY_ATTEMPTS"), 8)
        self.assertEqual(env_overrides.get("MLB_WEEKLY_PROP_SEQUENCE_ENABLED"), 1)
        self.assertEqual(env_overrides.get("MLB_WEEKLY_PROP_SEQUENCE"), "hits,runs_scored")
        self.assertEqual(env_overrides.get("MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR"), 1)
        self.assertEqual(env_overrides.get("MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC"), 8)
        self.assertEqual(env_overrides.get("MLB_CANDIDATE_MIN_TOTAL"), 0)
        self.assertEqual(env_overrides.get("MLB_PROD12_MIN_LIFT_PCT"), -5.0)
        self.assertEqual(env_overrides.get("MLB_PROD12_MAX_PROP_DROP_PCT"), 3.5)

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

    @patch("backend.app.routers.ops.resolve_prod12_artifact")
    def test_mlb_prod12_artifact_ok(self, mock_artifact):
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8") as fh:
            fh.write("player_name,prop_line\nExample,2.5\n")
            csv_path = Path(fh.name)
        self.addCleanup(lambda: csv_path.unlink(missing_ok=True))
        mock_artifact.return_value = {
            "kind": "book_upload",
            "mlb_date": "2026-03-26",
            "path": csv_path,
            "exists": True,
            "size_bytes": csv_path.stat().st_size,
        }
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.get(
                "/api/ops/mlb/prod12/artifact?kind=book_upload&mlb_date=2026-03-26",
                headers={"X-Ops-Token": "secret"},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("mlb_book_upload_2026-03-26.csv", resp.headers.get("content-disposition", ""))
        self.assertIn("player_name,prop_line", resp.text)

    @patch("backend.app.routers.ops.resolve_prod12_artifact")
    def test_mlb_prod12_artifact_missing(self, mock_artifact):
        missing_path = Path("/tmp/does_not_exist_mlb_book_upload.csv")
        mock_artifact.return_value = {
            "kind": "book_upload",
            "mlb_date": "2026-03-26",
            "path": missing_path,
            "exists": False,
            "size_bytes": None,
        }
        with patch.dict(os.environ, {"OPS_API_TOKEN": "secret"}, clear=False):
            resp = self.client.get(
                "/api/ops/mlb/prod12/artifact?kind=book_upload&mlb_date=2026-03-26",
                headers={"X-Ops-Token": "secret"},
            )
        self.assertEqual(resp.status_code, 404)

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

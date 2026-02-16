import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from backend.scripts import ops_operator_incident as incident


class TestSharedOpsOperatorIncident(unittest.TestCase):
    def test_collect_incident_snapshot_shape(self):
        with tempfile.TemporaryDirectory() as td:
            history_path = Path(td) / "ops_history.jsonl"
            history_path.write_text(
                json.dumps(
                    {
                        "captured_at": "2026-02-15T09:00:00+00:00",
                        "ok": True,
                        "status": "pass",
                        "governance": {"ok": True},
                        "mlb_readiness": {"ok": True, "stat_count": 10, "roster_stale": False},
                        "season_activation": {"ok": True, "blocker_count": 0, "top_blocker": None},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.object(
                incident.ops_operator_summary,
                "collect_summary",
                return_value={
                    "ok": True,
                    "status": "pass",
                    "governance": {"ok": True, "status": "pass", "governance_ok": True, "season_activation_ok": True},
                    "mlb_readiness": {"ok": True, "status": "pass", "checks": {}},
                    "season_activation_report": {"ok": True, "status": "pass", "season_activation": {"blockers": []}},
                },
            ):
                payload = incident.collect_incident_snapshot(
                    stat_days=30,
                    stat_require_min=0,
                    roster_require_min=1,
                    roster_stale_hours=30,
                    season_history_input="artifacts/season_activation_history.jsonl",
                    season_history_limit=10,
                    season_max_age_hours=0,
                    ops_history_input=str(history_path),
                    ops_history_limit=5,
                    pipeline_history_input="artifacts/mlb_pipeline_history.jsonl",
                    pipeline_history_limit=5,
                )
        self.assertTrue(payload["ok"])
        self.assertIn("captured_at", payload)
        self.assertIn("summary", payload)
        self.assertIn("history_tail", payload)
        self.assertTrue(payload["history_available"])
        self.assertIn("pipeline_history_available", payload)
        self.assertIn("pipeline_history_tail", payload)
        self.assertEqual(payload["latest_regressions"], [])
        self.assertFalse(payload["regressed"])
        self.assertEqual(payload["history_tail"]["returned"], 1)
        self.assertEqual(payload["history_tail"]["rows"][0]["captured_at"], "2026-02-15T09:00:00+00:00")

    def test_main_strict_returns_nonzero_on_fail(self):
        with patch.object(
            incident,
            "collect_incident_snapshot",
            return_value={
                "ok": False,
                "status": "fail",
                "history_available": False,
                "latest_regressions": [],
                "regressed": False,
                "summary": {},
                "history_tail": {},
            },
        ):
            rc = incident.main(["--strict"])
        self.assertEqual(rc, 1)

    def test_main_outputs_json(self):
        with patch.object(
            incident,
            "collect_incident_snapshot",
            return_value={
                "ok": True,
                "status": "pass",
                "history_available": True,
                "latest_regressions": [],
                "regressed": False,
                "summary": {"ok": True},
                "history_tail": {"rows": []},
            },
        ):
            out = StringIO()
            with redirect_stdout(out):
                rc = incident.main([])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["status"], "pass")

    def test_collect_incident_snapshot_surfaces_latest_regressions(self):
        with tempfile.TemporaryDirectory() as td:
            history_path = Path(td) / "ops_history.jsonl"
            rows = [
                {
                    "captured_at": "2026-02-15T09:00:00+00:00",
                    "ok": True,
                    "status": "pass",
                    "governance": {"ok": True},
                    "mlb_readiness": {"ok": True, "stat_count": 100, "roster_stale": False},
                    "season_activation": {"ok": True, "blocker_count": 0, "top_blocker": None},
                },
                {
                    "captured_at": "2026-02-15T10:00:00+00:00",
                    "ok": False,
                    "status": "fail",
                    "governance": {"ok": False},
                    "mlb_readiness": {"ok": False, "stat_count": 80, "roster_stale": True},
                    "season_activation": {"ok": False, "blocker_count": 2, "top_blocker": "missing"},
                },
            ]
            history_path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
            with patch.object(
                incident.ops_operator_summary,
                "collect_summary",
                return_value={
                    "ok": False,
                    "status": "fail",
                    "governance": {"ok": False, "status": "fail", "governance_ok": False, "season_activation_ok": False},
                    "mlb_readiness": {"ok": False, "status": "fail", "checks": {}},
                    "season_activation_report": {"ok": False, "status": "fail", "season_activation": {"blockers": ["missing"]}},
                },
            ):
                payload = incident.collect_incident_snapshot(
                    stat_days=30,
                    stat_require_min=0,
                    roster_require_min=1,
                    roster_stale_hours=30,
                    season_history_input="artifacts/season_activation_history.jsonl",
                    season_history_limit=10,
                    season_max_age_hours=0,
                    ops_history_input=str(history_path),
                    ops_history_limit=5,
                    pipeline_history_input="artifacts/mlb_pipeline_history.jsonl",
                    pipeline_history_limit=5,
                )
        self.assertTrue(payload["history_available"])
        self.assertTrue(payload["regressed"])
        self.assertIn("overall_became_fail", payload["latest_regressions"])


if __name__ == "__main__":
    unittest.main()

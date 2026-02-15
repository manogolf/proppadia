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
                )
        self.assertTrue(payload["ok"])
        self.assertIn("summary", payload)
        self.assertIn("history_tail", payload)
        self.assertEqual(payload["history_tail"]["returned"], 1)

    def test_main_strict_returns_nonzero_on_fail(self):
        with patch.object(
            incident,
            "collect_incident_snapshot",
            return_value={"ok": False, "status": "fail", "summary": {}, "history_tail": {}},
        ):
            rc = incident.main(["--strict"])
        self.assertEqual(rc, 1)

    def test_main_outputs_json(self):
        with patch.object(
            incident,
            "collect_incident_snapshot",
            return_value={"ok": True, "status": "pass", "summary": {"ok": True}, "history_tail": {"rows": []}},
        ):
            out = StringIO()
            with redirect_stdout(out):
                rc = incident.main([])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["status"], "pass")


if __name__ == "__main__":
    unittest.main()


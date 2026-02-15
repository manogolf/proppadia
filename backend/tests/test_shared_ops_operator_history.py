import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from backend.scripts import ops_operator_last as oplast
from backend.scripts import ops_operator_log as oplog


class TestSharedOpsOperatorHistory(unittest.TestCase):
    def test_log_appends_compact_row(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "ops.jsonl"
            with patch.object(
                oplog,
                "collect_summary",
                return_value={
                    "ok": True,
                    "status": "pass",
                    "governance": {"ok": True, "status": "pass", "governance_ok": True, "season_activation_ok": True},
                    "mlb_readiness": {"ok": True, "status": "pass", "checks": {}},
                    "season_activation_report": {"ok": True, "status": "pass", "season_activation": {"blockers": []}},
                },
            ):
                rc = oplog.main(["--output", str(out)])
            self.assertEqual(rc, 0)
            rows = out.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 1)
            payload = json.loads(rows[0])
            self.assertTrue(payload["ok"])
            self.assertIn("mlb_readiness", payload)

    def test_last_json_reports_regressions(self):
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "ops.jsonl"
            rows = [
                {
                    "captured_at": "2026-02-15T08:00:00+00:00",
                    "ok": True,
                    "status": "pass",
                    "governance": {"ok": True},
                    "mlb_readiness": {"ok": True, "stat_count": 100, "roster_stale": False},
                    "season_activation": {"ok": True, "blocker_count": 0, "top_blocker": None},
                },
                {
                    "captured_at": "2026-02-15T09:00:00+00:00",
                    "ok": False,
                    "status": "fail",
                    "governance": {"ok": False},
                    "mlb_readiness": {"ok": False, "stat_count": 80, "roster_stale": True},
                    "season_activation": {"ok": False, "blocker_count": 2, "top_blocker": "missing"},
                },
            ]
            inp.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
            out = StringIO()
            with redirect_stdout(out):
                rc = oplast.main(["--input", str(inp), "--json", "--limit", "2"])
            self.assertEqual(rc, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["history_count"], 2)
            self.assertEqual(payload["rows"][0]["captured_at"], "2026-02-15T08:00:00+00:00")
            self.assertIn("overall_became_fail", payload["rows"][1]["regressions"])


if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from backend.scripts import mlb_pipeline_last
from backend.scripts import mlb_pipeline_log


class TestSharedMlbPipelineHistory(unittest.TestCase):
    def test_log_appends_jsonl_row(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "history.jsonl"
            with patch.object(
                mlb_pipeline_log.mlb_pipeline_check,
                "collect_pipeline_check",
                return_value={
                    "captured_at": "2026-02-16T00:00:00+00:00",
                    "ok": True,
                    "status": "pass",
                    "failures": [],
                    "checks": [],
                },
            ):
                rc = mlb_pipeline_log.main(["--output", str(out)])
            self.assertEqual(rc, 0)
            rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 1)
            self.assertTrue(rows[0]["ok"])

    def test_last_json_reports_regressions(self):
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "history.jsonl"
            rows = [
                {
                    "captured_at": "2026-02-16T00:00:00+00:00",
                    "ok": True,
                    "status": "pass",
                    "failures": [],
                    "checks": [],
                },
                {
                    "captured_at": "2026-02-16T01:00:00+00:00",
                    "ok": False,
                    "status": "fail",
                    "failures": ["prediction_gate"],
                    "checks": [],
                },
            ]
            inp.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
            out = StringIO()
            with redirect_stdout(out):
                rc = mlb_pipeline_last.main(["--input", str(inp), "--json"])
            self.assertEqual(rc, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["returned"], 2)
            self.assertIn("overall_became_fail", payload["rows"][1]["regressions"])


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from backend.scripts import mlb_readiness_last
from backend.scripts import mlb_readiness_log


class TestSharedMlbReadinessHistory(unittest.TestCase):
    def test_log_appends_jsonl_row(self):
        with TemporaryDirectory() as tmp:
            out_file = Path(tmp) / "hist.jsonl"
            with patch.object(
                mlb_readiness_log,
                "collect_snapshot",
                return_value={
                    "captured_at": "2026-02-15T00:00:00+00:00",
                    "status": "pass",
                    "ok": True,
                    "checks": {},
                    "errors": {},
                },
            ):
                with redirect_stdout(StringIO()):
                    rc = mlb_readiness_log.main(["--output", str(out_file)])
            self.assertEqual(rc, 0)
            lines = out_file.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            payload = json.loads(lines[0])
            self.assertEqual(payload["status"], "pass")

    def test_last_json_reports_regressions(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "hist.jsonl"
            rows = [
                {
                    "captured_at": "2026-02-15T01:00:00+00:00",
                    "status": "pass",
                    "checks": {
                        "stat_derived": {"count": 100},
                        "roster": {"total_players": 1200, "stale": False},
                    },
                },
                {
                    "captured_at": "2026-02-15T02:00:00+00:00",
                    "status": "fail",
                    "checks": {
                        "stat_derived": {"count": 90},
                        "roster": {"total_players": 1100, "stale": True},
                    },
                },
            ]
            with path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")

            out = StringIO()
            with redirect_stdout(out):
                rc = mlb_readiness_last.main(["--input", str(path), "--json", "--limit", "2"])
            self.assertEqual(rc, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["history_count"], 2)
            self.assertEqual(payload["returned"], 2)
            regs = payload["rows"][1]["regressions"]
            self.assertIn("stat_derived_count_drop:100->90", regs)
            self.assertIn("roster_total_drop:1200->1100", regs)
            self.assertIn("roster_became_stale", regs)


if __name__ == "__main__":
    unittest.main()

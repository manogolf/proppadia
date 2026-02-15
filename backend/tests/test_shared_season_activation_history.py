import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from backend.scripts import season_activation_last
from backend.scripts import season_activation_log


class TestSharedSeasonActivationHistory(unittest.TestCase):
    def test_log_appends_jsonl_row(self):
        with TemporaryDirectory() as tmp:
            out_file = Path(tmp) / "season_activation_history.jsonl"
            with patch.object(
                season_activation_log,
                "build_status",
                return_value={
                    "ok": False,
                    "status": "fail",
                    "phase6_tracker": [],
                    "baseline_artifacts": {"has_mlb": False, "has_nhl": False},
                    "readiness": {"ready": False, "blockers": ["phase_6_1_incomplete"]},
                    "next_steps": [],
                },
            ):
                with redirect_stdout(StringIO()):
                    rc = season_activation_log.main(["--output", str(out_file)])
            self.assertEqual(rc, 1)
            lines = out_file.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            payload = json.loads(lines[0])
            self.assertEqual(payload["status"], "fail")

    def test_last_json_reports_new_blockers(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "season_activation_history.jsonl"
            rows = [
                {
                    "ok": False,
                    "status": "fail",
                    "phase6_tracker": [],
                    "baseline_artifacts": {"has_mlb": False, "has_nhl": False},
                    "readiness": {"ready": False, "blockers": ["phase_6_1_incomplete"]},
                },
                {
                    "ok": False,
                    "status": "fail",
                    "phase6_tracker": [],
                    "baseline_artifacts": {"has_mlb": True, "has_nhl": False},
                    "readiness": {
                        "ready": False,
                        "blockers": ["phase_6_1_incomplete", "phase_6_2_incomplete"],
                    },
                },
            ]
            with path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")

            out = StringIO()
            with redirect_stdout(out):
                rc = season_activation_last.main(["--input", str(path), "--json", "--limit", "2"])
            self.assertEqual(rc, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["history_count"], 2)
            self.assertEqual(payload["returned"], 2)
            self.assertEqual(payload["rows"][1]["new_blockers"], ["phase_6_2_incomplete"])


if __name__ == "__main__":
    unittest.main()


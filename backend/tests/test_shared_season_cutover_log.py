import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from backend.scripts import season_cutover_log as scl


class TestSharedSeasonCutoverLog(unittest.TestCase):
    def test_main_appends_jsonl_row(self):
        with TemporaryDirectory() as td:
            output = Path(td) / "cutover_history.jsonl"
            rc = scl.main(["--output", str(output)])
            self.assertEqual(rc, 0)
            self.assertTrue(output.exists())
            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "pass")
            self.assertEqual(rows[0]["timezone"], "America/New_York")
            self.assertEqual(len(rows[0]["lanes"]), 4)


if __name__ == "__main__":
    unittest.main()

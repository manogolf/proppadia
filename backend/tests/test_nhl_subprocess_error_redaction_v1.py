from __future__ import annotations

import json
import io
import contextlib
import subprocess
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

from backend.nhl import cli
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.scripts import run_nhl_postgame_reconciliation as reconciliation


_SENTINEL = "FAKE_DB_PASSWORD_SENTINEL_ONLY_FOR_TESTS"
_FAKE_DSN = f"postgresql://fixture-user:{_SENTINEL}@db.invalid/fixture"


class SubprocessErrorRedactionTests(unittest.TestCase):
    def test_daily_subprocess_failure_redacts_stderr_and_exception(self):
        failure = subprocess.CalledProcessError(
            17, ["psql", _FAKE_DSN],
            output=f"query context {_FAKE_DSN}", stderr=f"connection failed: {_FAKE_DSN}")
        stderr = io.StringIO()
        stdout = io.StringIO()
        with patch.object(cli.sp, "run", side_effect=failure), redirect_stderr(stderr), \
                contextlib.redirect_stdout(stdout):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                cli.run(["psql", _FAKE_DSN])

        logged = stderr.getvalue()
        logged += stdout.getvalue()
        self.assertIn("exit status 17", str(caught.exception))
        self.assertNotIn(_SENTINEL, logged)
        self.assertNotIn("db.invalid", logged)
        self.assertNotIn(_SENTINEL, str(caught.exception))
        self.assertNotIn("db.invalid", str(caught.exception))
        self.assertNotIn(_SENTINEL, str(caught.exception.output))
        self.assertNotIn(_SENTINEL, str(caught.exception.stderr))


    def test_parent_and_lane_failure_receipts_redact_exception_text(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.dict("os.environ", {"NHL_DAILY_RECEIPT_ROOT": str(root / "receipts")}):
                error = subprocess.CalledProcessError(
                    9, ["psql", _FAKE_DSN], stderr=f"failure URI={_FAKE_DSN}")
                with patch.object(cli, "_cmd_daily_impl", side_effect=error):
                    with self.assertRaises(subprocess.CalledProcessError) as caught:
                        cli.cmd_daily(with_odds=False)
                self.assertNotIn(_SENTINEL, str(caught.exception))
                self.assertNotIn("db.invalid", str(caught.exception))

            package = next((root / "receipts").glob("run_id=*"))
            receipt_text = (package / "parent_receipt.json").read_text()
            self.assertNotIn(_SENTINEL, receipt_text)
            self.assertNotIn("db.invalid", receipt_text)
            receipt = json.loads(receipt_text)
            self.assertIn("exit status 9", receipt["failure"]["error_message"])

        lane = DailyRunRecorder(
            run_id="fixture-redaction", command=["fixture"], phase="REFRESH")
        lane.fail_lane("saves", error, blocking=False)
        lane_text = json.dumps(lane.payload(), sort_keys=True)
        self.assertNotIn(_SENTINEL, lane_text)
        self.assertNotIn("db.invalid", lane_text)
        self.assertIn("exit status 9", lane.lane("saves").error_message)


    def test_postgame_child_failure_rethrows_only_redacted_command(self):
        failure = subprocess.CalledProcessError(
            23, ["collector", _FAKE_DSN], stderr=f"dsn rejected: {_FAKE_DSN}")
        with patch.object(reconciliation.subprocess, "run", side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                reconciliation._run(["collector", _FAKE_DSN], "2026-09-24", _FAKE_DSN)

        self.assertIn("exit status 23", str(caught.exception))
        self.assertNotIn(_SENTINEL, str(caught.exception))
        self.assertNotIn("db.invalid", str(caught.exception))
        self.assertNotIn(_SENTINEL, str(caught.exception.stderr))


if __name__ == "__main__":
    unittest.main()

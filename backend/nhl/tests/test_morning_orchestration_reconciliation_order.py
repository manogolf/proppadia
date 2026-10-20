from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.scripts import run_nhl_morning_orchestration as morning


class MorningOrchestrationReconciliationOrderTests(unittest.TestCase):
    def _run_fixture(self, root: Path, scenario: str) -> tuple[int, dict]:
        output = io.StringIO()
        # Fixture scenarios run no child commands; an offline value keeps them
        # independent of the operator's backend/.env.
        env_file = root / "fixture.env"
        env_file.write_text("SUPABASE_DB_URL=postgresql://offline.invalid/fixture\n")
        args = ["morning", "--slate-date", "2026-10-18", "--fixture-scenario", scenario,
                "--output-root", str(root), "--env-file", str(env_file)]
        with patch.object(sys, "argv", args), contextlib.redirect_stdout(output):
            code = morning.main()
        receipt_path = Path(output.getvalue().strip().splitlines()[-1])
        return code, json.loads(receipt_path.read_text())

    def test_fixture_runs_certified_daily_stage_before_slate_export(self):
        with tempfile.TemporaryDirectory() as temp:
            code, receipt = self._run_fixture(Path(temp), "valid_empty")
        self.assertEqual(code, 0)
        stages = [item["stage_id"] for item in receipt["stages"]]
        self.assertLess(stages.index("01_db_and_environment"),
                        stages.index("02_reconciliation_first_daily_prerequisites"))
        self.assertLess(stages.index("02_reconciliation_first_daily_prerequisites"),
                        stages.index("03_certified_slate_health_gate"))
        self.assertLess(stages.index("03_certified_slate_health_gate"),
                        stages.index("04_canonical_slate_export"))

    def test_reconciliation_failure_stops_before_slate_health_or_export(self):
        with tempfile.TemporaryDirectory() as temp:
            code, receipt = self._run_fixture(Path(temp), "reconciliation_failure")
        self.assertEqual(code, 1)
        stages = [item["stage_id"] for item in receipt["stages"]]
        self.assertEqual(stages, ["01_db_and_environment",
                                  "02_reconciliation_first_daily_prerequisites"])
        self.assertEqual(receipt["overall_status"], "FAILED_BLOCKING")


if __name__ == "__main__":
    unittest.main()

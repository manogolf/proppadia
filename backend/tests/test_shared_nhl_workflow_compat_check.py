import unittest
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
from unittest.mock import patch

from backend.scripts import check_nhl_workflow_compat as compat


class TestSharedNhlWorkflowCompatCheck(unittest.TestCase):
    def test_main_passes_with_existing_file(self):
        with patch("builtins.print"):
            with patch.object(compat, "ROOT", Path(".")):
                with patch.object(
                    compat,
                    "REQUIRED",
                    [Path("backend/scripts/check_nhl_workflow_compat.py")],
                ):
                    rc = compat.main([])
        self.assertEqual(rc, 0)

    def test_main_fails_when_required_file_missing(self):
        with patch("builtins.print"):
            with patch.object(compat, "ROOT", Path(".")):
                with patch.object(compat, "REQUIRED", [Path("missing/nope.py")]):
                    rc = compat.main([])
        self.assertEqual(rc, 1)

    def test_main_fails_when_compile_fails(self):
        with patch("builtins.print"):
            with patch.object(compat, "ROOT", Path(".")):
                with patch.object(
                    compat,
                    "REQUIRED",
                    [Path("backend/scripts/check_nhl_workflow_compat.py")],
                ):
                    with patch.object(
                        compat.py_compile, "compile", side_effect=Exception("compile boom")
                    ):
                        rc = compat.main([])
        self.assertEqual(rc, 1)

    def test_main_quiet_prints_summary_only(self):
        out = StringIO()
        with patch.object(compat, "ROOT", Path(".")):
            with patch.object(
                compat,
                "REQUIRED",
                [Path("backend/scripts/check_nhl_workflow_compat.py")],
            ):
                with redirect_stdout(out):
                    rc = compat.main(["--quiet"])
        self.assertEqual(rc, 0)
        printed = out.getvalue()
        self.assertIn("Summary:", printed)
        self.assertNotIn("NHL workflow compatibility check:", printed)
        self.assertNotIn("- OK backend/scripts/check_nhl_workflow_compat.py", printed)

    def test_main_json_outputs_machine_readable_summary(self):
        out = StringIO()
        with patch.object(compat, "ROOT", Path(".")):
            with patch.object(
                compat,
                "REQUIRED",
                [Path("backend/scripts/check_nhl_workflow_compat.py")],
            ):
                with redirect_stdout(out):
                    rc = compat.main(["--json"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["required_files"], 1)
        self.assertEqual(payload["missing_files"], 0)
        self.assertEqual(payload["compile_failures"], 0)


if __name__ == "__main__":
    unittest.main()

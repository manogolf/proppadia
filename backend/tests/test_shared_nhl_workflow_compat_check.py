import unittest
from contextlib import redirect_stdout
from io import StringIO
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


if __name__ == "__main__":
    unittest.main()

import unittest
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
                    rc = compat.main()
        self.assertEqual(rc, 0)

    def test_main_fails_when_required_file_missing(self):
        with patch("builtins.print"):
            with patch.object(compat, "ROOT", Path(".")):
                with patch.object(compat, "REQUIRED", [Path("missing/nope.py")]):
                    rc = compat.main()
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
                        rc = compat.main()
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()

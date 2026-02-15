import unittest

from backend.scripts.check_workflow_command_paths import (
    collect_missing_references,
    module_to_candidate_paths,
)
from backend.scripts.check_workflow_schedule_inventory import _extract_schedule_info


class TestWorkflowScheduleInventory(unittest.TestCase):
    def test_extract_schedule_info_parses_crons_and_strips_comments(self):
        text = """
on:
  schedule:
    - cron: "15 6 * * *" # daily
    - cron: '30 12 * * *'
  workflow_dispatch: {}
"""
        has_schedule, crons = _extract_schedule_info(text)
        self.assertTrue(has_schedule)
        self.assertEqual(crons, ["15 6 * * *", "30 12 * * *"])

    def test_extract_schedule_info_handles_manual_only(self):
        text = """
on:
  workflow_dispatch: {}
"""
        has_schedule, crons = _extract_schedule_info(text)
        self.assertFalse(has_schedule)
        self.assertEqual(crons, [])


class TestWorkflowCommandPathAudit(unittest.TestCase):
    def test_module_to_candidate_paths_shape(self):
        candidates = module_to_candidate_paths("backend.scripts.foo")
        self.assertEqual(candidates[0].as_posix().endswith("backend/scripts/foo.py"), True)
        self.assertEqual(
            candidates[1].as_posix().endswith("backend/scripts/foo/__init__.py"), True
        )

    def test_collect_missing_references_ignores_non_backend_modules(self):
        text = """
python -m pip install -U pip
python -m venv .venv
python -m backend.scripts.check_workflow_command_paths
python backend/scripts/check_workflow_schedule_inventory.py
"""
        missing = collect_missing_references(text)
        self.assertEqual(missing, [])

    def test_collect_missing_references_detects_missing_path_and_module(self):
        text = """
python backend/scripts/not_real_script.py
python -m backend.scripts.not_a_real_module
"""
        missing = collect_missing_references(text)
        self.assertIn("path:backend/scripts/not_real_script.py", missing)
        self.assertIn("module:backend.scripts.not_a_real_module", missing)


if __name__ == "__main__":
    unittest.main()

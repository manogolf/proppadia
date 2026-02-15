import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import cron_governance_snapshot as snapshot


class TestSharedCronGovernanceSnapshot(unittest.TestCase):
    def test_main_passes_with_all_green_checks(self):
        def _mk_side_effect(payload: dict):
            def _fn(_args):
                print(json.dumps(payload))
                return 0

            return _fn

        out = StringIO()
        with patch.object(
            snapshot.check_workflow_schedule_inventory,
            "main",
            side_effect=_mk_side_effect({"status": "pass"}),
        ), patch.object(
            snapshot.check_workflow_command_paths,
            "main",
            side_effect=_mk_side_effect({"status": "pass"}),
        ), patch.object(
            snapshot.check_nhl_workflow_compat,
            "main",
            side_effect=_mk_side_effect({"status": "pass"}),
        ), patch.object(
            snapshot.phase_status_snapshot,
            "main",
            side_effect=_mk_side_effect({"status": "pass"}),
        ), patch.object(
            snapshot.season_activation_status,
            "main",
            side_effect=_mk_side_effect({"ok": True, "status": "pass"}),
        ), redirect_stdout(out):
            rc = snapshot.main()

        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pass")
        self.assertIn("workflow_inventory", payload["checks"])
        self.assertIn("workflow_path_audit", payload["checks"])
        self.assertIn("nhl_workflow_compat", payload["checks"])
        self.assertIn("phase_status", payload["checks"])
        self.assertIn("season_activation", payload["checks"])

    def test_main_fails_when_any_check_fails(self):
        def _ok(_args):
            print(json.dumps({"status": "pass"}))
            return 0

        def _fail(_args):
            print(json.dumps({"status": "fail"}))
            return 1

        out = StringIO()
        with patch.object(
            snapshot.check_workflow_schedule_inventory,
            "main",
            side_effect=_ok,
        ), patch.object(
            snapshot.check_workflow_command_paths,
            "main",
            side_effect=_fail,
        ), patch.object(
            snapshot.check_nhl_workflow_compat,
            "main",
            side_effect=_ok,
        ), patch.object(
            snapshot.phase_status_snapshot,
            "main",
            side_effect=_ok,
        ), patch.object(
            snapshot.season_activation_status,
            "main",
            side_effect=_ok,
        ), redirect_stdout(out):
            rc = snapshot.main()

        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")

    def test_main_fails_when_season_activation_not_ready(self):
        def _ok(_args):
            print(json.dumps({"status": "pass"}))
            return 0

        def _activation_fail(_args):
            print(json.dumps({"ok": False, "status": "fail"}))
            return 1

        out = StringIO()
        with patch.object(snapshot.check_workflow_schedule_inventory, "main", side_effect=_ok), patch.object(
            snapshot.check_workflow_command_paths, "main", side_effect=_ok
        ), patch.object(snapshot.check_nhl_workflow_compat, "main", side_effect=_ok), patch.object(
            snapshot.phase_status_snapshot, "main", side_effect=_ok
        ), patch.object(snapshot.season_activation_status, "main", side_effect=_activation_fail), redirect_stdout(out):
            rc = snapshot.main()

        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")


if __name__ == "__main__":
    unittest.main()

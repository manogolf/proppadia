import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import assistant_handoff_bundle as bundle


class TestSharedAssistantHandoffBundle(unittest.TestCase):
    def test_main_pass_payload_shape(self):
        def _mk_check(status: str):
            def _fn(_args):
                print(json.dumps({"status": status}))
                return 0 if status == "pass" else 1

            return _fn

        out = StringIO()
        with patch.object(bundle.check_workflow_schedule_inventory, "main", side_effect=_mk_check("pass")), patch.object(
            bundle.check_workflow_command_paths, "main", side_effect=_mk_check("pass")
        ), patch.object(bundle.check_nhl_workflow_compat, "main", side_effect=_mk_check("pass")), patch.object(
            bundle, "collect_snapshot", return_value={"ok": True, "status": "pass", "checks": {}, "errors": {}}
        ), patch.object(
            bundle, "_history_tail", return_value={"input": "x", "history_count": 0, "returned": 0, "rows": []}
        ), redirect_stdout(out):
            rc = bundle.main([])

        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pass")
        self.assertIn("ops_vars", payload)
        self.assertIn("governance", payload)
        self.assertIn("mlb_readiness", payload)
        self.assertIn("mlb_readiness_history", payload)

    def test_main_fail_when_readiness_fails(self):
        def _ok(_args):
            print(json.dumps({"status": "pass"}))
            return 0

        out = StringIO()
        with patch.object(bundle.check_workflow_schedule_inventory, "main", side_effect=_ok), patch.object(
            bundle.check_workflow_command_paths, "main", side_effect=_ok
        ), patch.object(bundle.check_nhl_workflow_compat, "main", side_effect=_ok), patch.object(
            bundle, "collect_snapshot", return_value={"ok": False, "status": "fail", "checks": {}, "errors": {}}
        ), patch.object(
            bundle, "_history_tail", return_value={"input": "x", "history_count": 1, "returned": 1, "rows": []}
        ), redirect_stdout(out):
            rc = bundle.main([])

        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")


if __name__ == "__main__":
    unittest.main()

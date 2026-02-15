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
            bundle.phase_status_snapshot, "main", side_effect=_mk_check("pass")
        ), patch.object(
            bundle, "collect_snapshot", return_value={"ok": True, "status": "pass", "checks": {}, "errors": {}}
        ), patch.object(
            bundle, "_history_tail", return_value={"input": "x", "history_count": 0, "returned": 0, "rows": []}
        ), patch.object(
            bundle, "_season_activation_tail", return_value={"input": "y", "history_count": 0, "returned": 0, "rows": []}
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
        self.assertIn("season_activation_history", payload)
        self.assertIn("phase_status", payload["governance"]["checks"])

    def test_main_fail_when_readiness_fails(self):
        def _ok(_args):
            print(json.dumps({"status": "pass"}))
            return 0

        out = StringIO()
        with patch.object(bundle.check_workflow_schedule_inventory, "main", side_effect=_ok), patch.object(
            bundle.check_workflow_command_paths, "main", side_effect=_ok
        ), patch.object(bundle.check_nhl_workflow_compat, "main", side_effect=_ok), patch.object(
            bundle.phase_status_snapshot, "main", side_effect=_ok
        ), patch.object(
            bundle, "collect_snapshot", return_value={"ok": False, "status": "fail", "checks": {}, "errors": {}}
        ), patch.object(
            bundle, "_history_tail", return_value={"input": "x", "history_count": 1, "returned": 1, "rows": []}
        ), patch.object(
            bundle, "_season_activation_tail", return_value={"input": "y", "history_count": 0, "returned": 0, "rows": []}
        ), redirect_stdout(out):
            rc = bundle.main([])

        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")

    def test_main_fail_when_phase_snapshot_fails(self):
        def _ok(_args):
            print(json.dumps({"status": "pass"}))
            return 0

        def _bad(_args):
            print(json.dumps({"status": "fail"}))
            return 1

        out = StringIO()
        with patch.object(bundle.check_workflow_schedule_inventory, "main", side_effect=_ok), patch.object(
            bundle.check_workflow_command_paths, "main", side_effect=_ok
        ), patch.object(bundle.check_nhl_workflow_compat, "main", side_effect=_ok), patch.object(
            bundle.phase_status_snapshot, "main", side_effect=_bad
        ), patch.object(
            bundle, "collect_snapshot", return_value={"ok": True, "status": "pass", "checks": {}, "errors": {}}
        ), patch.object(
            bundle, "_history_tail", return_value={"input": "x", "history_count": 1, "returned": 1, "rows": []}
        ), patch.object(
            bundle, "_season_activation_tail", return_value={"input": "y", "history_count": 1, "returned": 1, "rows": []}
        ), redirect_stdout(out):
            rc = bundle.main([])

        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fail")

    def test_season_activation_tail_reports_new_blockers(self):
        with patch.object(
            bundle,
            "_load_history",
            return_value=[
                {"readiness": {"blockers": ["a"]}},
                {"readiness": {"blockers": ["a", "b"]}},
            ],
        ):
            payload = bundle._season_activation_tail("artifacts/season_activation_history.jsonl", 2)
        self.assertEqual(payload["history_count"], 2)
        self.assertEqual(payload["returned"], 2)
        self.assertEqual(payload["rows"][1]["new_blockers"], ["b"])


if __name__ == "__main__":
    unittest.main()

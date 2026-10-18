import json
import subprocess
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.cross_market_shadow.core import _run_nonblocking_challenger
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as capture


class PriorDayOutcomeHandoffTest(unittest.TestCase):
    slate = "2026-10-01"
    prior = "2026-09-30"
    game_ids = [2026020006, 2026020007, 2026020008]
    run_id = "fixture_acquisition_run"

    @staticmethod
    def completed(payload):
        return subprocess.CompletedProcess([], 0, json.dumps(payload), "")

    def test_missing_yesterday_package_runs_governed_path_before_capture(self):
        outcomes = pd.DataFrame({"game_id": self.game_ids})
        identity = {"status": "DATABASE_IDENTITY_PREFLIGHT_VALID", "database_preflight": {
            "authorized_new_official_lookup_ids": [], "classification": {
                "new_official_lookup": [], "conflict": [],
            },
        }}
        responses = [
            self.completed({"status": "COMPLETE", "run_id": self.run_id,
                            "game_ids": self.game_ids}),
            self.completed({"status": "LOCAL_INPUTS_VALID"}),
            self.completed(identity),
            subprocess.CompletedProcess([], 0, "reconciliation complete", ""),
        ]
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(capture, "_prior_day_regular_season_game_ids",
                             return_value=self.game_ids), \
                patch.object(capture.subprocess, "run", side_effect=responses) as run, \
                patch.object(capture, "_verify_prior_day_package", return_value={
                    "status": "CERTIFIED_FINAL", "game_ids": self.game_ids,
                }), patch.object(capture, "load_official_outcomes", return_value=outcomes):
            result = capture.ensure_prior_day_official_outcomes(
                "unused-dsn", self.slate, outcome_root=Path(tmp),
                now=datetime(2026, 10, 1, 9, 0, tzinfo=ZoneInfo("America/New_York")),
            )

        self.assertEqual(result["status"], "GOVERNED_RECONCILIATION_COMPLETE")
        self.assertEqual(result["game_ids"], self.game_ids)
        self.assertEqual(run.call_count, 4)
        commands = [call.args[0] for call in run.call_args_list]
        self.assertIn("--authority-roster-acquisition", commands[0])
        self.assertIn("--prior-learning", commands[0])
        self.assertIn("--local-input-preflight", commands[1])
        self.assertIn("--prior-learning", commands[1])
        self.assertIn("--database-identity-preflight", commands[2])
        self.assertIn("--prior-learning", commands[2])
        self.assertIn("--execute", commands[3])
        self.assertIn("--prior-learning", commands[3])
        self.assertTrue(all(self.run_id in " ".join(command) for command in commands[1:]))

    def test_identity_authorization_requirement_stops_before_reconciliation(self):
        identity = {"status": "DATABASE_IDENTITY_PREFLIGHT_VALID", "database_preflight": {
            "authorized_new_official_lookup_ids": [8489999], "classification": {
                "new_official_lookup": [8489999], "conflict": [],
            },
        }}
        responses = [
            self.completed({"status": "COMPLETE", "run_id": self.run_id,
                            "game_ids": self.game_ids}),
            self.completed({"status": "LOCAL_INPUTS_VALID"}),
            self.completed(identity),
        ]
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(capture, "_prior_day_regular_season_game_ids",
                             return_value=self.game_ids), \
                patch.object(capture.subprocess, "run", side_effect=responses) as run:
            with self.assertRaisesRegex(
                RuntimeError, "CROSS_MARKET_PRIOR_DAY_PLAYER_IDENTITY_AUTHORIZATION_REQUIRED"
            ):
                capture.ensure_prior_day_official_outcomes(
                    "unused-dsn", self.slate, outcome_root=Path(tmp),
                    now=datetime(2026, 10, 1, 9, 0, tzinfo=ZoneInfo("America/New_York")),
                )
        self.assertEqual(run.call_count, 3)
        self.assertNotIn("--execute", run.call_args.args[0])

    def test_existing_complete_prior_day_package_is_discovered_without_acquisition(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / self.prior / "reconciliation=fixture").mkdir(parents=True)
            with patch.object(capture, "_prior_day_regular_season_game_ids",
                              return_value=self.game_ids), \
                    patch.object(capture, "_verify_prior_day_package", return_value={
                        "status": "CERTIFIED_FINAL", "game_ids": self.game_ids,
                    }) as verify, patch.object(capture.subprocess, "run") as run:
                result = capture.ensure_prior_day_official_outcomes(
                    "unused-dsn", self.slate, outcome_root=root,
                    now=datetime(2026, 10, 1, 9, 0, tzinfo=ZoneInfo("America/New_York")),
                )
        self.assertEqual(result["status"], "EXISTING_GOVERNED_PACKAGE_VALID")
        verify.assert_called_once()
        run.assert_not_called()

    def test_empty_database_game_set_requires_verified_empty_official_schedule(self):
        with patch.object(capture, "_prior_day_regular_season_game_ids", return_value=[]), \
                patch.object(capture, "require_prior_day_certificate", return_value={
                    "status": "VERIFIED_EMPTY_SCHEDULE", "slate_date": self.prior,
                }) as certify, patch.object(capture.subprocess, "run") as run:
            result = capture.ensure_prior_day_official_outcomes(
                "unused-dsn", self.slate,
                now=datetime(2026, 10, 1, 9, 0, tzinfo=ZoneInfo("America/New_York")),
            )
        self.assertEqual(result["status"], "NO_PRIOR_DAY_REGULAR_SEASON_GAMES")
        certify.assert_called_once()
        run.assert_not_called()

    def test_prior_day_certificate_must_match_expected_game_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / self.prior / "reconciliation=fixture").mkdir(parents=True)
            with patch.object(capture, "verify_reconciliation_certificate", return_value={
                "status": "CERTIFIED_FINAL", "game_ids": [self.game_ids[0]],
            }), patch.object(capture, "load_official_outcomes") as load:
                with self.assertRaisesRegex(
                    RuntimeError, "CROSS_MARKET_PRIOR_DAY_CERTIFIED_GAME_SET_MISMATCH"
                ):
                    capture._verify_prior_day_package(root, self.prior, self.game_ids)
            load.assert_not_called()

    def test_child_failure_diagnostics_are_bounded_and_redacted(self):
        secret = "postgresql://fixture-user:fixture-secret@db.invalid/fixture"
        failure = subprocess.CompletedProcess(
            [], 5, "stdout " + secret + "x" * 2000, "stderr " + secret)
        with patch.object(capture.subprocess, "run", return_value=failure):
            with self.assertRaises(capture.PriorDayChildFailure) as caught:
                capture._run_prior_day_child("authority_roster_acquisition", ["fixture"])
        receipt = caught.exception.diagnostics
        self.assertEqual(receipt["stage"], "authority_roster_acquisition")
        self.assertEqual(receipt["exit_status"], 5)
        self.assertLessEqual(len(receipt["stdout_tail"]), 1200)
        self.assertLessEqual(len(receipt["stderr_tail"]), 1200)
        self.assertNotIn("fixture-secret", json.dumps(receipt))
        self.assertNotIn("db.invalid", json.dumps(receipt))

    def test_optional_challengers_fail_independently_of_control_and_each_other(self):
        value, status = _run_nonblocking_challenger(
            lambda: (_ for _ in ()).throw(ValueError("puck challenger fixture")))
        self.assertIsNone(value)
        self.assertEqual(status["status"], "FAILED_NONBLOCKING")
        self.assertIn("puck challenger fixture", status["failure"])

        value, status = _run_nonblocking_challenger(lambda: {"predictions": 3})
        self.assertEqual(value, {"predictions": 3})
        self.assertEqual(status["status"], "COMPLETE")

        value, status = _run_nonblocking_challenger(
            lambda: (_ for _ in ()).throw(ValueError("moneyline challenger fixture")))
        self.assertIsNone(value)
        self.assertEqual(status["status"], "FAILED_NONBLOCKING")
        self.assertIn("moneyline challenger fixture", status["failure"])


if __name__ == "__main__":
    unittest.main()

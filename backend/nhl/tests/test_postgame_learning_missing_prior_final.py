from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from backend.nhl import cli, postgame_learning
from backend.nhl.postgame_reconcile.core import _select_cross_market_final_run


class PriorFinalCaptureGapTests(unittest.TestCase):
    def test_daily_morning_blocks_not_final_before_current_slate_acquisition(self):
        recorder = cli.DailyRunRecorder(run_id="morning-test", command=[], phase="EARLY")
        prior_not_final = {"prior_slate_date": "2026-10-17",
                           "official_outcomes_status": "NOT_FINAL",
                           "reconciliation_status": "NOT_FINAL",
                           "reconciliation_package": None}
        with tempfile.TemporaryDirectory() as temp, patch.object(cli, "ROOT", Path(temp)), patch.object(
            cli, "require_db_url", return_value="test-dsn"
        ), patch.object(cli, "run"), patch.object(cli, "et_today", return_value="2026-10-18"), patch.object(
            cli, "et_yesterday", return_value="2026-10-17"
        ), patch.object(cli, "prior_et_slate", return_value="2026-10-17"), patch.object(
            cli, "_bootstrap_daily_canonical_slate", return_value=({}, [], "empty-slate-hash")
        ), patch.object(cli, "ensure_prior_learning", return_value=prior_not_final), patch.object(
            cli, "_refresh_all_team_rosters_for_daily", side_effect=RuntimeError("REACHED_ROSTER_STAGE")
        ) as refresh, patch.object(cli, "_bootstrap_daily_canonical_slate") as schedule:
            with patch.dict("os.environ", {}, clear=False), self.assertRaisesRegex(
                RuntimeError, "PRIOR_DAY_RECONCILIATION_BLOCKING:NOT_FINAL"
            ):
                cli._cmd_daily_impl(with_odds=False, morning_only=False, odds_phase="EARLY",
                                    daily_run_id="morning-test", recorder=recorder)
        refresh.assert_not_called()
        schedule.assert_not_called()

    def test_one_valid_final_capture_is_still_selected(self):
        run = Path("valid-final-capture")
        selected, resolution = _select_cross_market_final_run(
            runs=[run], schedule=pd.DataFrame({"game_type_code": [2, 2]}),
            activation={"capture_enabled": True},
        )
        self.assertEqual(selected, run)
        self.assertEqual(resolution["status"], "REQUIRED_CARD_PRESENT")

    def test_valid_prior_capture_continues_through_governed_reconciliation(self):
        acquired = {"status": "COMPLETE", "run_id": "acquisition-run",
                    "request_accounting": {"total_logical_requests": 4}}
        responses = [SimpleNamespace(returncode=0, stdout=json.dumps(acquired), stderr=""),
                     SimpleNamespace(returncode=2, stdout="NOT_FINAL", stderr="")]
        with tempfile.TemporaryDirectory() as temp, patch.object(
            postgame_learning, "ROOT", Path(temp)
        ), patch.object(postgame_learning, "_existing_reconciliation", return_value=None), patch.object(
            postgame_learning, "_retained_schedule_game_count", return_value=2
        ), patch.object(postgame_learning.subprocess, "run", side_effect=responses) as run:
            result = postgame_learning.ensure_prior_learning("2026-10-16")

        self.assertEqual(result["reconciliation_status"], "NOT_FINAL")
        self.assertEqual(run.call_count, 2)
        self.assertIn("--prior-learning", run.call_args_list[0].args[0])
        self.assertIn("--prior-learning", run.call_args_list[1].args[0])
        self.assertIn("--authority-roster-acquisition", run.call_args_list[0].args[0])
        self.assertIn("--execute", run.call_args_list[1].args[0])

    def test_missing_or_duplicate_capture_still_blocks_actual_reconciliation(self):
        schedule = pd.DataFrame({"game_type_code": [2]})
        activation = {"capture_enabled": True}
        with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:0"):
            _select_cross_market_final_run(runs=[], schedule=schedule, activation=activation)
        with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:2"):
            _select_cross_market_final_run(
                runs=[Path("final-a"), Path("final-b")], schedule=schedule,
                activation=activation, allow_missing_active_final=True,
            )

    def test_only_prior_learning_can_use_reference_only_missing_capture_path(self):
        schedule = pd.DataFrame({"game_type_code": [2]})
        activation = {"capture_enabled": True}
        with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:0"):
            _select_cross_market_final_run(runs=[], schedule=schedule, activation=activation)
        selected, resolution = _select_cross_market_final_run(
            runs=[], schedule=schedule, activation=activation,
            allow_missing_active_final=True,
        )
        self.assertIsNone(selected)
        self.assertEqual(resolution["status"], "MISSING_ACTIVE_CARD_PRIOR_LEARNING")


if __name__ == "__main__":
    unittest.main()

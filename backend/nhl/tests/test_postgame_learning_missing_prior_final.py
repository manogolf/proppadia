from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from backend.nhl import cli, postgame_learning
from backend.nhl.postgame_reconcile.core import (
    _resolve_prop_prediction_source,
    _select_cross_market_final_run,
)
from backend.nhl.scripts.run_nhl_postgame_reconciliation import prior_learning_mode_allowed


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

    def test_prior_learning_flag_is_limited_to_governed_handoff_stages(self):
        for mode in ("authority_roster_acquisition", "local_input_preflight",
                     "database_identity_preflight", "execute"):
            with self.subTest(mode=mode):
                self.assertTrue(prior_learning_mode_allowed(**{mode: True}))
        self.assertFalse(prior_learning_mode_allowed())

    def test_missing_saves_and_cross_market_cards_are_ungradeable_only_in_prior_learning(self):
        schedule = pd.DataFrame({"game_type_code": [2]})
        activation = {"capture_enabled": True}
        with self.assertRaisesRegex(
                RuntimeError, "IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:0"):
            _select_cross_market_final_run(
                runs=[], schedule=schedule, activation=activation)
        cross, status = _select_cross_market_final_run(
            runs=[], schedule=schedule, activation=activation,
            allow_missing_active_final=True)
        self.assertIsNone(cross)
        self.assertEqual(status["status"], "MISSING_ACTIVE_CARD_PRIOR_LEARNING")

        for slate_date in ("2026-10-17", "2026-10-18"):
            with self.subTest(slate_date=slate_date):
                with self.assertRaisesRegex(
                        RuntimeError, "IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:0"):
                    _resolve_prop_prediction_source(
                        runs=[], lane="SAVES", slate_date=slate_date,
                        schedule=pd.DataFrame())
                saves = _resolve_prop_prediction_source(
                    runs=[], lane="SAVES", slate_date=slate_date,
                    schedule=pd.DataFrame(), allow_missing_prediction=True)
                self.assertEqual(saves["status"], "NOT_GRADEABLE_NO_PREDICTION")

    def test_prior_learning_does_not_soften_duplicate_or_corrupt_present_cards(self):
        schedule = pd.DataFrame({"game_type_code": [2]})
        activation = {"capture_enabled": True}
        with self.assertRaisesRegex(
                RuntimeError, "IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:2"):
            _select_cross_market_final_run(
                runs=[Path("one"), Path("two")], schedule=schedule,
                activation=activation, allow_missing_active_final=True)
        with self.assertRaisesRegex(
                RuntimeError, "IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:2"):
            _resolve_prop_prediction_source(
                runs=[Path("one"), Path("two")], lane="SAVES",
                slate_date="2026-10-18", schedule=pd.DataFrame(),
                allow_missing_prediction=True)
        with patch("backend.nhl.postgame_reconcile.core._verify_prop_source",
                   side_effect=RuntimeError("IMMUTABLE_SAVES_HASH_MISMATCH")):
            with self.assertRaisesRegex(RuntimeError, "SAVES_HASH_MISMATCH"):
                _resolve_prop_prediction_source(
                    runs=[Path("corrupt")], lane="SAVES",
                    slate_date="2026-10-18", schedule=pd.DataFrame(),
                    allow_missing_prediction=True)


class RetainedScheduleCertificateTests(unittest.TestCase):
    def _write(self, root: Path, slate_date: str, games: int, *, tamper: bool = False):
        # Mirror import_schedule_today: the file is indented, and the recorded
        # hash is of the compact, key-sorted JSON.
        import hashlib
        raw = {"gameWeek": [{"date": slate_date, "games": [{"id": i} for i in range(games)]}]}
        day = root / "artifacts/operational/nhl/slates" / slate_date
        day.mkdir(parents=True)
        (day / "raw_schedule_response.json").write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n")
        compact = (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode()
        (day / "slate_health.json").write_text(json.dumps({
            "slate_date": slate_date, "downstream_ready": True,
            "source": f"https://api-web.nhle.com/v1/schedule/{slate_date}",
            "raw_source_hash": ("0" * 64) if tamper else hashlib.sha256(compact).hexdigest(),
            "normalized_game_count": games,
        }))

    def test_schedule_written_by_the_importer_verifies(self):
        with tempfile.TemporaryDirectory() as temp:
            self._write(Path(temp), "2026-10-19", 4)
            with patch.object(postgame_learning, "ROOT", Path(temp)):
                self.assertEqual(postgame_learning._retained_schedule_game_count("2026-10-19"), 4)

    def test_changed_schedule_bytes_still_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            self._write(Path(temp), "2026-10-19", 4, tamper=True)
            with patch.object(postgame_learning, "ROOT", Path(temp)), \
                    self.assertRaisesRegex(RuntimeError, "PRIOR_DATE_SCHEDULE_CERTIFICATE_INVALID"):
                postgame_learning._retained_schedule_game_count("2026-10-19")


if __name__ == "__main__":
    unittest.main()

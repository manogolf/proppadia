from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl import cli
from backend.nhl.performance_summary import _validate_production_sog_game_set


class DailyStartupOrderingTests(unittest.TestCase):
    def test_no_prior_games_requires_verified_empty_schedule(self):
        result = {
            "prior_slate_date": "2026-10-17",
            "reconciliation_status": "NO_PRIOR_GAMES",
            "official_outcomes_status": "NO_PRIOR_GAMES",
            "reconciliation_package": None,
        }
        certificate = {"status": "VERIFIED_EMPTY_SCHEDULE",
                      "slate_date": "2026-10-17", "schedule_sha256": "schedule-hash"}
        with patch.object(cli, "require_prior_day_certificate", return_value=certificate) as verify:
            self.assertEqual(cli._require_daily_prior_certificate("2026-10-17", result), certificate)
        verify.assert_called_once_with("2026-10-17")

    def test_verified_no_prior_games_permits_ordered_current_slate_work(self):
        events = []
        recorder = DailyRunRecorder(run_id="empty-prior-test", command=[], phase="EARLY")
        prior_result = {"prior_slate_date": "2026-10-17",
                        "reconciliation_status": "NO_PRIOR_GAMES",
                        "official_outcomes_status": "NO_PRIOR_GAMES",
                        "reconciliation_package": None}
        empty_certificate = {"status": "VERIFIED_EMPTY_SCHEDULE",
                             "slate_date": "2026-10-17", "schedule_sha256": "empty-hash"}
        game = SimpleNamespace(game_id=2026020001)
        with patch.object(cli, "require_db_url", return_value="db"), patch.object(
            cli, "run", side_effect=lambda *args, **kwargs: None
        ), patch.object(cli, "et_today", return_value="2026-10-18"), patch.object(
            cli, "et_yesterday", return_value="2026-10-17"), patch.object(
            cli, "prior_et_slate", return_value="2026-10-17"), patch.object(
            cli, "ensure_prior_learning",
            side_effect=lambda _: events.append("reconciliation") or prior_result.copy(),
        ), patch.object(cli, "require_prior_day_certificate", return_value=empty_certificate), patch.object(
            cli, "_bootstrap_daily_canonical_slate",
            side_effect=lambda **kwargs: events.append("schedule") or
            ({"completion_status": "READY"}, [game], "current-hash"),
        ), patch.object(cli, "_capture_daily_slate_roster",
                        side_effect=lambda **kwargs: events.append("roster")), patch.object(
            cli, "_refresh_all_team_rosters_for_daily",
            side_effect=lambda **kwargs: events.append("downstream") or
            (_ for _ in ()).throw(RuntimeError("stop-after-no-prior-continuation")),
        ):
            with patch.dict("os.environ", {}, clear=False), self.assertRaisesRegex(
                RuntimeError, "stop-after-no-prior-continuation"
            ):
                cli._cmd_daily_impl(with_odds=False, morning_only=False,
                                    odds_phase="EARLY", daily_run_id="test",
                                    recorder=recorder)
        self.assertEqual(events, ["reconciliation", "schedule", "roster", "downstream"])
        self.assertEqual(recorder.prior_learning["certificate"], empty_certificate)

    def test_no_package_not_final_and_incomplete_statuses_block(self):
        for status, outcome_status in (("NO_PACKAGE", "NOT_FINAL"),
                                       ("NOT_FINAL", "NOT_FINAL"),
                                       ("CREATED", "INCOMPLETE")):
            result = {"prior_slate_date": "2026-10-17",
                      "reconciliation_status": status,
                      "official_outcomes_status": outcome_status,
                      "reconciliation_package": None}
            with self.subTest(status=status), patch.object(
                cli, "require_prior_day_certificate"
            ) as verify:
                with self.assertRaises(RuntimeError):
                    cli._require_daily_prior_certificate("2026-10-17", result)
                verify.assert_not_called()

    def test_bootstrap_records_canonical_set_from_schedule_before_lane_checks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            slate_dir = root / "artifacts/operational/nhl/slates/2026-10-11"
            slate_dir.mkdir(parents=True)
            health = {"slate_date": "2026-10-11", "downstream_ready": True,
                      "completion_status": "READY", "source": "NHL_API_SCHEDULE"}
            (slate_dir / "slate_health.json").write_text(json.dumps(health))
            games = [SimpleNamespace(game_id=2026020084), SimpleNamespace(game_id=2026020085)]
            recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
            with patch.object(cli, "ROOT", root), patch.object(cli, "run") as run, patch.object(
                cli, "load_canonical_slate", return_value=games
            ) as load:
                _, frozen, game_hash = cli._bootstrap_daily_canonical_slate(
                    slate="2026-10-11", recorder=recorder)
            run.assert_called_once()
            load.assert_called_once()
            self.assertEqual([g.game_id for g in frozen], [2026020084, 2026020085])
            self.assertEqual(recorder.canonical_game_ids, [2026020084, 2026020085])
            self.assertEqual(game_hash, canonical_game_set_hash([2026020084, 2026020085]))

    def test_missing_health_is_uninitialized_not_a_valid_empty_slate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
            with patch.object(cli, "ROOT", root), patch.object(cli, "run"):
                with self.assertRaisesRegex(RuntimeError, "NHL slate health missing"):
                    cli._bootstrap_daily_canonical_slate(slate="2026-10-11", recorder=recorder)
            self.assertIsNone(recorder.canonical_game_set_hash)

    def test_prior_day_certification_precedes_schedule_and_roster(self):
        events = []
        recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
        game = SimpleNamespace(game_id=2026020084)
        prior_result = {"prior_slate_date": "2026-10-17",
                        "reconciliation_status": "CREATED",
                        "official_outcomes_status": "FINAL",
                        "reconciliation_package": "/certificate/package"}
        certificate = {"status": "CERTIFIED_FINAL",
                       "package_path": "/certificate/package",
                       "package_manifest_sha256": "manifest-hash",
                       "game_ids": [2026020083], "game_count": 1}
        with patch.object(cli, "require_db_url", return_value="db"), patch.object(
            cli, "run", side_effect=lambda *a, **k: events.append("db")
        ), patch.object(cli, "et_today", return_value="2026-10-18"), patch.object(
            cli, "et_yesterday", return_value="2026-10-17"
        ), patch.object(cli, "prior_et_slate", return_value="2026-10-17"), patch.object(
            cli, "ensure_prior_learning",
            side_effect=lambda _: events.append("reconciliation") or prior_result.copy(),
        ), patch.object(cli, "require_prior_day_certificate", return_value=certificate), patch.object(
            cli, "_bootstrap_daily_canonical_slate",
            side_effect=lambda **kw: events.append("schedule") or
            ({"completion_status": "READY"}, [game], "canonical-hash"),
        ), patch.object(cli, "_capture_daily_slate_roster",
                        side_effect=lambda **kw: events.append("roster") or
                        (_ for _ in ()).throw(RuntimeError("stop-after-order-check"))):
            with patch.dict("os.environ", {}, clear=False), self.assertRaisesRegex(
                RuntimeError, "stop-after-order-check"
            ):
                cli._cmd_daily_impl(with_odds=False, morning_only=False,
                                    odds_phase="EARLY", daily_run_id="test",
                                    recorder=recorder)
        self.assertLess(events.index("reconciliation"), events.index("schedule"))
        self.assertLess(events.index("schedule"), events.index("roster"))
        self.assertEqual(recorder.prior_learning["certificate"], certificate)

    def test_roster_capture_uses_one_official_roster_child_and_receipt_identity(self):
        recorder = DailyRunRecorder(run_id="daily-1", command=[], phase="EARLY")
        recorder.set_canonical(
            slate_date="2026-10-18", season=2026, game_ids=[2026020084],
            game_set_hash="canonical-hash",
        )
        recorder.start_lane("roster")
        with patch.object(cli, "run", return_value=SimpleNamespace(returncode=0)) as run, patch.object(
            cli, "ROSTER_OBSERVATION_ROOT", Path("/roster-observations")
        ):
            run.side_effect = lambda cmd, **kwargs: (
                recorder.record_child({
                    "lane": "roster", "command_identity": str(cmd[-1]),
                    "exit_status": 0, "status": "COMPLETE",
                    "schema_version": "NHL_ROSTER_CHILD_SUMMARY_V2",
                    "database_write_capable": True,
                    "database_write_status": "COMMITTED",
                    "transaction_disposition": "COMMITTED",
                    "database_row_counts": {"roster_status_upsert_rows": 48},
                    "database_row_counts_complete": False,
                    "roster_observation": "/immutable/2026-10-18/obs-1",
                    "roster_manifest_sha256": "abc123",
                    "database_stage_summary": {"roster_status_rows_present": 48},
                }) or SimpleNamespace(returncode=0)
            )
            cli._capture_daily_slate_roster(
                slate="2026-10-18", odds_phase="EARLY", daily_run_id="daily-1",
                recorder=recorder, reuse_roster_observation=None,
                canonical_games=[SimpleNamespace(game_id=2026020084)],
                game_hash="canonical-hash",
            )

        self.assertEqual(run.call_count, 1)
        command = run.call_args.args[0]
        self.assertEqual(Path(command[-1]).name, "refresh_players_and_roster_today.py")
        self.assertNotEqual(Path(command[-1]).name, "import_roster_today.py")
        self.assertEqual(run.call_args.kwargs["env"]["SLATE_DATE"], "2026-10-18")
        self.assertEqual(run.call_args.kwargs["env"]["NHL_DAILY_PHASE"], "EARLY")
        self.assertEqual(recorder.lane("roster").status, "COMPLETE")
        self.assertEqual(recorder.roster_observation["path"], "/immutable/2026-10-18/obs-1")
        self.assertEqual(recorder.roster_observation["manifest_sha256"], "abc123")
        self.assertEqual(
            recorder.lane("roster").database_stage_summary["roster_status_rows_present"], 48
        )

    def test_schedule_failure_prevents_roster_acquisition(self):
        recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
        with tempfile.TemporaryDirectory() as temp, patch.object(
            cli, "ROOT", Path(temp)
        ), patch.object(cli, "require_db_url", return_value="db"), patch.object(
            cli, "run"
        ), patch.object(cli, "et_today", return_value="2026-10-18"), patch.object(
            cli, "et_yesterday", return_value="2026-10-17"), patch.object(
            cli, "prior_et_slate", return_value="2026-10-17"), patch.object(
            cli, "ensure_prior_learning", return_value={
                "prior_slate_date": "2026-10-17",
                "reconciliation_status": "NO_PRIOR_GAMES",
                "official_outcomes_status": "NO_PRIOR_GAMES",
                "reconciliation_package": None,
            }
        ), patch.object(cli, "require_prior_day_certificate", return_value={
            "status": "VERIFIED_EMPTY_SCHEDULE", "slate_date": "2026-10-17",
        }), patch.object(
            cli, "_bootstrap_daily_canonical_slate",
            side_effect=RuntimeError("schedule-not-ready"),
        ), patch.object(cli, "_capture_daily_slate_roster") as capture:
            with patch.dict("os.environ", {}, clear=False), self.assertRaisesRegex(
                RuntimeError, "schedule-not-ready"
            ):
                cli._cmd_daily_impl(
                    with_odds=False, morning_only=False, odds_phase="EARLY",
                    daily_run_id="test", recorder=recorder,
                )
        capture.assert_not_called()

    def test_nonfinal_reconciliation_prevents_schedule_and_roster(self):
        recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
        with patch.object(cli, "require_db_url", return_value="db"), patch.object(
            cli, "run"
        ), patch.object(cli, "et_today", return_value="2026-10-18"), patch.object(
            cli, "et_yesterday", return_value="2026-10-17"), patch.object(
            cli, "prior_et_slate", return_value="2026-10-17"), patch.object(
            cli, "ensure_prior_learning", return_value={
                "prior_slate_date": "2026-10-17",
                "reconciliation_status": "NOT_FINAL",
                "official_outcomes_status": "NOT_FINAL",
                "reconciliation_package": None,
            }
        ), patch.object(cli, "_bootstrap_daily_canonical_slate") as schedule, patch.object(
            cli, "_capture_daily_slate_roster") as capture:
            with patch.dict("os.environ", {}, clear=False), self.assertRaisesRegex(
                RuntimeError, "PRIOR_DAY_RECONCILIATION_BLOCKING:NOT_FINAL"
            ):
                cli._cmd_daily_impl(
                    with_odds=False, morning_only=False, odds_phase="EARLY",
                    daily_run_id="test", recorder=recorder,
                )
        schedule.assert_not_called()
        capture.assert_not_called()

    def test_roster_capture_does_not_touch_prior_learning_feature_tables(self):
        source = (cli.SCRIPTS_DIR / "refresh_players_and_roster_today.py").read_text()
        self.assertIn("write_roster_observation(", source)
        self.assertIn("with conn.transaction():", source)
        self.assertIn("merge_roster_status_from_temp(", source)
        self.assertIn("ROSTER_OBSERVATION_REQUIRES_OFFICIAL_RESPONSES", source)
        self.assertNotIn("ensure_prior_learning", source)
        daily_source = Path(cli.__file__).read_text()
        self.assertLess(
            daily_source.index("recorder.prior_learning = ensure_prior_learning(prior_date_et)"),
            daily_source.index("_bootstrap_daily_canonical_slate(\n        slate=slate"),
        )
        self.assertLess(
            daily_source.index("_bootstrap_daily_canonical_slate(\n        slate=slate"),
            daily_source.index("# 2) Seed features for today (SOG + Saves)."),
        )

    def test_explicit_roster_reuse_disables_provider_fetch(self):
        recorder = DailyRunRecorder(run_id="daily-1", command=[], phase="EARLY")
        reused = Path("/immutable/2026-10-18/obs-1")
        with patch.object(cli, "verify_roster_observation_reuse", return_value={
            "path": str(reused), "manifest_sha256": "abc123",
        }) as verify, patch.object(cli, "run", side_effect=lambda *a, **k: (
            recorder.record_child({
                "lane": "roster", "command_identity": "refresh_players_and_roster_today.py",
                "exit_status": 0, "status": "COMPLETE",
                "schema_version": "NHL_ROSTER_CHILD_SUMMARY_V2",
                "database_stage_summary": {"roster_status_rows_present": 48},
                "database_write_capable": True,
                "database_write_status": "COMMITTED",
                "transaction_disposition": "COMMITTED",
                "database_row_counts": {}, "database_row_counts_complete": False,
            }) or SimpleNamespace(returncode=0)
        )) as run:
            cli._capture_daily_slate_roster(
                slate="2026-10-18", odds_phase="EARLY", daily_run_id="daily-1",
                recorder=recorder, reuse_roster_observation=reused,
                canonical_games=[SimpleNamespace(game_id=2026020084)],
                game_hash="canonical-hash",
            )

        verify.assert_called_once()
        self.assertEqual(run.call_count, 1)
        env = run.call_args.kwargs["env"]
        self.assertEqual(env["NHL_REUSE_ROSTER_OBSERVATION"], str(reused.resolve()))
        self.assertEqual(env["NHL_FETCH_DISABLE"], "1")
        self.assertEqual(recorder.lane("roster").status, "REUSED")

    def test_valid_initialized_empty_schedule_is_explicit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            slate_dir = root / "artifacts/operational/nhl/slates/2026-10-11"
            slate_dir.mkdir(parents=True)
            (slate_dir / "slate_health.json").write_text(json.dumps({
                "slate_date": "2026-10-11", "downstream_ready": True,
                "completion_status": "VALID_EMPTY_SLATE",
            }))
            recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
            with patch.object(cli, "ROOT", root), patch.object(cli, "run") as run:
                health, games, game_hash = cli._bootstrap_daily_canonical_slate(
                    slate="2026-10-11", recorder=recorder)
            self.assertEqual(health["completion_status"], "VALID_EMPTY_SLATE")
            self.assertEqual(games, [])
            self.assertEqual(game_hash, canonical_game_set_hash([]))
            self.assertEqual(recorder.canonical_game_ids, [])
            self.assertEqual(recorder.canonical_game_set_hash, canonical_game_set_hash([]))
            run.assert_called_once()

    def test_production_sog_accepts_full_canonical_lineage_with_eligible_subset(self):
        canonical = [10, 11, 12, 13]
        receipt = {"canonical_game_ids": canonical,
                   "canonical_game_set_hash": canonical_game_set_hash(canonical),
                   "eligible_pregame_game_ids": [12, 13]}
        output = {"canonical_game_set_hash": receipt["canonical_game_set_hash"]}
        _validate_production_sog_game_set(receipt, output,
                                          pd.DataFrame({"game_id": [12, 13]}))

    def test_production_sog_grades_predicted_games_when_an_eligible_game_has_none(self):
        canonical = [10, 11, 12, 13]
        receipt = {"canonical_game_ids": canonical,
                   "canonical_game_set_hash": canonical_game_set_hash(canonical),
                   "eligible_pregame_game_ids": [12, 13]}
        output = {"canonical_game_set_hash": receipt["canonical_game_set_hash"]}
        self.assertEqual(_validate_production_sog_game_set(
            receipt, output, pd.DataFrame({"game_id": [13]})), [12])

    def test_production_sog_rejects_a_real_canonical_mismatch(self):
        canonical = [10, 11, 12, 13]
        receipt = {"canonical_game_ids": canonical,
                   "canonical_game_set_hash": canonical_game_set_hash(canonical),
                   "eligible_pregame_game_ids": [12, 13]}
        output = {"canonical_game_set_hash": receipt["canonical_game_set_hash"]}
        with self.assertRaisesRegex(ValueError, "PRODUCTION_SOG_CANONICAL_GAME_SET_MISMATCH"):
            _validate_production_sog_game_set(receipt, output,
                                              pd.DataFrame({"game_id": [11, 13]}))

    def test_production_sog_allows_initialized_empty_full_slate(self):
        empty_hash = canonical_game_set_hash([])
        _validate_production_sog_game_set(
            {"canonical_game_ids": [], "canonical_game_set_hash": empty_hash,
             "eligible_pregame_game_ids": []},
            {"canonical_game_set_hash": empty_hash}, pd.DataFrame({"game_id": []}))


if __name__ == "__main__":
    unittest.main()

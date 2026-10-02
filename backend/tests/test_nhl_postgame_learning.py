import hashlib
import json
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.daily_capture import CanonicalGame
from backend.nhl.game_phase import phase_for_game_type, regular_season_evaluation_eligible
from backend.nhl.postgame_reconcile.core import _grade_game_lines
from backend.nhl.prediction_lineage import build_canonical_game_map
from backend.nhl.postgame_learning import (
    current_et_slate, ensure_prior_learning, prior_et_slate,
)
from backend.nhl.daily_orchestration import DailyRunRecorder


class NHLPostgameLearningTest(unittest.TestCase):
    def test_official_game_type_is_canonical_phase_authority(self):
        self.assertEqual(phase_for_game_type(2), "REGULAR_SEASON")
        self.assertTrue(regular_season_evaluation_eligible(2))
        self.assertEqual(phase_for_game_type(1), "PRESEASON")
        self.assertFalse(regular_season_evaluation_eligible(1))

    def test_daily_receipt_retains_prior_learning_status(self):
        recorder = DailyRunRecorder(
            run_id="fixture", command=["daily"], phase="EARLY",
            started_at=datetime(2026, 10, 2, 4, 30, tzinfo=ZoneInfo("UTC")))
        recorder.prior_learning = {"prior_slate_date": "2026-09-30", "reconciliation_status": "REUSED_VALID_PACKAGE"}
        payload = recorder.payload()
        self.assertEqual(payload["prior_day_learning"], recorder.prior_learning)
        self.assertEqual(payload["started_at_et"], "2026-10-02T00:30:00-04:00")
        self.assertEqual(payload["operational_timezone"], "America/New_York")
        self.assertNotIn("started_at_pt", payload)

    def test_prior_slate_is_yesterday_in_new_york_time(self):
        clock = datetime(2026, 10, 1, 9, 10, tzinfo=ZoneInfo("America/New_York"))
        self.assertEqual(prior_et_slate(clock), "2026-09-30")

    def test_slate_and_prior_date_at_et_pt_calendar_boundary(self):
        # At this instant it is Oct 2 in New York, but still Oct 1 in Los Angeles.
        clock = datetime(2026, 10, 2, 4, 30, tzinfo=ZoneInfo("UTC"))
        self.assertEqual(current_et_slate(clock), "2026-10-02")
        self.assertEqual(prior_et_slate(clock), "2026-10-01")

    def test_prediction_lineage_uses_eastern_game_date(self):
        game = CanonicalGame(
            2026020001, "2026-10-02T04:30:00Z", "H", "A",
            home_team_id=1, away_team_id=2)
        lineage = build_canonical_game_map([game], slate="2026-10-02")
        self.assertEqual(lineage[2026020001].game_date, "2026-10-02")

    def test_automatic_reconciliation_supplies_typed_authority_and_roster_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "reconciliations"
            slate = "2026-10-01"
            schedule = Path(tmp) / "raw_schedule_response.json"
            schedule.write_text(json.dumps({"gameWeek": [{"date": slate, "games": [{"id": 1}]}]}))
            (Path(tmp) / "slates" / slate).mkdir(parents=True)
            (Path(tmp) / "slates" / slate / "raw_schedule_response.json").write_text(schedule.read_text())
            responses = [
                subprocess.CompletedProcess([], 0, json.dumps({
                    "status": "COMPLETE", "run_id": "authority-roster-run",
                    "request_accounting": {"total_logical_requests": 6},
                }), ""),
                subprocess.CompletedProcess([], 2, json.dumps({"failure": "NOT_OFFICIAL_FINAL"}), ""),
            ]
            with patch("backend.nhl.postgame_learning.ROOT", Path(tmp)), \
                    patch("backend.nhl.postgame_learning.subprocess.run", side_effect=responses) as run:
                result = ensure_prior_learning(
                    slate, reconciliation_root=root, cross_market_root=Path(tmp) / "cross",)
            self.assertEqual(result["official_outcomes_status"], "NOT_FINAL")
            self.assertEqual(result["provider_calls"], 6)
            self.assertEqual(run.call_count, 2)
            execute = run.call_args_list[1].args[0]
            self.assertIn("--execute", execute)
            self.assertIn("AUTHORITY_RESPONSE_SOURCE=authority-roster-run", execute)
            self.assertIn("ROSTER_RESPONSE_SOURCE=authority-roster-run", execute)
            self.assertFalse(any("PLAYER_IDENTITY_RESPONSE_SOURCE=" in token for token in execute))
            self.assertFalse((root / slate).exists())
            self.assertFalse((root / ".locks" / f"{slate}.lock").exists())

    def test_no_prior_games_is_nonfatal_and_does_not_start_reconciler(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            slate = "2026-10-01"
            schedule = root / "artifacts/operational/nhl/slates" / slate / "raw_schedule_response.json"
            schedule.parent.mkdir(parents=True)
            schedule.write_text(json.dumps({"gameWeek": [{"date": slate, "games": []}]}))
            with patch("backend.nhl.postgame_learning.ROOT", root), \
                    patch("backend.nhl.postgame_learning.subprocess.run") as run:
                result = ensure_prior_learning(slate, reconciliation_root=root / "reconciliations")
            self.assertEqual(result["reconciliation_status"], "NO_PRIOR_GAMES")
            self.assertEqual(result["official_outcomes_status"], "NO_PRIOR_GAMES")
            run.assert_not_called()

    def test_failed_authority_acquisition_leaves_no_lock_or_partial_package(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "reconciliations"
            slate = "2026-10-01"
            schedule = Path(tmp) / "slates" / slate / "raw_schedule_response.json"
            schedule.parent.mkdir(parents=True)
            schedule.write_text(json.dumps({"gameWeek": [{"date": slate, "games": [{"id": 1}]}]}))
            failed = subprocess.CompletedProcess([], 5, "", "source acquisition failed")
            with patch("backend.nhl.postgame_learning.ROOT", Path(tmp)), \
                    patch("backend.nhl.postgame_learning.subprocess.run", return_value=failed):
                with self.assertRaisesRegex(RuntimeError, "AUTHORITY_ROSTER_ACQUISITION_FAILED"):
                    ensure_prior_learning(slate, reconciliation_root=root)
            self.assertFalse((root / ".locks" / f"{slate}.lock").exists())
            self.assertFalse((root / slate).exists())

    def test_moneyline_and_puck_line_use_regular_season_results(self):
        base = pd.DataFrame([{
            "game_type_code": 2, "official_full_game_winner": "HOME",
            "official_final_home_goals": 4, "official_final_away_goals": 1,
            "model_favored_team": "HOME", "home_team": "HOME",
        }])
        ml, pl = _grade_game_lines(base.copy(), base.copy())
        self.assertEqual(ml.grading_status.iloc[0], "REGULAR_SEASON_GRADED")
        self.assertEqual(ml.regular_season_evaluation_target.iloc[0], 1)
        self.assertTrue(bool(ml.prediction_correct.iloc[0]))
        self.assertEqual(pl.actual_margin_class.iloc[0], "HOME_BY_2_PLUS")

    def test_existing_completed_package_is_reused_and_restatement_is_versioned(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "reconciliations"
            package = root / "2026-09-29" / "reconciliation=abc"
            package.mkdir(parents=True)
            games = pd.DataFrame([{
                "game_type_code": 2, "official_full_game_winner": "HOME",
                "official_final_home_goals": 3, "official_final_away_goals": 1,
                "official_final": True, "outcome_source_timestamp_utc": "2026-09-30T10:00:00Z",
            }])
            ml = pd.DataFrame([{
                **games.iloc[0].to_dict(), "model_favored_team": "HOME", "home_team": "HOME",
            }])
            pl = ml.copy()
            games.to_csv(package / "canonical_game_outcomes.csv", index=False)
            ml.to_csv(package / "graded_moneyline.csv", index=False)
            pl.to_csv(package / "graded_puck_line.csv", index=False)
            (package / "summary.json").write_text(json.dumps({"status": "COMPLETE", "slate_date": "2026-09-29", "games": 1}))
            (package / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
            files = sorted(path for path in package.iterdir() if path.is_file())
            (package / "SHA256SUMS").write_text("".join(
                f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in files
            ))
            result = ensure_prior_learning(
                "2026-09-29", reconciliation_root=root,
                cross_market_root=Path(tmp) / "cross_market", create_if_missing=False,
            )
            self.assertEqual(result["reconciliation_status"], "REUSED_VALID_PACKAGE")
            self.assertTrue(Path(result["phase_restatement"]).is_dir())
            self.assertEqual(pd.read_csv(Path(result["phase_restatement"]) / "graded_moneyline.csv").grading_status.iloc[0], "REGULAR_SEASON_GRADED")
            result2 = ensure_prior_learning(
                "2026-09-29", reconciliation_root=root,
                cross_market_root=Path(tmp) / "cross_market", create_if_missing=False,
            )
            self.assertEqual(result2["phase_restatement"], result["phase_restatement"])


if __name__ == "__main__":
    unittest.main()

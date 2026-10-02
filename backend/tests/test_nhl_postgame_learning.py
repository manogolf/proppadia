import hashlib
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.game_phase import phase_for_game_type, regular_season_evaluation_eligible
from backend.nhl.postgame_reconcile.core import _grade_game_lines
from backend.nhl.postgame_learning import ensure_prior_learning, prior_et_slate
from backend.nhl.daily_orchestration import DailyRunRecorder


class NHLPostgameLearningTest(unittest.TestCase):
    def test_official_game_type_is_canonical_phase_authority(self):
        self.assertEqual(phase_for_game_type(2), "REGULAR_SEASON")
        self.assertTrue(regular_season_evaluation_eligible(2))
        self.assertEqual(phase_for_game_type(1), "PRESEASON")
        self.assertFalse(regular_season_evaluation_eligible(1))

    def test_daily_receipt_retains_prior_learning_status(self):
        recorder = DailyRunRecorder(run_id="fixture", command=["daily"], phase="EARLY")
        recorder.prior_learning = {"prior_slate_date": "2026-09-30", "reconciliation_status": "REUSED_VALID_PACKAGE"}
        self.assertEqual(recorder.payload()["prior_day_learning"], recorder.prior_learning)

    def test_prior_slate_is_yesterday_in_new_york_time(self):
        clock = datetime(2026, 10, 1, 9, 10, tzinfo=ZoneInfo("America/New_York"))
        self.assertEqual(prior_et_slate(clock), "2026-09-30")

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

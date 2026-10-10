from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.prediction_lineage import (
    POINTS_LINES, pregame_game_eligibility, validate_prediction_output,
)


class PredictionLineageEligibilityTests(unittest.TestCase):
    def test_all_started_slate_has_clean_ready_noop_classification(self):
        games = [
            {"game_id": 2026020001, "start_time_utc": "2026-10-02T01:00:00Z"},
            {"game_id": 2026020002, "start_time_utc": "2026-10-02T02:00:00Z"},
        ]
        eligibility = pregame_game_eligibility(games, "2026-10-02T02:00:00Z")
        self.assertEqual(eligibility["eligible_pregame_game_ids"], [])
        self.assertEqual(eligibility["started_excluded_game_ids"], [2026020001, 2026020002])
        recorder = DailyRunRecorder(run_id="late-run", command=[], phase="EARLY")
        recorder.set_canonical(
            slate_date="2026-10-01", season=2026,
            game_ids=[game["game_id"] for game in games],
            game_set_hash=canonical_game_set_hash(game["game_id"] for game in games))
        recorder.set_pregame_eligibility(
            eligible_game_ids=eligibility["eligible_pregame_game_ids"],
            started_excluded_game_ids=eligibility["started_excluded_game_ids"],
            cutoff_utc="2026-10-02T02:00:00Z")
        for lane in recorder.lanes:
            recorder.finish_lane(lane, status="SKIPPED_NO_PREGAME_GAMES",
                                 reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
        self.assertEqual(recorder.classification(), "READY")
        payload = recorder.payload()
        self.assertEqual(payload["canonical_game_count"], 2)
        self.assertEqual(payload["eligible_pregame_game_count"], 0)
        self.assertEqual(payload["started_excluded_game_count"], 2)

    def _frame(self, game_ids):
        starts = {2026020001: "2026-10-02T01:00:00Z", 2026020002: "2026-10-02T03:00:00Z"}
        rows = []
        for game_id in game_ids:
            for line in POINTS_LINES:
                rows.append({
                    "player_id": 9, "game_id": game_id, "game_date": "2026-10-01",
                    "game_start_utc": starts[game_id], "home_team_id": 1, "away_team_id": 2,
                    "parent_daily_run_id": "mixed-run",
                    "feature_input_cutoff_utc": "2026-10-02T02:00:00Z",
                    "canonical_game_set_hash": canonical_game_set_hash([2026020001, 2026020002]),
                    "line": line, "prob_over": 0.5,
                })
        return pd.DataFrame(rows)

    def test_eligible_only_prediction_validates_against_full_canonical_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.csv"
            self._frame([2026020002]).to_csv(path, index=False)
            result = validate_prediction_output(
                path=path, lane="points", canonical_games=[
                    {"game_id": 2026020001, "start_time_utc": "2026-10-02T01:00:00Z", "home_team_id": 1, "away_team_id": 2},
                    {"game_id": 2026020002, "start_time_utc": "2026-10-02T03:00:00Z", "home_team_id": 1, "away_team_id": 2},
                ], slate="2026-10-01", parent_daily_run_id="mixed-run",
                feature_input_cutoff_utc="2026-10-02T02:00:00Z",
                expected_game_set_hash=canonical_game_set_hash([2026020001, 2026020002]),
                expected_lines=POINTS_LINES,
            )
        self.assertEqual(result["canonical_game_count"], 2)
        self.assertEqual(result["eligible_pregame_game_ids"], [2026020002])
        self.assertEqual(result["started_excluded_game_ids"], [2026020001])

    def test_started_game_prediction_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.csv"
            self._frame([2026020001, 2026020002]).to_csv(path, index=False)
            with self.assertRaisesRegex(RuntimeError, "PREDICTION_STARTED_GAME_INCLUDED"):
                validate_prediction_output(
                    path=path, lane="points", canonical_games=[
                        {"game_id": 2026020001, "start_time_utc": "2026-10-02T01:00:00Z", "home_team_id": 1, "away_team_id": 2},
                        {"game_id": 2026020002, "start_time_utc": "2026-10-02T03:00:00Z", "home_team_id": 1, "away_team_id": 2},
                    ], slate="2026-10-01", parent_daily_run_id="mixed-run",
                    feature_input_cutoff_utc="2026-10-02T02:00:00Z",
                    expected_game_set_hash=canonical_game_set_hash([2026020001, 2026020002]),
                    expected_lines=POINTS_LINES,
                )


if __name__ == "__main__":
    unittest.main()

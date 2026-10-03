from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.daily_orchestration import DailyRunRecorder, RECEIPT_SCHEMA
from backend.nhl.prediction_lineage import (
    POINTS_LINES,
    SAVES_LINES,
    SOG_LINES,
    prepare_scoring_input,
    validate_prediction_output,
    validate_sog_prediction_artifacts,
    wide_line_column,
)


GAME_ID = 2026020001
PLAYER_IDS = (101, 102)
SLATE = "2026-10-01"
RUN_ID = "test_sog_receipt_run"
GAME = {
    "game_id": GAME_ID,
    "start_time_utc": "2026-10-02T02:00:00Z",
    "home_team_id": 1,
    "away_team_id": 2,
}
GAME_HASH = canonical_game_set_hash([GAME_ID])


class SogPredictionReceiptMetadataTests(unittest.TestCase):
    def _sog_artifacts(self, root: Path):
        run_dir = root / RUN_ID
        run_dir.mkdir()
        scored = run_dir / "sog_predictions_wide_calibrated.csv"
        unscored = run_dir / "sog_predictions_unscored.csv"
        pd.DataFrame([
            {"game_id": GAME_ID, "player_id": player, "game_date": SLATE,
             "p_over_1_5": 0.7, "p_over_2_5": 0.4, "p_over_3_5": 0.2}
            for player in PLAYER_IDS
        ]).to_csv(scored, index=False)
        pd.DataFrame([
            {"game_id": GAME_ID, "player_id": 103, "game_date": SLATE,
             "line": line, "reason": "MISSING_EXPOSURE"}
            for line in SOG_LINES
        ]).to_csv(unscored, index=False)
        return scored, unscored

    def _validate_sog(self, root: Path):
        scored, unscored = self._sog_artifacts(root)
        return validate_sog_prediction_artifacts(
            scored_path=scored, unscored_path=unscored, slate=SLATE,
            parent_daily_run_id=RUN_ID, canonical_game_ids=[GAME_ID],
            expected_game_set_hash=GAME_HASH,
        )

    def test_sog_scored_artifact_reports_natural_identity_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            scored, _ = self._validate_sog(Path(tmp))
        self.assertEqual(scored["natural_identity_count"], 2)

    def test_sog_scored_artifact_reports_line_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            scored, _ = self._validate_sog(Path(tmp))
        self.assertEqual(scored["line_count"], len(SOG_LINES))

    def test_sog_scored_artifact_reports_conditional_prediction_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            scored, _ = self._validate_sog(Path(tmp))
        self.assertEqual(scored["conditional_prediction_count"], 2 * len(SOG_LINES))

    def test_sog_row_count_matches_wide_artifact_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            scored, _ = self._validate_sog(Path(tmp))
        self.assertEqual(scored["row_count"], scored["natural_identity_count"])

    def test_sog_unscored_identities_are_counted_separately(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, unscored = self._validate_sog(Path(tmp))
        self.assertEqual(unscored["unscored_identity_count"], 1)
        self.assertEqual(unscored["unscored_row_count"], len(SOG_LINES))

    def test_sog_unscored_reason_counts_preserve_line_grain_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, unscored = self._validate_sog(Path(tmp))
        self.assertEqual(unscored["unscored_reason_counts"], {"MISSING_EXPOSURE": 3})

    def test_duplicate_scored_identity_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            scored_path, unscored_path = self._sog_artifacts(Path(tmp))
            scored = pd.read_csv(scored_path)
            pd.concat([scored, scored.iloc[[0]]], ignore_index=True).to_csv(
                scored_path, index=False)
            with self.assertRaisesRegex(RuntimeError, "SOG_DUPLICATE_SCORED_IDENTITY"):
                validate_sog_prediction_artifacts(
                    scored_path=scored_path, unscored_path=unscored_path,
                    slate=SLATE, parent_daily_run_id=RUN_ID,
                    canonical_game_ids=[GAME_ID], expected_game_set_hash=GAME_HASH)

    def test_scored_and_unscored_identity_overlap_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            scored_path, unscored_path = self._sog_artifacts(Path(tmp))
            unscored = pd.read_csv(unscored_path)
            unscored["player_id"] = PLAYER_IDS[0]
            unscored.to_csv(unscored_path, index=False)
            with self.assertRaisesRegex(RuntimeError, "SOG_SCORED_UNSCORED_IDENTITY_OVERLAP"):
                validate_sog_prediction_artifacts(
                    scored_path=scored_path, unscored_path=unscored_path,
                    slate=SLATE, parent_daily_run_id=RUN_ID,
                    canonical_game_ids=[GAME_ID], expected_game_set_hash=GAME_HASH)

    def _lineage_frame(self, rows):
        result = pd.DataFrame(rows)
        result["game_date"] = SLATE
        result["game_start_utc"] = GAME["start_time_utc"]
        result["home_team_id"] = GAME["home_team_id"]
        result["away_team_id"] = GAME["away_team_id"]
        result["parent_daily_run_id"] = RUN_ID
        result["feature_input_cutoff_utc"] = "2026-10-01T20:00:00Z"
        result["canonical_game_set_hash"] = GAME_HASH
        return result

    def test_points_validation_receipt_behavior_remains_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "points.csv"
            rows = [
                {"game_id": GAME_ID, "player_id": 101, "line": line,
                 "prob_over": 0.5}
                for line in POINTS_LINES
            ]
            self._lineage_frame(rows).to_csv(path, index=False)
            result = validate_prediction_output(
                path=path, lane="points", canonical_games=[GAME], slate=SLATE,
                parent_daily_run_id=RUN_ID,
                feature_input_cutoff_utc="2026-10-01T20:00:00Z",
                expected_game_set_hash=GAME_HASH, expected_lines=POINTS_LINES)
        self.assertEqual(result["natural_identity_count"], 1)
        self.assertEqual(result["conditional_prediction_count"], 3)
        self.assertTrue(result["validated_prediction_identity"])

    def test_saves_validation_receipt_behavior_remains_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "saves.csv"
            row = {"game_id": GAME_ID, "player_id": 101}
            row.update({wide_line_column(line): 0.5 for line in SAVES_LINES})
            self._lineage_frame([row]).to_csv(path, index=False)
            result = validate_prediction_output(
                path=path, lane="saves", canonical_games=[GAME], slate=SLATE,
                parent_daily_run_id=RUN_ID,
                feature_input_cutoff_utc="2026-10-01T20:00:00Z",
                expected_game_set_hash=GAME_HASH, expected_lines=SAVES_LINES)
        self.assertEqual(result["natural_identity_count"], 1)
        self.assertEqual(result["conditional_prediction_count"], len(SAVES_LINES))
        self.assertTrue(result["validated_prediction_identity"])

    def test_daily_receipt_schema_retains_sog_count_metadata(self):
        recorder = DailyRunRecorder(
            run_id=RUN_ID, command=["daily"], phase="EARLY",
            started_at=datetime(2026, 10, 1, tzinfo=timezone.utc))
        recorder.finish_lane("legacy_sog", outputs=[{
            "natural_identity_count": 2,
            "line_count": 3,
            "conditional_prediction_count": 6,
            "row_count": 2,
        }])
        payload = recorder.payload()
        self.assertEqual(payload["schema_version"], RECEIPT_SCHEMA)
        self.assertEqual(
            payload["lanes"]["legacy_sog"]["outputs"][0]["conditional_prediction_count"],
            6,
        )


if __name__ == "__main__":
    unittest.main()

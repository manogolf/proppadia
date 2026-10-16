from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.points_hgb_shadow import capture_from_files


class HgbOfficialTeamShotCaptureTests(unittest.TestCase):
    def test_daily_capture_retains_source_bound_team_game_history_and_feature(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            logs_path = root / "official_skater_logs.csv"
            outcomes_path = root / "official_outcomes.csv"
            slate_path = root / "canonical_slate.csv"
            logs = pd.DataFrame([
                {"game_id": 2026020899, "player_id": 11, "team_id": 1, "is_home": True,
                 "home_team_id": 1, "away_team_id": 2, "game_date": "2026-04-29",
                 "start_time_utc": "2026-04-29T23:00:00Z", "toi_minutes": 10,
                 "pp_toi_minutes": 0, "shots_on_goal": 0, "shot_attempts": 1},
                {"game_id": 2026020899, "player_id": 12, "team_id": 1, "is_home": True,
                 "home_team_id": 1, "away_team_id": 2, "game_date": "2026-04-29",
                 "start_time_utc": "2026-04-29T23:00:00Z", "toi_minutes": 10,
                 "pp_toi_minutes": 0, "shots_on_goal": 2, "shot_attempts": 3},
            ])
            outcomes = pd.DataFrame([
                {"game_id": 2026020899, "player_id": pid, "game_date": "2026-04-29",
                 "canonical_season": 2026, "team_id": 1, "official_goals": 0,
                 "official_assists": 0, "official_sog": sog, "outcome_source_timestamp_utc": "2026-04-30T12:00:00Z"}
                for pid, sog in ((11, 0), (12, 4))
            ])
            slate = pd.DataFrame([{"game_id": 2026020900, "player_id": 21, "game_date": "2026-04-30",
                "game_start_utc": "2026-04-30T23:00:00Z", "team_id": 1, "is_home": True,
                "home_team_id": 1, "away_team_id": 2}])
            logs.to_csv(logs_path, index=False)
            outcomes.to_csv(outcomes_path, index=False)
            slate.to_csv(slate_path, index=False)
            result = capture_from_files(logs_path=logs_path, outcomes_paths=[outcomes_path],
                slate_path=slate_path, phoenix_predictions_path=None, phoenix_prediction_sha256=None,
                phoenix_model_identity_sha256=None, phoenix_feature_contract_sha256=None,
                phoenix_feature_cutoff_utc=None, slate_date="2026-04-30", season=2026,
                capture_phase="EARLY", parent_run_id="hgb_test_run",
                feature_cutoff_utc="2026-04-30T12:00:00Z", output_root=root / "capture",
                canonical_game_ids=[2026020900], source_artifacts={
                    "official_skater_logs": logs_path,
                    "canonical_slate": slate_path,
                    "official_outcomes_01": outcomes_path,
                }, source_snapshot_metadata={"transaction_isolation": "repeatable read",
                    "transaction_read_only": "on", "source_manifest_sha256": "fixture"})
            capture = Path(result["capture_path"])
            feature = pd.read_csv(capture / "features.csv")
            history = pd.read_csv(capture / "prior_team_game_contributions.csv")
            self.assertEqual(float(feature.iloc[0].team_d10_sf_per_game), 4.0)
            self.assertEqual(int(history.iloc[0].valid_game_count), 1)
            self.assertEqual(str(history.iloc[0].contributing_game_ids), "2026020899")
            self.assertIn("2026-04-30", history.iloc[0].source_timestamps)
            self.assertEqual(result["source_to_feature_capture_status"], "EVIDENCE_CAPTURE_COMPLETE")
            self.assertEqual(result["source_to_feature_validation"]["maximum_absolute_feature_difference"], 0.0)
            self.assertEqual(result["source_to_feature_validation"]["missing_official_sog_rows"], 0)
            self.assertEqual(result["source_to_feature_validation"]["team_shot_feature_contract"],
                             "NHL_POINTS_TEAM_D10_OFFICIAL_CANONICAL_V1")
            self.assertTrue((capture / "SHA256SUMS").is_file())


if __name__ == "__main__":
    unittest.main()

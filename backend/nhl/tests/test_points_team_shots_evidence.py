from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.points_team_shots_evidence import (
    reconstruct_from_raw_rows,
    verify_evidence_package,
    write_evidence_package,
)


def targets(*, team_id: int = 1, date: str = "2026-01-10") -> pd.DataFrame:
    return pd.DataFrame([{"player_id": 7, "game_id": 100, "game_date": date, "team_id": team_id}])


def raw_rows() -> pd.DataFrame:
    return pd.DataFrame([
        {"player_id": 1, "game_id": 10, "game_date": "2026-01-01", "team_id": 1, "shots_on_goal": 0, "shot_attempts": 4, "toi_minutes": 12.0},
        {"player_id": 2, "game_id": 10, "game_date": "2026-01-01", "team_id": 1, "shots_on_goal": 2, "shot_attempts": 5, "toi_minutes": 13.0},
        {"player_id": 1, "game_id": 11, "game_date": "2026-01-05", "team_id": 1, "shots_on_goal": 5, "shot_attempts": 9, "toi_minutes": 14.0},
        # Same target date: excluded regardless of game id ordering.
        {"player_id": 1, "game_id": 99, "game_date": "2026-01-10", "team_id": 1, "shots_on_goal": 60, "shot_attempts": 70, "toi_minutes": 15.0},
    ])


class TeamShotEvidenceTests(unittest.TestCase):
    def test_absent_prior_games_map_to_zero(self):
        target, history, _ = reconstruct_from_raw_rows(raw_rows().iloc[0:0], targets())
        self.assertEqual(target.iloc[0].valid_prior_game_count, 0)
        self.assertEqual(target.iloc[0].team_d10_sf_per_game, 0.0)
        self.assertTrue(history.empty)

    def test_valid_zero_sog_game_remains_a_denominator_observation(self):
        target, history, _ = reconstruct_from_raw_rows(raw_rows(), targets())
        self.assertEqual(target.iloc[0].valid_prior_game_count, 2)
        self.assertAlmostEqual(target.iloc[0].team_d10_sf_per_game, 3.5)
        self.assertTrue((history.history_game_id.to_numpy() == [10, 11]).all())

    def test_missing_skater_records_are_flagged_incomplete_without_invented_zero_rows(self):
        target, history, raw = reconstruct_from_raw_rows(
            raw_rows(), targets(), expected_team_game_rows={(10, 1): 3, (11, 1): 1, (99, 1): 1})
        self.assertFalse(bool(target.iloc[0].source_coverage_complete))
        self.assertEqual(int(history.iloc[0].expected_player_rows), 3)
        self.assertEqual(int(history.iloc[0].source_player_rows), 2)
        self.assertEqual(len(raw.loc[raw.game_id.eq(10)]), 2)

    def test_duplicate_player_game_source_rows_rejected(self):
        duplicated = pd.concat([raw_rows(), raw_rows().iloc[[0]]], ignore_index=True)
        with self.assertRaisesRegex(ValueError, "DUPLICATE_RAW_PLAYER_GAME_ROW"):
            reconstruct_from_raw_rows(duplicated, targets())

    def test_team_id_alias_normalization(self):
        alias_rows = raw_rows().copy()
        alias_rows.loc[alias_rows.game_id.eq(10), "team_id"] = 99
        target, history, raw = reconstruct_from_raw_rows(alias_rows, targets(), team_aliases={99: 1})
        self.assertEqual(target.iloc[0].canonical_team_id, 1)
        self.assertEqual(float(history.official_or_raw_team_sog_sum.sum()), 7.0)
        self.assertTrue(raw.loc[raw.team_id.eq(99), "canonical_team_id"].eq(1).all())

    def test_target_date_and_120_day_window_are_strict(self):
        rows = raw_rows()
        rows.loc[len(rows)] = {"player_id": 3, "game_id": 9, "game_date": "2025-09-12", "team_id": 1,
                               "shots_on_goal": 40, "shot_attempts": 50, "toi_minutes": 10.0}
        rows.loc[len(rows)] = {"player_id": 4, "game_id": 8, "game_date": "2025-09-11", "team_id": 1,
                               "shots_on_goal": 80, "shot_attempts": 90, "toi_minutes": 10.0}
        target, history, _ = reconstruct_from_raw_rows(rows, targets())
        self.assertNotIn(99, history.history_game_id.tolist())
        self.assertIn(9, history.history_game_id.tolist())  # inclusive 120-day lower boundary
        self.assertNotIn(8, history.history_game_id.tolist())
        self.assertAlmostEqual(target.iloc[0].team_d10_sf_per_game, (2 + 5 + 40) / 3)

    def test_incomplete_capture_is_never_marked_supported(self):
        target, _, _ = reconstruct_from_raw_rows(raw_rows(), targets(), source_coverage_complete=False)
        self.assertFalse(bool(target.iloc[0].source_coverage_complete))

    def test_rerun_ids_are_create_only_and_content_checksums_verify(self):
        raw = raw_rows().iloc[0:3].copy()
        target = targets()
        feature_bytes = b"game_id,player_id,team_d10_sf_per_game\n100,7,3.5\n"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            one = write_evidence_package(root=root, run_id="daily_a", slate_date="2026-01-10",
                feature_export_bytes=feature_bytes, targets=target, raw_rows=raw,
                acquired_at_utc="2026-01-10T12:00:00Z")
            two = write_evidence_package(root=root, run_id="daily_b", slate_date="2026-01-10",
                feature_export_bytes=feature_bytes, targets=target, raw_rows=raw,
                acquired_at_utc="2026-01-10T12:01:00Z")
            self.assertEqual(verify_evidence_package(one)["source_completeness_validation"]["status"], "PASS")
            self.assertEqual(verify_evidence_package(two)["parent_daily_run_id"], "daily_b")
            self.assertEqual((one / "target_team_shots.csv").read_bytes(), (two / "target_team_shots.csv").read_bytes())
            with self.assertRaises(FileExistsError):
                write_evidence_package(root=root, run_id="daily_a", slate_date="2026-01-10",
                    feature_export_bytes=feature_bytes, targets=target, raw_rows=raw,
                    acquired_at_utc="2026-01-10T12:02:00Z")


if __name__ == "__main__":
    unittest.main()

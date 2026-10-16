from __future__ import annotations

import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.points_team_shots_evidence import (
    bind_scorer_input,
    capture_points_export_snapshot,
    reconstruct_from_raw_rows,
    verify_evidence_package,
    write_evidence_package,
)
from backend.nhl.prediction_lineage import canonical_game_set_hash, prepare_scoring_input
from backend.nhl.scripts import export_nhl_points_hgb_features as hgb_exporter


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

    def test_partial_and_full_ten_game_windows(self):
        rows = []
        for game in range(10):
            rows.append({"player_id": game + 1, "game_id": 200 + game,
                         "game_date": (pd.Timestamp("2026-01-01") + pd.Timedelta(days=game)).date().isoformat(),
                         "team_id": 1, "shots_on_goal": game, "shot_attempts": 2 * game, "toi_minutes": 10.0})
        full_target = targets(date="2026-01-10")
        target, history, _ = reconstruct_from_raw_rows(pd.DataFrame(rows), full_target)
        self.assertEqual(int(target.iloc[0].valid_prior_game_count), 9)
        partial_target, partial_history, _ = reconstruct_from_raw_rows(pd.DataFrame(rows[:3]), full_target)
        self.assertEqual(int(partial_target.iloc[0].valid_prior_game_count), 3)
        rows.append({"player_id": 11, "game_id": 210, "game_date": "2026-01-10", "team_id": 1,
                     "shots_on_goal": 100, "shot_attempts": 100, "toi_minutes": 10.0})
        full_target, full_history, _ = reconstruct_from_raw_rows(
            pd.DataFrame(rows), targets(date="2026-01-11"))
        self.assertEqual(int(full_target.iloc[0].valid_prior_game_count), 10)
        self.assertEqual(len(full_history), 10)
        self.assertEqual(float(full_target.iloc[0].team_d10_sf_per_game), 14.5)

    def test_missing_source_statistics_are_retained_and_counted(self):
        rows = raw_rows().iloc[:2].copy()
        rows.loc[rows.index[0], "shots_on_goal"] = None
        rows.loc[rows.index[1], "shot_attempts"] = None
        target, history, raw = reconstruct_from_raw_rows(rows, targets())
        self.assertEqual(int(history.iloc[0].missing_sog_rows), 1)
        self.assertEqual(int(history.iloc[0].missing_attempts_rows), 1)
        self.assertTrue(pd.isna(raw.iloc[0].shots_on_goal))
        self.assertEqual(float(history.iloc[0].official_or_raw_team_sog_sum), 2.0)

    def test_export_evidence_reconstructs_phoenix_scorer_input_and_hgb_path_is_separate(self):
        source = raw_rows().iloc[:3].copy()
        source["game_start_utc"] = "2026-01-01T00:00:00Z"
        source["canonical_team_id"] = 1
        source["home_team_id"] = 1
        source["away_team_id"] = 2
        target = targets()
        target["canonical_team_id"] = 1
        target["game_start_utc"] = "2026-01-10T22:00:00Z"
        target["home_team_id"] = 1
        target["away_team_id"] = 2
        target["is_home"] = 1
        exported = pd.DataFrame([{"game_id": 100, "player_id": 7,
                                  "team_d10_sf_per_game": 3.5, "is_home": 1}])
        feature_bytes = exported.to_csv(index=False, lineterminator="\n").encode()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = write_evidence_package(root=root, run_id="phoenix_run", slate_date="2026-01-10",
                feature_export_bytes=feature_bytes, targets=target, raw_rows=source,
                acquired_at_utc="2026-01-10T12:00:00Z", same_snapshot_as_feature_export=True,
                snapshot_metadata={"transaction_isolation": "repeatable read", "transaction_read_only": "on"})
            scorer_source = root / "source.csv"
            exported.to_csv(scorer_source, index=False)
            scorer_path = root / "points_scoring_input.csv"
            game = SimpleNamespace(game_id=100, start_time_utc="2026-01-10T22:00:00Z",
                                   home_team_id=1, away_team_id=2)
            prepare_scoring_input(source_path=scorer_source, output_path=scorer_path,
                canonical_games=[game], slate="2026-01-10", parent_daily_run_id="phoenix_run",
                feature_input_cutoff_utc="2026-01-10T12:00:00Z",
                expected_game_set_hash=canonical_game_set_hash([100]))
            bind_scorer_input(package, scorer_path)
            retained = pd.read_csv(package / "phoenix_scorer_input.csv")
            self.assertEqual(float(retained.iloc[0].team_d10_sf_per_game), 3.5)
            manifest = verify_evidence_package(package)
            self.assertTrue(manifest["same_snapshot_as_feature_export"])
            self.assertEqual(manifest["source_completeness_validation"]["status"], "PASS")

        # The production Phoenix output is the SQL COPY contract; the HGB
        # feature path independently consumes official outcomes and source logs.
        history = pd.DataFrame([
            {"game_id": 10, "player_id": 1, "team_id": 1, "is_home": 1, "toi_minutes": 10,
             "pp_toi_minutes": 0, "shots_on_goal": 0, "shot_attempts": 2, "start_time_utc": "2026-01-01T00:00:00Z"},
            {"game_id": 10, "player_id": 2, "team_id": 1, "is_home": 1, "toi_minutes": 10,
             "pp_toi_minutes": 0, "shots_on_goal": 2, "shot_attempts": 3, "start_time_utc": "2026-01-01T00:00:00Z"},
        ])
        outcomes = pd.DataFrame([{"game_id": 10, "player_id": 1, "game_date": "2026-01-01",
                                  "canonical_season": 2026, "goals": 0, "assists": 0},
                                 {"game_id": 10, "player_id": 2, "game_date": "2026-01-01",
                                  "canonical_season": 2026, "goals": 0, "assists": 0}])
        normalized_history = hgb_exporter.normalize_history(history, outcomes)
        hgb_slate = hgb_exporter.normalize_slate(pd.DataFrame([{
            "game_id": 2026020100, "player_id": 7, "game_date": "2026-01-10", "game_start_utc": "2026-01-10T22:00:00Z",
            "is_home": 1, "home_team_id": 1, "away_team_id": 2}]), 2026)
        hgb_features = hgb_exporter.build_features(normalized_history, hgb_slate)
        self.assertEqual(float(hgb_features.iloc[0].team_d10_sf_per_game), 2.0)
        self.assertNotEqual(float(hgb_features.iloc[0].team_d10_sf_per_game), 3.5)
        hgb_history_evidence = hgb_exporter.build_team_shot_history_evidence(normalized_history, hgb_slate)
        self.assertEqual(hgb_history_evidence.iloc[0].contributing_game_ids, "10")
        self.assertEqual(int(hgb_history_evidence.iloc[0].valid_game_count), 1)
        self.assertEqual(float(hgb_history_evidence.iloc[0].team_d10_sf_per_game), 2.0)

    def test_read_only_export_wrapper_writes_snapshot_and_receipt_bound_package(self):
        feature_bytes = b"game_id,player_id,team_d10_sf_per_game\n100,7,3.5\n"
        raw_csv = ("player_id,game_id,game_date,game_start_utc,team_id,canonical_team_id,home_team_id,"
                   "away_team_id,shots_on_goal,shot_attempts,toi_minutes\n"
                   "1,10,2026-01-01,2026-01-01T00:00:00Z,1,1,1,2,0,4,12\n"
                   "2,10,2026-01-01,2026-01-01T00:00:00Z,1,1,1,2,2,5,13\n"
                   "1,11,2026-01-05,2026-01-05T00:00:00Z,1,1,1,2,5,9,14\n")
        target_csv = ("player_id,game_id,game_date,game_start_utc,team_id,canonical_team_id,home_team_id,"
                      "away_team_id,is_home\n7,100,2026-01-10,2026-01-10T22:00:00Z,1,1,1,2,1\n")

        def fake_psql(command, **kwargs):
            script = Path(command[-1]).read_text()
            self.assertIn("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", script)
            self.assertIn(str(export_sql.resolve()), script)
            output_paths = []
            for line in script.splitlines():
                if line.startswith("\\o '"):
                    output_paths.append(Path(line[len("\\o '"):-1]))
            for path, content in zip(output_paths, (feature_bytes, raw_csv.encode(), target_csv.encode(),
                    b"transaction_timestamp_utc,transaction_isolation,transaction_read_only,postgres_snapshot\n"
                    b"2026-01-10T12:00:00Z,repeatable read,on,1:2:\n")):
                path.write_bytes(content)
            return SimpleNamespace(returncode=0, stderr=b"")

        with tempfile.TemporaryDirectory() as temporary:
            export_sql = Path(temporary) / "export_points.sql"
            export_sql.write_text("COPY (SELECT 1) TO STDOUT WITH CSV HEADER;\n")
            root = Path(temporary) / "evidence"
            with patch("backend.nhl.points_team_shots_evidence.subprocess.run", side_effect=fake_psql):
                result = capture_points_export_snapshot(
                    db_url="postgresql://fixture", slate_date="2026-01-10", season=2026,
                    run_id="capture_a", export_sql=export_sql, output_root=root,
                    psql_environment={})
            self.assertEqual(result["status"], "EVIDENCE_CAPTURE_COMPLETE")
            self.assertEqual(result["feature_export_bytes"], feature_bytes)
            self.assertTrue(result["same_snapshot_as_feature_export"])
            manifest = verify_evidence_package(Path(result["path"]))
            self.assertEqual(manifest["snapshot_metadata"]["transaction_read_only"], "on")
            self.assertEqual(manifest["source_completeness_validation"]["maximum_absolute_difference"], 0.0)
            self.assertEqual(manifest["feature_export_sql_sha256"], __import__("hashlib").sha256(export_sql.read_bytes()).hexdigest())

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

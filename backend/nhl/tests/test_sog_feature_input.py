from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.sog_feature_input import begin_capture, finalize_capture


class SogFeatureInputTests(unittest.TestCase):
    def _source(self, root: Path, *, sog=None) -> Path:
        path = root / "source.csv"
        pd.DataFrame([{
            "player_id": 10, "game_id": 2026020001, "game_date": "2026-10-01",
            "shots_on_goal": sog, "d10_sog_per60": 2.1, "season_5on5_icetime_per_game": 12.0,
        }]).to_csv(path, index=False)
        return path

    def _capture(self, root: Path, *, sog=None):
        source = self._source(root, sog=sog)
        return begin_capture(
            source_path=source, root=root / "retained", season=2026,
            slate_date="2026-10-01", run_id="run-1", canonical_game_ids=[2026020001],
            canonical_game_starts_utc={2026020001: "2026-10-02T02:00:00Z"},
            cutoff_utc="2026-10-01T20:00:00Z")

    def test_capture_is_byte_identical_and_reports_schema_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture = self._capture(root)
            self.assertEqual(capture["source_sha256"], capture["retained_sha256"])
            self.assertEqual(capture["source_path"].read_bytes(),
                             (capture["staging_dir"] / "sog_features.csv").read_bytes())
            self.assertEqual(capture["row_count"], 1)
            self.assertIn("d10_sog_per60", capture["ordered_columns"])

    def test_capture_rejects_populated_target_label(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "TARGET_GAME_SOG_PRESENT"):
                self._capture(Path(tmp), sog=2)

    def test_capture_rejects_post_start_cutoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root)
            with self.assertRaisesRegex(ValueError, "NO_PREGAME_GAMES_REMAIN"):
                begin_capture(source_path=source, root=root / "retained", season=2026,
                    slate_date="2026-10-01", run_id="run-1", canonical_game_ids=[2026020001],
                    canonical_game_starts_utc={2026020001: "2026-10-02T02:00:00Z"},
                    cutoff_utc="2026-10-02T02:00:00Z")

    def test_mixed_start_capture_keeps_canonical_identity_and_retains_only_eligible_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "mixed.csv"
            ids = list(range(2026020001, 2026020015))
            cutoff = "2026-10-02T02:00:00Z"
            pd.DataFrame([
                {"player_id": game_id, "game_id": game_id, "game_date": "2026-10-01",
                 "shots_on_goal": None, "d10_sog_per60": 1.8}
                for game_id in ids
            ]).to_csv(source, index=False)
            capture = begin_capture(
                source_path=source, root=root / "retained", season=2026,
                slate_date="2026-10-01", run_id="mixed-run",
                canonical_game_ids=ids,
                canonical_game_starts_utc={
                    game_id: ("2026-10-02T01:00:00Z" if game_id == ids[0]
                              else f"2026-10-02T{3 + index:02d}:00:00Z")
                    for index, game_id in enumerate(ids)
                }, cutoff_utc=cutoff,
            )
            self.assertEqual(capture["canonical_game_ids"], ids)
            self.assertEqual(capture["eligible_pregame_game_ids"], ids[1:])
            self.assertEqual(capture["started_excluded_game_ids"], [ids[0]])
            retained = pd.read_csv(capture["staging_dir"] / "sog_features.csv")
            self.assertEqual(retained.game_id.tolist(), ids[1:])

    def test_finalize_binds_prediction_and_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture = self._capture(root)
            scorer = root / "scorer.py"
            scorer.write_text("# fixture scorer\n")
            prediction = root / "predictions.csv"
            prediction.write_text("prediction\n0.5\n")

            def replay(command, **kwargs):
                Path(command[command.index("--out") + 1]).write_bytes(prediction.read_bytes())
                Path(command[command.index("--unscored-out") + 1]).write_text("empty\n")

            with patch("backend.nhl.sog_feature_input.subprocess.run", side_effect=replay):
                binding = finalize_capture(capture, scorer_path=scorer,
                    model_family="poisson_baseline", model_version="baseline_v1",
                    fitted_model_identity_sha256="model-hash", prediction_path=prediction,
                    python_executable="python")
            manifest = json.loads(Path(binding["feature_input_manifest_path"]).read_text())
            self.assertEqual(manifest["prediction_artifact_sha256"], binding["prediction_artifact_sha256"])
            self.assertEqual(manifest["replay_classification"], "SOG_PRODUCTION_FEATURE_REPLAY_PASS")
            self.assertTrue((Path(binding["feature_input_path"]).parent / "RUN_COMPLETE.json").is_file())
            self.assertTrue((Path(binding["feature_input_path"]).parent / "SHA256SUMS").is_file())

    def test_run_package_is_create_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture = self._capture(root)
            with self.assertRaises(FileExistsError):
                self._capture(root)


if __name__ == "__main__":
    unittest.main()

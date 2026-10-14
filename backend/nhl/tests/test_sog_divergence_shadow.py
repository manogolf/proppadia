from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import verify_package
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.scripts.score_sog_poisson_baseline import score_predictions
from backend.nhl.sog_divergence_shadow import MODEL_IDENTITY, build_predictions, capture


class SogDivergenceShadowTests(unittest.TestCase):
    def features(self):
        return pd.DataFrame([
            {"game_id": 10, "player_id": 1, "team_id": 2, "opponent_id": 3, "is_home": True,
             "game_date": "2026-10-14", "season": 2026, "d10_sog_per60": 6., "d20_sog_per60": 4.,
             "d5_sog_per60": 3., "d10_toi_min_avg": 15., "d20_toi_min_avg": 14., "d5_toi_min_avg": 13.},
            {"game_id": 10, "player_id": 2, "team_id": 2, "opponent_id": 3, "is_home": True,
             "game_date": "2026-10-14", "season": 2026, "d10_sog_per60": 4., "d20_sog_per60": 4.5,
             "d5_sog_per60": 3., "d10_toi_min_avg": 12., "d20_toi_min_avg": 13., "d5_toi_min_avg": 11.},
            {"game_id": 10, "player_id": 3, "team_id": 2, "opponent_id": 3, "is_home": True,
             "game_date": "2026-10-14", "season": 2026, "d10_sog_per60": 3., "d20_sog_per60": 5.,
             "d5_sog_per60": 2., "d10_toi_min_avg": 10., "d20_toi_min_avg": 12., "d5_toi_min_avg": 9.},
            {"game_id": 10, "player_id": 4, "team_id": 2, "opponent_id": 3, "is_home": True,
             "game_date": "2026-10-14", "season": 2026, "d10_sog_per60": 5., "d20_sog_per60": None,
             "d5_sog_per60": 2., "d10_toi_min_avg": 11., "d20_toi_min_avg": 12., "d5_toi_min_avg": 10.},
        ])

    def build(self):
        features = self.features()
        production, _ = score_predictions(features)
        frame = build_predictions(features, production, slate_date="2026-10-14", run_id="parent",
                                  feature_cutoff_utc="2026-10-14T18:00:00Z", canonical_game_ids=[10])
        return features, production, frame

    def test_threshold_midpoint_sides_and_missing_d20(self):
        _, _, rows = self.build()
        by_player = rows.drop_duplicates("player_id").set_index("player_id")
        self.assertFalse(bool(by_player.loc[2, "correction_applied"]))
        self.assertEqual(by_player.loc[2, "correction_reason"], "DIVERGENCE_WITHIN_THRESHOLD")
        self.assertTrue(bool(by_player.loc[1, "correction_applied"]))
        self.assertEqual(by_player.loc[1, "corrected_rate_per60"], 5.)
        self.assertTrue(bool(by_player.loc[3, "correction_applied"]))
        self.assertEqual(by_player.loc[3, "corrected_rate_per60"], 4.)
        self.assertEqual(by_player.loc[4, "correction_reason"], "D20_UNAVAILABLE_NO_CORRECTION")
        self.assertEqual(by_player.loc[4, "corrected_rate_per60"], 5.)

    def test_toi_identity_monotonicity_population_and_production_hash(self):
        features, production, rows = self.build()
        self.assertEqual(set(rows.player_id), set(production.player_id))
        self.assertEqual(len(rows), len(production) * 3)
        self.assertEqual(set(rows.production_selected_toi_minutes), {10., 11., 12., 15.})
        inactive = rows[~rows.correction_applied]
        self.assertTrue(inactive.shadow_lambda.sub(inactive.production_lambda).abs().le(1e-12).all())
        self.assertTrue(inactive.probability_delta.abs().le(1e-12).all())
        for _, group in rows.groupby(["game_id", "player_id"]):
            p = group.set_index("line").shadow_p_over
            self.assertGreaterEqual(p.loc[1.5], p.loc[2.5])
            self.assertGreaterEqual(p.loc[2.5], p.loc[3.5])
        with tempfile.TemporaryDirectory() as tmp:
            original = Path(tmp) / "prod.csv"
            production.to_csv(original, index=False)
            digest = hashlib.sha256(original.read_bytes()).hexdigest()
            build_predictions(features, production, slate_date="2026-10-14", run_id="parent",
                              feature_cutoff_utc="2026-10-14T18:00:00Z", canonical_game_ids=[10])
            self.assertEqual(hashlib.sha256(original.read_bytes()).hexdigest(), digest)

    def test_immutable_capture_replay_and_supported_receipt_lane(self):
        features, production, _ = self.build()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            feature_path, production_path = root / "features.csv", root / "production.csv"
            features.to_csv(feature_path, index=False)
            production.to_csv(production_path, index=False)
            feature_sha = hashlib.sha256(feature_path.read_bytes()).hexdigest()
            scorer = Path("backend/nhl/scripts/score_sog_poisson_baseline.py").resolve()
            result = capture(feature_path=feature_path, production_path=production_path,
                output_root=root / "captures", run_id="parent", slate_date="2026-10-14", season=2026,
                feature_sha256=feature_sha, feature_cutoff_utc="2026-10-14T18:00:00Z",
                canonical_game_ids=[10], started_excluded_game_ids=[], scorer_path=scorer)
            package = Path(result["path"])
            verify_package(package)
            self.assertTrue((package / "RUN_COMPLETE.json").is_file())
            manifest = json.loads((package / "manifest.json").read_text())
            self.assertEqual(manifest["model_identity"], MODEL_IDENTITY)
            self.assertEqual(manifest["deterministic_replay"], "PASS")
            self.assertEqual(manifest["prediction_row_count"], len(production) * 3)
            recorder = DailyRunRecorder(run_id="parent", command=["daily"], phase="EARLY")
            recorder.start_lane("sog_d10_d20_divergence_shadow", inputs=[{"feature_sha": feature_sha}])
            recorder.finish_lane("sog_d10_d20_divergence_shadow", outputs=[result],
                                 provider_requests=0, credits_consumed=0)
            lane = recorder.lane("sog_d10_d20_divergence_shadow")
            self.assertEqual(lane.status, "COMPLETE")
            self.assertEqual(lane.inputs[0]["feature_sha"], feature_sha)
            self.assertEqual(lane.provider_requests, 0)
            self.assertEqual(lane.credits_consumed, 0)
            bad_scorer = root / "missing-scorer.py"
            with self.assertRaises(FileNotFoundError):
                capture(feature_path=feature_path, production_path=production_path,
                    output_root=root / "invalid-captures", run_id="invalid-parent",
                    slate_date="2026-10-14", season=2026, feature_sha256=feature_sha,
                    feature_cutoff_utc="2026-10-14T18:00:00Z", canonical_game_ids=[10],
                    started_excluded_game_ids=[], scorer_path=bad_scorer)
            incomplete = root / "invalid-captures" / "season=2026" / "slate_date=2026-10-14" / "run_id=invalid-parent" / f".{MODEL_IDENTITY}.incomplete"
            self.assertTrue(incomplete.is_dir())
            self.assertFalse((incomplete / "RUN_COMPLETE.json").exists())


if __name__ == "__main__":
    unittest.main()

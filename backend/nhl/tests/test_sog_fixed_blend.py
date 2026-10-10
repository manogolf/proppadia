from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import verify_package
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.sog_fixed_blend import MODELS, build_predictions, capture, grade, performance_metrics
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail, score_predictions


class FixedBlendTests(unittest.TestCase):
    def fixture(self):
        frame = pd.DataFrame([
            {"game_id": 11, "player_id": 101, "team_id": 1, "opponent_id": 2,
             "is_home": True, "game_date": "2026-10-10", "season": 2026,
             "shots_on_goal": None, "d10_sog_per60": 6., "d20_sog_per60": 4.,
             "d10_prior_game_count": 10, "d20_prior_game_count": 13,
             "d10_toi_min_avg": 15., "d20_toi_min_avg": 14., "d5_toi_min_avg": 13.},
            {"game_id": 11, "player_id": 102, "team_id": 1, "opponent_id": 2,
             "is_home": True, "game_date": "2026-10-10", "season": 2026,
             "shots_on_goal": None, "d10_sog_per60": 4., "d20_sog_per60": 5.,
             "d10_prior_game_count": 5, "d20_prior_game_count": 8,
             "d10_toi_min_avg": 12., "d20_toi_min_avg": 13., "d5_toi_min_avg": 11.},
        ])
        production, _ = score_predictions(frame)
        return frame, production

    def test_fixed_identity_rates_history_counts_and_shared_toi(self):
        frame, prod = self.fixture()
        results = build_predictions(frame, prod, run_id="r", slate_date="2026-10-10", season=2026,
                                    feature_sha256="x", cutoff_utc="2026-10-10T12:00:00Z",
                                    canonical_game_ids=[11])
        primary, comparator = [results[m["identity"]] for m in MODELS]
        self.assertEqual(MODELS[0]["identity"], "NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1")
        self.assertEqual(MODELS[1]["identity"], "NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1")
        self.assertEqual(primary.loc[primary.player_id.eq(101), "blended_rate_per60"].iloc[0], 4.5)
        self.assertEqual(primary.loc[primary.player_id.eq(101), "d20_prior_game_count"].iloc[0], 13)
        self.assertEqual(primary.loc[primary.player_id.eq(102), "d20_prior_game_count"].iloc[0], 8)
        self.assertTrue(primary.selected_toi_minutes.isin([12., 15.]).all())
        self.assertEqual(set(primary.line), {1.5, 2.5, 3.5})
        self.assertFalse(primary.duplicated(["game_id", "player_id", "line", "model_identity"]).any())
        self.assertEqual(len(primary), len(comparator))
        row = primary[(primary.player_id == 101) & (primary.line == 1.5)].iloc[0]
        self.assertEqual(row.p_over, _poisson_tail(row.shadow_lambda, 2))
        self.assertAlmostEqual(row.p_over + row.p_under, 1.0)

    def test_capture_is_immutable_bound_and_replayable(self):
        frame, prod = self.fixture()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            feature = root / "features.csv"
            production = root / "production.csv"
            scorer = Path("backend/nhl/scripts/score_sog_poisson_baseline.py").resolve()
            frame.to_csv(feature, index=False)
            prod.to_csv(production, index=False)
            import hashlib
            digest = hashlib.sha256(feature.read_bytes()).hexdigest()
            packages = capture(feature_path=feature, production_path=production, output_root=root / "out",
                               run_id="run-1", slate_date="2026-10-10", season=2026,
                               feature_sha256=digest, cutoff_utc="2026-10-10T12:00:00Z",
                               canonical_game_ids=[11], scorer_path=scorer)
            self.assertEqual(len(packages), 2)
            for item in packages:
                path = Path(item["path"])
                verify_package(path)
                self.assertEqual(item["feature_input_sha256"], digest)
                self.assertTrue((path / "production_control.csv").is_file())
                self.assertTrue(item["prediction_sha256"])

    def test_mixed_slate_blends_score_only_filtered_eligible_features(self):
        features, production = self.fixture()
        eligible_features = features.copy()
        eligible_features["game_id"] = 12
        eligible_production = production.copy()
        eligible_production["game_id"] = 12
        results = build_predictions(
            eligible_features, eligible_production, run_id="mixed-run",
            slate_date="2026-10-10", season=2026, feature_sha256="fixture",
            cutoff_utc="2026-10-10T12:00:00Z", canonical_game_ids=[11, 12])
        for frame in results.values():
            self.assertEqual(set(frame.game_id), {12})

    def test_official_outcome_grading_retains_unsettled_and_exact_lines(self):
        frame, prod = self.fixture()
        predictions = build_predictions(frame, prod, run_id="r", slate_date="2026-10-10", season=2026,
                                        feature_sha256="x", cutoff_utc="2026-10-10T12:00:00Z",
                                        canonical_game_ids=[11])[MODELS[0]["identity"]]
        result = grade(predictions, pd.DataFrame([{"game_id": 11, "player_id": 101, "official_sog": 3}]))
        self.assertEqual(int(result.brier.notna().sum()), 3)
        self.assertEqual(int(result.outcome_status.eq("UNRESOLVED_UNGRADED").sum()), 3)
        self.assertTrue(result.loc[result.player_id.eq(101), "brier"].notna().all())
        metrics = performance_metrics(result)
        self.assertEqual(metrics["n"], 3)
        self.assertIn("3.5", metrics["by_line"])

    def test_research_lane_is_nonblocking_and_prod_scorer_unchanged(self):
        recorder = DailyRunRecorder(run_id="r", command=[], phase="EARLY")
        self.assertFalse(recorder.lane("sog_fixed_blend_shadows").blocking)
        frame, _ = self.fixture()
        before, _ = score_predictions(frame)
        after, _ = score_predictions(frame)
        pd.testing.assert_frame_equal(before, after)


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.nhl.scripts import build_nhl_points_history_volume_sensitivity as study
from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, MODEL_PATH


PACKAGE = study.OUT


class PointsHistoryVolumeSensitivityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = joblib.load(MODEL_PATH)

    def test_completed_package_replay_and_policy_gates(self):
        summary = json.loads((PACKAGE / "summary.json").read_text())
        self.assertLess(summary["2026_reproduction_max_error"], 1e-12)
        self.assertLess(summary["2025_reproduction_max_error"], 1e-12)
        self.assertEqual(summary["provider_calls"], 0)
        self.assertEqual(summary["paid_credits"], 0)
        self.assertEqual(summary["production_db_mutations"], 0)
        self.assertFalse(summary["calibration"])
        self.assertFalse(summary["retraining"])
        self.assertFalse(summary["market_feature"])
        self.assertFalse(summary["production_or_funnel_changed"])
        self.assertTrue((PACKAGE / "lineage_manifest.json").is_file())

    def test_ppg_transformation_changes_only_two_history_features(self):
        features = pd.DataFrame({"game_id": [1], "player_id": [2]})
        for col in FEATURES:
            features[col] = 1.0
        features["current_season_points_prior"] = 2.0
        features["current_season_games_prior"] = 2.0
        features["expected_points"] = 0.4
        features["prob_over_0_5"] = 0.3
        prepared = study.outcome_blind_assignments(features)
        self.assertEqual(prepared.season_ppg_prior.iloc[0], 1.0)
        result = study.maturity_replay(prepared, self.model, [5])
        self.assertEqual(result[0]["counterfactual_points_prior"], 5.0)
        self.assertTrue(result[0]["only_current_season_points_and_games_changed"])
        self.assertTrue(result[0]["continuous_points_semantics"])

    def test_outcome_blind_assignment_does_not_depend_on_outcomes(self):
        frame = pd.DataFrame({"game_id": [1, 2], "player_id": [3, 4],
                              "current_season_points_prior": [1, 0],
                              "current_season_games_prior": [2, 0],
                              "prob_over_0_5": [.3, .4], "realized_points": [0, 3]})
        left = study.outcome_blind_assignments(frame)
        changed = frame.copy()
        changed["realized_points"] = [9, 0]
        right = study.outcome_blind_assignments(changed)
        self.assertEqual(left.ppg_band.astype(str).tolist(), right.ppg_band.astype(str).tolist())
        self.assertEqual(left.games_band.astype(str).tolist(), right.games_band.astype(str).tolist())
        self.assertEqual(left.ppg_band.iloc[1], "NO_PRIOR_GAMES")

    def test_support_and_oct15_package_are_outcome_blind(self):
        density = pd.read_csv(PACKAGE / "training_joint_support_density.csv")
        self.assertGreater(int(density.n.sum()), 0)
        oct_rows = pd.read_csv(PACKAGE / "oct15_outcome_blind_maturity_rows.csv")
        self.assertTrue(len(oct_rows) > 0)
        forbidden = {"realized_points", "official_points", "selected_win", "selected_loss"}
        self.assertTrue(forbidden.isdisjoint(oct_rows.columns))
        oct_metrics = pd.read_csv(PACKAGE / "oct15_outcome_blind_maturity_metrics.csv")
        self.assertTrue(oct_metrics.outcomes_joined.eq(False).all())

    def test_reverse_counterfactual_is_deterministic_and_no_market_feature(self):
        rows = pd.read_csv(PACKAGE / "2025_reverse_counterfactual_rows.csv")
        self.assertEqual(len(rows), 3 * 31430)
        self.assertTrue(rows.only_current_season_points_and_games_changed.all())
        self.assertTrue(set(FEATURES).isdisjoint({"p_over_mkt", "p_under_mkt"}))
        self.assertTrue(np.allclose(rows.counterfactual_points_prior,
                                    rows.season_ppg_prior * rows.counterfactual_games_prior))


if __name__ == "__main__":
    unittest.main()

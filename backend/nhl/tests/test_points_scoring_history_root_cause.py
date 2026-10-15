"""Contract checks for the NHL Points scoring-history diagnosis package."""
import json
from pathlib import Path
import unittest

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "artifacts/analysis/nhl/points_scoring_history_root_cause/2026-10-15"


class PointsScoringHistoryRootCauseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.summary = json.loads((PACKAGE / "summary.json").read_text())

    def test_reproduction_and_strict_prior_history_reconstruction(self):
        self.assertEqual(self.summary["reproduction_gate"], "PASS")
        self.assertEqual(self.summary["settled_2026_n"], 714)
        self.assertLess(self.summary["max_reproduction_error"], 1e-12)
        checks = self.summary["2026_scoring_history_reconstruction"]
        self.assertEqual(set(checks), {
            "player_points_last10", "current_season_points_prior",
            "current_season_games_prior", "mean_toi_last10", "mean_pp_toi_last10",
        })
        self.assertTrue(all(c["exact_match_rate"] == 1 for c in checks.values()))

    def test_rate_semantics_and_2025_replay_limit_are_explicit(self):
        contract = json.loads((PACKAGE / "scoring_history_feature_contract.json").read_text())
        rows = pd.read_csv(PACKAGE / "scoring_history_feature_contract.csv").set_index("feature")
        self.assertIn("already averaged", rows.loc["player_points_last10", "type"])
        self.assertIn("do not divide by 10", rows.loc["player_points_last10", "type"])
        self.assertNotIn("market", " ".join(contract["feature_order"]).lower())
        self.assertEqual(self.summary["2026_scoring_history_reconstruction"]["player_points_last10"]["n"], 714)
        replay = self.summary["2025_scoring_history_reconstruction"]
        self.assertEqual(replay["holdout_rows"], 47239)
        self.assertFalse(replay["raw_per_game_history_available_for_independent_reconstruction"])
        self.assertLess(replay["holdout_prediction_replay_max_abs_error"], 1e-12)

    def test_pending_slate_is_outcome_blind_and_scope_is_diagnostic(self):
        pending = pd.read_csv(PACKAGE / "oct15_pending_regime_rows.csv")
        self.assertTrue(pending["status"].eq("PENDING_OUTCOME").all())
        self.assertFalse({"realized_points", "official_points", "selected_win", "selected_loss"} & set(pending))
        for key in ("calibration", "retraining", "market_feature_added", "production_or_funnel_changed"):
            self.assertFalse(self.summary[key])
        self.assertEqual(self.summary["provider_calls"], 0)
        self.assertEqual(self.summary["paid_credits"], 0)
        self.assertEqual(self.summary["production_db_mutations"], 0)

    def test_counterfactual_artifacts_change_only_declared_inputs(self):
        for filename, flag in [
            ("last10_counterfactual_replay.csv", "only_target_feature_changed"),
            ("season_points_counterfactual.csv", "only_season_points_feature_changed"),
            ("season_games_counterfactual.csv", "only_season_games_feature_changed"),
        ]:
            frame = pd.read_csv(PACKAGE / filename)
            self.assertTrue(frame[flag].all(), filename)
        joint = pd.read_csv(PACKAGE / "joint_points_games_counterfactual.csv")
        self.assertTrue(joint["both_history_features_changed"].all())
        self.assertTrue((joint["preserved_ppg"] - joint["ppg_scenario"]).abs().lt(1e-12).all())

    def test_scorecard_and_manifest_exist(self):
        scorecard = pd.read_csv(PACKAGE / "root_cause_scorecard.csv")
        self.assertIn("attempts support defect", set(scorecard.candidate))
        self.assertTrue((PACKAGE / "lineage_manifest.json").is_file())
        self.assertTrue((PACKAGE / "report.md").is_file())
        self.assertTrue((PACKAGE / "bounded_repair_proposals.csv").is_file())


if __name__ == "__main__":
    unittest.main()

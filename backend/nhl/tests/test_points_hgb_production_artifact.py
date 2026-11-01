"""HGB Points production artifact selection: OFFSET+DEFENSE (P2.39) with the
PP-corrected refit (P2.31) as one-step rollback and V1 behind it."""
import hashlib
import json
import unittest
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(contract: dict) -> str:
    body = {k: v for k, v in contract.items() if k != "model_identity_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


class ProductionArtifactTests(unittest.TestCase):
    def test_production_uses_the_offset_defense_refit(self):
        self.assertEqual(scorer.CONTRACT_PATH, scorer.OFFSET_DEFENSE_CONTRACT_PATH)
        contract = json.loads(scorer.CONTRACT_PATH.read_text())
        self.assertEqual(scorer.MODEL_PATH.name, contract["fitted_artifact"])
        self.assertEqual(sha(scorer.MODEL_PATH), contract["fitted_artifact_sha256"])
        self.assertEqual(identity(contract), contract["model_identity_sha256"])
        self.assertTrue(contract["model_identity_sha256"].startswith("37771281"))
        self.assertEqual(contract["reproducibility"]["status"], "PASS")
        self.assertEqual(contract["feature_columns"], scorer.CORE_FEATURES + ["is_defense"])
        self.assertEqual(scorer.FEATURES, contract["feature_columns"])
        self.assertEqual(joblib.load(scorer.MODEL_PATH)["feature_columns"], contract["feature_columns"])

    def test_same_recipe_role_and_targets_as_the_rollback(self):
        new = json.loads(scorer.OFFSET_DEFENSE_CONTRACT_PATH.read_text())
        old = json.loads(scorer.PP_CORRECTED_CONTRACT_PATH.read_text())
        for key in ("research_model_id", "history_contract", "hyperparameters", "thresholds", "target"):
            self.assertEqual(new[key], old[key], key)
        self.assertEqual(new["supersedes_model_identity_sha256"], old["model_identity_sha256"])
        self.assertNotEqual(new["model_identity_sha256"], old["model_identity_sha256"])

    def test_rollback_targets_are_intact(self):
        for path, prefix in ((scorer.PP_CORRECTED_CONTRACT_PATH, "fb701c67"), (scorer.V1_CONTRACT_PATH, "a53f43d5")):
            contract = json.loads(path.read_text())
            self.assertEqual(sha(path.parent / contract["fitted_artifact"]), contract["fitted_artifact_sha256"])
            self.assertTrue(contract["model_identity_sha256"].startswith(prefix))
            self.assertEqual(scorer.model_features(joblib.load(path.parent / contract["fitted_artifact"])),
                             scorer.CORE_FEATURES)

    def test_offset_lifts_stars_and_position_lowers_defensemen(self):
        model = joblib.load(scorer.MODEL_PATH)
        row = {"game_id": 1, "player_id": 1, "is_home": 1, "d10_sog_per60": 6.7, "attempts_d10_per60": 14.6,
               "player_points_last10": 2.7, "current_season_points_prior": 34, "current_season_games_prior": 12,
               "mean_toi_last10": 24.2, "mean_pp_toi_last10": 3.5, "team_d10_sf_per_game": 23.9,
               "last10_team_sog_share": 0.113, "is_defense": 0}
        star = scorer.score(pd.DataFrame([row]), model).expected_points.iloc[0]
        previous = scorer.score(pd.DataFrame([row]), joblib.load(
            scorer.PP_CORRECTED_CONTRACT_PATH.parent / "nhl_points_count_hgb_v1_pp_corrected.joblib")).expected_points.iloc[0]
        self.assertGreater(star, previous)
        defense = scorer.score(pd.DataFrame([{**row, "is_defense": 1}]), model).expected_points.iloc[0]
        self.assertLess(defense, star)

    def test_missing_position_stops_scoring(self):
        row = {name: 1.0 for name in scorer.FEATURES}
        row.update({"game_id": 1, "player_id": 1, "is_defense": np.nan})
        with self.assertRaisesRegex(ValueError, "HGB_POSITION_FEATURE_INVALID"):
            scorer.score(pd.DataFrame([row]), joblib.load(scorer.MODEL_PATH))


if __name__ == "__main__":
    unittest.main()


class UnknownAttemptsTests(unittest.TestCase):
    """P2.37: an unknown attempt rate is not scored as zero."""

    def frame(self, attempts, sog):
        import pandas as pd
        row = {name: 1.0 for name in scorer.FEATURES}
        row.update({"game_id": 1, "player_id": 1, "attempts_d10_per60": attempts, "d10_sog_per60": sog})
        return pd.DataFrame([row])

    def test_unknown_attempts_use_the_training_ratio_of_the_sog_rate(self):
        import joblib
        fitted = joblib.load(scorer.MODEL_PATH)
        imputed = scorer.score(self.frame(None, 9.0), fitted).expected_points.iloc[0]
        explicit = scorer.score(self.frame(9.0 * scorer.TRAINING_ATTEMPTS_PER_SOG, 9.0), fitted).expected_points.iloc[0]
        zero = scorer.score(self.frame(0.0, 9.0), fitted).expected_points.iloc[0]
        self.assertEqual(imputed, explicit)
        self.assertNotEqual(imputed, zero)

    def test_known_values_and_no_history_rows_are_unchanged(self):
        self.assertEqual(scorer.impute_unknown_attempts(self.frame(12.0, 9.0)).iloc[0], 12.0)
        self.assertTrue(scorer.impute_unknown_attempts(self.frame(None, None)).isna().iloc[0])

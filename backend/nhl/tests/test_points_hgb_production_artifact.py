"""HGB Points production artifact selection (P2.31): PP-corrected refit with V1 rollback."""
import hashlib
import json
import unittest
from pathlib import Path

from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(contract: dict) -> str:
    body = {k: v for k, v in contract.items() if k != "model_identity_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


class ProductionArtifactTests(unittest.TestCase):
    def test_production_uses_the_pp_corrected_refit(self):
        self.assertEqual(scorer.CONTRACT_PATH, scorer.PP_CORRECTED_CONTRACT_PATH)
        contract = json.loads(scorer.CONTRACT_PATH.read_text())
        self.assertEqual(scorer.MODEL_PATH.name, contract["fitted_artifact"])
        self.assertEqual(sha(scorer.MODEL_PATH), contract["fitted_artifact_sha256"])
        self.assertEqual(identity(contract), contract["model_identity_sha256"])
        self.assertTrue(contract["model_identity_sha256"].startswith("fb701c67"))
        self.assertEqual(contract["reproducibility"]["status"], "PASS")

    def test_same_recipe_and_role_as_v1(self):
        new = json.loads(scorer.PP_CORRECTED_CONTRACT_PATH.read_text())
        v1 = json.loads(scorer.V1_CONTRACT_PATH.read_text())
        for key in ("research_model_id", "feature_columns", "history_contract", "hyperparameters", "thresholds", "target"):
            self.assertEqual(new[key], v1[key], key)
        self.assertEqual(new["feature_columns"], scorer.FEATURES)
        self.assertEqual(new["supersedes_model_identity_sha256"], v1["model_identity_sha256"])
        self.assertNotEqual(new["model_identity_sha256"], v1["model_identity_sha256"])

    def test_v1_rollback_target_is_intact(self):
        v1 = json.loads(scorer.V1_CONTRACT_PATH.read_text())
        artifact = scorer.V1_CONTRACT_PATH.parent / v1["fitted_artifact"]
        self.assertEqual(sha(artifact), v1["fitted_artifact_sha256"])
        self.assertTrue(v1["model_identity_sha256"].startswith("a53f43d5"))


if __name__ == "__main__":
    unittest.main()

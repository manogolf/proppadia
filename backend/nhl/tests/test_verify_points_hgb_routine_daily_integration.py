"""The HGB routine integration verifier runs end to end on the production artifact."""
import json
import unittest

from backend.nhl.points_hgb_promotion import evaluate_promotion
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
from backend.nhl.scripts.verify_nhl_points_hgb_routine_daily_integration import EVIDENCE_PATH, build_evidence


class VerifierTests(unittest.TestCase):
    def test_fixture_passes_through_the_capture_helper(self):
        evidence = build_evidence()
        self.assertEqual(evidence["classification"], "HGB_ROUTINE_DAILY_INTEGRATION_PASS")
        self.assertEqual(evidence["deterministic_replay"], "DETERMINISTIC_REPLAY_PASS")
        self.assertEqual(evidence["coherence_crossing_count"], 0)
        contract = json.loads(scorer.CONTRACT_PATH.read_text())
        self.assertEqual(evidence["model_identity_sha256"], contract["model_identity_sha256"])

    def test_stored_evidence_names_the_production_artifact(self):
        stored = json.loads(EVIDENCE_PATH.read_text())
        contract = json.loads(scorer.CONTRACT_PATH.read_text())
        self.assertEqual(stored["model_artifact_sha256"], contract["fitted_artifact_sha256"])
        self.assertEqual(stored["model_identity_sha256"], contract["model_identity_sha256"])
        self.assertEqual(evaluate_promotion()["gates"]["J"]["status"], "PASS")


if __name__ == "__main__":
    unittest.main()

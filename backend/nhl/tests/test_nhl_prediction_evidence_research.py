from __future__ import annotations

import unittest

from backend.nhl.research.prediction_evidence import (
    CHRONOLOGY_UNVERIFIED,
    CORROBORATED_PREGAME,
    VERIFIED_PROSPECTIVE,
    classify_evidence,
    compare_replay,
    select_latest_groups,
)


def evidence(**overrides):
    values = {
        "scheduled_start_utc": "2026-10-18T22:00:00Z",
        "feature_cutoff_utc": "2026-10-18T16:00:00Z",
        "prediction_timestamp_utc": "2026-10-18T16:00:01Z",
        "durable_publication_timestamp_utc": "2026-10-18T16:00:02Z",
        "contemporaneous_execution_witness": True,
        "source_content_hash_bound_at_publication": False,
        "canonical_identity_valid": True,
        "prediction_values_valid": True,
        "input_snapshot_complete": True,
        "model_identity_valid": True,
        "manifest_valid": True,
        "reconciliation_certificate_bound": False,
        "roster_evidence_bound": False,
        "conflicting_evidence": False,
    }
    values.update(overrides)
    return classify_evidence(**values)


class PredictionEvidenceResearchTests(unittest.TestCase):
    def test_missing_governed_receipt_proof_can_be_corrob_pregame(self):
        result = evidence()
        self.assertEqual(result.classification, CORROBORATED_PREGAME)
        self.assertEqual(result.operational_compliance, "GOVERNED_PROOF_NOT_ESTABLISHED")

    def test_full_governed_hash_bound_evidence_is_verified_prospective(self):
        result = evidence(source_content_hash_bound_at_publication=True,
                          reconciliation_certificate_bound=True,
                          roster_evidence_bound=True)
        self.assertEqual(result.classification, VERIFIED_PROSPECTIVE)

    def test_missing_timestamps_are_chronology_unverified(self):
        self.assertEqual(evidence(feature_cutoff_utc=None).classification,
                         CHRONOLOGY_UNVERIFIED)

    def test_post_start_and_conflicting_timestamps_are_unverified(self):
        late = evidence(durable_publication_timestamp_utc="2026-10-18T22:00:00Z")
        conflict = evidence(prediction_timestamp_utc="2026-10-18T15:00:00Z")
        self.assertEqual(late.classification, CHRONOLOGY_UNVERIFIED)
        self.assertIn("TIMESTAMP_ORDER_CONTRADICTION", conflict.reasons)
        self.assertEqual(conflict.classification, CHRONOLOGY_UNVERIFIED)

    def test_incomplete_inputs_or_identity_do_not_become_corrob(self):
        self.assertEqual(evidence(input_snapshot_complete=False).classification,
                         CHRONOLOGY_UNVERIFIED)
        self.assertEqual(evidence(canonical_identity_valid=False).classification,
                         CHRONOLOGY_UNVERIFIED)

    def test_replay_comparison_accepts_reproducible_and_rejects_value_drift(self):
        source = [{"game": 1, "line": 1.5, "p": 0.4}]
        same = compare_replay(source, [{"game": 1, "line": 1.5, "p": 0.4}],
                              identity_fields=("game", "line"), value_fields=("p",))
        changed = compare_replay(source, [{"game": 1, "line": 1.5, "p": 0.41}],
                                 identity_fields=("game", "line"), value_fields=("p",))
        self.assertEqual(same["status"], "REPRODUCIBLE")
        self.assertEqual(changed["status"], "FAILED_VALUE_MISMATCH")

    def test_duplicate_replay_identity_and_partial_games_are_explicit(self):
        duplicate = compare_replay(
            [{"game": 1, "player": 2, "p": .4}],
            [{"game": 1, "player": 3, "p": .4}],
            identity_fields=("game", "player"), value_fields=("p",),
        )
        self.assertEqual(duplicate["status"], "FAILED_IDENTITY_MISMATCH")
        selected, excluded = select_latest_groups([
            {"atomic_group_identity": "game-1-player-2", "classification": CORROBORATED_PREGAME,
             "durable_publication_timestamp_utc": "2026-10-18T16:00:02Z",
             "prediction_payload_sha256": "h1", "source_path": "/a"},
            {"atomic_group_identity": "game-2-player-4", "classification": CHRONOLOGY_UNVERIFIED,
             "durable_publication_timestamp_utc": None,
             "prediction_payload_sha256": "h2", "source_path": "/b"},
        ])
        self.assertEqual(len(selected), 1)
        self.assertEqual(len(excluded), 1)


if __name__ == "__main__":
    unittest.main()

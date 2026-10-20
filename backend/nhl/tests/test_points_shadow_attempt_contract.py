from __future__ import annotations

import json
import unittest
from pathlib import Path

from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.cli import _structured_child_summary
from backend.nhl.points_shadow.core import (
    verify_feature_contract_identity,
    verify_fixed_input_parity,
    verify_frozen_identity,
)


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "backend/nhl/points_shadow/phoenix_shadow_attempt_missingness_v1.json"


class PhoenixAttemptContractTests(unittest.TestCase):
    def test_new_explicit_shadow_contract_is_bound_to_current_export_sql(self):
        identity = json.loads(CONTRACT_PATH.read_text())
        identity["_identity_path"] = str(CONTRACT_PATH)
        verified = verify_feature_contract_identity(identity)
        self.assertEqual(
            verified["version"],
            "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2_ATTEMPT_MISSINGNESS_V1",
        )
        self.assertEqual(verified["identity_path"], str(CONTRACT_PATH))
        self.assertEqual(
            verified["feature_construction_sha256"],
            identity["feature_construction"]["sha256"],
        )

    def test_frozen_contract_still_rejects_changed_export_sql(self):
        with self.assertRaisesRegex(RuntimeError, "POINTS_FEATURE_CONSTRUCTION_HASH_DRIFT"):
            verify_feature_contract_identity()

    @unittest.skipUnless(
        (Path(__file__).resolve().parents[3] / "backend/nhl/models/latest/points").is_dir(),
        "frozen Phoenix Points model files are local artifacts")
    def test_explicit_shadow_contract_reuses_frozen_model_without_repinning_it(self):
        identity = json.loads(CONTRACT_PATH.read_text())
        identity["_identity_path"] = str(CONTRACT_PATH)
        verified = verify_frozen_identity(identity)
        self.assertEqual(verified["feature_contract"]["version"],
                         "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2_ATTEMPT_MISSINGNESS_V1")
        self.assertEqual(verified["model_version"], "phoenix_v1_frozen_2026_control")
        self.assertEqual(verify_fixed_input_parity(identity)["status"], "EXACT_BYTE_PARITY")

    def test_explicit_contract_override_rejects_unapproved_identity_path(self):
        identity = json.loads(CONTRACT_PATH.read_text())
        identity["_identity_path"] = str(ROOT / "unreviewed_points_contract.json")
        with self.assertRaisesRegex(RuntimeError, "POINTS_EXPLICIT_FEATURE_CONTRACT_PATH_NOT_APPROVED"):
            verify_frozen_identity(identity)

    def test_daily_receipt_retains_bounded_attempt_source_and_promotion_evidence(self):
        recorder = DailyRunRecorder(run_id="receipt-test", command=["daily"], phase="EARLY")
        source = {
            "lane": "shared_prerequisites",
            "exit_status": 0,
            "status": "COMPLETE",
            "schema_version": "NHL_SHOT_ATTEMPTS_SOURCE_SUMMARY_V1",
            "pbp_required_games": 11,
            "pbp_reconciled_games": 9,
            "pbp_failure_reasons": {"SOG_RECONCILIATION_FAILED": 2},
            "verified_observations": 324,
            "observed_positive_rows": 283,
            "observed_zero_rows": 41,
            "unknown_rows": 72,
            "confirmed_discrepancy_count": 0,
            "diagnostic_artifact": {
                "path": "artifacts/run/attempt_source_diagnostic.json",
                "sha256": "a" * 64,
            },
            "conflicts": [{"should_not_be_copied": True}],
        }
        promotion = {
            "lane": "shared_prerequisites",
            "exit_status": 0,
            "status": "COMPLETE",
            "schema_version": "NHL_SHOT_ATTEMPTS_PROMOTION_SUMMARY_V1",
            "promoted_rows": 396,
            "preserved_existing_values": 324,
            "filled_nulls": 0,
            "unknown_after_promotion": 72,
        }
        recorder.record_child(_structured_child_summary(
            "NHL_CHILD_SUMMARY_JSON=" + json.dumps(source)))
        recorder.record_child(_structured_child_summary(
            "NHL_CHILD_SUMMARY_JSON=" + json.dumps(promotion)))
        recorded_source, recorded_promotion = recorder.children
        self.assertEqual(recorded_source["pbp_reconciled_games"], 9)
        self.assertEqual(recorded_source["diagnostic_artifact"]["sha256"], "a" * 64)
        self.assertNotIn("conflicts", recorded_source)
        self.assertEqual(recorded_promotion["promoted_rows"], 396)
        self.assertEqual(recorded_promotion["preserved_existing_values"], 324)
        receipt = recorder.payload()
        self.assertEqual(len(receipt["child_summaries"]), 2)
        self.assertEqual(receipt["child_summaries"][0]["pbp_failure_reasons"],
                         {"SOG_RECONCILIATION_FAILED": 2})
        self.assertEqual(receipt["child_summaries"][1]["filled_nulls"], 0)


if __name__ == "__main__":
    unittest.main()

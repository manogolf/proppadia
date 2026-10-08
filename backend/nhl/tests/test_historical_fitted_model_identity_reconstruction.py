import tempfile
import unittest
from pathlib import Path

from backend.nhl.scripts import reconstruct_historical_fitted_model_identity as reconstruction


class HistoricalIdentityReconstructionTests(unittest.TestCase):
    def test_retained_prediction_hashes_verify_without_claiming_identity(self):
        rows, summary = reconstruction.collect_rows()
        self.assertEqual(summary["retained_receipts_examined"], 45)
        self.assertEqual(summary["scored_lane_runs_by_lane"], {"sog": 28, "points": 28, "saves": 28})
        self.assertEqual(summary["prediction_hash_failures"], 0)
        self.assertEqual(summary["receipt_package_integrity_failures"], 0)
        self.assertTrue(all(row["prediction_sha256_verified_on_disk"] for row in rows))
        self.assertTrue(all(row["receipt_package_integrity_verified"] for row in rows))
        self.assertTrue(all(row["fitted_model_identity_sha256"] is None for row in rows))
        self.assertTrue(all(not row["pulse_comparability_eligible"] for row in rows))
        self.assertTrue(all(row["reconstruction_classification"] ==
                            "HISTORICAL_REPOSITORY_STATE_UNPROVEN" for row in rows))

    def test_retrospective_package_is_create_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "identity-ledger"
            reconstruction.write_package(output)
            self.assertTrue((output / "SHA256SUMS").is_file())
            with self.assertRaises(FileExistsError):
                reconstruction.write_package(output)


if __name__ == "__main__":
    unittest.main()

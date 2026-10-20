import csv
import json
import tempfile
import unittest
from pathlib import Path

from backend.nhl.scripts import reconstruct_historical_fitted_model_identity as reconstruction


class HistoricalIdentityReconstructionTests(unittest.TestCase):
    def test_recorded_ledger_counts_are_the_2026_10_08_reconstruction(self):
        # The committed package is the historical record; live receipts keep
        # arriving for slates inside the window, so live counts are not fixed.
        summary = json.loads((reconstruction.DEFAULT_OUTPUT / "summary.json").read_text())
        with (reconstruction.DEFAULT_OUTPUT / "ledger.csv").open(newline="") as stream:
            ledger = list(csv.DictReader(stream))
        self.assertEqual(summary["retained_receipts_examined"], 45)
        self.assertEqual(summary["scored_lane_runs_by_lane"], {"sog": 28, "points": 28, "saves": 28})
        self.assertEqual(len(ledger), 84)

    @unittest.skipUnless(reconstruction.RECEIPTS.is_dir(),
                         "retained NHL daily receipts are local operational artifacts")
    def test_retained_prediction_hashes_verify_without_claiming_identity(self):
        rows, summary = reconstruction.collect_rows()
        self.assertEqual(summary["prediction_hash_failures"], 0)
        self.assertEqual(summary["receipt_package_integrity_failures"], 0)
        self.assertTrue(all(row["prediction_sha256_verified_on_disk"] for row in rows))
        self.assertTrue(all(row["receipt_package_integrity_verified"] for row in rows))
        self.assertTrue(all(row["fitted_model_identity_sha256"] is None for row in rows))
        self.assertTrue(all(not row["pulse_comparability_eligible"] for row in rows))
        self.assertTrue(all(row["reconstruction_classification"] ==
                            "HISTORICAL_REPOSITORY_STATE_UNPROVEN" for row in rows))
        with (reconstruction.DEFAULT_OUTPUT / "ledger.csv").open(newline="") as stream:
            recorded = {(row["run_id"], row["lane"], row["prediction_sha256"])
                        for row in csv.DictReader(stream)}
        live = {(row["run_id"], row["lane"], row["prediction_sha256"]) for row in rows}
        self.assertLessEqual(recorded, live)

    @unittest.skipUnless(reconstruction.RECEIPTS.is_dir(),
                         "retained NHL daily receipts are local operational artifacts")
    def test_retrospective_package_is_create_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "identity-ledger"
            reconstruction.write_package(output)
            self.assertTrue((output / "SHA256SUMS").is_file())
            with self.assertRaises(FileExistsError):
                reconstruction.write_package(output)


if __name__ == "__main__":
    unittest.main()

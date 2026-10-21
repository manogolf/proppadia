"""Daily runs carry re-verifiable prior-day certificate and roster proofs."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl import reconciliation_certificate as certificates


def _receipt(root: Path, *, certificate: dict) -> dict:
    roster = root / "roster"
    roster.mkdir()
    (roster / "observation_summary.json").write_text(json.dumps({"season": 2026, "phase": "EARLY"}))
    (roster / "SHA256SUMS").write_text("abc  observation_summary.json\n")
    digest = hashlib.sha256((roster / "SHA256SUMS").read_bytes()).hexdigest()
    return {"slate_date": "2026-10-12", "canonical_game_ids": [2026020061],
            "canonical_game_set_hash": "games",
            "prior_day_learning": {"certificate": certificate,
                                   "reconciliation_package": "/packages/2026-10-11"},
            "roster_observation": {"path": str(roster), "manifest_sha256": digest}}


class DailyReceiptSourceProofTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def _verify(self, receipt, verified):
        roster_hash = receipt["roster_observation"]["manifest_sha256"]
        with patch.object(certificates, "verify_prior_day_certificate", return_value=verified) as cert, \
                patch("backend.nhl.daily_orchestration.verify_roster_observation_reuse",
                      return_value={"manifest_sha256": roster_hash}):
            return certificates.daily_receipt_source_proofs(receipt), cert

    def test_a_run_before_certificates_is_verified_from_its_recorded_package(self):
        receipt = _receipt(self.root, certificate={})
        proofs, cert = self._verify(receipt, {"status": "CERTIFIED_FINAL",
                                              "package_manifest_sha256": "package"})
        self.assertEqual(proofs["certificate_sha256"], "package")
        self.assertEqual(cert.call_args.kwargs["fallback_package"], "/packages/2026-10-11")
        self.assertEqual(cert.call_args.args[1], "2026-10-11")

    def test_a_recorded_certificate_that_does_not_match_its_package_fails(self):
        receipt = _receipt(self.root, certificate={"status": "CERTIFIED_FINAL",
                                                   "package_manifest_sha256": "other"})
        with self.assertRaises(RuntimeError):
            self._verify(receipt, {"status": "CERTIFIED_FINAL", "package_manifest_sha256": "package"})

    def test_a_changed_roster_package_fails(self):
        receipt = _receipt(self.root, certificate={})
        receipt["roster_observation"]["manifest_sha256"] = "changed"
        with self.assertRaises(RuntimeError):
            self._verify(receipt, {"status": "CERTIFIED_FINAL", "package_manifest_sha256": "package"})


if __name__ == "__main__":
    unittest.main()

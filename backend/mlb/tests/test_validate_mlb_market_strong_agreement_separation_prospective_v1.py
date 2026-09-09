import tempfile
import unittest
from pathlib import Path

from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as study
from backend.mlb.scripts import validate_mlb_market_strong_agreement_separation_prospective_v1 as validator


class ProspectiveAgreementSeparationValidatorTest(unittest.TestCase):
    def test_empty_pre_outcome_package_passes_without_decision(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); output, ledger = base / "output", base / "ledger.sqlite3"
            study.initialize(output, ledger)
            summary = study.report(output, ledger)
            result = validator.validate(output, ledger)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(summary["classification"], "SEPARATION_EVIDENCE_INSUFFICIENT")
            self.assertFalse(summary["decision_made"])


if __name__ == "__main__":
    unittest.main()

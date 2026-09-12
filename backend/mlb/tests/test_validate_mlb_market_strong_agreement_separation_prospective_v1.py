import sqlite3
import tempfile
import unittest
from pathlib import Path

from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as study
from backend.mlb.scripts import validate_mlb_market_strong_agreement_separation_prospective_v1 as validator


class ProspectiveAgreementSeparationValidatorTest(unittest.TestCase):
    def test_additional_append_only_trigger_does_not_hide_required_base_triggers(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); output, ledger = base / "output", base / "ledger.sqlite3"
            study.initialize(output, ledger)
            study.report(output, ledger)
            with sqlite3.connect(ledger) as conn:
                conn.execute(
                    "CREATE TRIGGER extension_no_delete BEFORE DELETE ON study_metadata "
                    "BEGIN SELECT RAISE(ABORT,'extension append-only'); END"
                )
                conn.commit()
            result = validator.validate(output, ledger)
            self.assertTrue(result["checks"]["append_only_triggers"])

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

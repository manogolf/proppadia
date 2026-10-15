import json
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "artifacts/analysis/nhl/points_attempts_feature_causal_replay/2026-10-15"


class PointsAttemptsFeatureCausalReplayTests(unittest.TestCase):
    def test_contract_and_source_gap_are_bound(self):
        summary = json.loads((PACKAGE / "summary.json").read_text())
        hist = summary["historical_contract_reproduction"]
        self.assertEqual(hist["rows"], 47239)
        self.assertTrue(hist["reproduction_gate_pass"])
        self.assertEqual(hist["exact_match_rate"], 1.0)
        self.assertLessEqual(hist["max_abs_difference"], 1e-12)
        self.assertEqual(summary["raw_attempt_nonmissing_rows"], 0)
        audit = pd.read_csv(PACKAGE / "zero_introduction_audit.csv")
        self.assertTrue(bool(audit.loc[audit.stage == "export_nhl_points_hgb_features.normalize_history", "zero_created"].iloc[0]))

    def test_2026_reconstruction_binding_and_no_imputation(self):
        summary = json.loads((PACKAGE / "summary.json").read_text())
        rows = pd.read_csv(PACKAGE / "reconstructed_2026_feature_rows.csv")
        self.assertEqual(len(rows), summary["reconstruction_target_rows"])
        self.assertEqual(len(rows), 2347)
        self.assertGreater(int(rows.trustworthy_reconstruction.sum()), 0)
        self.assertEqual(summary["reconstruction_eligible_rows"], 3)
        self.assertEqual(int(rows.attempt_nonmissing_games.sum()), 0)
        self.assertEqual(summary["global_causal_effect"]["effect"], "NOT_ESTIMABLE_NO_TRUSTWORTHY_RECONSTRUCTED_2026_ATTEMPTS")
        self.assertEqual(summary["baseline_replay_parity"]["status"], "PASS")
        self.assertLess(summary["baseline_replay_parity"]["max_expected_count_abs_error"], 1e-12)

    def test_historical_zeroing_and_oct15_are_diagnostic_only(self):
        summary = json.loads((PACKAGE / "summary.json").read_text())
        self.assertEqual(summary["retraining"], False)
        self.assertEqual(summary["calibration"], False)
        self.assertEqual(summary["market_feature_injection"], False)
        self.assertEqual(summary["production_or_funnel_mutation"], False)
        self.assertEqual(summary["provider_calls"], 0)
        self.assertEqual(summary["paid_credits"], 0)
        self.assertEqual(summary["production_db_mutations"], 0)
        self.assertEqual(summary["oct15"]["tag"], "OUTCOME_BLIND_COUNTERFACTUAL")
        self.assertEqual(summary["oct15"]["reconstruction_eligible_rows"], 84)
        self.assertEqual(summary["oct15"]["excluded_source_gap_rows"], 463)
        self.assertEqual(summary["historical_2025_zeroing"]["rows"], 47239)


if __name__ == "__main__":
    unittest.main()

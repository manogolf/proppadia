import json
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "artifacts/analysis/nhl/points_hgb_confidence_root_cause/2026-10-15"


class PointsHgbConfidenceRootCausePackageTests(unittest.TestCase):
    def test_reproduction_gate_and_population_contracts(self):
        summary = json.loads((PACKAGE / "summary.json").read_text())
        self.assertEqual(summary["reproduction_gate"], "PASS")
        self.assertEqual(summary["2026_capture_count"], 22)
        self.assertEqual(summary["2026_capture_rows_reproduced"], 6698)
        self.assertLess(summary["2026_max_reproduction_error"], 1e-12)
        self.assertLess(summary["2025_holdout_reproduction_max_abs_error"], 1e-12)
        self.assertEqual(summary["2026_settled_rows"], 714)
        self.assertEqual(summary["2025_holdout_rows"], 47239)

    def test_training_and_operational_attempt_support_is_visible(self):
        df = pd.read_csv(PACKAGE / "attempt_rate_diagnostics.csv")
        row = df[(df.feature == "attempts_d10_per60") & (df.band == "<15")].iloc[0]
        self.assertEqual(int(row.n), 711)
        scorecard = pd.read_csv(PACKAGE / "root_cause_scorecard.csv")
        candidate = scorecard[scorecard.candidate == "operational attempt-rate feature collapse"].iloc[0]
        self.assertEqual(candidate.classification, "STRONGLY_SUPPORTED")

    def test_2025_confidence_bins_cover_holdout_and_oct15_is_outcome_blind(self):
        confidence = pd.read_csv(PACKAGE / "2025_confidence_map.csv")
        self.assertEqual(int(confidence.n.sum()), 47239)
        pending = pd.read_csv(PACKAGE / "oct15_pending_diagnostic.csv")
        summary = pending[pending.row_type == "SUMMARY"].iloc[0]
        self.assertEqual(int(summary.prediction_rows), 547)
        self.assertFalse(bool(summary.outcomes_joined))
        repeats = pd.read_csv(PACKAGE / "output_saturation_repeat_values.csv")
        self.assertTrue(set(repeats.population) == {
            "2026_frozen_expected_points", "2026_p_over_0_5",
            "2025_frozen_expected_points", "2025_p_over_0_5",
        })


if __name__ == "__main__":
    unittest.main()

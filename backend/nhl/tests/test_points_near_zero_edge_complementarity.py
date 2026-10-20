import importlib.util
import json
import unittest
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts/analyze_nhl_points_near_zero_edge_complementarity.py"
SPEC = importlib.util.spec_from_file_location("points_near_zero", SCRIPT)
study = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(study)


class NearZeroEdgeTests(unittest.TestCase):
    def test_edge_bands_have_frozen_half_open_boundaries(self):
        self.assertEqual(study.band(-.03, study.EDGE_BANDS), "NEGATIVE_NEAR_ZERO_MINUS_3_TO_MINUS_1")
        self.assertEqual(study.band(-.01, study.EDGE_BANDS), "NEGATIVE_NEAR_ZERO_MINUS_1_TO_ZERO")
        self.assertEqual(study.band(0, study.EDGE_BANDS), "ZERO_TO_PLUS_1")
        self.assertEqual(study.band(.01, study.EDGE_BANDS), "PLUS_1_TO_PLUS_3")
        self.assertEqual(study.band(.15, study.EDGE_BANDS), "GT_PLUS_15")

    def test_edge_is_selected_probability_minus_raw_implied(self):
        row = study.enrich({"model_p_over": ".55", "price_over": "+100", "price_under": "-110",
                            "market_p_over": ".52381", "market_p_under": ".52381"})
        self.assertAlmostEqual(row["selected_production_edge"], .05)
        self.assertAlmostEqual(row["signed_probability_gap"], .0616279069767442)

    def test_band_assignment_is_independent_of_outcome(self):
        before = study.enrich({"model_p_over": ".55", "price_over": "+100", "price_under": "-110", "line": ".5"})
        after = study.enrich({"model_p_over": ".55", "price_over": "+100", "price_under": "-110", "line": ".5", "official_points": "3"})
        self.assertEqual(before["edge_band"], after["edge_band"])

    def test_proper_score_contributions(self):
        row = study.enrich({"model_p_over": ".8", "price_over": "+100", "price_under": "-110",
                            "market_p_over": ".52381", "market_p_under": ".52381", "over_outcome": "1"})
        self.assertAlmostEqual(row["model_brier_contribution"], .04)
        self.assertLess(row["model_logloss_contribution"], row["market_logloss_contribution"])

    @unittest.skipUnless((study.HISTORY / "2026-10-10/points_attachment_integrity.json").is_file(),
                         "retained 2026-10-10 to 2026-10-14 Points attachment archives are local artifacts")
    def test_retained_price_integrity_and_study_periods(self):
        rows, hashes = study.load_rows()
        self.assertIn("2026-10-10_integrity", hashes)
        self.assertIn("2026-10-14_points_attachment_integrity.json", hashes)
        self.assertEqual(sorted({r["slate_date"] for r in rows}), ["2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14"])
        self.assertTrue(all(r["selected_production_edge"] is not None for r in rows))
        for row in rows:
            stored = study.n(row.get("edge_over" if row["model_side"] == "OVER" else "edge_under"))
            if stored is not None:
                self.assertAlmostEqual(row["selected_production_edge"], stored, places=10)
        contract = json.loads((study.ROOT / "artifacts/analysis/nhl/points_consideration_funnel_freeze/2026-10-14/frozen_funnel_contract.json").read_text())
        self.assertEqual(contract["contract_version"], "NHL_POINTS_CONSIDERATION_FUNNEL_V1")
        self.assertEqual(contract["discovery_window"]["start"], "2026-10-10")
        self.assertEqual(contract["prospective_start_date"], "2026-10-14")


if __name__ == "__main__":
    unittest.main()

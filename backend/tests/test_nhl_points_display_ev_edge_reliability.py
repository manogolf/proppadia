import csv
import json
import unittest
from pathlib import Path

from backend.nhl.scripts import analyze_nhl_points_display_ev_edge_reliability as study

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15'


class DisplayEVEdgeReliabilityTests(unittest.TestCase):
    def test_american_probability_ev_and_fair_odds(self):
        self.assertAlmostEqual(study.implied(-200), 2/3)
        self.assertAlmostEqual(study.implied(150), .4)
        self.assertAlmostEqual(study.ev_unit(.5, 100), 0)
        self.assertEqual(study.fair_american(.6), -150)

    def test_fixed_bands_are_left_closed_and_outcome_independent(self):
        self.assertEqual(study.band(.03, study.EDGE_BANDS), '+3 to +5')
        self.assertEqual(study.band(.20, study.EDGE_BANDS), '+20%+')
        self.assertEqual(study.band(.10, study.EV_BANDS), '+10 to +20')

    def test_ev_edge_identity_and_price_dependence(self):
        p, price = .6, -150
        q = study.implied(price)
        edge = p-q
        self.assertAlmostEqual(study.ev_unit(p, price), edge/q)
        self.assertNotAlmostEqual(study.ev_unit(p, -250), study.ev_unit(p, 150))

    def test_six_selected_side_ui_fixtures(self):
        fixtures = [
            (-169, -203, .066, .042), (-222, -254, .041, .028),
            (-120, -117, -.012, -.006), (180, 142, .157, .056),
            (-233, -177, -.087, -.061), (-130, 227, -.459, -.259),
        ]
        for market, fair, expected_ev, expected_edge in fixtures:
            p = study.implied(fair)
            self.assertEqual(study.fair_american(p), fair)
            self.assertAlmostEqual(round(study.ev_unit(p, market) * 100, 1), round(expected_ev * 100, 1))
            self.assertAlmostEqual(round((p-study.implied(market))*100, 1), round(expected_edge*100, 1))

    def test_funnel_stage1_exact_reconciliation_and_unchanged(self):
        result = json.loads((OUT/'funnel_stage1_reconciliation.json').read_text())
        self.assertEqual((result['population_rows'], result['negative_edge_rows'], result['nonnegative_rows']), (658, 120, 538))
        self.assertEqual(result['exact_matches'], 658)
        self.assertEqual(result['mismatch_count'], 0)
        self.assertFalse(result['funnel_v1_modified'])

    def test_correction_notice_and_source_contract(self):
        notice = (OUT/'CORRECTION_NOTICE.md').read_text()
        self.assertIn('SUPERSEDED_DISPLAY_ORIENTATION_ERROR', notice)
        contract = json.loads((OUT/'display_formula_contract.json').read_text())
        self.assertIn('selected_probability', contract)
        self.assertIn('selected_market_price', contract)

    def test_historical_display_formula_reproduction_passes(self):
        result = json.loads((OUT/'formula_reproduction.json').read_text())
        self.assertEqual(result['status'], 'PASS')
        self.assertLessEqual(result['edge_error_max'], 1e-12)
        self.assertLessEqual(result['ev_error_max'], 1e-12)
        self.assertEqual(result['failing_rows'], 0)

    def test_pending_rows_are_not_graded_and_lineage_has_no_side_effects(self):
        with (OUT/'oct15_pending_map.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        self.assertTrue(rows)
        self.assertEqual({r['status'] for r in rows}, {'PENDING_OUTCOME'})
        lineage=json.loads((OUT/'lineage_manifest.json').read_text())
        self.assertEqual((lineage['provider_calls'],lineage['paid_credits'],lineage['production_db_mutations']),(0,0,0))


if __name__ == '__main__':
    unittest.main()

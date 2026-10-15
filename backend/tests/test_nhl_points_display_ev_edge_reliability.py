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

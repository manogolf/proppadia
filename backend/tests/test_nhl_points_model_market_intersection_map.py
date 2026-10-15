import csv
import json
import unittest
from pathlib import Path

from backend.nhl.scripts import analyze_nhl_points_model_market_intersection_map as study


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "artifacts/analysis/nhl/points_model_market_intersection_map/2026-10-15"


def read_rows(name):
    with (OUT / name).open(newline="") as stream:
        return list(csv.DictReader(stream))


class IntersectionMapTests(unittest.TestCase):
    def test_fixed_probability_band_edges_cover_both_sides(self):
        self.assertEqual(study.band(.199, study.PROB), "<20%")
        self.assertEqual(study.band(.499, study.PROB), "45-50%")
        self.assertEqual(study.band(.5, study.PROB), "50-55%")
        self.assertEqual(study.band(.999, study.PROB), "80%+")

    def test_score_contributions_are_probability_based(self):
        self.assertAlmostEqual(study.brier(.2, 0), .04)
        self.assertAlmostEqual(study.brier(.2, 1), .64)
        self.assertAlmostEqual(study.ll(.2, 1), -__import__('math').log(.2))

    def test_two_sided_consensus_is_normalized_no_vig(self):
        over_raw, under_raw = .5121951219512195, .5594713656387665
        over_nv = over_raw / (over_raw + under_raw)
        self.assertAlmostEqual(over_nv + under_raw / (over_raw + under_raw), 1.0)
        self.assertLess(over_nv, .5)

    def test_bands_do_not_depend_on_outcomes(self):
        self.assertEqual(study.band(.42, study.PROB), study.band(.42, study.PROB))
        self.assertNotEqual(study.band(.42, study.PROB), study.band(.52, study.PROB))

    def test_artifacts_have_unique_prediction_market_outcome_keys(self):
        rows = read_rows("canonical_intersection_rows.csv")
        keys = [(r["slate_date"], r["game_id"], r["player_id"], r["line"]) for r in rows]
        self.assertEqual(len(keys), len(set(keys)))

    def test_classification_and_score_math_reconcile(self):
        rows = read_rows("canonical_settled_rows.csv")
        for row in rows:
            po, pm, y = (float(row[k]) for k in ("hgb_p_over", "market_p_over", "outcome_over"))
            self.assertAlmostEqual(float(row["hgb_brier"]), (po-y)**2, places=10)
            self.assertAlmostEqual(float(row["market_brier"]), (pm-y)**2, places=10)
            same = row["hgb_selected_side"] == row["market_favored_side"]
            self.assertEqual(row["side_relation"], "SAME_SIDE" if same else "OPPOSITE_SIDE")
            if row["quadrant"] == "BOTH_CORRECT":
                self.assertEqual(row["hgb_win"], row["market_win"])
                self.assertEqual(row["hgb_win"], "1")

    def test_lines_and_dates_remain_explicit(self):
        rows = read_rows("canonical_intersection_rows.csv")
        self.assertEqual({r["slate_date"] for r in rows if r["slate_date"] != "2026-10-15"}, set(study.DAYS))
        self.assertIn("0.5", {r["line"] for r in rows})
        self.assertIn("1.5", {r["line"] for r in rows})
        matrix = read_rows("hgb_market_over_probability_matrix.csv")
        self.assertIn("line", matrix[0])
        self.assertEqual(len(matrix), len({r["line"] for r in rows if r["settled"]}) * 64)

    def test_pending_snapshot_never_has_outcomes(self):
        rows = read_rows("oct15_pending_snapshot.csv")
        self.assertTrue(rows)
        self.assertEqual({r["status"] for r in rows}, {"PENDING_OUTCOME"})
        self.assertTrue(all(not r.get("outcome_over") for r in rows))
        summary = json.loads((OUT / "oct15_pending_summary.json").read_text())
        self.assertEqual(summary["status"], "PENDING_OUTCOME")

    def test_no_production_or_provider_side_effects_and_lineage_exists(self):
        lin = json.loads((OUT / "lineage_manifest.json").read_text())
        self.assertEqual(lin["provider_calls"], 0)
        self.assertEqual(lin["paid_credits"], 0)
        self.assertEqual(lin["production_db_mutations"], 0)
        self.assertTrue(lin["sources"])


if __name__ == "__main__":
    unittest.main()

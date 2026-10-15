import csv
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "artifacts/analysis/nhl/points_full_prediction_range/2026-10-15"


def rows(name):
    with (OUT / name).open(newline="") as stream:
        return list(csv.DictReader(stream))


class FullPredictionRangeTests(unittest.TestCase):
    def test_fixed_population_reconciles(self):
        summary = json.loads((OUT / "summary.json").read_text())
        self.assertEqual(summary["settled_n"], 714)
        self.assertEqual(summary["total"]["wins"] + summary["total"]["losses"], 714)
        for filename in ("edge_band_full_metrics.csv", "ev_band_full_metrics.csv", "model_confidence_bands.csv", "price_bands.csv"):
            self.assertEqual(sum(int(r["n"]) for r in rows(filename)), 714, filename)

    def test_edge_sides_and_dates_reconcile(self):
        edge = rows("edge_band_full_metrics.csv")
        under, over = rows("under_edge_bands.csv"), rows("over_edge_bands.csv")
        self.assertEqual(sum(int(r["n"]) for r in under), 689)
        self.assertEqual(sum(int(r["n"]) for r in over), 25)
        self.assertEqual(sum(int(r["n"]) for r in edge), 714)
        dates = [r for r in rows("date_stability.csv") if not r.get("edge_band")]
        self.assertEqual(sum(int(r["n"]) for r in dates), 714)

    def test_returns_are_explicitly_descriptive_and_oct15_pending(self):
        returns = rows("descriptive_return_by_edge.csv")
        self.assertTrue(all(r["return_label"] == "RETAINED_ROW_PRICE_DESCRIPTIVE_RETURN" for r in returns))
        self.assertEqual(len(rows("oct15_outcome_free.csv")), 213)
        self.assertTrue(all(r["status"] == "PENDING_OUTCOME" for r in rows("oct15_outcome_free.csv")))

    def test_all_interaction_cells_emitted(self):
        self.assertEqual(len(rows("edge_price_matrix.csv")), 13 * 9)
        self.assertEqual(len(rows("edge_model_confidence_matrix.csv")), 13 * 8)
        self.assertEqual(len(rows("edge_market_agreement_matrix.csv")), 13 * 2)


if __name__ == "__main__":
    unittest.main()

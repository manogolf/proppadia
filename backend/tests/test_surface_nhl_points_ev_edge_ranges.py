import csv
import json
import unittest
from pathlib import Path

from backend.nhl.scripts import surface_nhl_points_ev_edge_ranges as surface

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15/range_surface"


class PointsRangeSurfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        surface.main()

    def test_fixed_bands_and_primary_denominator(self):
        with (OUT / "edge_band_performance.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual([row["band"] for row in rows], surface.EDGE_BANDS)
        self.assertEqual(sum(int(row["n"]) for row in rows), 714)
        self.assertEqual(sum(int(row["wins"]) + int(row["losses"]) for row in rows), 714)
        with (OUT / "ev_band_performance.csv").open(newline="") as stream:
            ev_rows = list(csv.DictReader(stream))
        self.assertEqual([row["band"] for row in ev_rows], surface.EV_BANDS)
        self.assertEqual(sum(int(row["n"]) for row in ev_rows), 714)

    def test_selected_side_math_and_expected_loss_identity(self):
        with (OUT / "edge_band_performance.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        for row in rows:
            n = int(row["n"])
            if n:
                self.assertAlmostEqual(float(row["expected_losses"]), n * (1-float(row["expected_win_rate"])))
                self.assertAlmostEqual(float(row["excess_losses"]), int(row["losses"])-float(row["expected_losses"]))
                self.assertAlmostEqual(float(row["excess_loss_rate"]), float(row["excess_losses"])/n)

    def test_side_agreement_and_dates_reconcile(self):
        summary = json.loads((OUT / "summary.json").read_text())
        validation = json.loads((OUT / "validation.json").read_text())
        self.assertEqual(summary["under_n"] + summary["over_n"], 714)
        self.assertEqual(summary["same_side_n"] + summary["opposite_side_n"], 714)
        self.assertEqual(sum(validation["date_counts"].values()), 714)
        self.assertEqual(validation["funnel_v1_reconciliation"]["mismatch_count"], 0)
        self.assertFalse(validation["superseded_over_only_fields_used"])

    def test_chart_ready_outputs_and_static_charts_exist(self):
        for name in ("edge_band_performance_chart.csv", "ev_band_performance_chart.csv", "edge_excess_loss_chart.csv", "edge_market_brier_delta_chart.csv", "edge_expected_vs_realized.png", "ev_expected_vs_realized.png", "edge_excess_loss_rate.png", "edge_market_brier_delta.png"):
            self.assertTrue((OUT / name).is_file(), name)


if __name__ == "__main__":
    unittest.main()

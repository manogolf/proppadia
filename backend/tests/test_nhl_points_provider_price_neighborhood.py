"""Validation for the immutable NHL Points provider-price research package."""
import csv
import json
import unittest
from pathlib import Path

from backend.nhl.scripts import analyze_nhl_points_provider_price_neighborhood as study


OUT = study.OUT


def rows(name):
    with (OUT / name).open(newline="") as stream:
        return list(csv.DictReader(stream))


class ProviderPriceNeighborhoodTests(unittest.TestCase):
    def test_american_probability_and_fixed_price_bands(self):
        self.assertAlmostEqual(study.implied(-120), 120 / 220)
        self.assertAlmostEqual(study.implied(100), 0.5)
        self.assertEqual(study.band(-121, study.PRICE_BANDS), "LT_-120")
        self.assertEqual(study.band(-120, study.PRICE_BANDS), "-120_TO_-106")
        self.assertEqual(study.band(-105, study.PRICE_BANDS), "-105_TO_-101")
        self.assertEqual(study.band(100, study.PRICE_BANDS), "EVEN_+100")
        self.assertEqual(study.band(101, study.PRICE_BANDS), "+101_TO_+110")
        self.assertEqual(study.band(126, study.PRICE_BANDS), "+126_PLUS")

    def test_fixed_provider_edge_bands(self):
        self.assertEqual(study.band(-0.03, study.EDGE_BANDS), "NEG_-3_TO_-1PP")
        self.assertEqual(study.band(-0.01, study.EDGE_BANDS), "NEG_-1_TO_0PP")
        self.assertEqual(study.band(0.0, study.EDGE_BANDS), "0_TO_+1PP")
        self.assertEqual(study.band(0.01, study.EDGE_BANDS), "+1_TO_+3PP")

    def test_quote_identity_and_pregame_binding(self):
        quotes = rows("canonical_provider_quote_rows.csv")
        self.assertTrue(quotes)
        for quote in quotes:
            self.assertTrue(quote["provider"])
            self.assertTrue(quote["provider_event_id"])
            self.assertEqual(float(quote["line"]), 0.5)
            self.assertIn(quote["side"], {"OVER", "UNDER"})
            self.assertLessEqual(quote["quote_timestamp_utc"], quote["observation_timestamp_utc"])
            self.assertLess(quote["observation_timestamp_utc"], quote["commence_time_utc"])
            self.assertTrue(quote["raw_response_sha256"])
            self.assertTrue(quote["observation_manifest_sha256"])

    def test_production_edge_exact_reproduction_and_quote_edge(self):
        reproduced = rows("production_edge_reproduction.csv")
        deltas = [float(r["absolute_difference"]) for r in reproduced if r["absolute_difference"]]
        self.assertTrue(deltas)
        self.assertLessEqual(max(deltas), 1e-10)
        quotes = rows("canonical_provider_quote_rows.csv")
        selected = [q for q in quotes if q.get("provider_quote_edge")]
        self.assertTrue(selected)
        for quote in selected:
            expected = float(quote["model_selected_probability"]) - float(quote["provider_raw_implied_probability"])
            self.assertAlmostEqual(float(quote["provider_quote_edge"]), expected, places=12)
            self.assertEqual(quote["side"], quote["model_selected_side"])

    def test_same_provider_no_vig_requires_both_sides(self):
        quotes = rows("canonical_provider_quote_rows.csv")
        by_key = {}
        for q in quotes:
            by_key.setdefault((q["slate_date"], q["game_id"], q["player_id"], q["provider"]), {})[q["side"]] = q
        paired = rows("same_provider_no_vig_calculations.csv")
        self.assertTrue(paired)
        for q in paired:
            pair = by_key[(q["slate_date"], q["game_id"], q["player_id"], q["provider"])]
            self.assertEqual(set(pair), {"OVER", "UNDER"})
            p_over = study.implied(pair["OVER"]["price_american"])
            p_under = study.implied(pair["UNDER"]["price_american"])
            self.assertAlmostEqual(float(q["provider_no_vig_over"]), p_over / (p_over + p_under), places=12)

    def test_best_observed_is_prediction_unique_and_outcome_blind(self):
        quotes = rows("canonical_provider_quote_rows.csv")
        policy = rows("prediction_policy_rows.csv")
        by_pred = {}
        for q in quotes:
            if q["side"] == q.get("model_selected_side") and q["is_duplicate_key"] == "False":
                by_pred.setdefault((q["slate_date"], q["game_id"], q["player_id"]), []).append(q)
        best_rows = [r for r in policy if r["policy"] == "BEST_OBSERVED"]
        self.assertTrue(best_rows)
        for r in best_rows:
            candidates = by_pred[(r["slate_date"], r["game_id"], r["player_id"])]
            self.assertEqual(float(r["provider_side_price"]), max(float(q["price_american"]) for q in candidates))
            self.assertNotEqual(r["provider"], "ATTACHMENT_MEDIAN")
        neighborhood = rows("prediction_neighborhood_rows.csv")
        keys = [(r["slate_date"], r["game_id"], r["player_id"], r["neighborhood"]) for r in neighborhood]
        self.assertEqual(len(keys), len(set(keys)))

    def test_date_split_oct15_pending_only_and_funnel_untouched(self):
        inventory = rows("prediction_price_inventory.csv")
        oct15 = [r for r in inventory if r["slate_date"] == "2026-10-15"]
        self.assertTrue(oct15)
        self.assertTrue(all(r["grading_status"] == "PENDING" and r["outcome_over"] == "" for r in oct15))
        self.assertFalse(any(r["slate_date"] == "2026-10-15" for r in rows("date_neighborhood_performance.csv")))
        summary = json.loads((OUT / "summary.json").read_text())
        self.assertEqual(summary["scope"]["finalized_dates"], [f"2026-10-{d}" for d in ("10", "11", "12", "13", "14")])
        manifest = json.loads((OUT / "lineage_manifest.json").read_text())
        self.assertTrue(manifest["output_files"])


if __name__ == "__main__":
    unittest.main()

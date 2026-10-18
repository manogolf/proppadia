import unittest

from backend.nhl.scripts.export_nhl_all_candidate_evaluation import (
    candidate_key,
    deduplicate_candidates,
    evaluate_quote,
    quote_is_eligible,
)


class AllCandidateExportTests(unittest.TestCase):
    def _candidate(self, **overrides):
        row = {
            "canonical_season": 2026,
            "slate_date": "2026-10-16",
            "game_id": 1,
            "player_id": 42,
            "market_key": "shots_on_goal",
            "market_line": "2.5",
            "selected_side": "OVER",
            "model_family": "poisson",
            "model_version": "baseline_v1",
            "outcome_status": "UNRESOLVED",
        }
        row.update(overrides)
        return row

    def test_population_keeps_all_models_and_unresolved_candidates(self):
        production = self._candidate(model_role="PRODUCTION")
        shadow = self._candidate(model_version="blend_v1", model_role="SHADOW")
        rows = deduplicate_candidates([production, shadow])
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["outcome_status"] for row in rows}, {"UNRESOLVED"})

    def test_identical_duplicate_is_collapsed_but_conflict_is_rejected(self):
        row = self._candidate()
        self.assertEqual(len(deduplicate_candidates([row, dict(row)])), 1)
        with self.assertRaisesRegex(ValueError, "conflicting duplicate"):
            deduplicate_candidates([row, {**row, "outcome_status": "SETTLED"}])

    def test_quote_requires_both_timestamps_before_prediction_and_start(self):
        prediction = {
            "prediction_timestamp_utc": "2026-10-16T20:00:00+00:00",
            "scheduled_start_utc": "2026-10-16T23:00:00+00:00",
        }
        quote = {
            "source_quote_timestamp_utc": "2026-10-16T19:00:00+00:00",
            "observation_timestamp_utc": "2026-10-16T19:30:00+00:00",
        }
        self.assertTrue(quote_is_eligible(quote, prediction))
        self.assertFalse(quote_is_eligible({**quote, "observation_timestamp_utc": "2026-10-16T20:01:00+00:00"}, prediction))
        self.assertFalse(quote_is_eligible({**quote, "source_quote_timestamp_utc": "2026-10-16T23:00:00+00:00"}, prediction))

    def test_quote_without_timezone_is_not_eligible(self):
        self.assertFalse(quote_is_eligible(
            {"source_quote_timestamp_utc": "2026-10-16T19:00:00", "observation_timestamp_utc": "2026-10-16T19:30:00Z"},
            {"prediction_timestamp_utc": "2026-10-16T20:00:00Z", "scheduled_start_utc": "2026-10-16T23:00:00Z"},
        ))

    def test_economics_require_valid_quote_and_probability(self):
        valid = evaluate_quote({"american_price": "-110", "implied_probability": 0.5}, 0.6)
        self.assertAlmostEqual(valid["decimal_price"], 1.9090909, places=6)
        self.assertAlmostEqual(valid["expected_value_per_unit"], 0.1454545, places=6)
        self.assertAlmostEqual(valid["model_market_edge"], 0.1, places=6)
        invalid = evaluate_quote({"american_price": "", "implied_probability": ""}, 0.6)
        self.assertEqual(invalid["ev_status"], "UNAVAILABLE_MISSING_OR_INVALID_PRICE_OR_PROBABILITY")
        self.assertEqual(invalid["model_market_edge"], "")

    def test_natural_key_keeps_line_side_and_model_identity_separate(self):
        base = self._candidate()
        self.assertNotEqual(candidate_key(base), candidate_key({**base, "market_line": "3.5"}))
        self.assertNotEqual(candidate_key(base), candidate_key({**base, "model_version": "other"}))


if __name__ == "__main__":
    unittest.main()

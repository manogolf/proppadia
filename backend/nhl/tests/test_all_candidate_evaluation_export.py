import unittest

from backend.nhl.scripts.export_nhl_all_candidate_evaluation import (
    candidate_key,
    deduplicate_candidates,
    deduplicate_quotes,
    evaluate_quote,
    missing_required_arms,
    quote_is_eligible,
    scope_regular_season_pregame,
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

    def test_required_production_shadow_and_challenger_arms_are_checked(self):
        rows = [
            {"market_key": "shots_on_goal", "model_version": "baseline_v1"},
            {"market_key": "shots_on_goal", "model_version": "NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1"},
            {"market_key": "shots_on_goal", "model_version": "NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1"},
        ]
        required = {
            ("shots_on_goal", "baseline_v1"),
            ("shots_on_goal", "NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1"),
            ("shots_on_goal", "NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1"),
            ("puck_line_full_game", "NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2"),
        }
        self.assertEqual(missing_required_arms(rows, required), {("puck_line_full_game", "NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2")})

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

    def test_quote_rows_keep_book_identity_without_expanding_candidates(self):
        base = {
            "candidate_key": "candidate-1", "sportsbook_key": "draftkings",
            "provider_market_key": "player_shots_on_goal_alternate",
            "source_quote_timestamp_utc": "2026-10-16T17:59:00Z",
            "observation_timestamp_utc": "2026-10-16T18:00:00Z",
            "quote_qualification_status": "QUALIFIED_SOURCE_AND_OBSERVATION_PRECEDE_PREDICTION",
        }
        latest_primary = {**base, "provider_market_key": "player_shots_on_goal", "observation_timestamp_utc": "2026-10-16T17:00:00Z"}
        later_book = {**base, "sportsbook_key": "betonlineag", "provider_market_key": "player_shots_on_goal", "observation_timestamp_utc": "2026-10-16T19:00:00Z"}
        ineligible = {**base, "sportsbook_key": "badbook", "quote_qualification_status": "UNQUALIFIED"}
        result = deduplicate_quotes([base, latest_primary, later_book, ineligible])
        self.assertEqual(len(result), 2)
        self.assertEqual({r["sportsbook_key"] for r in result}, {"draftkings", "betonlineag"})
        dk = next(r for r in result if r["sportsbook_key"] == "draftkings")
        self.assertEqual(dk["provider_market_key"], "player_shots_on_goal")

    def test_quote_source_requires_timestamp_and_sportsbook_key(self):
        valid = {
            "candidate_key": "c", "sportsbook_key": "book", "provider_market_key": "h2h",
            "observation_timestamp_utc": "2026-10-16T19:00:00Z",
            "source_quote_timestamp_utc": "2026-10-16T18:59:00Z",
            "quote_qualification_status": "QUALIFIED_SOURCE_AND_OBSERVATION_PRECEDE_PREDICTION",
        }
        result = deduplicate_quotes([valid, {**valid, "sportsbook_key": ""}, {**valid, "observation_timestamp_utc": ""}])
        self.assertEqual(result, [valid])

    def test_full_window_scope_excludes_preseason_started_and_unproven_rows(self):
        valid = self._candidate(
            game_id="2026020021", game_type="2", canonical_phase="REGULAR_SEASON",
            prediction_timestamp_utc="2026-10-16T19:00:00Z",
            scheduled_start_utc="2026-10-16T23:00:00Z",
        )
        preseason = self._candidate(
            game_id="2026010001", game_type="1",
            prediction_timestamp_utc="2026-09-19T19:00:00Z",
            scheduled_start_utc="2026-09-19T23:00:00Z",
        )
        started = self._candidate(
            game_id="2026020022", game_type="2",
            prediction_timestamp_utc="2026-10-16T23:00:00Z",
            scheduled_start_utc="2026-10-16T23:00:00Z",
        )
        unproven = self._candidate(game_id="2026020023", game_type="2")
        scoped, exclusions = scope_regular_season_pregame([valid, preseason, started, unproven])
        self.assertEqual(len(scoped), 1)
        self.assertEqual(scoped[0]["canonical_phase"], "REGULAR_SEASON")
        self.assertEqual(sum(exclusions.values()), 3)
        self.assertEqual(exclusions[("2026-10-16", "NOT_REGULAR_SEASON")], 1)
        self.assertEqual(exclusions[("2026-10-16", "PREDICTION_AT_OR_AFTER_SCHEDULED_START")], 1)
        self.assertEqual(exclusions[("2026-10-16", "PREGAME_CHRONOLOGY_UNPROVEN")], 1)


if __name__ == "__main__":
    unittest.main()

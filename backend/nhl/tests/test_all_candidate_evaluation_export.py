import unittest

from backend.nhl.scripts.export_nhl_all_candidate_evaluation import (
    canonical_moneyline_quote_side,
    candidate_key,
    deduplicate_candidates,
    deduplicate_quotes,
    evaluate_quote,
    missing_required_arms,
    moneyline_role_at_prediction,
    orient_moneyline_candidate,
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

    def test_moneyline_probabilities_keep_home_away_orientation_and_side(self):
        candidate = self._candidate(
            market_key="moneyline_home", market_line="", model_family="moneyline",
            model_version="NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3", selected_side="AWAY",
            model_role="PRODUCTION", prediction_identity="old-shared-id",
            scheduled_start_utc="2026-10-13T23:00:00Z",
        )
        row = orient_moneyline_candidate(
            candidate, .62, .38, "2026-10-13T20:00:00Z", "model-sha", "retained.csv", "source-sha",
            prediction_identity="v3-row-id",
        )
        self.assertEqual(row["home_win_probability"], .62)
        self.assertEqual(row["away_win_probability"], .38)
        self.assertEqual(row["probability_over"], .62)
        self.assertEqual(row["probability_under"], .38)
        self.assertEqual(row["selected_side"], "HOME")
        self.assertEqual(row["model_role"], "CHALLENGER")
        self.assertEqual(row["export_model_role"], "PRODUCTION")
        self.assertEqual(row["prediction_identity"], "v3-row-id")
        self.assertEqual(row["model_identity_sha256"], "model-sha")
        self.assertEqual(row["model_prediction_source_sha256"], "source-sha")

    def test_moneyline_missing_or_misoriented_probabilities_fail_closed(self):
        candidate = self._candidate(
            market_key="moneyline_home", model_family="moneyline",
            model_version="NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2",
        )
        args = ("2026-10-15T20:00:00Z", "model-sha", "retained.csv", "source-sha")
        with self.assertRaisesRegex(ValueError, "both be present"):
            orient_moneyline_candidate(candidate, "", .4, *args)
        with self.assertRaisesRegex(ValueError, "sum to 1"):
            orient_moneyline_candidate(candidate, .6, .6, *args)

    def test_moneyline_selection_matches_probabilities_and_cutover_roles(self):
        v2 = self._candidate(
            market_key="moneyline_home", model_family="moneyline",
            model_version="NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2", model_role="SHADOW",
        )
        pre = orient_moneyline_candidate(v2, .49, .51, "2026-10-13T20:00:00Z", "v2", "v2.csv", "hash")
        post = orient_moneyline_candidate(v2, .51, .49, "2026-10-15T20:00:00Z", "v2", "v2.csv", "hash")
        self.assertEqual(pre["selected_side"], "AWAY")
        self.assertEqual(pre["model_role"], "REFERENCE")
        self.assertEqual(post["selected_side"], "HOME")
        self.assertEqual(post["model_role"], "SHADOW")
        self.assertEqual(moneyline_role_at_prediction("NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3", "2026-10-15T20:00:00Z"), "PRODUCTION")

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

    def test_moneyline_quote_dedup_keeps_both_sides_per_candidate_and_book(self):
        base = {
            "candidate_key": "ml-candidate", "sportsbook_key": "betonlineag", "market_key": "moneyline_home",
            "quote_side": "HOME", "source_quote_timestamp_utc": "2026-10-16T19:00:00Z",
            "observation_timestamp_utc": "2026-10-16T19:01:00Z",
            "quote_qualification_status": "QUALIFIED_AUDITED_MONEYLINE_LEDGER",
        }
        away = {**base, "quote_side": "AWAY", "american_price": 105}
        result = deduplicate_quotes([base, {**base, "observation_timestamp_utc": "2026-10-16T18:59:00Z"}, away])
        self.assertEqual(len(result), 2)
        self.assertEqual({row["quote_side"] for row in result}, {"HOME", "AWAY"})

    def test_prop_opposite_side_quote_is_kept_separately_and_priced_at_its_own_probability(self):
        from backend.nhl.scripts.export_nhl_all_candidate_evaluation import _consider_quote
        from pathlib import Path
        candidate = {**self._candidate(), "candidate_key": "sog-1", "model_role": "PRODUCTION", "probability_over": 0.6,
                     "probability_under": 0.4, "prediction_timestamp_utc": "2026-10-16T20:00:00Z",
                     "scheduled_start_utc": "2026-10-16T23:00:00Z", "parent_run_id": "run"}
        output, diagnostics = {}, []
        for side, price in (("OVER", -150), ("UNDER", 130)):
            _consider_quote(output, diagnostics, candidate, {"bookmaker": "book", "market": "player_shots_on_goal",
                            "outcome": side.title()}, Path("obs"), "sha", "msha", "2026-10-16T19:00:00Z",
                            "2026-10-16T18:59:00Z", price, None, None, "42",
                            quote_side="" if side == "OVER" else side)
        selected = output[("sog-1", "book", "")]
        opposite = output[("sog-1", "book", "UNDER")]
        self.assertEqual(selected["quote_side"], "OVER")
        self.assertAlmostEqual(selected["expected_value_per_unit"], 0.6 * (1 + 100 / 150) - 1)
        self.assertAlmostEqual(opposite["expected_value_per_unit"], 0.4 * 2.3 - 1)
        self.assertEqual(len(deduplicate_quotes([{**selected, "decimal_price": 1.667}, opposite])), 2)

    def test_moneyline_quote_team_names_follow_provider_home_away_orientation(self):
        self.assertEqual(canonical_moneyline_quote_side("CAR", "CAR", "FLA"), "HOME")
        self.assertEqual(canonical_moneyline_quote_side("FLA", "CAR", "FLA"), "AWAY")
        self.assertEqual(canonical_moneyline_quote_side("unknown", "CAR", "FLA"), "")
        self.assertEqual(canonical_moneyline_quote_side("", "", ""), "")

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


class ResilientCrossMarketFillTests(unittest.TestCase):
    """Resilient cross-market grades fill only games the original file lacks."""

    HEADER = ("canonical_season,slate_date,game_id,scheduled_start_time_utc,challenger_home_win_probability,"
              "v2_home_win_probability,official_full_game_winner\n")

    def _row(self, game):
        return f"2026,2026-10-17,{game},2026-10-18T00:00:00Z,0.6,0.55,HOME\n"

    def _moneyline_games(self, root):
        from backend.nhl.scripts.export_nhl_all_candidate_evaluation import load_candidates
        predictions, _ = load_candidates(root)
        return sorted((p.get("slate_date"), str(p.get("game_id")), p.get("model_role"))
                      for p in predictions if p.get("market_key") == "moneyline_home")

    def test_resilient_rows_fill_an_empty_original_grade_file(self):
        import tempfile, hashlib, json
        from pathlib import Path
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = root / "postgame_reconciliation/2026-10-17/reconciliation=fixture"
            package.mkdir(parents=True)
            (package / "graded_moneyline.csv").write_text(self.HEADER)
            sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            (package / "SHA256SUMS").write_text(f"{sha(package / 'graded_moneyline.csv')}  graded_moneyline.csv\n")
            restatement = root / "postgame_reconciliation/learning_restatements/2026-10-17/restatement=fixture"
            restatement.mkdir(parents=True)
            (restatement / "graded_resilient_moneyline_production_champion.csv").write_text(
                self.HEADER + self._row(2026020131))
            (restatement / "lineage.json").write_text(json.dumps({
                "contract_version": "NHL_RESILIENT_CROSS_MARKET_RESTATEMENT_V1",
                "source_package": str(package), "source_hashes": {"SHA256SUMS": sha(package / "SHA256SUMS")}}))
            (restatement / "SHA256SUMS").write_text("".join(
                f"{sha(p)}  {p.name}\n" for p in sorted(restatement.iterdir()) if p.name != "SHA256SUMS"))
            games = self._moneyline_games(root)
        self.assertEqual([g[1] for g in games], ["2026020131", "2026020131"])  # production + shadow

    def test_a_game_graded_in_both_files_counts_once(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = root / "postgame_reconciliation/2026-10-17/reconciliation=fixture"
            package.mkdir(parents=True)
            (package / "graded_moneyline.csv").write_text(self.HEADER + self._row(2026020131))
            (package / "graded_resilient_moneyline_production_champion.csv").write_text(
                self.HEADER + self._row(2026020131) + self._row(2026020132))
            games = self._moneyline_games(root)
        self.assertEqual(sorted({g[1] for g in games}), ["2026020131", "2026020132"])
        self.assertEqual(len(games), 4)  # two games x (production, shadow), none doubled


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import json
from datetime import date

import pandas as pd

from backend.nhl.eightrain_adapter import (
    UPLOAD_COLUMNS, build_rows, fair_american, fair_american_d,
    classify_export_date, format_win_probability, load_catalogs, validate_upload,
)
from backend.nhl.scripts.select_sog_candidates_live import DEFAULT_POLICY_JSON, _load_policy, main


class EightRainAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.package = self.root / "package"
        self.package.mkdir()
        pd.DataFrame([{
            "game_id": 101, "game_date": "2026-09-30", "home_team": "UTA", "away_team": "BOS",
        }]).to_csv(self.package / "schedule_event_identity.csv", index=False)
        pd.DataFrame([{
            "game_id": 101, "v2_home_win_probability": .55, "v2_away_win_probability": .45,
            "model_version": "MONEYLINE_V2_REFERENCE",
        }]).to_csv(self.package / "v2_immutable_predictions.csv", index=False)
        pd.DataFrame([{
            "game_id": 101, "away_by_2_plus_probability": .25,
            "one_goal_game_probability": .40, "home_by_2_plus_probability": .35,
        }]).to_csv(self.package / "puck_line_v1_immutable_predictions.csv", index=False)
        self.spec = {"league": {"code": "nhl"}, "markets": {
            "h2h": {"bet": ["home", "away"]}, "spread": {"bet": ["home", "away"]},
        }, "stats": [{"code": "shots_on_goal", "bet": ["over", "under"]},
                     {"code": "points", "bet": ["over", "under"]},
                     {"code": "saves", "bet": ["over", "under"]}]}
        self.team_map = {"UTA": "utah-mammoth", "BOS": "bos-boston-bruins"}
        self.player_map = {("alex example", "bos-boston-bruins"): "alex-example"}
        self.allowed = {x["code"]: set(x["bet"]) for x in self.spec["stats"]}

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _build(self, props=None):
        return build_rows(package_dir=self.package, spec=self.spec, team_map=self.team_map,
                          player_map=self.player_map, allowed_bets=self.allowed,
                          prop_candidates=props)

    def test_exact_column_order_and_doubleheader(self):
        rows, _ = self._build()
        self.assertEqual(list(rows.columns), UPLOAD_COLUMNS)
        self.assertEqual(rows.DOUBLEHEADER.unique().tolist(), ["0"])

    def test_team_catalog_codes_including_utah(self):
        rows, _ = self._build()
        self.assertEqual(set(rows.HOME), {"utah-mammoth"})
        self.assertEqual(set(rows.AWAY), {"bos-boston-bruins"})

    def test_moneyline_two_sided_rows(self):
        rows, meta = self._build()
        ml = rows[rows.MARKET.eq("h2h")]
        self.assertEqual(set(ml.SIDE), {"home", "away"})
        self.assertTrue(ml.SELECTOR.eq("").all())
        self.assertTrue(ml.POINT.eq("").all())
        self.assertEqual([x["model_identity"] for x in meta["provenance"] if x["market"] == "h2h"],
                         ["MONEYLINE_V2_REFERENCE", "MONEYLINE_V2_REFERENCE"])

    def test_puck_line_signed_spread_and_probability_pair(self):
        rows, _ = self._build()
        pl = rows[rows.MARKET.eq("spread")].set_index("SIDE")
        self.assertEqual(pl.loc["home", "POINT"], "-1.5")
        self.assertEqual(pl.loc["away", "POINT"], "+1.5")
        self.assertEqual(fair_american(.35), 186)
        self.assertEqual(fair_american(.65), -round(100 * .65 / .35))

    def test_prop_code_selector_and_complementary_side(self):
        props = pd.DataFrame([{
            "game_id": 101, "game_date": "2026-09-30", "player_name": "Alex Example",
            "team": "BOS", "market": "shots_on_goal", "line": 2.5,
            "model_pick": "over", "model_side_prob": .58,
        }])
        rows, _ = self._build(props)
        prop = rows[rows.SECTION.eq("player_prop")]
        self.assertEqual(len(prop), 2)
        self.assertEqual(set(prop.MARKET), {"shots_on_goal"})
        self.assertEqual(set(prop.SELECTOR), {"alex-example"})
        self.assertEqual(set(prop.SIDE), {"over", "under"})
        self.assertEqual(set(prop.POINT), {"2.5"})

    def test_player_code_missing_is_reported_not_guessed(self):
        props = pd.DataFrame([{
            "game_id": 101, "game_date": "2026-09-30", "player_name": "Unknown Player",
            "team": "BOS", "market": "points", "line": .5,
            "model_pick": "under", "model_side_prob": .7,
        }])
        rows, meta = self._build(props)
        self.assertEqual(len(rows), 4)
        self.assertEqual(len(meta["unmapped_players"]), 1)

    def test_unique_global_name_fallback_handles_stale_team_and_rejects_ambiguous_name(self):
        props = pd.DataFrame([{
            "game_id": 101, "game_date": "2026-09-30", "player_name": "Alex Example",
            "team": "UTA", "market": "points", "line": 0.5,
            "model_pick": "over", "model_side_prob": .7,
        }])
        rows, meta = build_rows(
            package_dir=self.package, spec=self.spec, team_map=self.team_map,
            player_map={}, allowed_bets=self.allowed, prop_candidates=props,
            unique_player_code_by_name={"alex example": "alex-example"},
        )
        prop_rows = rows[rows.SECTION.eq("player_prop")]
        self.assertEqual(set(prop_rows.SELECTOR), {"alex-example"})
        self.assertEqual(meta["players_mapped_by_unique_name_fallback"], 1)

        rows, meta = build_rows(
            package_dir=self.package, spec=self.spec, team_map=self.team_map,
            player_map={}, allowed_bets=self.allowed, prop_candidates=props,
            unique_player_code_by_name={}, ambiguous_player_names={"alex example"},
        )
        self.assertTrue(rows[rows.SECTION.eq("player_prop")].empty)
        self.assertEqual(len(meta["ambiguous_players"]), 1)

    def test_duplicate_prop_candidate_fails_closed(self):
        props = pd.DataFrame([{
            "game_id": 101, "game_date": "2026-09-30", "player_name": "Alex Example",
            "team": "BOS", "market": "saves", "line": 20.5,
            "model_pick": "over", "model_side_prob": .55,
        }] * 2)
        with self.assertRaisesRegex(ValueError, "DUPLICATE_UPLOAD_KEY"):
            self._build(props)

    def test_decimal_and_d_suffix_fair_odds_format_helpers(self):
        self.assertEqual(fair_american(.55), -122)
        self.assertEqual(fair_american_d(.55), "-122d")
        self.assertEqual(fair_american_d(.45), "+122d")
        self.assertEqual(format_win_probability(.55, "decimal"), "0.55")
        self.assertEqual(format_win_probability(.55, "american_d"), "-122d")
        self.assertEqual(format_win_probability(.55), "-122")

    def test_decimal_and_d_suffix_probability_pairs_validate(self):
        rows, _ = self._build()
        for representation in ("decimal", "american_d"):
            candidate = rows.copy()
            candidate["WIN %"] = [format_win_probability(p, representation) for p in
                                  (.55, .45, .35, .65)]
            result = validate_upload(candidate, spec=self.spec,
                                     team_codes=set(self.team_map.values()),
                                     player_codes={"alex-example"})
            self.assertEqual(result["pair_failures"], 0)
            self.assertEqual(result["probability_pair_failures"], 0)

    def test_active_policy_is_explicit_frozen_source(self):
        path = Path(DEFAULT_POLICY_JSON)
        policy = _load_policy(path)
        self.assertEqual(policy["over:1.5"].min_ev, .03)
        self.assertEqual(policy["over:1.5"].min_gap, .04)
        self.assertEqual(policy["under:2.5"].min_gap, .08)
        self.assertNotEqual(path.as_posix(), "tmp/nhl_sog_walkforward_research_summary.json")

    def test_explicit_alternate_policy_can_be_selected_without_changing_default(self):
        alternate = self.root / "sog_policy_experiment_v2.json"
        alternate.write_text(json.dumps({"policy_name": "TEST_POLICY_V2", "thresholds_for_next_slate": {
            "over:1.5": {"min_ev": .07, "min_gap": .05},
        }}))
        policy = _load_policy(alternate)
        active = _load_policy(Path(DEFAULT_POLICY_JSON))
        self.assertEqual((policy["over:1.5"].min_ev, policy["over:1.5"].min_gap), (.07, .05))
        self.assertEqual((active["over:1.5"].min_ev, active["over:1.5"].min_gap), (.03, .04))

    def test_frozen_policy_contains_all_documented_thresholds(self):
        policy = _load_policy(Path(DEFAULT_POLICY_JSON))
        expected = {
            "over:1.5": (.03, .04), "over:2.5": (.03, 0.0),
            "over:3.5": (.03, 0.0), "under:1.5": (.03, .02),
            "under:2.5": (.03, .08), "under:3.5": (.03, .04),
        }
        self.assertEqual(set(policy), set(expected))
        for segment, (min_ev, min_gap) in expected.items():
            self.assertEqual((policy[segment].min_ev, policy[segment].min_gap), (min_ev, min_gap))

    def test_missing_active_policy_fails_clearly(self):
        with patch("sys.argv", ["select_sog_candidates_live.py", "--policy-json", str(self.root / "missing.json")]):
            with self.assertRaisesRegex(SystemExit, "active candidate policy not found"):
                main()

    def test_walkforward_operator_writes_research_output_not_active_policy(self):
        command_deck = Path("bin/nhl_ops.sh").read_text()
        self.assertIn("--out-summary-json tmp/nhl_sog_walkforward_research_summary.json", command_deck)
        self.assertNotIn("--out-summary-json backend/nhl/config/nhl_sog_active_candidate_policy_v1.json", command_deck)

    def test_current_slate_required_and_prior_day_explicitly_test_only(self):
        today = date(2026, 10, 1)
        with self.assertRaisesRegex(ValueError, "OPERATIONAL_EXPORT_MUST_USE_CURRENT_SLATE"):
            classify_export_date("2026-09-30", current_date=today)
        self.assertEqual(classify_export_date("2026-09-30", current_date=today, test_only=True),
                         "TEST_ONLY_NON_OPERATIONAL")
        self.assertEqual(classify_export_date("2026-10-01", current_date=today),
                         "OPERATIONAL_CURRENT_SLATE")

    def test_test_only_export_requires_classification_report(self):
        with patch("sys.argv", [
            "export_nhl_8rain_upload.py", "--package-dir", str(self.package),
            "--catalog-dir", str(self.root / "catalog"), "--date", "2026-09-30",
            "--out-csv", str(self.root / "prior.csv"), "--test-only",
        ]):
            from backend.nhl.scripts.export_nhl_8rain_upload import main as export_main
            with self.assertRaisesRegex(SystemExit, "--report-json is required with --test-only"):
                export_main()

    def test_player_selector_is_taken_verbatim_from_catalog(self):
        catalog = self.root / "catalog"
        catalog.mkdir()
        (catalog / "model_spec.json").write_text(json.dumps({
            "league": {"code": "nhl"}, "markets": {"h2h": {}, "spread": {}},
            "stats": [{"code": "shots_on_goal", "bet": ["over", "under"]}],
        }))
        (catalog / "teams.json").write_text(json.dumps({"data": [
            {"abbreviation": "BOS", "code": "exact-bos-code"},
            {"abbreviation": "UTA", "code": "exact-uta-code"},
        ]}))
        (catalog / "players.json").write_text(json.dumps({"data": [
            {"name": "Alex Example", "team": "exact-bos-code", "code": "provider-player-code"},
        ]}))
        spec, team_map, player_map, allowed = load_catalogs(catalog)
        self.assertEqual(team_map["BOS"], "exact-bos-code")
        self.assertEqual(player_map[("alex example", "exact-bos-code")], "provider-player-code")
        props = pd.DataFrame([{
            "game_id": 101, "game_date": "2026-09-30", "player_name": "Alex Example",
            "team": "BOS", "market": "shots_on_goal", "line": 2.5,
            "model_pick": "over", "model_side_prob": .58,
        }])
        rows, _ = build_rows(package_dir=self.package, spec=spec, team_map=team_map,
                             player_map=player_map, allowed_bets=allowed, prop_candidates=props)
        self.assertEqual(set(rows.loc[rows.SECTION.eq("player_prop"), "SELECTOR"]),
                         {"provider-player-code"})

    def test_upload_validator_checks_pairing_and_reference_only(self):
        rows, meta = self._build()
        valid = validate_upload(rows, spec=self.spec, team_codes=set(self.team_map.values()),
                                player_codes={"alex-example"})
        self.assertEqual(valid["rows"], 4)
        self.assertFalse(meta["challengers_included"])
        with self.assertRaisesRegex(ValueError, "UNPAIRED_MARKET_ROWS"):
            validate_upload(rows.iloc[:-1], spec=self.spec, team_codes=set(self.team_map.values()),
                            player_codes={"alex-example"})

    def test_probability_complement_is_checked_after_fair_odds_rounding(self):
        rows, _ = self._build()
        rows.loc[rows.MARKET.eq("h2h") & rows.SIDE.eq("home"), "WIN %"] = 500
        with self.assertRaisesRegex(ValueError, "PROBABILITY_PAIR_NOT_NORMALIZED"):
            validate_upload(rows, spec=self.spec, team_codes=set(self.team_map.values()),
                            player_codes={"alex-example"})

    def test_wrong_puck_line_orientation_fails(self):
        rows, _ = self._build()
        rows.loc[rows.MARKET.eq("spread") & rows.SIDE.eq("away"), "POINT"] = "-1.5"
        with self.assertRaisesRegex(ValueError, "SPREAD_POINT_SIGN_INVALID"):
            validate_upload(rows, spec=self.spec, team_codes=set(self.team_map.values()),
                            player_codes={"alex-example"})


if __name__ == "__main__":
    unittest.main()

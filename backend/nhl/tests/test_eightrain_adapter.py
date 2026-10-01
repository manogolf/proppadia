from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.eightrain_adapter import (
    UPLOAD_COLUMNS, build_rows, fair_american, fair_american_d,
    format_win_probability, validate_upload,
)


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

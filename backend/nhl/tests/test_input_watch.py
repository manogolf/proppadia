"""Daily input watch: sanity flags, disagreement reasons, and a run that never blocks."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from backend.nhl import input_watch as watch

RANGES = json.loads(watch.POINTS_RANGES_PATH.read_text())


def points_row(player_id, **values):
    row = {"game_id": 1, "player_id": player_id, "player_name": f"P{player_id}", "is_home": 1,
           "d10_sog_per60": 8.0, "attempts_d10_per60": 16.0, "player_points_last10": 0.5,
           "current_season_points_prior": 5, "current_season_games_prior": 10,
           "mean_toi_last10": 18.0, "mean_pp_toi_last10": 2.0, "team_d10_sf_per_game": 28.0,
           "last10_team_sog_share": 0.085, "prob_over_0_5": 0.4, "prob_over_1_5": 0.1,
           "prob_over_2_5": 0.02, "game_start_utc": "2026-10-28T23:00:00Z"}
    row.update(values)
    return row


class SanityTests(unittest.TestCase):
    def test_impossible_and_substituted_values_are_reported(self):
        frame = pd.DataFrame([
            points_row(1),
            points_row(2, attempts_d10_per60=4.0),          # attempts below shots
            points_row(3, attempts_d10_per60=None),         # unknown, imputed
            points_row(4, team_d10_sf_per_game=11.0),       # implausible team shots
            points_row(5, player_points_last10=2.7),        # outside training range
        ])
        checks = {c["check"]: c for c in watch.points_sanity(frame, RANGES)}
        self.assertTrue(checks["ATTEMPTS_BELOW_SOG"]["flag"])
        self.assertEqual(checks["ATTEMPTS_BELOW_SOG"]["examples"], ["P2"])
        self.assertTrue(checks["TEAM_SHOTS_IMPLAUSIBLE"]["flag"])
        self.assertEqual(checks["ATTEMPTS_UNKNOWN_IMPUTED"]["count"], 1)
        # One of five players outside the range is above the drift share.
        self.assertTrue(checks["OUT_OF_TRAINING_RANGE:player_points_last10"]["flag"])

    def test_sog_inputs_are_checked_against_the_frozen_training_ranges(self):
        sog_ranges = json.loads(watch.SOG_RANGES_PATH.read_text())
        self.assertEqual(sog_ranges["training_frame_sha256"],
                         "4042e31c758c6b0083272cf82904426b8732e06a9f2837c79e7e0d2e9aa093f6")
        rows = [{"game_id": 1, "player_id": i, "player_name": f"S{i}", "feature_sog60_10": 7.0,
                 "feature_att60_10": 14.0, "feature_toi_avg_10": 17.0, "feature_pp_toi_avg_10": 1.0,
                 "feature_team_sf_10": 28.0, "feature_league_prior": 5.5} for i in range(1, 6)]
        rows[0]["feature_sog60_10"] = 30.0                      # far above training p99
        checks = {c["check"]: c for c in watch.sog_sanity(pd.DataFrame(rows), sog_ranges)}
        self.assertTrue(checks["OUT_OF_TRAINING_RANGE:feature_sog60_10"]["flag"])
        self.assertEqual(checks["OUT_OF_TRAINING_RANGE:feature_sog60_10"]["examples"], ["S1"])
        self.assertFalse(checks["OUT_OF_TRAINING_RANGE:feature_toi_avg_10"]["flag"])
        # 2026 defenseman league prior (3.947) is below both training values: reported, not flagged.
        for row in rows:
            row["feature_league_prior"] = 3.947
        checks = {c["check"]: c for c in watch.sog_sanity(pd.DataFrame(rows), sog_ranges)}
        self.assertEqual(checks["OUT_OF_TRAINING_RANGE:feature_league_prior"]["count"], 5)
        self.assertFalse(checks["OUT_OF_TRAINING_RANGE:feature_league_prior"]["flag"])

    def test_clean_inputs_raise_no_flags(self):
        frame = pd.DataFrame([points_row(i) for i in range(1, 30)])
        self.assertFalse(any(c["flag"] for c in watch.points_sanity(frame, RANGES)))


class ReasonTests(unittest.TestCase):
    def test_star_ceiling_missed_games_and_defenseman(self):
        slate = pd.DataFrame([points_row(i) for i in range(1, 10)]).assign(
            season_ppg=lambda f: f.current_season_points_prior / f.current_season_games_prior)
        star = pd.Series(points_row(99, current_season_points_prior=34, current_season_games_prior=12,
                                    player_points_last10=2.7, selected_side="UNDER")).astype(object)
        star["season_ppg"] = 34 / 12
        reasons = watch._points_reasons(star, slate, RANGES)
        self.assertTrue(any("ceiling" in r for r in reasons))
        self.assertTrue(any("outside training range" in r for r in reasons))
        returning = pd.Series(points_row(98, current_season_games_prior=3, current_season_points_prior=3,
                                         last10_team_sog_share=0.04, attempts_d10_per60=None,
                                         is_defense=1, selected_side="UNDER")).astype(object)
        returning["season_ppg"] = 1.0
        reasons = watch._points_reasons(returning, slate, RANGES)
        for text in ("attempts unknown", "only 3 games", "missed games", "defenseman"):
            self.assertTrue(any(text in r for r in reasons), text)


class RunTests(unittest.TestCase):
    def test_run_writes_report_and_explains_the_largest_gap(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            temp = Path(temp)
            capture = temp / "capture"
            capture.mkdir()
            rows = pd.DataFrame([points_row(i) for i in range(1, 12)])
            feature_cols = [c for c in rows.columns if not c.startswith("prob_")]
            rows[feature_cols].to_csv(capture / "features.csv", index=False)
            rows[["game_id", "player_id", "player_name", "prob_over_0_5", "prob_over_1_5",
                  "prob_over_2_5"]].to_csv(capture / "predictions.csv", index=False)

            def quotes(_root, candidates, price_both_sides):
                self.assertTrue(price_both_sides)
                out = []
                for c in candidates:
                    if c["market_line"] != "0.5":
                        continue
                    # Player 1: the market makes our UNDER an underdog.
                    under = 0.40 if c["player_id"] == "1" else 0.60
                    for side, p in (("UNDER", under), ("OVER", 1 - under)):
                        out.append({"candidate_key": c["candidate_key"], "sportsbook_key": "book",
                                    "quote_side": side, "selected_side": c["selected_side"],
                                    "implied_probability": p, "american_price": 100,
                                    "quote_qualification_status": "QUALIFIED_X"})
                return out, []

            with mock.patch.object(watch.candidate_export, "load_retained_quotes", side_effect=quotes), \
                    mock.patch.object(watch.candidate_export, "deduplicate_quotes", side_effect=lambda q: q):
                result = watch.run_input_watch(
                    slate_date="2026-10-28", run_id="r", prediction_run_dir=temp / "none",
                    points_capture_dir=capture, operational_root=temp, output_root=temp / "out",
                    now_utc="2026-10-28T18:00:00Z")
            top = result["disagreements"]["lanes"]["points"][0]
            self.assertEqual(top["player_id"], 1)
            self.assertAlmostEqual(top["gap"], 0.2, places=4)
            self.assertTrue((Path(result["path"]) / "input_watch.md").is_file())
            self.assertEqual(result["database_writes"], 0)

    def test_quote_failure_leaves_the_rest_of_the_watch(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            temp = Path(temp)
            capture = temp / "capture"
            capture.mkdir()
            rows = pd.DataFrame([points_row(1, attempts_d10_per60=4.0)])
            rows.to_csv(capture / "features.csv", index=False)
            rows.to_csv(capture / "predictions.csv", index=False)
            with mock.patch.object(watch.candidate_export, "load_retained_quotes", side_effect=OSError("x")):
                result = watch.run_input_watch(
                    slate_date="2026-10-28", run_id="r", prediction_run_dir=temp,
                    points_capture_dir=capture, operational_root=temp, output_root=temp / "out")
            self.assertEqual(result["disagreements"]["status"], "UNAVAILABLE")
            self.assertIn("points:ATTEMPTS_BELOW_SOG", result["flags"])


if __name__ == "__main__":
    unittest.main()
